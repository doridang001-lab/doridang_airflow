"""
EasyPOS(이지포스) '상품조회' 엑셀 자동 다운로드 및 덮어쓰기 저장.

처리:
1) Playwright로 EasyPOS 로그인 → [기초정보] → [상품조회] 진입
2) [조회] → [엑셀] 다운로드
3) OneDrive analytics 아래에 덮어쓰기 저장 (1개 파일만)
   - ANALYTICS_DB/easypos_product/상품조회.xlsx
"""

import logging
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import pandas as pd
from playwright.sync_api import sync_playwright, Frame, Page, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError

from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR
from modules.transform.utility.playwright_launcher import launch_chromium

# EasyPOS 로그인/클릭 유틸 재사용
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (  # noqa: E402
    HEADLESS_MODE,
    _login,
    _get_main_frame,
    _close_blocking_popups,
    _NEXACRO_INIT_SCRIPT,
    _debug_dump,
    _ensure_playwright_chromium_installed,
)

logger = logging.getLogger(__name__)


# ============================================================
# EasyPOS UI selectors (NexacroN)
# ============================================================
_MENU_BASIC_INFO = "mainframe_childframe_form_divTop_img_TA_top_menu1"  # 기초정보

_BTN_SEARCH_SELECTORS = [
    "#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch",
]

_BTN_EXCEL_SELECTORS = [
    "#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommExcel",
]

_PRODUCT_GRID_SELECTOR = "#mainframe_childframe_form_divMain_divWork_grdItem"


def _wait_product_screen(page: Page) -> Frame:
    """공통 버튼만 생긴 빈 화면을 상품조회 준비 완료로 오인하지 않는다."""
    mf = _get_main_frame(page)
    mf.wait_for_function(
        r"""() => {
            const f = window.application?.mainframe?.childframe?.form;
            const w = f?.divMain?.divWork;
            return String(f?.pvMenuNm || '').replace(/\s/g, '') === '상품조회'
                && !!f.pvUrl && w?.url === f.pvUrl
                && f.divMain._is_loaded && w._is_loaded && w._is_created
                && w.dsItem && w.grdItem && w.grdItemExcel
                && typeof w.fnCallBack === 'function'
                && typeof w.fnCommSearch_onclick === 'function'
                && typeof w.fnCommExcel_onclick === 'function';
        }""",
        timeout=60_000,
        polling=250,
    )
    mf.locator(_PRODUCT_GRID_SELECTOR).wait_for(state="visible", timeout=10_000)
    return mf


def _navigate_product_by_menu_url(page: Page, target_needles: list[str]) -> bool:
    """실제 좌측 메뉴와 사이트의 진입 함수를 사용해 권한·작업 프레임을 초기화한다."""
    result = _get_main_frame(page).evaluate(
        r"""(needles) => {
            const app = window.application;
            const form = app.mainframe.childframe.form;
            const ds = app.gdsLeftMenu;
            if (!ds || typeof form.fnOpenMenu !== 'function') return {ok: false};
            for (let i = 0; i < ds.rowcount; i++) {
                const name = String(ds.getColumn(i, 'MENU_NAME') || '');
                if (!needles.some(n => n.replace(/\s/g, '') === name.replace(/\s/g, ''))) continue;
                const id = ds.getColumn(i, 'MENU_CD');
                const url = ds.getColumn(i, 'SMART_MENU_URL');
                if (!url) continue;
                form.fnOpenMenu(id);
                return {ok: true, name, url};
            }
            return {ok: false};
        }""",
        target_needles,
    )
    logger.info("EasyPOS 상품 메뉴 진입: %s", result)
    if not result.get("ok"):
        return False
    _wait_product_screen(page)
    return True


def _search_product(page: Page) -> int:
    """이번 조회의 콜백과 상품 행 수를 확인한 뒤 엑셀 내보내기를 허용한다."""
    mf = _wait_product_screen(page)
    mf.evaluate(
        """() => {
            const w = window.application.mainframe.childframe.form.divMain.divWork;
            w.__easyposProductQuery = null;
            const original = w.fnCallBack;
            w.fnCallBack = function(svc, code, message) {
                try {
                    const result = original.apply(this, arguments);
                    if (svc === 'svcItemSearch') {
                        this.__easyposProductQuery = {code: Number(code), rows: this.dsItem.rowcount};
                    }
                    return result;
                } finally {
                    if (svc === 'svcItemSearch') this.fnCallBack = original;
                }
            };
        }"""
    )
    mf.locator(_BTN_SEARCH_SELECTORS[0]).click(timeout=10_000)
    mf.wait_for_function(
        "() => !!window.application?.mainframe?.childframe?.form?.divMain?.divWork?.__easyposProductQuery",
        timeout=60_000,
        polling=250,
    )
    result = mf.evaluate("window.application.mainframe.childframe.form.divMain.divWork.__easyposProductQuery")
    if result["code"] < 0 or result["rows"] <= 0:
        raise RuntimeError(f"상품조회 실패 또는 상품 없음: {result}")
    logger.info("EasyPOS 상품 조회 완료: %d건", result["rows"])
    return result["rows"]


def _export_url_from_response(body: str, response_url: str) -> str | None:
    """Nexacro SSV 내보내기 완료 응답에서 생성된 파일 위치만 읽는다."""
    if not body.startswith("SSV:"):
        raise RuntimeError("EasyPOS 엑셀 생성 응답이 SSV 형식이 아닙니다.")
    records = body.split("\x1e")
    code = next((record.split("=", 1)[1] for record in records if record.startswith("ErrorCode:") and "=" in record), None)
    if code != "0":
        raise RuntimeError("EasyPOS 서버가 엑셀 생성 실패를 반환했습니다.")
    columns = []
    in_response = False
    for record in records:
        if record.startswith("Dataset:"):
            in_response = record == "Dataset:RESPONSE"
            columns = []
        elif in_response and record.startswith("_RowType_"):
            columns = [field.split(":", 1)[0] for field in record.split("\x1f")]
        elif in_response and columns:
            row = dict(zip(columns, record.split("\x1f")))
            if row.get("command") != "export" or row.get("eof") != "1" or not row.get("url"):
                continue
            url = urljoin(response_url, row["url"])
            origin, target = urlsplit(response_url), urlsplit(url)
            if (target.scheme, target.netloc) != (origin.scheme, origin.netloc) or not target.path.startswith("/export/"):
                raise RuntimeError("EasyPOS 엑셀 생성 응답의 다운로드 경로가 올바르지 않습니다.")
            return url
    return None


def _download_product_xlsx(page: Page, destination: Path) -> None:
    """브라우저 이벤트가 누락돼도 UI가 생성한 동일 엑셀을 현재 세션으로 받는다."""
    state = {"url": None, "error": None}
    downloads = []

    def on_download(download):
        downloads.append(download)

    def on_response(response):
        if urlsplit(response.url).path != "/XExportImport":
            return
        try:
            if not response.ok:
                raise RuntimeError(f"EasyPOS 엑셀 생성 HTTP 오류: {response.status}")
            url = _export_url_from_response(response.text(), response.url)
            if url:
                state["url"] = url
        except (RuntimeError, PlaywrightError) as exc:
            state["error"] = exc

    page.on("response", on_response)
    page.on("download", on_download)
    try:
        mf = _wait_product_screen(page)
        mf.locator(_BTN_EXCEL_SELECTORS[0]).click(timeout=10_000)
        deadline = time.monotonic() + 120
        while not downloads and not state["url"] and state["error"] is None:
            if time.monotonic() >= deadline:
                raise PlaywrightTimeoutError("상품 엑셀 생성/다운로드 응답 대기 120초 초과")
            page.wait_for_timeout(250)
        if state["error"] is not None:
            raise state["error"]
        if downloads:
            downloads[0].save_as(str(destination))
        else:
            logger.info("브라우저 다운로드 이벤트 미발생: 서버가 생성한 상품 엑셀을 현재 세션으로 수신")
            response = page.context.request.get(state["url"], timeout=60_000)
            try:
                if not response.ok:
                    raise RuntimeError(f"생성된 상품 엑셀 수신 HTTP 오류: {response.status}")
                destination.write_bytes(response.body())
            finally:
                response.dispose()
    finally:
        page.remove_listener("response", on_response)
        page.remove_listener("download", on_download)


def _cleanup_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for p in download_dir.glob("*"):
        try:
            if p.is_file():
                p.unlink(missing_ok=True)
        except Exception:
            continue


def _add_nexacro_init_script(context) -> None:
    # 매출 수집과 동일하게 백그라운드 visibility 전환에 따른 Nexacro 초기화 중단을 방지한다.
    context.add_init_script(_NEXACRO_INIT_SCRIPT)


def _navigate_to_product_search(page: Page, mf: Frame) -> Frame:
    """지연 로그인 팝업을 닫고 상품조회 화면을 한 번만 연다."""
    # 초기 대시보드의 자식 폼 preload가 끝나기 전에 divMain을 교체하면
    # Nexacro의 preload 카운터만 남아 다음 작업 화면 생성이 멈춘다.
    mf.wait_for_function(
        """() => {
            const m = window.application?.mainframe?.childframe?.form?.divMain;
            return !!m?._is_created && m._load_manager?.preloadCnt === 0;
        }""",
        timeout=60_000,
        polling=250,
    )
    # 대시보드 비동기 로딩 뒤 팝업이 늦게 생길 수 있다. 좌표 클릭은 이를 감지하지 못한다.
    for attempt in range(3):
        mf = _get_main_frame(page)
        _close_blocking_popups(page, mf)
        try:
            mf.locator(f"#{_MENU_BASIC_INFO}").click(timeout=5_000)
            break
        except PlaywrightTimeoutError:
            if attempt == 2:
                raise
    mf = _get_main_frame(page)
    mf.wait_for_function(
        r"""() => {
            const ds = window.application?.gdsLeftMenu;
            if (!ds) return false;
            for (let i = 0; i < ds.rowcount; i++) {
                if (String(ds.getColumn(i, 'MENU_NAME')).replace(/\s/g, '') === '상품조회') return true;
            }
            return false;
        }""",
        timeout=15_000,
        polling=250,
    )
    if not _navigate_product_by_menu_url(page, ["상품조회"]):
        raise RuntimeError("EasyPOS 실제 좌측 메뉴에서 상품조회를 찾지 못했습니다.")
    return _get_main_frame(page)


def _atomic_copy_replace(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f"{dest.name}.tmp"
    try:
        shutil.copy2(src, tmp)
        try:
            os.replace(tmp, dest)
        except PermissionError as e:
            # Windows/OneDrive bind mount 에서 대상 파일이 잠겨있으면 rename/replace 가 종종 실패함.
            # 이 경우 atomic 보장은 포기하고, 인플레이스 덮어쓰기를 시도한다.
            logger.warning("atomic replace 실패(권한/잠금): %s -> %s | fallback copy2", tmp, dest)
            try:
                shutil.copy2(src, dest)
            except Exception:
                raise e
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _guess_code_column(columns: list[str]) -> str | None:
    candidates = [
        "상품코드",
        "품목코드",
        "상품번호",
        "코드",
        "상품ID",
        "상품코드(내부)",
    ]
    for c in candidates:
        if c in columns:
            return c
    for c in columns:
        if "코드" in c:
            return c
    return None


def _normalize_xlsx(xlsx_path: Path, snapshot_date: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Excel 숫자형 코드 "1234.0" 방지
    code_col = _guess_code_column(list(df.columns))
    if code_col and code_col in df.columns:
        code_series = df[code_col].fillna("").astype(str).str.strip()
        code_series = code_series.map(lambda v: re.sub(r"\.0$", "", v))
        pk_base = code_series.where(code_series != "", df.index.astype(str))
        df["상품코드_pk"] = pk_base
    else:
        df["상품코드_pk"] = df.index.astype(str)

    df["snapshot_date"] = snapshot_date
    df["collected_at_utc"] = datetime.utcnow().isoformat()
    df["_pk"] = df["상품코드_pk"].astype(str) + "|" + snapshot_date
    return df


def _validate_product_xlsx(path: Path) -> int:
    """빈 파일·오류 응답·다른 화면의 엑셀로 운영 상품 파일을 덮어쓰지 않는다."""
    try:
        df = pd.read_excel(path, dtype=str, engine="openpyxl")
    except Exception as exc:
        raise RuntimeError(f"상품조회 엑셀을 읽을 수 없습니다: {path.name}") from exc
    df.columns = [str(column).strip() for column in df.columns]
    required = {"상품코드", "상품명"}
    if not required.issubset(df.columns):
        raise RuntimeError(f"상품조회 엑셀 필수 컬럼 누락: {sorted(required - set(df.columns))}")
    valid = df["상품코드"].fillna("").str.strip().ne("") & df["상품명"].fillna("").str.strip().ne("")
    count = int(valid.sum())
    if not count:
        raise RuntimeError("상품조회 엑셀에 유효한 상품이 없습니다.")
    logger.info("상품조회 엑셀 검증 완료: %s | 상품 %d건", path.name, count)
    return count


def download_easypos_product(**context) -> str:
    """EasyPOS 상품조회 엑셀 다운로드 후, 다운로드된 xlsx 경로를 XCom으로 전달."""
    snapshot_date = (context.get("ds") or "").strip() or datetime.now().strftime("%Y-%m-%d")

    download_dir = TEMP_DIR / "easypos_product_download"
    _cleanup_download_dir(download_dir)

    with sync_playwright() as p:
        def _open_browser():
            try:
                _browser = launch_chromium(
                    p,
                    headless=HEADLESS_MODE,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
                )
            except Exception as e:
                # 컨테이너/업그레이드 등으로 playwright 브라우저가 누락된 경우 자동 설치 시도
                msg = str(e)
                if "Executable doesn't exist" in msg or "playwright install" in msg:
                    _ensure_playwright_chromium_installed()
                    _browser = launch_chromium(
                        p,
                        headless=HEADLESS_MODE,
                        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
                    )
                else:
                    raise

            _bctx = _browser.new_context(
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True,
                locale="ko-KR",
                timezone_id="Asia/Seoul",
            )
            _add_nexacro_init_script(_bctx)
            return _browser, _bctx, _bctx.new_page()

        browser = None
        last_err: Exception | None = None
        for attempt in range(1, 4):
            browser = None
            page = None
            stage = "browser"
            try:
                browser, bctx, page = _open_browser()
                stage = "login"
                mf = _login(page)
                stage = "product_screen"
                mf = _navigate_to_product_search(page, mf)
                stage = "product_query"
                _search_product(page)
                stage = "product_export"
                tmp_xlsx = download_dir / f"easypos_product_{snapshot_date}.xlsx"
                _download_product_xlsx(page, tmp_xlsx)
                stage = "xlsx_validation"
                _validate_product_xlsx(tmp_xlsx)

                context["ti"].xcom_push(key="downloaded_path", value=str(tmp_xlsx))
                logger.info("EasyPOS 상품조회 다운로드 완료: %s", tmp_xlsx)
                return f"다운로드 완료: {tmp_xlsx}"
            except (PlaywrightError, RuntimeError) as e:
                last_err = e
                logger.warning("EasyPOS 상품 수집 실패: stage=%s attempt=%d/3 error=%s", stage, attempt, type(e).__name__)
                if page is not None and not page.is_closed():
                    _debug_dump(page, f"easypos_product_{stage}_attempt_{attempt}")
                if attempt >= 3:
                    raise
            finally:
                try:
                    if browser is not None:
                        browser.close()
                except Exception:
                    pass
            time.sleep(5)

        if last_err is not None:
            raise last_err
        raise RuntimeError("EasyPOS 상품조회 다운로드 실패")


def save_easypos_product(**context) -> str:
    """다운로드된 상품조회 엑셀을 덮어쓰기 방식으로 저장(엑셀 1개 파일만)."""
    snapshot_date = (context.get("ds") or "").strip() or datetime.now().strftime("%Y-%m-%d")
    downloaded_path = context["ti"].xcom_pull(task_ids="download_easypos_product", key="downloaded_path")
    # 요청사항: 엑셀 1개만 저장 (덮어쓰기)
    # - 기본: ANALYTICS_DB/easypos_product/상품조회.xlsx
    # - override: EASYPOS_PRODUCT_XLSX_PATH (절대경로)
    override_path = (os.getenv("EASYPOS_PRODUCT_XLSX_PATH") or "").strip()
    if override_path:
        dest_xlsx = Path(override_path)
    else:
        base_dir = ANALYTICS_DB / "easypos_product"
        dest_xlsx = base_dir / "상품조회.xlsx"

    if not downloaded_path:
        if dest_xlsx.exists() and dest_xlsx.stat().st_size > 0:
            logger.warning(
                "download_easypos_product XCom이 없어 기존 상품조회.xlsx를 유지합니다: %s",
                dest_xlsx,
            )
            context["ti"].xcom_push(key="dest_xlsx", value=str(dest_xlsx))
            return f"기존 상품조회.xlsx 유지 | date={snapshot_date} | xlsx={dest_xlsx}"
        raise ValueError("download_easypos_product의 XCom(downloaded_path)이 비어있습니다.")

    src = Path(str(downloaded_path))
    if not src.exists():
        raise FileNotFoundError(f"다운로드 파일이 없습니다: {src}")
    _validate_product_xlsx(src)

    # OneDrive/Excel 잠금이 간헐적으로 발생하므로, 저장 자체에서 짧게 재시도한다.
    last_err: Exception | None = None
    for attempt in range(1, 7):
        try:
            _atomic_copy_replace(src, dest_xlsx)
            last_err = None
            break
        except PermissionError as e:
            last_err = e
            wait_s = 10 * attempt
            logger.warning(
                "상품조회.xlsx 덮어쓰기 실패(잠금/권한) (attempt=%d/6). %ds 후 재시도: %s",
                attempt,
                wait_s,
                dest_xlsx,
            )
            time.sleep(wait_s)

    if last_err is not None:
        raise PermissionError(
            f"저장 대상 파일을 덮어쓸 수 없습니다(잠금/권한).\n"
            f"- 대상: {dest_xlsx}\n"
            f"- 해결: Excel/미리보기/동기화가 파일을 열고 있으면 닫고 다시 실행하세요.\n"
            f"- 원본 다운로드: {src}\n"
        ) from last_err

    context["ti"].xcom_push(key="dest_xlsx", value=str(dest_xlsx))

    # 예전 버전에서 생성된 부가 파일은(요청사항에 맞게) best-effort로 정리
    try:
        extras = []
        if not override_path:
            base_dir = ANALYTICS_DB / "easypos_product"
            extras = [
                base_dir / "products.csv",
                base_dir / "products_snapshot.csv",
                base_dir / "latest_상품조회.xlsx",
            ]
        for p in extras:
            try:
                if p.exists() and p.is_file():
                    p.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass

    return f"덮어쓰기 저장 완료 | date={snapshot_date} | xlsx={dest_xlsx}"
