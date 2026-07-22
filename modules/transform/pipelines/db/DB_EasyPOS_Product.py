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

import pandas as pd
from playwright.sync_api import sync_playwright, Frame, Page, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import TargetClosedError

from modules.load.load_onedrive import onedrive_csv_save
from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR
from modules.transform.utility.playwright_launcher import launch_chromium

# EasyPOS 로그인/클릭 유틸 재사용
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (  # noqa: E402
    HEADLESS_MODE,
    WAIT_TIMEOUT,
    _login,
    _get_main_frame,
    _find_first_selector,
    _click_handle,
    _debug_dump,
    _ensure_playwright_chromium_installed,
)

logger = logging.getLogger(__name__)


# ============================================================
# EasyPOS UI selectors (NexacroN)
# ============================================================
_MENU_BASIC_INFO = "mainframe_childframe_form_divTop_img_TA_top_menu1"  # 기초정보

_BTN_SEARCH_SELECTORS = [
    "[id$='btnCommSearch']",
    "[id*='divCommonBtn'][id*='btnCommSearch']",
    "xpath=//*[contains(@id,'btnCommSearch')]",
    "xpath=//*[contains(normalize-space(.),'조회') and contains(@id,'btn')]",
]

_BTN_EXCEL_SELECTORS = [
    "[id$='btnCommExcel']",
    "[id*='btnCommExcel']",
    "xpath=//*[contains(@id,'btnCommExcel')]",
    "xpath=//*[contains(normalize-space(.),'엑셀') and (contains(@id,'btn') or contains(@id,'Excel'))]",
]

_PRODUCT_MENU_SELECTORS = [
    # 좌측 메뉴 영역 내 '상품조회' 텍스트
    "xpath=//*[contains(@id,'divLeftMenu') or contains(@id,'divLeftMainList') or contains(@id,'grdLeft')]//*[normalize-space(.)='상품조회']",
    "xpath=//*[normalize-space(.)='상품조회']",
]


def _is_clickable_handle(handle) -> bool:
    if handle is None:
        return False
    try:
        if hasattr(handle, "is_visible") and not handle.is_visible():
            return False
    except Exception:
        return False
    try:
        return handle.bounding_box() is not None
    except Exception:
        return False


def _find_first_clickable_selector(mf: Frame, selectors: list[str]):
    for selector in selectors:
        try:
            for el in mf.query_selector_all(selector):
                if _is_clickable_handle(el):
                    return el, selector
        except Exception:
            continue
    return None, None


def _navigate_product_by_menu_url(page: Page, target_needles: list[str]) -> bool:
    """Nexacro 메뉴 데이터셋에서 상품 화면 URL을 찾아 childframe에 직접 로드."""
    mf = _get_main_frame(page)
    result = mf.evaluate(
        """(needles) => {
            try {
                var form = window.application.mainframe.childframe.form;
                var rows = [];

                function pushRow(source, idx, nm, url, id) {
                    nm = nm || '';
                    url = url || '';
                    if (!nm && !url) return;
                    rows.push({source: source, i: idx, nm: String(nm), url: String(url), id: id || ''});
                }

                ['dsLeftMenuParnas', 'dsLeftMenuParnas00', 'dsLeftMenuSample', 'dsTopMenu'].forEach(function(dsName) {
                    var ds = form[dsName];
                    if (!ds || ds.rowcount === undefined) return;
                    var rowCount = ds.rowcount || 0;
                    for (var i = 0; i < rowCount; i++) {
                        var nm = ds.getColumn(i, 'MENU_NAME') || ds.getColumn(i, 'MENU_NM') || ds.getColumn(i, 'MENU_NM_KOR') || '';
                        var url = ds.getColumn(i, 'SMART_MENU_URL') || ds.getColumn(i, 'PROGRAM_PATH') || ds.getColumn(i, 'MENU_URL') || '';
                        var id = ds.getColumn(i, 'MENU_ID') || ds.getColumn(i, 'PROGRAM_ID') || '';
                        pushRow(dsName, i, nm, url, id);
                    }
                });

                var grd = form.divLeftMenu && form.divLeftMenu.divLeftMainList && form.divLeftMenu.divLeftMainList.grdLeft;
                if (grd && typeof form.gfnGetMenuUrl === 'function') {
                    var rowCount = 100;
                    try {
                        var ds = null;
                        if (grd._datasets && grd._datasets.length > 0) ds = grd._datasets[0];
                        else if (typeof grd.getBindDataset === 'function') ds = grd.getBindDataset();
                        if (ds && ds.rowcount) rowCount = ds.rowcount;
                    } catch(e) {}
                    for (var j = 0; j < rowCount; j++) {
                        try {
                            pushRow('gfnGetMenuUrl', j, form.gfnGetMenuName(j), form.gfnGetMenuUrl(j), form.gfnGetMenuId(j));
                        } catch(e) { break; }
                    }
                }

                var candidates = rows.filter(function(row) {
                    return needles.some(function(n) { return row.nm.indexOf(n) >= 0; }) && row.url;
                });
                candidates.sort(function(a, b) {
                    function score(row) {
                        if (row.nm.indexOf('상품조회') >= 0) return 0;
                        if (row.nm.indexOf('상품 등록') >= 0 || row.nm.indexOf('상품등록') >= 0) return 1;
                        return 2;
                    }
                    return score(a) - score(b);
                });

                if (candidates.length === 0) {
                    return {ok: false, reason: 'target menu url not found', sample: rows.slice(0, 30)};
                }

                var target = candidates[0];
                var cf = window.application.mainframe.childframe;
                if (typeof cf.set_formurl !== 'function') {
                    return {ok: false, reason: 'childframe.set_formurl not function', target: target, candidates: candidates.slice(0, 10)};
                }
                cf.set_formurl(target.url);
                return {ok: true, target: target, candidates: candidates.slice(0, 10)};
            } catch(e) {
                return {ok: false, reason: e.toString()};
            }
        }""",
        target_needles,
    )
    logger.info("EasyPOS 상품 메뉴 URL 직접 진입 결과: %s", result)
    if not isinstance(result, dict) or not result.get("ok"):
        return False
    time.sleep(3.0)
    mf = _get_main_frame(page)
    btn, _ = _find_first_clickable_selector(mf, _BTN_SEARCH_SELECTORS + _BTN_EXCEL_SELECTORS)
    return btn is not None


def _cleanup_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for p in download_dir.glob("*"):
        try:
            if p.is_file():
                p.unlink(missing_ok=True)
        except Exception:
            continue


def _add_nexacro_init_script(context) -> None:
    # DB_EasyPOS_Sales.collect_receipts 에서 사용하던 안정화 스크립트(간소화)
    context.add_init_script(
        """
        (function() {
            var _origGetById = Document.prototype.getElementById;
            Document.prototype.getElementById = function(id) {
                var el = _origGetById.call(this, id);
                if (!el && id === 'loadingImg') {
                    return {
                        style: {display: 'block'},
                        id: 'loadingImg',
                        setAttribute: function() {},
                        removeAttribute: function() {}
                    };
                }
                return el;
            };
            Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
        })();
        """
    )


def _navigate_to_product_search(page: Page, mf: Frame, *, _reload_retry: bool = False) -> Frame:
    """기초정보 → 상품조회 화면으로 이동."""
    # 상단 '기초정보' 메뉴 클릭
    try:
        menu = mf.query_selector(f"#{_MENU_BASIC_INFO}")
        if menu is not None:
            _click_handle(page, menu, "기초정보 메뉴", prefer_mouse=True)
            time.sleep(1.5)
    except Exception:
        # 메뉴 클릭 실패는 아래 상품조회 메뉴 클릭에서 fallback
        pass

    # 좌측 메뉴(grid) 로드 대기
    mf = _get_main_frame(page)
    try:
        mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8_000)
    except Exception:
        logger.warning("grdLeft 메뉴 로드 대기 타임아웃(계속 시도)")

    def _find_grd_row_by_text(texts: list[str]):
        mf2 = _get_main_frame(page)
        for el in mf2.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if txt in texts:
                return el, txt
        return None, None

    menu_snapshot: list[str] = []

    def _collect_menu_texts(limit: int = 60) -> list[str]:
        texts: list[str] = []
        mf2 = _get_main_frame(page)
        for el in mf2.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if txt:
                texts.append(txt)
        # 중복 축약(순서 유지)
        seen: set[str] = set()
        uniq: list[str] = []
        for t in texts:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq[:limit]

    def _click_first_row_containing(
        needles: list[str],
        *,
        double: bool = False,
        label: str = "",
        exact: bool = False,
        exclude: list[str] | None = None,
    ) -> bool:
        mf2 = _get_main_frame(page)
        for el in mf2.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if not txt:
                continue
            if exclude and any(n in txt for n in exclude):
                continue
            matched = txt in needles if exact else any(n in txt for n in needles)
            if matched:
                box = el.bounding_box()
                if box:
                    cx = box["x"] + box["width"] / 2
                    cy = box["y"] + box["height"] / 2
                    if double:
                        page.mouse.dblclick(cx, cy)
                        time.sleep(0.2)
                        page.keyboard.press("Enter")
                    else:
                        page.mouse.click(cx, cy)
                    logger.info("%s 클릭 @ (%.0f, %.0f) txt=%r", label or "메뉴", cx, cy, txt)
                    return True
                try:
                    el.click()
                    if double:
                        el.click(click_count=2)
                    logger.info("%s 클릭(element.click) txt=%r", label or "메뉴", txt)
                    return True
                except Exception:
                    return False
        return False

    target_needles = ["상품조회", "상품 조회"]
    expand_needles = ["상품관리", "상품 관리"]

    # 스크롤 포함 탐색: 상품관리를 펼치고 '상품조회'를 찾는다.
    for attempt in range(1, 5):
        for scroll_round in range(0, 6):
            # 먼저 상품관리(상위메뉴) 클릭해서 하위 항목 노출 유도
            _click_first_row_containing(expand_needles, double=False, label="상품관리")
            time.sleep(0.8)

            if _click_first_row_containing(
                target_needles,
                double=True,
                label="상품조회",
                exact=True,
                exclude=["상품등록", "상품 등록"],
            ):
                time.sleep(2.0)
                mf = _get_main_frame(page)
                b, _ = _find_first_clickable_selector(mf, _BTN_SEARCH_SELECTORS)
                e, _ = _find_first_clickable_selector(mf, _BTN_EXCEL_SELECTORS)
                if b is not None or e is not None:
                    return _get_main_frame(page)
                if _navigate_product_by_menu_url(page, target_needles):
                    return _get_main_frame(page)
                try:
                    mf.wait_for_selector("[id*='btnCommSearch'],[id*='btnCommExcel']", timeout=WAIT_TIMEOUT)
                    return _get_main_frame(page)
                except Exception:
                    _debug_dump(page, f"easypos_product_view_not_ready_attempt_{attempt}_{scroll_round}")

            # 다음 영역으로 스크롤해서 추가 메뉴 탐색
            try:
                page.mouse.wheel(0, 800)
            except Exception:
                pass
            time.sleep(0.4)

        menu_snapshot = _collect_menu_texts(limit=30)
        logger.warning("상품조회 메뉴 탐색 실패 (attempt=%d/4) | 메뉴샘플=%s", attempt, menu_snapshot)

        # 탭 재클릭으로 좌측메뉴 리셋 + 스크롤 초기화
        mf = _get_main_frame(page)
        tab_el = mf.query_selector(f"#{_MENU_BASIC_INFO}")
        if tab_el is not None:
            try:
                _click_handle(page, tab_el, "기초정보 메뉴(리셋)", prefer_mouse=True)
            except Exception:
                pass
        try:
            page.mouse.wheel(0, -5000)
        except Exception:
            pass
        time.sleep(2.0)

    _debug_dump(page, "easypos_product_menu_not_found")

    # grdLeft가 계속 비어있으면 페이지 새로고침 후 1회 재시도
    if not _reload_retry:
        logger.warning("상품조회 메뉴 탐색 전체 실패 — 페이지 새로고침 후 재로그인 1회 재시도")
        mf = _login(page)
        return _navigate_to_product_search(page, mf, _reload_retry=True)

    raise RuntimeError(f"EasyPOS 좌측 메뉴에서 '상품조회'를 찾지 못했습니다. 메뉴샘플={menu_snapshot}")


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
                    args=["--disable-blink-features=AutomationControlled"],
                )
            except Exception as e:
                # 컨테이너/업그레이드 등으로 playwright 브라우저가 누락된 경우 자동 설치 시도
                msg = str(e)
                if "Executable doesn't exist" in msg or "playwright install" in msg:
                    _ensure_playwright_chromium_installed()
                    _browser = launch_chromium(
                        p,
                        headless=HEADLESS_MODE,
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                else:
                    raise

            _bctx = _browser.new_context(
                accept_downloads=True,
                locale="ko-KR",
                timezone_id="Asia/Seoul",
            )
            _add_nexacro_init_script(_bctx)
            return _browser, _bctx, _bctx.new_page()

        browser = None
        last_err: Exception | None = None
        for attempt in range(1, 4):
            browser, bctx, page = _open_browser()
            try:
                mf = _login(page)
                mf = _navigate_to_product_search(page, mf)

                # 조회
                search_btn, search_sel = _find_first_clickable_selector(mf, _BTN_SEARCH_SELECTORS)
                if search_btn is None:
                    _debug_dump(page, f"easypos_product_search_btn_not_found_attempt_{attempt}")
                    raise RuntimeError("상품조회 화면에서 '조회' 버튼을 찾지 못했습니다.")

                _click_handle(page, search_btn, f"조회({search_sel})", prefer_mouse=True)
                time.sleep(2.5)

                # 엑셀 다운로드
                mf = _get_main_frame(page)
                excel_btn, excel_sel = _find_first_clickable_selector(mf, _BTN_EXCEL_SELECTORS)
                if excel_btn is None:
                    _debug_dump(page, f"easypos_product_excel_btn_not_found_attempt_{attempt}")
                    raise RuntimeError("상품조회 화면에서 '엑셀' 버튼을 찾지 못했습니다.")

                with page.expect_download(timeout=120_000) as dl_info:
                    _click_handle(page, excel_btn, f"엑셀({excel_sel})", prefer_mouse=True)
                download = dl_info.value

                tmp_xlsx = download_dir / f"easypos_product_{snapshot_date}.xlsx"
                download.save_as(str(tmp_xlsx))

                context["ti"].xcom_push(key="downloaded_path", value=str(tmp_xlsx))
                logger.info("EasyPOS 상품조회 다운로드 완료: %s", tmp_xlsx)
                return f"다운로드 완료: {tmp_xlsx}"
            except (TargetClosedError, PlaywrightTimeoutError, RuntimeError) as e:
                last_err = e
                if attempt >= 3:
                    raise
                logger.warning("EasyPOS 상품조회 다운로드 실패(attempt=%d/3) → 브라우저 재시작: %s", attempt, e)
                time.sleep(5)
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

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
