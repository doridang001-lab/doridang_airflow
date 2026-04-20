"""
UnionPOS(asp2.unionpos.co.kr) 영수증 원본 수집 파이프라인 (Playwright 기반)

처리 흐름:
1. 실행 날짜 결정 (conf 또는 yesterday)
2. 로그인 → 날짜 설정 → 영수증 목록 + 상세품목 수집
3. 파티션 CSV 저장 (RAW_UNIONPOS_SALES / brand=도리당 / store={매장} / ym={} /)
   - unionpos_receipt_list.csv  : 영수증 헤더
   - unionpos_receipt_items.csv : 영수증 상세품목
4. log.parquet 실행 이력 기록
"""

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeout

from modules.transform.utility.paths import RAW_UNIONPOS_SALES

logger = logging.getLogger(__name__)

UNIONPOS_LOGIN_URL   = "https://asp2.unionpos.co.kr/"
UNIONPOS_RECEIPT_URL = "https://asp2.unionpos.co.kr/v2/sales/detail/receipt2"
UNIONPOS_ID = "SZ36060"
UNIONPOS_PW = "97784"

HEADLESS_MODE    = os.getenv("AIRFLOW_HOME") is not None
_RETRY_MAX       = 3
_RETRY_BASE_WAIT = 5  # 초; 실제 대기: 5, 15


# ── 로그인 ────────────────────────────────────────────────────────────────────

def _login(page: Page) -> None:
    page.goto(UNIONPOS_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    page.fill("#userId", UNIONPOS_ID)
    page.fill("#password", UNIONPOS_PW)
    page.click("#btnLogin")
    # /v2/ 경로로 이동 완료될 때까지 대기 (루트 URL에서 멈추는 경우 방지)
    page.wait_for_url("**/v2/**", timeout=30000)
    logger.info(f"UnionPOS 로그인 완료 | URL: {page.url}")


# ── 날짜 설정 ─────────────────────────────────────────────────────────────────

def _set_date_input(page: Page, start: str, end: str) -> None:
    """#startDate / #endDate 입력 필드를 직접 조작 (fn_SetDate 미사용)"""
    page.evaluate(
        """([s, e]) => {
            var si = document.getElementById('startDate');
            var ei = document.getElementById('endDate');
            if (si) { si.removeAttribute('readonly'); si.value = s; si.setAttribute('readonly', 'readonly'); }
            if (ei) { ei.removeAttribute('readonly'); ei.value = e; ei.setAttribute('readonly', 'readonly'); }
        }""",
        [start, end],
    )


def _set_date_and_search(page: Page, sale_date: str) -> None:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # 날짜 필드가 렌더링될 때까지 대기
    page.wait_for_selector("#startDate", state="visible", timeout=15000)

    if sale_date == yesterday:
        try:
            page.click("button:has-text('어제')", timeout=5000)
            logger.info(f"어제 버튼 클릭: {sale_date}")
        except PlaywrightTimeout:
            # fn_SetDate 는 내부 변수(search)가 없으면 ReferenceError → DOM 직접 조작으로 대체
            _set_date_input(page, sale_date, sale_date)
            _click_search(page)
            logger.info(f"날짜 직접 설정 + 조회 (fallback): {sale_date}")
    else:
        _set_date_input(page, sale_date, sale_date)
        _click_search(page)
        logger.info(f"날짜 직접 설정 + 조회: {sale_date}")

    try:
        page.wait_for_selector("#tableList tbody tr", timeout=20000)
    except PlaywrightTimeout:
        logger.warning(f"[{sale_date}] 테이블 로드 타임아웃")


def _click_search(page: Page) -> None:
    for sel in ["button.btn-search", "button:has-text('조회')", "button:has-text('검색')",
                "input[type='button'][value='조회']"]:
        try:
            page.click(sel, timeout=3000)
            return
        except PlaywrightTimeout:
            continue
    logger.error("조회 버튼을 찾지 못했습니다")
    raise PlaywrightTimeout("조회 버튼 미발견")


# ── 영수증 목록 파싱 ──────────────────────────────────────────────────────────

def _parse_page_receipts(page: Page) -> list[dict]:
    rows = page.query_selector_all("#tableList tbody tr")
    receipts = []
    for row in rows:
        cls = row.get_attribute("class") or ""
        if "tr-sum" in cls:
            continue
        tds = row.query_selector_all("td")
        if len(tds) < 15:
            continue
        try:
            num = tds[0].inner_text().strip()
            if not num.isdigit():
                continue
            link    = tds[2].query_selector("a")
            onclick = link.get_attribute("onclick") or "" if link else ""
            strong  = tds[2].query_selector("strong")
            sale_dt = strong.inner_text().strip() if strong else tds[2].inner_text().strip()
            receipts.append({
                "data": {
                    "번호":       num,
                    "매장명":     tds[1].inner_text().strip(),
                    "판매일시":   sale_dt,
                    "포스번호":   tds[3].inner_text().strip(),
                    "영수증번호": tds[4].inner_text().strip(),
                    "결제합계":   tds[5].inner_text().strip(),
                    "현금":       tds[6].inner_text().strip(),
                    "카드":       tds[7].inner_text().strip(),
                    "기타":       tds[8].inner_text().strip(),
                    "할인":       tds[9].inner_text().strip(),
                    "판매타입":   tds[10].inner_text().strip(),
                    "테이블코드": tds[11].inner_text().strip(),
                    "고객수":     tds[12].inner_text().strip(),
                    "공급가액":   tds[13].inner_text().strip(),
                    "부가세":     tds[14].inner_text().strip(),
                },
                "onclick": onclick,
            })
        except Exception as e:
            logger.warning(f"영수증 행 파싱 실패: {e}")
    return receipts


# ── 상세품목 팝업 파싱 ─────────────────────────────────────────────────────────

def _parse_receipt_items_popup(
    page: Page,
    browser_context: BrowserContext,
    onclick: str,
    receipt_no: str,
    sale_datetime: str,
) -> list[dict]:
    """
    expect_popup 대신 context.pages 기반 직접 감지.
    expect_popup 컨텍스트 매니저는 팝업 닫힘 시 메인 페이지를 죽이는 부작용이 있음.
    """
    items = []

    # onclick 실행 전 현재 페이지 집합 스냅샷
    pages_before = {id(p) for p in browser_context.pages}

    try:
        page.evaluate(onclick)
    except Exception as e:
        logger.warning(f"onclick 실행 실패 (영수증={receipt_no}): {e}")
        return items

    # 새 창이 열릴 시간 대기
    time.sleep(1.5)

    new_pages = [
        p for p in browser_context.pages
        if id(p) not in pages_before and not p.is_closed()
    ]

    if new_pages:
        popup_page = new_pages[0]
        try:
            popup_page.wait_for_selector("#receiptList tbody tr", timeout=15000)
            items = _extract_items(popup_page, receipt_no, sale_datetime)
        except Exception as e:
            logger.warning(f"상세품목 팝업 파싱 실패 (영수증={receipt_no}): {e}")
        finally:
            try:
                popup_page.close()
            except Exception:
                pass
    else:
        # 새 창 없음 → 같은 페이지 모달
        try:
            page.wait_for_selector("#receiptList tbody tr", timeout=15000)
            items = _extract_items(page, receipt_no, sale_datetime)
        except PlaywrightTimeout:
            logger.warning(f"상세품목 모달 타임아웃 (영수증={receipt_no})")
        finally:
            try:
                page.click(".btnReceiptPopupClose", timeout=5000)
                page.wait_for_selector(".btnReceiptPopupClose", state="hidden", timeout=5000)
            except PlaywrightTimeout:
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                except Exception:
                    pass

    return items


def _extract_items(page: Page, receipt_no: str, sale_datetime: str) -> list[dict]:
    items = []
    for irow in page.query_selector_all("#receiptList tbody tr"):
        tds = irow.query_selector_all("td")
        if not tds:
            continue
        try:
            strong    = tds[2].query_selector("strong") if len(tds) > 2 else None
            item_name = strong.inner_text().strip() if strong else (tds[2].inner_text().strip() if len(tds) > 2 else "")
            items.append({
                "영수증번호": receipt_no,
                "판매일시":   sale_datetime,
                "번호":       tds[0].inner_text().strip() if len(tds) > 0 else "",
                "상품코드":   tds[1].inner_text().strip() if len(tds) > 1 else "",
                "상품명":     item_name,
                "단가":       tds[3].inner_text().strip() if len(tds) > 3 else "",
                "수량":       tds[4].inner_text().strip() if len(tds) > 4 else "",
                "합계":       tds[5].inner_text().strip() if len(tds) > 5 else "",
                "할인":       tds[6].inner_text().strip() if len(tds) > 6 else "",
                "분류명":     tds[7].inner_text().strip() if len(tds) > 7 else "",
                "판매타입":   tds[8].inner_text().strip() if len(tds) > 8 else "",
            })
        except Exception as e:
            logger.warning(f"상세품목 행 파싱 실패: {e}")
    return items


# ── 페이지네이션 ──────────────────────────────────────────────────────────────

def _has_next_page(page: Page) -> bool:
    el = page.query_selector("img[src='/images/common/go_next.png']")
    return el is not None and el.is_visible()


def _click_next_page(page: Page) -> bool:
    try:
        img = page.query_selector("img[src='/images/common/go_next.png']")
        if not img:
            return False
        before_count = len(page.query_selector_all("#tableList tbody tr"))
        img.evaluate("el => el.parentElement.click()")
        # 행 수 변화로 페이지 전환 감지
        page.wait_for_function(
            f"() => document.querySelectorAll('#tableList tbody tr').length !== {before_count}",
            timeout=15000,
        )
        page.wait_for_selector("#tableList tbody tr", timeout=10000)
        return True
    except PlaywrightTimeout:
        logger.warning("다음 페이지 전환 타임아웃 → 마지막 페이지 처리")
        return False
    except Exception as e:
        logger.warning(f"다음 페이지 클릭 실패: {e}")
        return False


# ── Task 함수 ─────────────────────────────────────────────────────────────────

def resolve_sale_dates(manual_date_range=None, **context) -> str:
    """실행 날짜 결정 (MANUAL_DATE_RANGE > dag_run.conf > yesterday)"""
    if manual_date_range is not None:
        if not (isinstance(manual_date_range, (tuple, list)) and len(manual_date_range) == 2):
            raise ValueError(f"MANUAL_DATE_RANGE 형식 오류: {manual_date_range!r}")
        date_from, date_to = str(manual_date_range[0]), str(manual_date_range[1])
        source = "MANUAL_DATE_RANGE"
    else:
        conf = context.get("dag_run").conf or {}
        date_from = conf.get("sale_date_from")
        date_to   = conf.get("sale_date_to")
        source = "dag_run.conf"

    if date_from or date_to:
        if not date_from or not date_to:
            raise ValueError("시작일과 종료일은 함께 지정해야 합니다.")
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d")
        if dt_from > dt_to:
            raise ValueError(f"시작일({date_from}) > 종료일({date_to})")
        sale_dates = []
        cur = dt_from
        while cur <= dt_to:
            sale_dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
    else:
        sale_dates = [(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")]
        source = "yesterday(기본)"

    context["ti"].xcom_push(key="sale_dates", value=sale_dates)
    logger.info(f"날짜 범위 [{source}]: {sale_dates}")
    return f"날짜 범위 결정 완료 [{source}]: {sale_dates[0]} ~ {sale_dates[-1]} ({len(sale_dates)}일)"


def collect_and_save(**context) -> str:
    """UnionPOS 영수증 목록 + 상세품목 수집 후 파티션 CSV 저장 (Playwright, 최대 3회 재시도)"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    saved_files: list[str] = []
    last_exc: Exception | None = None

    for attempt in range(1, _RETRY_MAX + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=HEADLESS_MODE,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )
                try:
                    browser_context = browser.new_context()
                    page = browser_context.new_page()
                    _collect_session(page, browser_context, sale_dates, saved_files, context)
                finally:
                    browser.close()
            break  # 성공

        except Exception as exc:
            last_exc = exc
            is_transient = isinstance(exc, PlaywrightTimeout) or _is_network_error(exc)
            if is_transient and attempt < _RETRY_MAX:
                wait_sec = _RETRY_BASE_WAIT * (3 ** (attempt - 1))
                logger.warning(f"오류 (attempt {attempt}/{_RETRY_MAX}) → {wait_sec}초 후 재시도: {exc}")
                time.sleep(wait_sec)
            else:
                raise

    context["ti"].xcom_push(key="saved_files", value=saved_files)
    return f"수집 저장 완료: {len(saved_files)}개 파일"


def _is_network_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in (
        "connection", "timeout", "network", "refused", "remotedisconnected",
        "target closed", "browser has been closed",
    ))


def _collect_session(
    page: Page,
    browser_context: BrowserContext,
    sale_dates: list[str],
    saved_files: list[str],
    context,
) -> None:
    from modules.load.load_onedrive import onedrive_csv_save

    _login(page)

    for sale_date in sale_dates:
        logger.info(f"=== UnionPOS 수집 시작: {sale_date} ===")
        page.goto(UNIONPOS_RECEIPT_URL, wait_until="domcontentloaded", timeout=30000)

        _set_date_and_search(page, sale_date)

        rows_check = page.query_selector_all("#tableList tbody tr")
        if not rows_check:
            logger.info(f"[{sale_date}] 데이터 없음 — 스킵")
            continue

        all_list_records: list[dict] = []
        all_item_records: list[dict] = []
        page_num = 1

        while True:
            logger.info(f"[{sale_date}] 페이지 {page_num} 파싱")
            receipts = _parse_page_receipts(page)
            all_list_records.extend([r["data"] for r in receipts])

            for r in receipts:
                # 메인 페이지가 살아있는지 확인
                if page.is_closed():
                    raise PlaywrightTimeout("메인 페이지가 닫혔습니다")

                rec     = r["data"]
                onclick = r["onclick"]
                if not onclick:
                    logger.warning(f"onclick 없음: 영수증번호={rec['영수증번호']}")
                    continue
                items = _parse_receipt_items_popup(
                    page, browser_context, onclick, rec["영수증번호"], rec["판매일시"]
                )
                all_item_records.extend(items)
                logger.info(f"  영수증 {rec['영수증번호']}: {len(items)}개 품목")

            if _has_next_page(page):
                moved = _click_next_page(page)
                if not moved:
                    break
                page_num += 1
            else:
                break

        logger.info(
            f"[{sale_date}] 수집 완료: 영수증 {len(all_list_records)}건, 품목 {len(all_item_records)}건"
        )

        # ── CSV 저장 ──────────────────────────────────────────────────────
        ym      = sale_date[:7]
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        store_names = list({r["매장명"] for r in all_list_records if r.get("매장명")}) or ["unknown"]

        for store_name in store_names:
            store_short = store_name.replace("도리당 ", "", 1)
            base = RAW_UNIONPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}"
            base.mkdir(parents=True, exist_ok=True)

            list_rows = [r for r in all_list_records
                         if r.get("매장명") == store_name or store_name == "unknown"]
            if list_rows:
                list_df = pd.DataFrame(list_rows)
                n = len(list_df.columns)
                list_df["sale_date"]    = sale_date
                list_df["ym"]           = ym
                list_df["collected_at"] = now_str
                list_df["_pk"] = list_df.iloc[:, :n].apply(
                    lambda row: hashlib.md5("|".join(row.astype(str)).encode()).hexdigest(), axis=1,
                )
                dest_list = base / "unionpos_receipt_list.csv"
                result = onedrive_csv_save(
                    df=list_df, file_path=str(dest_list),
                    pk_col="_pk", timestamp_col="collected_at", if_exists="append",
                )
                saved_files.append(str(dest_list))
                logger.info(f"영수증목록 저장: {dest_list.name} | 신규={result.get('inserted', 0)}")

            receipt_nos = {r["영수증번호"] for r in list_rows}
            item_rows = [r for r in all_item_records if r.get("영수증번호") in receipt_nos]
            if item_rows:
                item_df = pd.DataFrame(item_rows)
                n2 = len(item_df.columns)
                item_df["sale_date"]    = sale_date
                item_df["ym"]           = ym
                item_df["collected_at"] = now_str
                item_df["_pk"] = item_df.iloc[:, :n2].apply(
                    lambda row: hashlib.md5("|".join(row.astype(str)).encode()).hexdigest(), axis=1,
                )
                dest_items = base / "unionpos_receipt_items.csv"
                result2 = onedrive_csv_save(
                    df=item_df, file_path=str(dest_items),
                    pk_col="_pk", timestamp_col="collected_at", if_exists="append",
                )
                saved_files.append(str(dest_items))
                logger.info(f"상세품목 저장: {dest_items.name} | 신규={result2.get('inserted', 0)}")


def write_log(**context) -> str:
    """log.parquet 실행 이력 기록"""
    saved_files = context["ti"].xcom_pull(task_ids="collect_and_save", key="saved_files") or []
    sale_dates  = context["ti"].xcom_pull(task_ids="resolve_dates",    key="sale_dates")  or []
    log_path    = RAW_UNIONPOS_SALES / "log.parquet"

    try:
        RAW_UNIONPOS_SALES.mkdir(parents=True, exist_ok=True)
        now_ts = pd.Timestamp.now(tz="Asia/Seoul")
        rows = []
        for file_str in saved_files:
            try:
                p = Path(file_str)
                parts = p.parts
                store_short = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("store=")), "unknown")
                ym          = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("ym=")),    "unknown")
                rows.append({
                    "run_at":    now_ts,
                    "sale_date": sale_dates[0] if sale_dates else "unknown",
                    "page_type": p.stem,
                    "store":     store_short,
                    "ym":        ym,
                    "result":    "success",
                    "file_path": str(p),
                })
            except Exception as e:
                logger.warning(f"로그 행 파싱 실패: {file_str} | {e}")

        new_df = pd.DataFrame(
            rows,
            columns=["run_at", "sale_date", "page_type", "store", "ym", "result", "file_path"],
        )
        if log_path.exists() and log_path.stat().st_size > 0:
            out_df = pd.concat([pd.read_parquet(log_path), new_df], ignore_index=True)
        else:
            out_df = new_df

        if not out_df.empty and "run_at" in out_df.columns:
            out_df["run_at"] = pd.to_datetime(out_df["run_at"], errors="coerce")
            out_df = out_df.sort_values("run_at", ascending=False).reset_index(drop=True)

        out_df.to_parquet(log_path, index=False)
        logger.info(f"log.parquet 기록: {log_path} | {len(new_df)}건")
        return f"log.parquet 기록 완료: {len(new_df)}건"

    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
