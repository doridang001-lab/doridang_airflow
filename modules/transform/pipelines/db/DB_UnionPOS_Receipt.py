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
from modules.transform.utility.playwright_launcher import launch_chromium

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
) -> tuple[list[dict], str]:
    """
    expect_popup 대신 context.pages 기반 직접 감지.
    expect_popup 컨텍스트 매니저는 팝업 닫힘 시 메인 페이지를 죽이는 부작용이 있음.
    """
    items: list[dict] = []
    log_status: str = ""

    # onclick 실행 전 현재 페이지 집합 스냅샷
    pages_before = {id(p) for p in browser_context.pages}

    try:
        page.evaluate(onclick)
    except Exception as e:
        logger.warning(f"onclick 실행 실패 (영수증={receipt_no}): {e}")
        return items, ""

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
            log_status = _extract_receipt_log_status(popup_page)
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
            log_status = _extract_receipt_log_status(page)
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

    return items, log_status


def _extract_receipt_log_status(page: Page) -> str:
    """영수증 팝업 > '로그 정보' 탭에서 '상태' 값만 추출.

    - 반환값: '|' 로 join 한 unique 상태값 (순서 유지)
      예) '판매' 또는 '판매|취소'
    """
    try:
        page.click("#receiptLogInfo", timeout=5000)
    except Exception:
        return ""

    try:
        page.wait_for_selector("#receiptLogList", timeout=10000)
    except Exception:
        return ""

    statuses: list[str] = []
    try:
        for row in page.query_selector_all("#receiptLogList tbody tr"):
            tds = row.query_selector_all("td")
            # 상태 컬럼: 11번째(0-base index 10)
            if len(tds) < 11:
                continue
            txt = (tds[10].inner_text() or "").strip().replace("\n", " ")
            if txt:
                statuses.append(txt)
    except Exception:
        return ""

    if not statuses:
        return ""

    uniq: list[str] = []
    seen: set[str] = set()
    for s in statuses:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return "|".join(uniq)


def _atomic_csv_write(file_path: Path, df: pd.DataFrame) -> None:
    """Write CSV atomically (tempfile + rename) to reduce OneDrive/lock issues."""
    import tempfile

    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=str(file_path.parent), suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as f:
            df.to_csv(f, index=False)
            f.flush()
            os.fsync(f.fileno())
        if file_path.exists():
            os.unlink(file_path)
        os.rename(tmp_path, file_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


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

def resolve_sale_dates(manual_date_range=None, lookback_days=None, **context) -> str:
    """실행 날짜 결정 (MANUAL_DATE_RANGE > conf > lookback > yesterday)"""
    if manual_date_range is not None:
        if not (isinstance(manual_date_range, (tuple, list)) and len(manual_date_range) == 2):
            raise ValueError(f"MANUAL_DATE_RANGE 형식 오류: {manual_date_range!r}")
        date_from, date_to = str(manual_date_range[0]), str(manual_date_range[1])
        source = "MANUAL_DATE_RANGE"
    else:
        conf = (getattr(context.get("dag_run"), "conf", None) or {})
        sale_date = conf.get("sale_date")
        date_from = sale_date or conf.get("sale_date_from")
        date_to   = sale_date or conf.get("sale_date_to")
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
    elif lookback_days:
        today = datetime.now()
        sale_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, lookback_days + 1)]
        source = f"lookback {lookback_days}일"
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
                browser = launch_chromium(
                    p,
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
                items, log_status = _parse_receipt_items_popup(
                    page, browser_context, onclick, rec["영수증번호"], rec["판매일시"]
                )
                rec["상태"] = log_status
                if log_status:
                    for it in items:
                        it["상태"] = log_status
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
                pk_cols = [c for c in list_df.columns if c != "상태"]
                list_df["sale_date"]    = sale_date
                list_df["ym"]           = ym
                list_df["collected_at"] = now_str
                list_df["_pk"] = list_df[pk_cols].apply(
                    lambda row: hashlib.md5("|".join(row.astype(str)).encode()).hexdigest(), axis=1,
                )
                dest_list = base / "unionpos_receipt_list.csv"
                # Upsert(상태 업데이트 포함): 기존 PK와 매칭되는 행은 '상태'만 갱신하고, 신규 PK만 append.
                existing_df = pd.DataFrame()
                if dest_list.exists() and dest_list.stat().st_size > 0:
                    try:
                        existing_df = pd.read_csv(dest_list, dtype=str, encoding="utf-8-sig")
                    except Exception:
                        existing_df = pd.read_csv(dest_list, dtype=str, encoding="utf-8")

                existing_keys: set[str] = set()
                if not existing_df.empty and "_pk" in existing_df.columns:
                    existing_keys = set(existing_df["_pk"].astype(str))

                incoming_keys = set(list_df["_pk"].astype(str))
                inserted = len(incoming_keys - existing_keys) if existing_keys else len(incoming_keys)
                duplicated = len(incoming_keys & existing_keys) if existing_keys else 0

                if not existing_df.empty:
                    if "상태" not in existing_df.columns:
                        existing_df["상태"] = ""

                    if "상태" in list_df.columns:
                        status_map = dict(zip(list_df["_pk"].astype(str), list_df["상태"].astype(str)))
                        new_status = existing_df["_pk"].astype(str).map(status_map)
                        mask = new_status.notna() & (new_status.astype(str).str.strip() != "")
                        existing_df.loc[mask, "상태"] = new_status[mask].astype(str)

                    new_rows_df = list_df[~list_df["_pk"].astype(str).isin(existing_keys)]
                    combined_df = pd.concat([existing_df, new_rows_df], ignore_index=True)
                else:
                    combined_df = list_df

                # "상태" 컬럼을 우측 끝으로 이동
                if "상태" in combined_df.columns:
                    combined_df = combined_df[[c for c in combined_df.columns if c != "상태"] + ["상태"]]

                _atomic_csv_write(dest_list, combined_df)
                result = {"inserted": inserted, "duplicated": duplicated, "total": len(list_df)}
                saved_files.append(str(dest_list))
                logger.info(f"영수증목록 저장: {dest_list.name} | 신규={result.get('inserted', 0)}")

            receipt_nos = {r["영수증번호"] for r in list_rows}
            item_rows = [r for r in all_item_records if r.get("영수증번호") in receipt_nos]
            if item_rows:
                item_df = pd.DataFrame(item_rows)
                pk_cols2 = [c for c in item_df.columns if c != "상태"]
                item_df["sale_date"]    = sale_date
                item_df["ym"]           = ym
                item_df["collected_at"] = now_str
                item_df["_pk"] = item_df[pk_cols2].apply(
                    lambda row: hashlib.md5("|".join(row.astype(str)).encode()).hexdigest(), axis=1,
                )
                dest_items = base / "unionpos_receipt_items.csv"
                # Upsert(상태 업데이트 포함): 기존 PK와 매칭되는 행은 '상태'만 갱신하고, 신규 PK만 append.
                existing_df2 = pd.DataFrame()
                if dest_items.exists() and dest_items.stat().st_size > 0:
                    try:
                        existing_df2 = pd.read_csv(dest_items, dtype=str, encoding="utf-8-sig")
                    except Exception:
                        existing_df2 = pd.read_csv(dest_items, dtype=str, encoding="utf-8")

                existing_keys2: set[str] = set()
                if not existing_df2.empty and "_pk" in existing_df2.columns:
                    existing_keys2 = set(existing_df2["_pk"].astype(str))

                incoming_keys2 = set(item_df["_pk"].astype(str))
                inserted2 = len(incoming_keys2 - existing_keys2) if existing_keys2 else len(incoming_keys2)
                duplicated2 = len(incoming_keys2 & existing_keys2) if existing_keys2 else 0

                if not existing_df2.empty:
                    if "상태" not in existing_df2.columns:
                        existing_df2["상태"] = ""

                    if "상태" in item_df.columns:
                        status_map2 = dict(zip(item_df["_pk"].astype(str), item_df["상태"].astype(str)))
                        new_status2 = existing_df2["_pk"].astype(str).map(status_map2)
                        mask2 = new_status2.notna() & (new_status2.astype(str).str.strip() != "")
                        existing_df2.loc[mask2, "상태"] = new_status2[mask2].astype(str)

                    new_rows_df2 = item_df[~item_df["_pk"].astype(str).isin(existing_keys2)]
                    combined_df2 = pd.concat([existing_df2, new_rows_df2], ignore_index=True)
                else:
                    combined_df2 = item_df

                # "상태" 컬럼을 우측 끝으로 이동
                if "상태" in combined_df2.columns:
                    combined_df2 = combined_df2[[c for c in combined_df2.columns if c != "상태"] + ["상태"]]

                _atomic_csv_write(dest_items, combined_df2)
                result2 = {"inserted": inserted2, "duplicated": duplicated2, "total": len(item_df)}
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


def verify_missing(**context) -> str:
    """수집 후 raw 파일 존재 여부로 누락 날짜 확인 (로그만, 재수집 없음)."""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    if not sale_dates:
        return "검증 스킵 (sale_dates 없음)"

    brand_dir = RAW_UNIONPOS_SALES / "brand=도리당"
    missing = []
    for date_str in sale_dates:
        ym = date_str[:7]
        found = False
        if brand_dir.exists():
            for store_dir in brand_dir.iterdir():
                csv_path = store_dir / f"ym={ym}" / "unionpos_receipt_list.csv"
                if not csv_path.exists():
                    continue
                try:
                    df = pd.read_csv(csv_path, dtype=str, usecols=["sale_date"])
                    if date_str in df["sale_date"].values:
                        found = True
                        break
                except Exception:
                    pass
        if not found:
            missing.append(date_str)

    if missing:
        logger.warning("UnionPOS 수집 후 누락 날짜 감지: %s", missing)
        return f"누락 {len(missing)}건: {missing}"
    return f"누락 없음: {len(sale_dates)}일 정상 수집"
