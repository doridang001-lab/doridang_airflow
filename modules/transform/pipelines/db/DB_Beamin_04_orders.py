"""배민 주문내역 수집 파이프라인.

수집 흐름:
  account_id/password → 매장별 독립 Chrome 세션
    → orders/history 이동
    → 가게 필터 선택 (store_id) → 날짜 필터 어제 설정
    → 배달완료 주문만 수집
    → 전 페이지 수집 (행별 개별 expand/extract/collapse → 페이지네이션)
    → CSV 저장 (upsert by 주문번호)
    → Chrome quit (팝업 메모리 해제)

저장 경로:
  analytics/baemin_macro/orders/
    brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM}.csv
"""

import logging
import os
import random
import re
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    clean_chrome_profile,
    human_click,
    launch_browser,
    login_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_ORDERS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    record_manual_reingest_marker,
)
from modules.transform.pipelines.db.beamin_store_io import (
    order_date,
    order_ym,
    read_table,
    replace_covered_date_range,
    write_table,
)

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

ORDERS_URL = "https://self.baemin.com/orders/history"

_CRASH_KEYWORDS = (
    "Remote end closed", "Connection aborted", "RemoteDisconnected",
    "Connection refused", "Max retries exceeded", "NewConnectionError",
    "invalid session id", "chrome not reachable", "disconnected",
    "tab crashed", "session deleted", "Timed out receiving message from renderer",
)

def _is_crash(exc: Exception) -> bool:
    s = str(exc)
    return any(k in s for k in _CRASH_KEYWORDS)


class OrdersCollectionInterrupted(RuntimeError):
    """Chrome died mid-collection; partial rows must not be saved as complete."""
_TABLE_ROW_CSS = "tr.Table_b_r4ax_1dwbr4on[data-index]"
_MAX_PAGES = 50
_PAGE_TRANSITION_TIMEOUT = 25
_MAX_VALIDATION_RETRY = 2
_MIN_SETTLE_RATE = float(os.getenv("BAEMIN_MIN_SETTLE_RATE", "0.9"))
_ORDER_COLLECTION_ATTEMPTS = 2
_ORDERS_PAGE_LOAD_TIMEOUT_SEC = 75
_ORDERS_SHELL_WAIT_SEC = 30
_ORDERS_TABLE_WAIT_SEC = 30
_ORDERS_NO_DATA_COLUMNS = [
    "target_date",
    "brand",
    "store",
    "store_id",
    "status",
    "reason",
    "collected_at",
]


def _orders_no_data_marker_path(brand: str, store: str, target_date: str) -> Path:
    ym = target_date[:7]
    return (
        BAEMIN_ORDERS_DB
        / "_no_data"
        / f"brand={brand}"
        / f"store={store}"
        / f"ym={ym}"
        / "orders_no_data.csv"
    )


def _orders_no_data_marker_paths(brand: str, store: str, target_date: str) -> list[Path]:
    csv_path = _orders_no_data_marker_path(brand, store, target_date)
    return [
        csv_path,
        csv_path.with_suffix(".parquet"),
        csv_path.with_suffix(".pq"),
    ]


def _read_orders_no_data_marker(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=_ORDERS_NO_DATA_COLUMNS)
    try:
        if path.suffix.lower() in {".parquet", ".pq"}:
            return pd.read_parquet(path).astype(str)
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as exc:
        logger.warning("orders no_data marker read failed: %s / %s", path, exc)
        return pd.DataFrame(columns=_ORDERS_NO_DATA_COLUMNS)


def _record_orders_no_data_marker(
    brand: str,
    store: str,
    store_id: str,
    target_date: str,
    *,
    reason: str = "total_summary_zero",
) -> Path:
    path = _orders_no_data_marker_path(brand, store, target_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = _read_orders_no_data_marker(path)
    row = {
        "target_date": target_date,
        "brand": brand,
        "store": store,
        "store_id": store_id,
        "status": "no_data",
        "reason": reason,
        "collected_at": pendulum.now(KST).isoformat(),
    }
    if "target_date" in df.columns:
        df = df[df["target_date"].astype(str) != target_date]
    else:
        df = pd.DataFrame(columns=_ORDERS_NO_DATA_COLUMNS)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df = df.reindex(columns=_ORDERS_NO_DATA_COLUMNS)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("orders no_data marker saved: brand=%s store=%s date=%s -> %s", brand, store, target_date, path)
    return path


def has_orders_no_data_marker(brand: str, store: str, target_date: str) -> bool:
    frames = [
        df
        for path in _orders_no_data_marker_paths(brand, store, target_date)
        if not (df := _read_orders_no_data_marker(path)).empty
    ]
    if not frames:
        return False
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "target_date" not in df.columns:
        return False
    rows = df[df["target_date"].astype(str) == target_date]
    if rows.empty:
        return False
    if "status" not in rows.columns:
        return True
    return rows["status"].astype(str).str.lower().eq("no_data").any()


SUSPECT_ZERO_REASON = "suspect_zero"
# 직전 영업일 데이터가 이 일수 이상 있으면 "그날만 0건"을 정상 빈값으로 믿지 않는다.
_SUSPECT_ZERO_MIN_HISTORY_DAYS = 3


def _orders_no_data_marker_rows(brand: str, store: str, target_date: str) -> pd.DataFrame:
    frames = [
        df
        for path in _orders_no_data_marker_paths(brand, store, target_date)
        if not (df := _read_orders_no_data_marker(path)).empty
    ]
    if not frames:
        return pd.DataFrame(columns=_ORDERS_NO_DATA_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "target_date" not in df.columns:
        return pd.DataFrame(columns=_ORDERS_NO_DATA_COLUMNS)
    return df[df["target_date"].astype(str) == target_date]


def orders_no_data_marker_reason(brand: str, store: str, target_date: str) -> str | None:
    """해당 일자 no_data 마커의 reason. 마커가 없으면 None."""
    rows = _orders_no_data_marker_rows(brand, store, target_date)
    if rows.empty or "reason" not in rows.columns:
        return None
    reasons = [str(value).strip() for value in rows["reason"].tolist() if str(value).strip()]
    if not reasons:
        return None
    if SUSPECT_ZERO_REASON in reasons:
        return SUSPECT_ZERO_REASON
    return reasons[-1]


def _orders_history_days(brand: str, store: str, target_date: str) -> int:
    """target_date 이전에 배달완료 데이터가 존재하는 영업일 수 (당월 + 전월)."""
    stems = []
    for ym in {target_date[:7], pendulum.parse(target_date).subtract(months=1).format("YYYY-MM")}:
        stems.append(BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}")

    days: set[str] = set()
    for stem in stems:
        try:
            df = read_table(stem)
        except Exception as exc:
            logger.warning("orders 이력 조회 실패: %s / %s", stem, exc)
            continue
        if df is None or df.empty or "주문시각" not in df.columns:
            continue
        if "주문상태" in df.columns:
            df = df[df["주문상태"].astype(str) == "배달완료"]
        if df.empty:
            continue
        days.update(
            date for date in order_date(df["주문시각"]).tolist() if date and date < target_date
        )
    return len(days)


def has_recent_orders_history(brand: str, store: str, target_date: str) -> bool:
    """이 매장이 target_date 직전에 꾸준히 주문이 있었는지.

    True인데 당일 0건이면 TotalSummary 빈값 오판일 가능성이 높다.
    """
    return _orders_history_days(brand, store, target_date) >= _SUSPECT_ZERO_MIN_HISTORY_DAYS


def _handle_zero_orders(brand: str, store: str, store_id: str, target_date: str) -> bool:
    """0건 조회 결과를 정상 빈값으로 받아들일지 판정한다.

    Returns:
        True  - 정상 빈값 (마커 기록 완료, 성공 처리 가능)
        False - 빈값 오판 의심 (suspect_zero 마커 기록, 실패로 올려 재시도)
    """
    if has_recent_orders_history(brand, store, target_date):
        logger.warning(
            "직전 영업일 데이터가 있는데 0건 조회, 빈값 오판 의심: brand=%s store=%s date=%s",
            brand,
            store,
            target_date,
        )
        _record_orders_no_data_marker(
            brand, store, store_id, target_date, reason=SUSPECT_ZERO_REASON
        )
        return False
    logger.info("정상 주문 없음: %s / %s", store, target_date)
    _record_orders_no_data_marker(brand, store, store_id, target_date)
    return True


def _short_error(exc: Exception) -> str:
    text = str(exc).splitlines()[0].strip()
    return text[:240]


def _wait_for_orders_table(driver, timeout: int = _ORDERS_TABLE_WAIT_SEC) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, _TABLE_ROW_CSS))
        )
        return True
    except TimeoutException:
        return False


def _wait_for_orders_page_shell(driver, timeout: int = _ORDERS_SHELL_WAIT_SEC) -> bool:
    """주문 row가 없어도 필터 조작이 가능한 orders 페이지 shell이면 준비 완료로 본다."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script(
                """
                const href = location.href || '';
                const filterBtns = [...document.querySelectorAll('button')]
                    .filter(b => (b.className || '').includes('Filter'));
                const hasStoreFilter = filterBtns.some(b =>
                    b.querySelector('.Badge_b_r4ax_19agxiso')
                );
                const hasDateFilter = filterBtns.some(b =>
                    !b.querySelector('.Badge_b_r4ax_19agxiso') &&
                    (
                        (b.textContent || '').includes('날짜') ||
                        /\\d{4}\\.\\s*\\d{2}\\.\\s*\\d{2}/.test(b.textContent || '')
                    )
                );
                const hasStoreSelect = [...document.querySelectorAll('select')]
                    .some(sel => sel.options && sel.options.length > 0);
                const hasTable = !!document.querySelector('table');
                return href.includes('/orders/history') &&
                    (hasStoreFilter || hasStoreSelect) &&
                    (hasDateFilter || hasTable);
                """
            )
        )
        return True
    except TimeoutException:
        return False


def _orders_shell_debug_state(driver) -> dict:
    try:
        return driver.execute_script(
            """
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => (b.className || '').includes('Filter'));
            return {
                url: location.href || '',
                title: document.title || '',
                filter_buttons: filterBtns.length,
                store_filter_buttons: filterBtns.filter(b =>
                    b.querySelector('.Badge_b_r4ax_19agxiso')
                ).length,
                selects: document.querySelectorAll('select').length,
                tables: document.querySelectorAll('table').length,
                body_text: (document.body?.innerText || '').slice(0, 160),
            };
            """
        ) or {}
    except Exception as exc:
        return {"error": _short_error(exc)}


def _open_orders_history(driver) -> None:
    driver.set_page_load_timeout(_ORDERS_PAGE_LOAD_TIMEOUT_SEC)
    try:
        driver.get(ORDERS_URL)
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# TotalSummary 읽기 / 검증 / 재수집
# ---------------------------------------------------------------------------

def _read_total_summary(driver) -> dict | None:
    """현재 필터 조건의 TotalSummary(건수, 금액)를 읽는다.

    필터가 완전히 적용된 후 호출해야 정확한 값을 반환한다.
    DOM: <span class="TotalSummary-module__SysK"><b ...>56</b>건</span>
    """
    return driver.execute_script(
        """
        const spans = document.querySelectorAll('span.TotalSummary-module__SysK');
        let count = null, amount = null;
        for (const span of spans) {
            const b = span.querySelector('b.TotalSummary-module__jikm');
            if (!b) continue;
            const val = parseInt(b.textContent.replace(/,/g, ''));
            const fullText = span.textContent.trim();
            if (fullText.endsWith('건')) count = val;
            else if (fullText.endsWith('원')) amount = val;
        }
        return (count !== null) ? {count, amount} : null;
        """
    )


def _validate_collected(rows: list[dict], expected: dict | None) -> dict:
    """수집 rows와 TotalSummary 기댓값을 비교한다.

    Returns:
        matched: True/False/None(TotalSummary 없음)
        actual_count: unique 주문번호 수
        actual_amount: 결제금액 합계 (isFirst row에만 값 있음)
    """
    actual_count = len({r["주문번호"] for r in rows if r.get("주문번호")})

    def _filled(value: object) -> bool:
        text = str(value if value is not None else "").strip()
        return text not in {"", "nan", "NaN", "None", "<NA>", "null", "NULL"}

    payment_order_ids = {
        str(r.get("주문번호") or "").strip()
        for r in rows
        if str(r.get("주문번호") or "").strip() and _filled(r.get("결제금액", ""))
    }
    settled_order_ids = {
        str(r.get("주문번호") or "").strip()
        for r in rows
        if str(r.get("주문번호") or "").strip() and _filled(r.get("입금예정금액", ""))
    }
    settle_denominator = len(payment_order_ids)
    settle_count = len(settled_order_ids & payment_order_ids)
    settle_rate = (
        settle_count / settle_denominator
        if settle_denominator
        else None
    )

    def _sum_amount(column: str) -> int:
        total = 0
        for r in rows:
            raw = r.get(column, "")
            if raw and str(raw).strip():
                try:
                    total += int(str(raw).replace(",", "").strip())
                except (ValueError, TypeError):
                    pass
        return total

    amount_candidates = {
        "결제금액": _sum_amount("결제금액"),
        "총결제금액": _sum_amount("총결제금액"),
        "상품금액": _sum_amount("상품금액"),
    }
    actual_amount = amount_candidates["결제금액"]
    amount_source = "결제금액"

    if expected is None:
        return {
            "matched": None,
            "actual_count": actual_count, "actual_amount": actual_amount,
            "expected_count": None, "expected_amount": None,
            "amount_source": amount_source,
            "amount_candidates": amount_candidates,
            "settle_rate": settle_rate,
            "settle_count": settle_count,
            "settle_denominator": settle_denominator,
        }
    expected_amount = expected.get("amount") or 0
    for source, amount in amount_candidates.items():
        if amount == expected_amount:
            actual_amount = amount
            amount_source = source
            break
    matched = (
        actual_count == expected["count"]
        and actual_amount == expected_amount
    )
    return {
        "matched": matched,
        "actual_count": actual_count, "actual_amount": actual_amount,
        "expected_count": expected["count"], "expected_amount": expected.get("amount"),
        "amount_source": amount_source,
        "amount_candidates": amount_candidates,
        "settle_rate": settle_rate,
        "settle_count": settle_count,
        "settle_denominator": settle_denominator,
    }


def _has_low_settle_rate(vr: dict) -> bool:
    settle_rate = vr.get("settle_rate")
    return (
        vr.get("matched") is True
        and settle_rate is not None
        and float(settle_rate) < _MIN_SETTLE_RATE
    )


def _block_low_settle_rate(vr: dict) -> dict:
    """정산정보 수집률이 낮으면 정상 저장하지 않고 재수집 대상으로 남긴다."""
    if not _has_low_settle_rate(vr):
        return vr
    vr["settlement_suspect"] = True
    vr["reason"] = "low_settle_rate"
    vr["matched"] = False
    vr["save_partial"] = False
    return vr


def _filter_rows_to_target_date(rows: list[dict], target_date: str | None, store: str) -> tuple[list[dict], bool]:
    if not rows or not target_date:
        return rows, False
    row_dates = order_date(pd.Series([row.get("주문시각", "") for row in rows]))
    valid_dates = sorted({value for value in row_dates.tolist() if value})
    if not valid_dates:
        return rows, False
    if valid_dates == [target_date]:
        return rows, False
    filtered = [
        row
        for row, row_date in zip(rows, row_dates.tolist(), strict=False)
        if row_date == target_date
    ]
    logger.warning(
        "주문 날짜 범위 혼입 감지, target_date 행만 저장: %s / target=%s / dates=%s / rows %d→%d",
        store,
        target_date,
        valid_dates,
        len(rows),
        len(filtered),
    )
    return filtered, True


def _date_filter_leakage_is_saveable(rows: list[dict], target_date: str | None) -> bool:
    if not rows or not target_date:
        return False
    row_dates = order_date(pd.Series([row.get("주문시각", "") for row in rows]))
    valid_dates = [value for value in row_dates.tolist() if value]
    if not valid_dates or target_date not in valid_dates:
        return False
    # 날짜 혼입 데이터는 TotalSummary 기준일을 신뢰할 수 없어 저장하지 않고 retry로 넘긴다.
    return False


def _date_filtered_validation(
    *,
    rows: list[dict],
    expected: dict | None,
    status_label: str,
    store: str,
    attempt: int,
    reason: str = "date_filter",
    save_partial: bool = False,
) -> dict:
    checked = _validate_collected(rows, None)
    return {
        "matched": False,
        "status": status_label,
        "store": store,
        "retried": attempt,
        "actual_count": checked.get("actual_count", len({r["주문번호"] for r in rows if r.get("주문번호")})),
        "actual_amount": checked.get("actual_amount", 0),
        "expected_count": expected.get("count") if expected else None,
        "expected_amount": expected.get("amount") if expected else None,
        "amount_source": "date_filter",
        "amount_candidates": checked.get("amount_candidates", {}),
        "reason": reason,
        "save_partial": save_partial,
    }


def _visible_order_date_state(driver, target_date: str) -> dict:
    order_times = driver.execute_script(
        """
        return [...document.querySelectorAll(arguments[0])]
            .map(row => (row.querySelector('td[data-td-index="2"]')?.textContent || '').replace(/\\s+/g, ' ').trim())
            .filter(Boolean);
        """,
        _TABLE_ROW_CSS,
    ) or []
    parsed_dates = order_date(pd.Series(order_times))
    valid_dates = sorted({value for value in parsed_dates.tolist() if value})
    summary = _read_total_summary(driver)
    return {
        "row_count": len(order_times),
        "dates": valid_dates,
        "matched": bool(valid_dates) and valid_dates == [target_date],
        "summary": summary,
    }


def _confirm_visible_order_date(driver, target_date: str, label: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    last_state: dict = {}
    empty_seen_at: float | None = None
    while time.time() < deadline:
        last_state = _visible_order_date_state(driver, target_date)
        if last_state["matched"]:
            return True
        if target_date in (last_state.get("dates") or []):
            logger.warning(
                "날짜 '%s' 필터에 다른 날짜가 섞여 실패 처리: target=%s state=%s",
                label,
                target_date,
                last_state,
            )
            empty_seen_at = None
        if last_state["row_count"] == 0:
            summary = last_state.get("summary")
            summary_count = (summary or {}).get("count") if isinstance(summary, dict) else None
            if summary_count in (0, None):
                now = time.time()
                if empty_seen_at is None:
                    empty_seen_at = now
                elif now - empty_seen_at >= 3.0:
                    logger.info(
                        "날짜 '%s' 필터 적용 후 표시 주문 없음으로 계속 진행: target=%s state=%s",
                        label,
                        target_date,
                        last_state,
                    )
                    return True
            else:
                empty_seen_at = None
        time.sleep(0.5)

    logger.warning(
        "날짜 '%s' 필터 실측 검증 실패: target=%s state=%s popup=%s",
        label,
        target_date,
        last_state,
        _date_popup_debug_state(driver),
    )
    return False


def _csv_already_covers(
    brand: str, store: str, target_date: str, status_label: str, summary: dict
) -> bool:
    """기존 CSV의 (날짜 + 주문상태) 합계가 TotalSummary와 일치하면 True."""
    try:
        ym = target_date[:7]
        stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}"
        df = read_table(stem)
        if df is None:
            return False
        df = df[order_date(df["주문시각"]).eq(target_date)]
        df = df[df["주문상태"].astype(str) == status_label]
        count = df["주문번호"].nunique()
        if count != summary.get("count", -1):
            return False
        if "입금예정금액" in df.columns and "결제금액" in df.columns:
            filled = (
                df["입금예정금액"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace({"nan": "", "NaN": "", "None": "", "<NA>": "", "null": "", "NULL": ""})
                .ne("")
            )
            payment_ids = set(
                df.loc[
                    df["결제금액"].fillna("").astype(str).str.strip().ne(""),
                    "주문번호",
                ].fillna("").astype(str).str.strip()
            )
            payment_ids.discard("")
            settled_ids = set(df.loc[filled, "주문번호"].fillna("").astype(str).str.strip())
            settled_ids.discard("")
            settle_rate = len(payment_ids & settled_ids) / len(payment_ids) if payment_ids else None
            if settle_rate is not None and settle_rate < _MIN_SETTLE_RATE:
                logger.warning(
                    "기존 CSV 합계는 일치하나 정산정보 수집률 낮음, 재수집 진행: %s/%s/%s rate=%.1f%% (%d/%d)",
                    brand,
                    store,
                    target_date,
                    settle_rate * 100,
                    len(payment_ids & settled_ids),
                    len(payment_ids),
                )
                return False
        expected_amount = summary.get("amount") or 0
        if expected_amount == 0:
            return count == summary.get("count", -1)
        for col in ("결제금액", "총결제금액", "상품금액"):
            if col not in df.columns:
                continue
            vals = df[col].astype(str)
            vals = vals[~vals.isin(["", "nan", "None", "NaN"])]
            total = vals.str.replace(",", "", regex=False).apply(
                lambda x: int(x) if x.strip().lstrip("-").isdigit() else 0
            ).sum()
            if total == expected_amount:
                return True
        return False
    except Exception:
        return False


def _collect_with_retry_on_mismatch(
    driver,
    store_info: dict,
    status_label: str,
    filter_fn,
    max_retry: int = _MAX_VALIDATION_RETRY,
    target_date: str | None = None,
) -> tuple[list[dict], dict]:
    """TotalSummary 검증 포함 수집. 불일치 시 필터 재적용 후 재수집.

    filter_fn(driver) → bool: 페이지 재이동 후 필터를 재적용하는 callable.

    Returns:
        (rows, validation_result)
        validation_result 포함 키: status, matched, actual_count, actual_amount,
                                    expected_count, expected_amount, retried
    """
    store = store_info.get("store", "?")
    rows: list[dict] = []
    vr: dict = {}

    for attempt in range(max_retry + 1):
        expected = None
        try:
            expected = _read_total_summary(driver)

            if attempt == 0 and expected and target_date:
                if _csv_already_covers(
                    store_info.get("brand", ""), store_info.get("store", "?"),
                    target_date, status_label, expected,
                ):
                    logger.info(
                        "기존 CSV 합계 일치, 추출 생략: %s / %s / %s",
                        store_info.get("store"), target_date, status_label,
                    )
                    vr = {
                        "matched": True, "status": status_label,
                        "store": store_info.get("store", "?"), "retried": 0,
                        "actual_count": expected["count"],
                        "actual_amount": expected.get("amount", 0),
                        "expected_count": expected["count"],
                        "expected_amount": expected.get("amount"),
                        "amount_source": "csv_skip", "amount_candidates": {},
                    }
                    return [], vr

            if target_date:
                _go_to_first_page(driver, store)
            collected_rows = _collect_all_pages(driver, store_info)
            rows, date_filtered = _filter_rows_to_target_date(collected_rows, target_date, store)
            if date_filtered:
                if rows:
                    vr = _validate_collected(rows, None)
                    vr.update(
                        {
                            "matched": True,
                            "status": status_label,
                            "store": store,
                            "retried": attempt,
                            "expected_count": None,
                            "expected_amount": None,
                            "amount_source": "date_filtered_rows",
                            "reason": "date_filtered_rows",
                        }
                    )
                    logger.warning(
                        "날짜 혼입 범위에서 target_date 행만 저장 대상으로 확정: %s / %s / %d행",
                        store,
                        target_date,
                        len(rows),
                    )
                    break
                vr = _date_filtered_validation(
                    rows=rows,
                    expected=expected,
                    status_label=status_label,
                    store=store,
                    attempt=attempt,
                )
                if attempt > 0:
                    logger.warning("날짜 혼입 재발로 조기 종료: %s / %s", store, target_date)
                    break
                logger.warning("날짜 혼입으로 TotalSummary 검증 생략 후 필터 재적용: %s", store)
                try:
                    try:
                        driver.get(ORDERS_URL)
                    except TimeoutException as exc:
                        logger.info("날짜 혼입 재필터 페이지 로드 지연, DOM 대기로 계속: %s / %s", store, _short_error(exc))
                        try:
                            driver.execute_script("window.stop();")
                        except Exception:
                            pass
                    if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                        logger.warning("날짜 혼입 재필터 페이지 로드 실패, 중단: %s", store)
                        break
                    if not filter_fn(driver):
                        logger.warning("날짜 혼입 재필터 적용 실패, 중단: %s", store)
                        break
                except Exception as e:
                    if _is_crash(e):
                        raise
                    logger.warning("날짜 혼입 재필터 준비 실패, 중단: %s / %s", store, e)
                    break
                time.sleep(2.0)
                continue
        except Exception as e:
            if _is_crash(e):
                raise
            raise
        vr = _validate_collected(rows, expected)
        vr["status"] = status_label
        vr["store"] = store
        vr["retried"] = attempt
        settle_rate = vr.get("settle_rate")
        low_settle_rate = _has_low_settle_rate(vr)
        if low_settle_rate:
            vr["settlement_suspect"] = True
            vr["reason"] = "low_settle_rate"

        if vr["matched"] is None and not rows:
            logger.warning(
                "TotalSummary 미확인 + 빈 rows, 성공 처리하지 않음: %s / %s (재시도 %d/%d)",
                store,
                status_label,
                attempt + 1,
                max_retry,
            )
            vr["matched"] = False
        elif vr["matched"] is not False and not low_settle_rate:
            if vr["matched"] is True:
                logger.info(
                    "합계 일치 [%s][%s]: %d건/%s원 source=%s settle_rate=%s",
                    store, status_label, vr["actual_count"], vr["actual_amount"],
                    vr.get("amount_source", ""),
                    None if settle_rate is None else f"{settle_rate:.1%}",
                )
            break

        if low_settle_rate:
            logger.warning(
                "정산정보 수집률 낮음 [%s][%s] rate=%.1f%% 정산=%s/%s 합계검증=일치 (재시도 %d/%d)",
                store,
                status_label,
                settle_rate * 100,
                vr.get("settle_count"),
                vr.get("settle_denominator"),
                attempt + 1,
                max_retry,
            )
        else:
            logger.warning(
                "합계 불일치 [%s][%s] 수집=%d건/%d원 기대=%s건/%s원 source=%s candidates=%s (재시도 %d/%d)",
                store, status_label,
                vr["actual_count"], vr["actual_amount"],
                vr["expected_count"], vr["expected_amount"],
                vr.get("amount_source", ""), vr.get("amount_candidates", {}),
                attempt + 1, max_retry,
            )

        if low_settle_rate and vr.get("matched") is True and attempt >= 1:
            logger.warning(
                "정산정보 수집률 낮음 재시도 중단, 재수집 대상으로 보존: %s / %s rate=%s",
                store,
                status_label,
                None if settle_rate is None else f"{settle_rate:.1%}",
            )
            break

        if attempt >= max_retry:
            break

        try:
            try:
                driver.get(ORDERS_URL)
            except TimeoutException as exc:
                logger.info("재수집 페이지 로드 지연, DOM 대기로 계속: %s / %s", store, _short_error(exc))
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass
            if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                logger.warning("재수집 페이지 로드 실패, 중단: %s", store)
                break
            if not filter_fn(driver):
                logger.warning("재수집 필터 적용 실패, 중단: %s", store)
                break
        except Exception as e:
            if _is_crash(e):
                raise
            logger.warning("재수집 준비 실패, 중단: %s / %s", store, e)
            break
        time.sleep(2.0)

    _block_low_settle_rate(vr)
    return rows, vr


# ---------------------------------------------------------------------------
# 공개 함수
# ---------------------------------------------------------------------------

def collect_orders_for_driver(
    driver,
    store_info: dict,
    target_date: str | None = None,
) -> dict:
    """기존 Chrome 세션으로 단일 매장의 주문내역을 수집한다.

    combined.py의 per-store 루프에서 로그인 없이 호출.
    TotalSummary 검증 포함: 불일치 시 최대 _MAX_VALIDATION_RETRY 회 재수집.

    Returns:
        {"ok": bool, "validation": list[dict]}
        validation 항목: status, store, matched, actual_count, actual_amount,
                         expected_count, expected_amount, retried
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand = store_info["brand"]
    store = store_info["store"]
    validation: list[dict] = []
    ok = True

    logger.info("주문내역 수집 시작: %s (%s) / %s", store, store_id, target_date)

    try:
        page_ready = False
        for page_attempt in range(2):
            try:
                _open_orders_history(driver)
            except Exception as exc:
                raise OrdersCollectionInterrupted(
                    f"orders navigation failed before table wait: {store} / {_short_error(exc)}"
                ) from exc

            if _wait_for_orders_page_shell(driver):
                page_ready = True
                break
            logger.info("주문내역 페이지 shell 미확인(%d/2): %s", page_attempt + 1, store)
            time.sleep(2.0)

        if not page_ready:
            raise OrdersCollectionInterrupted(
                f"Timed out receiving message from renderer: orders page shell not ready after fast waits: {store}"
            )

        if not _select_order_store(driver, store_id, store):
            logger.warning("가게 필터 선택 실패, 건너뜀: %s", store)
            return {"ok": False, "reason": "store_filter", "validation": validation}

        prev_sig = _page_signature(driver)
        prev_summary = _read_total_summary(driver)
        date_filter_uncertain = False
        if not _set_date(driver, target_date):
            logger.warning("날짜 설정 실패, 수집 중단: %s / %s", store, target_date)
            return {"ok": False, "reason": "date_filter", "validation": validation}

        if not _wait_for_filter_settle(driver, prev_sig, prev_summary):
            if not _confirm_visible_order_date(driver, target_date, "적용후", timeout=4.5):
                logger.warning("날짜 필터 적용 후 테이블 정착 실패, 수집 중단: %s / %s", store, target_date)
                return {"ok": False, "reason": "date_filter", "validation": validation}
            else:
                logger.info("날짜 필터 signature 변화 없음, 주문시각 실측으로 계속 진행: %s", store)

        # 배달완료 수집 + TotalSummary 검증
        def _filter_normal(d):
            return _select_order_store(d, store_id, store) and _set_date(d, target_date)

        rows, vr_normal = _collect_with_retry_on_mismatch(
            driver, store_info, "배달완료", _filter_normal,
            target_date=target_date,
        )
        validation.append(vr_normal)
        if date_filter_uncertain and vr_normal.get("reason") in (None, "matched"):
            vr_normal["reason"] = "date_filter_uncertain"

        if vr_normal.get("matched") is False:
            if vr_normal.get("save_partial") and rows:
                saved = _save_orders_csv(rows, brand, store, target_date)
                logger.warning(
                    "날짜 필터 혼입 부분 저장 후 실패 유지: brand=%s store=%s -> %s (%d행)",
                    brand,
                    store,
                    saved,
                    len(rows),
                )
            logger.warning("검증 불일치로 정상 주문 저장 생략: %s / %s", store, target_date)
            ok = False
            failure_reason = str(vr_normal.get("reason") or "validation_mismatch")
        elif rows:
            saved = _save_orders_csv(rows, brand, store, target_date)
            logger.info("저장 완료(정상): brand=%s store=%s → %s (%d행)", brand, store, saved, len(rows))
        elif not _handle_zero_orders(brand, store, store_id, target_date):
            ok = False
            failure_reason = SUSPECT_ZERO_REASON

        result = {"ok": ok, "validation": validation}
        if not ok:
            result["reason"] = failure_reason
        return result

    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("주문내역 수집 실패 (%s): %s", store, e)
        return {"ok": False, "reason": "exception", "validation": validation}


def collect_orders_for_account(
    account_id: str,
    password: str,
    store_list: list[dict],
    target_date: str | None = None,
) -> dict:
    """매장별 독립 Chrome 세션으로 주문내역을 수집한다.

    팝업 클릭(즉시할인 파트너부담/배민지원) 후 Chrome을 종료해 메모리를 완전 해제.
    매장마다 새 Chrome 세션 → login → collect → quit.

    Returns:
        {"failed": list[dict], "validation": list[dict]}
        failed: 수집 실패한 store_info 리스트
        validation: 매장별 TotalSummary 검증 결과
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed_stores: list[dict] = []
    validation: list[dict] = []

    for store_info in store_list:
        store_id = store_info["store_id"]
        brand = store_info["brand"]
        store = store_info["store"]
        failure_reason = "exception"

        logger.info("주문내역 수집 시작: %s (%s) / %s", store, store_id, target_date)

        succeeded = False
        for attempt in range(_ORDER_COLLECTION_ATTEMPTS):
            driver = None
            try:
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, password):
                    logger.warning("로그인 실패: %s / %s", account_id, store)
                    failure_reason = "login"
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                try:
                    _open_orders_history(driver)
                except Exception as exc:
                    logger.warning("주문내역 페이지 이동 실패, 건너뜀: %s / %s", store, _short_error(exc))
                    failure_reason = "navigation"
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                if not _wait_for_orders_page_shell(driver):
                    logger.warning(
                        "주문내역 페이지 shell 로드 실패, 건너뜀: %s state=%s",
                        store,
                        _orders_shell_debug_state(driver),
                    )
                    failure_reason = "page_shell"
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                if not _select_order_store(driver, store_id, store):
                    logger.warning("가게 필터 선택 실패, 건너뜀: %s", store)
                    failure_reason = "store_filter"
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                prev_sig = _page_signature(driver)
                prev_summary = _read_total_summary(driver)
                date_filter_uncertain = False
                if not _set_date(driver, target_date):
                    logger.warning(
                        "날짜 설정 실패, 수집 중단: %s / %s",
                        store,
                        target_date,
                    )
                    failure_reason = "date_filter"
                    break

                if not _wait_for_filter_settle(driver, prev_sig, prev_summary):
                    if not _confirm_visible_order_date(driver, target_date, "적용후", timeout=4.5):
                        logger.warning(
                            "날짜 필터 적용 후 테이블 정착 실패, 수집 중단: %s / %s",
                            store,
                            target_date,
                        )
                        failure_reason = "date_filter"
                        break
                    else:
                        logger.info("날짜 필터 signature 변화 없음, 주문시각 실측으로 계속 진행: %s", store)

                # 배달완료 수집 + TotalSummary 검증
                _sid, _sn = store_id, store  # closure 캡처용
                _td = target_date  # closure 캡처용

                def _filter_normal(d):
                    return _select_order_store(d, _sid, _sn) and _set_date(d, _td)

                rows, vr_normal = _collect_with_retry_on_mismatch(
                    driver, store_info, "배달완료", _filter_normal, target_date=target_date,
                )
                validation.append(vr_normal)
                if date_filter_uncertain and vr_normal.get("reason") in (None, "matched"):
                    vr_normal["reason"] = "date_filter_uncertain"

                if vr_normal.get("matched") is False:
                    if vr_normal.get("save_partial") and rows:
                        saved = _save_orders_csv(rows, brand, store, target_date)
                        logger.warning(
                            "날짜 필터 혼입 부분 저장 후 실패 유지: brand=%s store=%s -> %s (%d행)",
                            brand,
                            store,
                            saved,
                            len(rows),
                        )
                        failure_reason = str(vr_normal.get("reason") or "validation_mismatch")
                        break
                    logger.warning("검증 불일치로 정상 주문 저장 생략: %s / %s", store, target_date)
                    failure_reason = str(vr_normal.get("reason") or "validation_mismatch")
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        validation.pop()
                        continue
                    break
                elif rows:
                    saved = _save_orders_csv(rows, brand, store, target_date)
                    logger.info(
                        "저장 완료(정상): brand=%s store=%s → %s (%d행)", brand, store, saved, len(rows)
                    )
                elif not _handle_zero_orders(brand, store, store_id, target_date):
                    failure_reason = SUSPECT_ZERO_REASON
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                succeeded = True
                break

            except Exception as e:
                logger.warning(
                    "매장 수집 실패 (%s) attempt=%d/%d: %s",
                    store, attempt + 1, _ORDER_COLLECTION_ATTEMPTS, e,
                )
                if _is_crash(e) and attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                    try:
                        clean_chrome_profile(account_id)
                    except Exception as clean_exc:
                        logger.warning("Chrome 프로필 정리 실패(%s): %s", account_id, clean_exc)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                time.sleep(random.uniform(3.0, 5.0))

        if not succeeded:
            failed_stores.append({**store_info, "reason": failure_reason})

        time.sleep(random.uniform(1.5, 3.0))

    return {"failed": failed_stores, "validation": validation}


# ---------------------------------------------------------------------------
# 가게 필터 선택
# ---------------------------------------------------------------------------

def _select_order_store(driver, store_id: str, store_name: str) -> bool:
    """orders/history 가게 필터에서 매장을 선택하고 적용한다.

    XPath text() 매칭은 중첩 span 구조에서 실패하므로 JS badge 탐색으로 클릭.
    Atelier Select 컴포넌트라 Selenium Select() 대신 JS change 이벤트를 사용한다.
    """
    try:
        # 1. 가게 필터 버튼 클릭 — JS로 badge closest(button) 탐색 (XPath보다 안정적)
        badge_text_found = driver.execute_script(
            """
            // 가게 필터 버튼 = Filter 클래스 버튼 중 Badge 포함한 것
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => b.className.includes('Filter'));
            const storeFilterBtn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
            if (storeFilterBtn) {
                const badge = storeFilterBtn.querySelector('.Badge_b_r4ax_19agxiso');
                storeFilterBtn.click();
                return badge?.textContent.trim() || 'clicked';
            }
            // fallback: Badge 포함 버튼 직접 탐색
            const badges = document.querySelectorAll('.Badge_b_r4ax_19agxiso');
            for (const badge of badges) {
                const t = badge.textContent.trim();
                if (t.includes('가게') || t.includes('전체') || t.includes('음식')) {
                    const btn = badge.closest('button') || badge.closest('[role="button"]');
                    if (btn) { btn.click(); return t; }
                }
            }
            return false;
            """
        )
        if not badge_text_found:
            logger.warning("가게 필터 badge 미발견: %s (DOM에 .Badge_b_r4ax_19agxiso 없음)", store_name)
            return False
        logger.info("가게 필터 버튼 클릭: badge=%s", badge_text_found)
        # 2. select 요소 찾아서 JS로 값 설정 + React change 이벤트 dispatch
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script(
                """
                return [...document.querySelectorAll('select')]
                    .some(sel => [...sel.options].some(o => o.value === arguments[0]));
                """,
                store_id,
            )
        )
        select_result = driver.execute_script(
            """
            const target = arguments[0];
            const selects = [...document.querySelectorAll('select')];
            const sel = selects.find(s => [...s.options].some(o => o.value === target));
            if (!sel) {
                return {
                    ok: false,
                    options: selects.flatMap(s => [...s.options].map(o => `${o.value}:${o.textContent.trim()}`))
                };
            }
            sel.value = target;
            sel.dispatchEvent(new Event('change', {bubbles: true}));
            sel.dispatchEvent(new Event('input', {bubbles: true}));
            return {
                ok: sel.value === target,
                value: sel.value,
                text: sel.selectedOptions?.[0]?.textContent?.trim() || ''
            };
            """,
            store_id,
        )
        if not select_result or not select_result.get("ok"):
            logger.warning(
                "가게 select 값 설정 실패: %s store_id=%s result=%s",
                store_name, store_id, select_result,
            )
            return False
        logger.info(
            "가게 select 값 설정: %s store_id=%s option=%s",
            store_name, store_id, select_result.get("text", ""),
        )
        time.sleep(0.3)

        # 3. 적용 버튼 클릭 — JS fallback 포함
        apply_clicked = driver.execute_script(
            """
            const btns = document.querySelectorAll('button');
            for (const btn of btns) {
                if (btn.textContent.trim() === '적용' && !btn.disabled) {
                    btn.click(); return true;
                }
            }
            return false;
            """
        )
        if not apply_clicked:
            logger.warning("가게 필터 적용 버튼 미발견: %s", store_name)
            return False
        # badge가 가게 전체→매장명으로 바뀔 때까지 대기 (고정 2.0s 제거)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: "가게 전체" not in (d.execute_script(
                    """
                    const filterBtns = [...document.querySelectorAll('button')]
                        .filter(b => b.className.includes('Filter'));
                    const btn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
                    return btn?.querySelector('.Badge_b_r4ax_19agxiso')?.textContent.trim() || '';
                    """
                ) or "")
            )
        except TimeoutException:
            pass
        time.sleep(0.5)  # FilterContainer popup 닫힘 animation 완료 대기

        # 4. 검증: Badge 포함 Filter 버튼(가게 필터)의 badge 텍스트 확인
        current_badge = driver.execute_script(
            """
            // 가게 필터 버튼 = Badge 포함 Filter 버튼
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => b.className.includes('Filter'));
            const storeBtn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
            const badge = storeBtn?.querySelector('.Badge_b_r4ax_19agxiso');
            return badge ? badge.textContent.trim() : '';
            """
        ) or ""
        if "가게 전체" in current_badge:
            logger.warning("가게 선택 미반영 (still 전체): %s → store_id=%s", store_name, store_id)
            return False

        logger.info("가게 필터 선택 완료: %s → badge=%s", store_name, current_badge)
        return True

    except TimeoutException as e:
        logger.warning("가게 필터 선택 오류 (%s): %s", store_name, e)
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("가게 필터 선택 오류 (%s): %s", store_name, e)
        return False


# ---------------------------------------------------------------------------
# 날짜 필터
# ---------------------------------------------------------------------------

def _open_date_filter_popup(driver) -> str | None:
    """주문내역 날짜 필터 팝업을 연다."""
    clicked = driver.execute_script(
        """
        const filterBtns = [...document.querySelectorAll('button')]
            .filter(b => b.className.includes('Filter'));
        // 날짜 필터 = Badge 없는 Filter 버튼
        const dateFilterBtn = filterBtns.find(b => !b.querySelector('.Badge_b_r4ax_19agxiso'));
        if (dateFilterBtn) { dateFilterBtn.click(); return 'date_filter_btn'; }
        // fallback: p 또는 날짜 패턴 포함 버튼
        for (const btn of document.querySelectorAll('button')) {
            const t = btn.textContent;
            if (t.includes('날짜') || t.match(/\\d{4}\\.\\s*\\d{2}\\.\\s*\\d{2}/)) {
                if (!btn.querySelector('.Badge_b_r4ax_19agxiso')) { btn.click(); return 'text_btn'; }
            }
        }
        return false;
        """
    )
    if not clicked:
        logger.warning("날짜 필터 버튼 미발견 (filterBtns 없음)")
        return None
    logger.info("날짜 필터 버튼 클릭: %s", clicked)
    return str(clicked)


def _date_popup_debug_state(driver, html_limit: int = 2000) -> str:
    try:
        state = driver.execute_script(
            """
            const containers = [
                ...document.querySelectorAll('[role="dialog"], [class*="Popover"], [class*="Dropdown"], [class*="Date"], [data-atelier-component]')
            ].filter(el => {
                const text = el.textContent || '';
                return text.includes('오늘') || text.includes('어제') || text.includes('적용') || text.includes('기간');
            });
            const popup = containers[containers.length - 1] || document.body;
            return JSON.stringify({
                html: (popup.outerHTML || '').slice(0, arguments[0]),
                radios: [...document.querySelectorAll('input[type="radio"]')].map(r => ({
                    value: r.value || '',
                    checked: !!r.checked,
                    label: (r.closest('label')?.textContent || '').trim()
                })),
                labels: [...document.querySelectorAll('label')].map(l => (l.textContent || '').trim()).filter(Boolean).slice(0, 40),
                ariaButtons: [...document.querySelectorAll('button[aria-label]')].map(b => b.getAttribute('aria-label')).filter(Boolean).slice(0, 80),
                buttons: [...document.querySelectorAll('button')].map(b => (b.textContent || b.getAttribute('aria-label') || '').trim()).filter(Boolean).slice(0, 40)
            });
            """,
            html_limit,
        )
        return str(state)[: html_limit + 2000]
    except Exception as exc:
        return f"debug_state_error={_short_error(exc)}"


def _select_specific_date_mode(driver) -> bool:
    return bool(
        driver.execute_script(
            """
            const targetTexts = ['직접 설정', '직접선택', '직접 선택', '기간 선택', '날짜 선택', '사용자 설정'];
            const radios = [...document.querySelectorAll('input[type="radio"]')];
            const already = radios.find(r => r.value === 'directly' && r.checked);
            if (already) return true;
            const candidates = radios.filter(r => {
                const text = (r.closest('label')?.textContent || '').trim();
                const value = r.value || '';
                return targetTexts.some(t => text.includes(t) || value.includes(t))
                    || /custom|specific|range|calendar|date|directly/i.test(value);
            });
            const preferred = candidates.find(r => !r.checked) || candidates[0];
            if (preferred) {
                preferred.click();
                preferred.dispatchEvent(new Event('change', { bubbles: true }));
                return true;
            }
            for (const lbl of document.querySelectorAll('label')) {
                const text = (lbl.textContent || '').trim();
                if (targetTexts.some(t => text.includes(t))) {
                    lbl.click();
                    return true;
                }
            }
            return false;
            """
        )
    )


def _select_daily_mode(driver) -> bool:
    daily_radio = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "input[type='radio'][value='dailyWeekly']")
        )
    )
    checked = driver.execute_script(
        """
        arguments[0].click();
        arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
        return !!arguments[0].checked;
        """,
        daily_radio,
    )
    if checked:
        return True
    time.sleep(0.2)
    return bool(
        driver.execute_script(
            """
            arguments[0].click();
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            return !!arguments[0].checked;
            """,
            daily_radio,
        )
    )


def _select_relative_date_option(driver, label_text: str) -> bool:
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script(
            """
            const target = arguments[0];
            return [...document.querySelectorAll('label')]
                .some(lbl => {
                    const text = (lbl.textContent || '').replace(/\\s+/g, ' ').trim();
                    return text === target || text.startsWith(target + ' ');
                });
            """,
            label_text,
        )
    )

    def _click_option() -> bool:
        return bool(
            driver.execute_script(
                """
                const target = arguments[0];
                function clean(text) {
                    return (text || '').replace(/\\s+/g, ' ').trim();
                }
                function inputForLabel(label) {
                    const direct = label.querySelector('input[type="radio"]');
                    if (direct) return direct;
                    const forId = label.getAttribute('for');
                    if (forId) {
                        const escaped = window.CSS && CSS.escape ? CSS.escape(forId) : forId.replace(/"/g, '\\"');
                        return document.querySelector('#' + escaped);
                    }
                    return null;
                }
                for (const label of document.querySelectorAll('label')) {
                    const text = clean(label.textContent);
                    if (text !== target && !text.startsWith(target + ' ')) continue;
                    const input = inputForLabel(label);
                    if (!input) {
                        label.click();
                        return false;
                    }
                    input.click();
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    return !!input.checked;
                }
                return false;
                """,
                label_text,
            )
        )

    if _click_option():
        return True
    time.sleep(0.2)
    return _click_option()


def _parse_popup_date_text(text: object) -> str | None:
    match = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", str(text or ""))
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return f"{year:04d}-{month:02d}-{day:02d}"


def _relative_date_options_by_label(driver) -> dict[str, str | None]:
    labels = driver.execute_script(
        """
        return [...document.querySelectorAll('label')]
            .map(l => (l.textContent || '').replace(/\\s+/g, ' ').trim())
            .filter(Boolean)
            .slice(0, 80);
        """
    ) or []
    options: dict[str, str | None] = {}
    for idx, text in enumerate(labels):
        label = "오늘" if str(text).startswith("오늘") else "어제" if str(text).startswith("어제") else ""
        if not label:
            continue
        date_value = _parse_popup_date_text(text)
        if date_value is None:
            for next_text in labels[idx + 1 : idx + 4]:
                if str(next_text).startswith(("오늘", "어제")):
                    break
                date_value = _parse_popup_date_text(next_text)
                if date_value:
                    break
        options[label] = date_value
    return options


def _relative_date_label_for_target(driver, target_date: str, fallback_label: str) -> str | None:
    options = _relative_date_options_by_label(driver)
    dated = {label: value for label, value in options.items() if value}
    for label, value in dated.items():
        if value == target_date:
            return label
    if dated:
        logger.warning(
            "상대 날짜 옵션이 target_date와 불일치해 specific fallback 사용: target=%s options=%s",
            target_date,
            dated,
        )
        return None
    logger.warning(
        "상대 날짜 옵션 날짜 텍스트 미확인, fallback label 사용: target=%s label=%s options=%s",
        target_date,
        fallback_label,
        options,
    )
    return fallback_label


def _set_date_relative_for_target(
    driver,
    target_date: str,
    fallback_label: str,
    confirm_retry: bool = True,
) -> bool:
    try:
        clicked = _open_date_filter_popup(driver)
        if not clicked:
            return False

        if not _select_daily_mode(driver):
            logger.warning("날짜 '%s' 일 라디오 미체크: %s", fallback_label, _date_popup_debug_state(driver))
            return False

        label = _relative_date_label_for_target(driver, target_date, fallback_label)
        if not label:
            return False

        if not _select_relative_date_option(driver, label):
            logger.warning("날짜 '%s' 옵션 미체크: %s", label, _date_popup_debug_state(driver))
            return False
        time.sleep(0.2)

        apply_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-atelier-component='Button'][.//span[text()='적용']]")
            )
        )
        human_click(driver, apply_btn)

        confirmed = _confirm_visible_order_date(driver, target_date, label)
        if not confirmed:
            if confirm_retry:
                logger.warning("날짜 '%s' 실측 미확인, 날짜 필터 1회 재적용", label)
                return _set_date_relative_for_target(
                    driver,
                    target_date,
                    fallback_label,
                    confirm_retry=False,
                )
            logger.warning("날짜 '%s' 실측 최종 미확인", label)
            return False

        logger.info("날짜 필터 설정 완료: %s / target=%s", label, target_date)
        return True

    except TimeoutException as e:
        logger.warning("날짜 필터(%s) 설정 오류: %s", fallback_label, e)
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("날짜 필터(%s) 설정 오류: %s", fallback_label, e)
        return False


def _open_specific_date_picker(driver) -> bool:
    opened = driver.execute_script(
        """
        const triggers = [...document.querySelectorAll('[data-atelier-component="DatePicker.Trigger"]')];
        const visible = triggers.find(el => {
            const rect = el.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        }) || triggers[0];
        if (visible) {
            visible.click();
            return (visible.textContent || '').trim() || 'DatePicker.Trigger';
        }
        return false;
        """
    )
    if not opened:
        logger.warning("날짜 specific DatePicker 트리거 미발견: %s", _date_popup_debug_state(driver))
        return False
    logger.info("날짜 specific DatePicker 트리거 클릭: %s", opened)
    return True


def _click_visible_apply_button(driver, *, prefer_last: bool = True) -> bool:
    """현재 열린 날짜 UI의 visible 적용 버튼 하나를 누른다."""
    clicked = driver.execute_script(
        """
        const buttons = [...document.querySelectorAll('button')].filter(button => {
            const rect = button.getBoundingClientRect();
            const text = (button.textContent || '').replace(/\\s+/g, ' ').trim();
            return !button.disabled && rect.width > 0 && rect.height > 0 && text === '적용';
        });
        if (!buttons.length) return false;
        const button = arguments[0] ? buttons[buttons.length - 1] : buttons[0];
        button.click();
        return true;
        """,
        prefer_last,
    )
    return bool(clicked)


def _visible_specific_trigger_text(driver) -> str:
    try:
        return str(
            driver.execute_script(
                """
                const triggers = [...document.querySelectorAll('[data-atelier-component="DatePicker.Trigger"]')];
                const visible = triggers.find(el => {
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                }) || triggers[0];
                return (visible?.textContent || '').replace(/\\s+/g, ' ').trim();
                """
            )
            or ""
        )
    except Exception:
        return ""


def _specific_trigger_has_target(driver, target_date: str) -> bool:
    dt = pendulum.parse(target_date, tz=KST)
    text = _visible_specific_trigger_text(driver)
    patterns = [
        f"{dt.year}. {dt.month:02d}. {dt.day:02d}",
        f"{dt.year}.{dt.month:02d}.{dt.day:02d}",
        f"{dt.month:02d}. {dt.day:02d}",
        f"{dt.month:02d}.{dt.day:02d}",
    ]
    return bool(text) and any(pattern in text for pattern in patterns)


def _apply_specific_date_selection(driver, target_date: str) -> bool:
    """DatePicker 내부 적용 후 바깥 필터 적용까지 완료한다."""
    applied_any = False
    # DatePicker overlay가 body 끝에 붙는 구조라 마지막 visible 적용 버튼이 내부 적용인 경우가 많다.
    for _ in range(2):
        if _specific_trigger_has_target(driver, target_date):
            break
        if not _click_visible_apply_button(driver, prefer_last=True):
            break
        applied_any = True
        time.sleep(0.4)

    if not _specific_trigger_has_target(driver, target_date):
        logger.warning(
            "DatePicker 내부 적용 후 target 표시 미확인: target=%s trigger=%s popup=%s",
            target_date,
            _visible_specific_trigger_text(driver),
            _date_popup_debug_state(driver),
        )

    # outer 필터 팝업의 적용 버튼을 한 번 더 눌러 실제 주문 목록에 반영한다.
    for prefer_last in (False, True):
        if _click_visible_apply_button(driver, prefer_last=prefer_last):
            applied_any = True
            time.sleep(0.5)
            break
    return applied_any


def _set_date_specific(driver, target_date: str) -> bool:
    """DefaultDateFilter 달력에서 target_date 하루를 선택하고 적용한다.

    흐름:
        1. 날짜 필터 팝업 열기
        2. 직접/기간 날짜 선택 모드로 전환
        3. 달력 헤더에서 표시 월 확인 → 필요하면 이전달 버튼으로 이동
        4. target_date를 시작일/종료일로 클릭
        5. 적용 버튼 클릭
    """
    import re as _re

    try:
        dt = pendulum.parse(target_date, tz=KST)
        target_year = dt.year
        target_month = dt.month
        target_day = dt.day
        day_label = f"{target_day}일"

        clicked = _open_date_filter_popup(driver)
        if not clicked:
            return False

        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script(
                    "return !!document.querySelector('input[type=\"radio\"], button[data-atelier-component=\"Button\"]');"
                )
            )
        except TimeoutException:
            logger.warning("날짜 필터 팝업 로드 실패: %s", _date_popup_debug_state(driver))
            return False

        selected_specific = _select_specific_date_mode(driver)
        logger.info("날짜 필터 specific 모드 선택: %s", selected_specific)
        time.sleep(0.2)
        if not _open_specific_date_picker(driver):
            return False
        time.sleep(0.2)

        # 2. 달력 헤더 확인 후 목표 월로 이동
        def _get_calendar_ym(d):
            """달력에서 (year, month) 반환. 실패 시 None."""
            text = d.execute_script(
                """
                function hasCalendarMonth(text) {
                    return /(\\d{4})\\s*[.년]\\s*(\\d{1,2})\\s*[.월]?/.test(text || '');
                }
                function cleanText(el) {
                    return (el?.textContent || '').replace(/\\s+/g, ' ').trim();
                }
                const prev = [...document.querySelectorAll('button')]
                    .find(b => /이전\\s*달/.test(b.getAttribute('aria-label') || ''));
                if (prev) {
                    let node = prev.parentElement;
                    for (let i = 0; i < 5 && node; i++, node = node.parentElement) {
                        const t = cleanText(node);
                        if (t.includes('~')) continue;
                        if (hasCalendarMonth(t)) return t;
                    }
                }
                const anchors = [
                    '[data-atelier-component*="Calendar"]',
                    '[role="dialog"]',
                    '[class*="DatePicker"][class*="Header"]',
                    '[class*="CalendarHeader"]',
                ];
                for (const sel of anchors) {
                    for (const el of document.querySelectorAll(sel)) {
                        const t = cleanText(el);
                        if (t.includes('~')) continue;
                        if (hasCalendarMonth(t)) return t;
                    }
                }
                return null;
                """
            )
            if not text:
                return None
            m = _re.search(r"(\d{4})\s*[.년]\s*(\d{1,2})", text)
            return (int(m.group(1)), int(m.group(2))) if m else None

        # 달력이 열릴 때까지 대기
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script(
                "return !!document.querySelector('button[aria-label$=\"일\"], button[aria-label*=\"일\"]');"
            )
        )

        target_month_index = target_year * 12 + target_month
        # 최대 24번(2년치) 월 이동. 목표 월 확인 없이 같은 일자만 누르면 현재 월 데이터가 섞인다.
        ym_unreadable = 0
        for _ in range(24):
            ym = _get_calendar_ym(driver)
            if ym and (ym[0], ym[1]) == (target_year, target_month):
                break
            if not ym:
                ym_unreadable += 1
                if ym_unreadable >= 2:
                    logger.warning(
                        "달력 헤더 월 파싱 실패(셀렉터 회귀 의심): target=%s popup=%s",
                        target_date,
                        _date_popup_debug_state(driver),
                    )
                    break
                time.sleep(0.4)
                continue
            direction = "prev" if (ym[0] * 12 + ym[1]) > target_month_index else "next"
            moved = driver.execute_script(
                """
                const direction = arguments[0];
                const buttons = [...document.querySelectorAll('button')].filter(b => {
                    const rect = b.getBoundingClientRect();
                    return !b.disabled && rect.width > 0 && rect.height > 0;
                });
                function textOf(button) {
                    return [
                        button.getAttribute('aria-label') || '',
                        button.getAttribute('title') || '',
                        button.textContent || '',
                        button.className || '',
                    ].join(' ').trim();
                }
                const patterns = direction === 'prev'
                    ? [/이전\\s*달/, /이전/, /prev/i, /previous/i, /^<$/]
                    : [/다음\\s*달/, /다음/, /next/i, /^>$/];
                const target = buttons.find(button => patterns.some(pattern => pattern.test(textOf(button))));
                if (!target) return false;
                target.click();
                return textOf(target) || true;
                """,
                direction,
            )
            if not moved:
                logger.warning("달력 월 이동 버튼 미발견: target=%s direction=%s popup=%s", target_date, direction, _date_popup_debug_state(driver))
                break
            time.sleep(0.3)

        final_ym = _get_calendar_ym(driver)
        if final_ym != (target_year, target_month):
            logger.warning(
                "달력 목표 월 이동 실패: target=%s final_ym=%s popup=%s",
                target_date,
                final_ym,
                _date_popup_debug_state(driver),
            )
            return False

        # 3. 시작일 클릭
        clicked_start = driver.execute_script(
            """
            const targetDay = String(arguments[0]);
            const dayLabel = arguments[1];
            const btns = [...document.querySelectorAll('button')].filter(b => {
                const rect = b.getBoundingClientRect();
                const aria = b.getAttribute('aria-label') || '';
                const text = (b.textContent || '').trim();
                return !b.disabled
                    && rect.width > 0
                    && rect.height > 0
                    && (aria === dayLabel || aria.includes(dayLabel) || text === targetDay || text === dayLabel);
            });
            if (btns.length > 0) { btns[0].click(); return true; }
            return false;
            """,
            str(target_day),
            day_label,
        )
        if not clicked_start:
            logger.warning("달력 시작일 버튼 미발견: %s / popup=%s", day_label, _date_popup_debug_state(driver))
            return False

        time.sleep(0.2)

        # 4. 종료일 클릭 (같은 날 → 1일 범위)
        driver.execute_script(
            """
            const targetDay = String(arguments[0]);
            const dayLabel = arguments[1];
            const btns = [...document.querySelectorAll('button')].filter(b => {
                const rect = b.getBoundingClientRect();
                const aria = b.getAttribute('aria-label') || '';
                const text = (b.textContent || '').trim();
                return !b.disabled
                    && rect.width > 0
                    && rect.height > 0
                    && (aria === dayLabel || aria.includes(dayLabel) || text === targetDay || text === dayLabel);
            });
            if (btns.length > 0) btns[0].click();
            """,
            str(target_day),
            day_label,
        )
        time.sleep(0.2)

        # 5. DatePicker 내부 적용 → 필터 팝업 적용 순서로 반영
        if not _apply_specific_date_selection(driver, target_date):
            logger.warning("날짜 specific 적용 버튼 클릭 실패: %s / popup=%s", target_date, _date_popup_debug_state(driver))
            return False

        # 검증: 날짜 텍스트에 target_day 포함 확인
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script(
                    f"const t = document.querySelector('[data-atelier-component=\"DatePicker.Trigger\"]');"
                    f"return t && t.textContent.includes('{target_day}');"
                )
            )
        except TimeoutException:
            pass

        if not _confirm_visible_order_date(driver, target_date, target_date):
            logger.warning("날짜 필터(specific) 실측 최종 미확인: %s", target_date)
            return False

        logger.info("날짜 필터 설정 완료: %s", target_date)
        return True

    except TimeoutException as e:
        logger.warning("날짜 필터(specific) 설정 오류: %s / popup=%s", e, _date_popup_debug_state(driver))
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("날짜 필터(specific) 설정 오류: %s / popup=%s", e, _date_popup_debug_state(driver))
        return False


def _set_date(driver, target_date: str) -> bool:
    """target_date에 맞는 날짜 필터 함수를 선택해 호출한다."""
    today = pendulum.now(KST).format("YYYY-MM-DD")
    yesterday = pendulum.yesterday(KST).format("YYYY-MM-DD")
    if target_date == today:
        return _set_date_relative_for_target(driver, target_date, "오늘") or _set_date_specific(driver, target_date)
    if target_date == yesterday:
        return _set_date_relative_for_target(driver, target_date, "어제") or _set_date_specific(driver, target_date)
    return _set_date_specific(driver, target_date)


def _set_date_today(driver, confirm_retry: bool = True) -> bool:
    """날짜 필터를 '일 → 오늘'로 설정하고 적용한다."""
    target_date = pendulum.now(KST).format("YYYY-MM-DD")
    return _set_date_relative_for_target(driver, target_date, "오늘", confirm_retry=confirm_retry)


def _set_date_yesterday(driver, confirm_retry: bool = True) -> bool:
    """날짜 필터를 '일 → 어제'로 설정하고 적용한다."""
    target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")
    return _set_date_relative_for_target(driver, target_date, "어제", confirm_retry=confirm_retry)


# ---------------------------------------------------------------------------
# 페이지 순회 수집
# ---------------------------------------------------------------------------

def _collect_all_pages(driver, store_info: dict) -> list[dict]:
    """현재 필터 상태에서 전 페이지 주문내역을 수집한다.

    모든 행을 한꺼번에 펼치면 Chrome OOM crash 발생 → 행별 개별 expand/extract/collapse.
    """
    store = f"{store_info['brand']} {store_info['store']}"

    try:
        row_count = driver.execute_script(
            f"return document.querySelectorAll('{_TABLE_ROW_CSS}').length;"
        ) or 0
    except Exception as e:
        if _is_crash(e):
            raise OrdersCollectionInterrupted(f"Chrome died before row count: {e}") from e
        raise
    if row_count == 0:
        summary = _read_total_summary(driver)
        if summary and summary.get("count") == 0:
            logger.info("주문 없음 (테이블 행 0, TotalSummary 0건): %s", store)
        elif summary is None:
            logger.warning("테이블 0행이나 TotalSummary 미확인, 재시도 대상: %s", store)
        else:
            logger.warning("테이블 0행이나 TotalSummary=%s, 재시도 대상: %s", summary, store)
        return []

    all_rows: list[dict] = []
    iso = pendulum.now(KST).isoformat()
    settle_miss_count = 0

    for page_num in range(1, _MAX_PAGES + 1):
        logger.info("%d페이지 수집 중: %s", page_num, store)

        # data-index 목록 수집 (expand/collapse 중에도 안정적)
        try:
            indices = driver.execute_script(
                "return [...document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]')]"
                ".map(r => r.getAttribute('data-index'));"
            ) or []
        except Exception as e:
            if _is_crash(e):
                raise OrdersCollectionInterrupted(
                    f"Chrome died while reading row indices page={page_num}: {e}"
                ) from e
            raise

        page_rows: list[dict] = []
        for idx in indices:
            try:
                # 1. 행 펼치기
                driver.execute_script(_EXPAND_ROW_JS, idx)
                settle_ready = _wait_settle_section(driver, idx)
                if not settle_ready:
                    try:
                        driver.execute_script(_COLLAPSE_ROW_JS, idx)
                        time.sleep(0.1)
                        driver.execute_script(_EXPAND_ROW_JS, idx)
                        settle_ready = _wait_settle_section(driver, idx)
                    except Exception as wait_err:
                        if _is_crash(wait_err):
                            raise OrdersCollectionInterrupted(
                                f"Chrome died while re-expanding settle section idx={idx}: {wait_err}"
                            ) from wait_err
                        logger.warning("정산정보 재펼침 실패: %s idx=%s err=%s", store, idx, wait_err)
                if not settle_ready:
                    settle_miss_count += 1

                # 2. 단일 행 추출
                row_data = driver.execute_script(_EXTRACT_SINGLE_ROW_JS, idx, store, iso) or []

                # 3. 즉시할인 분해 (Chrome 죽으면 팝업만 skip, 데이터는 유지)
                #    분해에 성공하면 0은 반드시 '0'으로 기록하고, 공란은 오직
                #    '수집 실패'만 의미하게 한다
                #    (DB_DeliveryCommission 폴백이 이 구분에 의존).
                if row_data and str(row_data[0].get("즉시할인") or "").strip():
                    order_num = row_data[0].get("주문번호", "")
                    total = _to_int(row_data[0].get("즉시할인"))
                    if total <= 0:
                        row_data[0]["즉시할인_파트너부담"] = "0"
                        row_data[0]["즉시할인_배민지원"] = "0"
                    else:
                        try:
                            detail = _get_discount_popup_detail(driver, order_num, total)
                            if detail:
                                row_data[0]["즉시할인_파트너부담"] = detail.get("partner", "")
                                row_data[0]["즉시할인_배민지원"] = detail.get("baemin", "")
                        except Exception as popup_err:
                            if _is_crash(popup_err):
                                raise OrdersCollectionInterrupted(
                                    f"Chrome died in discount popup order={order_num}: {popup_err}"
                                ) from popup_err
                            # 행 접기는 건너뛰지 않는다. 펼쳐진 채 남으면 다음 행 처리가 흔들린다.
                            logger.warning(
                                "팝업 클릭 실패 - 할인 상세만 비우고 행 데이터 유지: %s", popup_err
                            )

                # 4. 행 접기
                driver.execute_script(_COLLAPSE_ROW_JS, idx)
                page_rows.extend(row_data)
                time.sleep(0.1)

            except Exception as row_err:
                if _is_crash(row_err) or isinstance(row_err, OrdersCollectionInterrupted):
                    raise OrdersCollectionInterrupted(
                        f"Chrome died while processing row idx={idx}: {row_err}"
                    ) from row_err
                logger.warning("행 처리 실패 (Chrome OOM?): idx=%s err=%s", idx, row_err)
                continue

        all_rows.extend(page_rows)
        logger.info(
            "%d페이지: %d행 수집 (누계 %d행)", page_num, len(page_rows), len(all_rows)
        )

        try:
            prev_sig = _page_signature(driver)
            has_next = _click_next_page(driver, page_num)
        except Exception as e:
            if _is_crash(e):
                raise OrdersCollectionInterrupted(
                    f"Chrome died before page transition page={page_num}: {e}"
                ) from e
            raise
        if not has_next:
            logger.info("마지막 페이지 도달: %s (%d페이지)", store, page_num)
            break

        if not _wait_for_page_transition(driver, prev_sig):
            # 전환 실패 시 즉시 종료하지 말고 next 1회 재클릭 후 재대기 (행 누락 방지)
            logger.info("%d→%d 페이지 전환 미감지 → next 재클릭 1회: %s", page_num, page_num + 1, store)
            try:
                _click_next_page(driver, page_num)
            except Exception as e:
                if _is_crash(e):
                    raise OrdersCollectionInterrupted(
                        f"Chrome died during page re-click page={page_num}: {e}"
                    ) from e
            if not _wait_for_page_transition(driver, prev_sig):
                logger.warning(
                    "%d→%d 페이지 전환 타임아웃(재시도 후), 수집 종료: %s",
                    page_num, page_num + 1, store,
                )
                break

        time.sleep(random.uniform(0.5, 0.8))

    if settle_miss_count:
        logger.warning("정산정보 미렌더: %s %d/%d행", store, settle_miss_count, len(all_rows))

    return all_rows


_DISCOUNT_SHEET_WAIT_SEC = 3.0
_DISCOUNT_SHEET_CLOSE_WAIT_SEC = 0.8
_DISCOUNT_SHEET_POLL_SEC = 0.1


def _to_int(value: object) -> int:
    try:
        return int(str(value if value is not None else "").strip())
    except (TypeError, ValueError):
        return 0


def _discount_sheet_present(driver) -> bool:
    return bool(driver.execute_script(_FIND_DISCOUNT_SHEET_JS))


def _wait_discount_sheet(driver, timeout: float = _DISCOUNT_SHEET_WAIT_SEC) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _discount_sheet_present(driver):
            return True
        time.sleep(_DISCOUNT_SHEET_POLL_SEC)
    return _discount_sheet_present(driver)


def _wait_discount_sheet_gone(
    driver, timeout: float = _DISCOUNT_SHEET_CLOSE_WAIT_SEC
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _discount_sheet_present(driver):
            return True
        time.sleep(_DISCOUNT_SHEET_POLL_SEC)
    return not _discount_sheet_present(driver)


def _close_discount_sheet(driver) -> bool:
    """열려 있는 즉시할인 상세시트를 닫고, 닫힌 것을 확인한다.

    닫힘 확인 없이 다음 주문으로 넘어가면 직전 주문의 시트를 다시 읽어
    파트너부담/배민지원 값이 복제된다.
    """
    for attempt in range(4):
        if driver.execute_script(_CLOSE_DISCOUNT_SHEET_ATTEMPT_JS, attempt):
            return True
        if _wait_discount_sheet_gone(driver):
            return True
    return not _discount_sheet_present(driver)


def _get_discount_popup_detail(driver, order_num: str, total: int) -> dict | None:
    """주문번호 행의 즉시할인 금액을 클릭 → 상세시트 파싱 → 닫기.

    확장 `content/02_baemin.js` 의 `_readInstantDiscount()` 와 동일 규격:
    - 클릭 전에 이전 시트를 강제로 닫는다 (직전 주문 값이 복제되던 버그)
    - 고정 sleep 대신 시트 열림/닫힘을 폴링한다
    - 해시 클래스가 회전해도 텍스트 폴백으로 찾는다
    - 한쪽만 읽히면 총액에서 차감해 복원한다

    Returns:
        {"partner": str, "baemin": str} 또는 None (분해 실패)
    """
    # 이전 주문의 시트가 남아 있으면 값이 복제된다 -> 먼저 강제 종료
    _close_discount_sheet(driver)

    if not driver.execute_script(_CLICK_DISCOUNT_AMOUNT_JS, order_num):
        return None

    if not _wait_discount_sheet(driver):
        logger.warning(
            "즉시할인 상세시트 미검출: order=%s total=%s", order_num, total
        )
        _close_discount_sheet(driver)
        return None

    parsed = driver.execute_script(_PARSE_DISCOUNT_SHEET_JS) or {}

    if not _close_discount_sheet(driver):
        logger.warning("즉시할인 상세시트 닫기 실패: order=%s", order_num)

    partner_raw = parsed.get("partner")
    support_raw = parsed.get("support")
    p = None if partner_raw in (None, "") else _to_int(partner_raw)
    s = None if support_raw in (None, "") else _to_int(support_raw)

    if p is not None and s is not None:
        if p + s != total:
            logger.warning(
                "즉시할인 합 불일치: order=%s total=%s partner=%s support=%s",
                order_num,
                total,
                p,
                s,
            )
        return {"partner": str(p), "baemin": str(s)}
    if s is not None:
        return {"partner": str(max(0, total - s)), "baemin": str(s)}
    if p is not None:
        return {"partner": str(p), "baemin": str(max(0, total - p))}

    logger.warning("즉시할인 분해 파싱 실패: order=%s total=%s", order_num, total)
    return None


def _get_first_order_id(driver) -> str:
    return driver.execute_script(
        """
        const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index]');
        return row?.querySelector('td[data-td-index="1"]')?.textContent.trim() || '';
        """
    ) or ""


def _page_signature(driver) -> str:
    """현재 페이지 식별자: 첫 주문ID|마지막 주문ID|행수.

    첫 주문ID만 보면 우연히 동일할 때 전환을 놓칠 수 있어, 마지막ID·행수까지 묶어
    페이지 전환을 더 확실히 감지한다.
    """
    return driver.execute_script(
        """
        const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
        if (!rows.length) return '';
        const idOf = (r) => r.querySelector('td[data-td-index="1"]')?.textContent.trim() || '';
        return idOf(rows[0]) + '|' + idOf(rows[rows.length - 1]) + '|' + rows.length;
        """
    ) or ""


def _click_next_page(driver, current_page: int) -> bool:
    return driver.execute_script(
        """
        const next = arguments[0] + 1;
        const btns = document.querySelectorAll(
            'ul.Pagination_b_r4ax_pb5p4v5 button.Pagination_b_r4ax_pb5p4vb'
        );
        for (const btn of btns) {
            if (parseInt(btn.textContent.trim()) === next) { btn.click(); return true; }
        }
        return false;
        """,
        current_page,
    )


def _go_to_first_page(driver, store: str) -> None:
    """날짜 fallback 수집 전 페이지네이션을 첫 페이지로 되돌린다."""
    try:
        prev_sig = _page_signature(driver)
        clicked = driver.execute_script(
            """
            const btns = [...document.querySelectorAll(
                'ul.Pagination_b_r4ax_pb5p4v5 button.Pagination_b_r4ax_pb5p4vb'
            )];
            const first = btns.find(btn => parseInt((btn.textContent || '').trim()) === 1);
            if (!first || first.disabled) return false;
            const selected = first.getAttribute('aria-current') === 'page'
                || first.className.includes('selected')
                || first.className.includes('Selected')
                || first.className.includes('active')
                || first.className.includes('Active');
            if (selected) return 'already_first';
            first.click();
            return true;
            """
        )
        if clicked is True:
            if not _wait_for_page_transition(driver, prev_sig):
                logger.info("1페이지 이동 signature 변화 없음, 현재 페이지로 계속: %s", store)
            else:
                logger.info("수집 시작 페이지를 1페이지로 초기화: %s", store)
        elif clicked == "already_first":
            logger.debug("이미 1페이지에서 수집 시작: %s", store)
    except Exception as exc:
        if _is_crash(exc):
            raise
        logger.warning("1페이지 초기화 실패, 현재 페이지로 계속: %s / %s", store, _short_error(exc))


def _wait_for_page_transition(driver, prev_sig: str) -> bool:
    """페이지 시그니처(_page_signature)가 바뀌면 전환 완료로 본다."""
    start = time.time()
    while time.time() - start < _PAGE_TRANSITION_TIMEOUT:
        time.sleep(0.3)
        curr = _page_signature(driver)
        if curr and curr != prev_sig:
            return True
    return False


def _wait_for_filter_settle(driver, prev_sig: str, prev_summary: dict | None) -> bool:
    """필터 적용 후 테이블 signature 또는 TotalSummary 변경을 기다린다."""
    start = time.time()
    while time.time() - start < _PAGE_TRANSITION_TIMEOUT:
        time.sleep(0.3)
        try:
            curr_sig = _page_signature(driver)
            curr_summary = _read_total_summary(driver)
        except Exception as exc:
            if _is_crash(exc):
                raise OrdersCollectionInterrupted(f"Chrome died while waiting filter settle: {exc}") from exc
            continue

        if curr_sig and curr_sig != prev_sig:
            return True
        if curr_summary is not None and curr_summary != prev_summary:
            return True
        if prev_summary is None and curr_summary is not None:
            return True
    return False


# ---------------------------------------------------------------------------
# JS 상수: 행별 expand / collapse / 단일 행 추출
# ---------------------------------------------------------------------------

# arguments[0]: data-index 문자열
_EXPAND_ROW_JS = r"""
const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + arguments[0] + '"]');
const svg = row?.querySelector('td[data-td-index="0"] svg');
if (svg && svg.getAttribute('aria-label') === '컨텐츠 펼치기') {
    svg.closest('td')?.click();
}
"""

# arguments[0]: data-index 문자열
_COLLAPSE_ROW_JS = r"""
const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + arguments[0] + '"]');
const svg = row?.querySelector('td[data-td-index="0"] svg');
if (svg && svg.getAttribute('aria-label') === '컨텐츠 접기') {
    svg.closest('td')?.click();
}
"""

# arguments[0]: data-index 문자열
_SETTLE_READY_JS = r"""
const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + arguments[0] + '"]');
if (!row) return false;

let detail = null;
let sibling = row.nextElementSibling;
while (sibling?.tagName === 'TR') {
    const cell = sibling.querySelector('td[colspan="9"]');
    if (cell) { detail = cell; break; }
    sibling = sibling.nextElementSibling;
}
if (!detail) return false;

for (const sec of detail.querySelectorAll('section, div')) {
    const head = (sec.firstElementChild?.textContent || '').replace(/\s+/g, '');
    if (head.includes('정산정보') && sec.children.length > 1) return true;
}
return false;
"""


# ---------------------------------------------------------------------------
# JS 상수: 즉시할인 상세시트
# 확장 content/02_baemin.js `_readInstantDiscount()` 와 동일 규격.
# 배민의 해시 클래스(vanilla-extract)는 재배포마다 회전하므로 텍스트 /
# data-atelier-component 기준 폴백을 함께 둔다. 가장 중요한 점은 상세 시트를
# 항상 닫고, 닫힐 때까지 기다리는 것이다.
# (이전 주문의 열린 시트를 다시 읽어 값이 복제되던 버그)
# ---------------------------------------------------------------------------

_DISCOUNT_SHEET_HELPERS_JS = r"""
function __dsClean(text) {
    if (!text) return '';
    return String(text).replace(/[,\uc6d0\s()]/g, '').trim();
}
function __dsVisible(el) {
    if (!el || !el.isConnected) return false;
    if (!el.getClientRects().length) return false;
    const st = getComputedStyle(el);
    return st.visibility !== 'hidden' && st.display !== 'none';
}
function __dsFindSheet() {
    const candidates = [...document.querySelectorAll(
        '.InstantDiscountDetailPageSheet-module__IbXh,'
        + '[role="dialog"],'
        + '[data-atelier-component="PageSheet"],'
        + '[class*="InstantDiscountDetailPageSheet-module__"]'
    )].filter((el) => {
        if (!__dsVisible(el)) return false;
        const t = el.textContent || '';
        return /\uc989\uc2dc\ud560\uc778/.test(t)
            && /(\ud30c\ud2b8\ub108\s*\ubd80\ub2f4|\uac00\uac8c\s*\ubd80\ub2f4|\uc810\uc8fc\s*\ubd80\ub2f4|\ubc30\ubbfc\s*\uc9c0\uc6d0)/.test(t);
    });
    if (!candidates.length) return null;
    return candidates.find((el) => !candidates.some((o) => o !== el && el.contains(o)))
        || candidates[0];
}
function __dsAmountEl(detailSection) {
    if (!detailSection) return null;
    const container = detailSection.querySelector('.InstantDiscountDetailPageSheet-module__u8LB');
    if (container) {
        const el = container.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
        if (el) return el;
    }
    const labelEl = [...detailSection.querySelectorAll('[data-atelier-component="Typography"], span')]
        .find((el) => (el.textContent || '').trim() === '\uc989\uc2dc\ud560\uc778');
    if (!labelEl) return null;
    let scope = labelEl.parentElement;
    for (let depth = 0; depth < 4 && scope; depth++) {
        const amount = [...scope.querySelectorAll('span')]
            .filter((el) => el !== labelEl && !el.contains(labelEl)
                && /\d/.test(el.textContent || '') && /\uc6d0/.test(el.textContent || ''))
            .pop();
        if (amount) return amount;
        scope = scope.parentElement;
    }
    return null;
}
"""

# 상세시트가 열려 있는지
_FIND_DISCOUNT_SHEET_JS = _DISCOUNT_SHEET_HELPERS_JS + r"""
return !!__dsFindSheet();
"""

# arguments[0]: 시도 횟수(0~3). 이미 닫혀 있으면 true.
_CLOSE_DISCOUNT_SHEET_ATTEMPT_JS = _DISCOUNT_SHEET_HELPERS_JS + r"""
const attempt = arguments[0];
const sheet = __dsFindSheet();
if (!sheet) return true;

if (attempt === 0) {
    const closer = sheet.querySelector(
        'button[aria-label*="\ub2eb\uae30"], button[aria-label*="close" i],'
        + 'svg[aria-label*="\ub2eb\uae30"], svg[aria-label*="close" i]'
    );
    const btn = closer && (closer.closest('button') || closer);
    if (btn) btn.click();
} else if (attempt === 1) {
    for (const type of ['keydown', 'keyup']) {
        document.dispatchEvent(new KeyboardEvent(type, {
            key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true
        }));
    }
} else if (attempt === 2) {
    let overlay = sheet.parentElement;
    while (overlay && overlay !== document.body) {
        if (getComputedStyle(overlay).position === 'fixed') break;
        overlay = overlay.parentElement;
    }
    (overlay && overlay !== document.body ? overlay : document.body).click();
} else {
    document.body.click();
}
return false;
"""

# 반환: {partner, support} (미검출 항목은 null) 또는 null(시트 없음)
_PARSE_DISCOUNT_SHEET_JS = _DISCOUNT_SHEET_HELPERS_JS + r"""
const sheet = __dsFindSheet();
if (!sheet) return null;

let partner = null, support = null;
for (const item of sheet.querySelectorAll('[data-atelier-component="TextListItem"], li')) {
    const label = item.textContent || '';
    const isPartner = /(\ud30c\ud2b8\ub108|\uac00\uac8c|\uc810\uc8fc)\s*\ubd80\ub2f4/.test(label);
    const isSupport = /\ubc30\ubbfc\s*\uc9c0\uc6d0/.test(label);
    if (!isPartner && !isSupport) continue;
    if (isPartner && partner !== null) continue;
    if (isSupport && support !== null) continue;

    const valueBox = item.querySelector('.TextListItem_b_r4ax_n197m77') || item.lastElementChild;
    if (!valueBox) continue;
    const spans = valueBox.querySelectorAll('span');
    let raw = spans.length ? spans[spans.length - 1].textContent : '';
    if (!/\d/.test(raw || '')) raw = valueBox.textContent || '';
    const value = __dsClean(raw);
    if (value === '') continue;

    if (isPartner) partner = value;
    else support = value;
}
return {partner, support};
"""

# arguments[0]: 주문번호. 해당 행의 즉시할인 금액 엘리먼트를 클릭한다.
_CLICK_DISCOUNT_AMOUNT_JS = _DISCOUNT_SHEET_HELPERS_JS + r"""
const targetOrder = arguments[0];
const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
for (const row of rows) {
    const cell = row.querySelector('td[data-td-index="1"]');
    const badge = cell?.querySelector('.Badge_b_r4ax_19agxiso, [data-atelier-component="Badge"] span');
    const num = (cell?.textContent.trim() || '')
        .replace(badge?.textContent.trim() || '', '').trim();
    if (num !== targetOrder) continue;

    let sib = row.nextElementSibling;
    while (sib?.tagName === 'TR') {
        const sec = sib.querySelector('td[colspan="9"] .DetailInfo-module__pZYe');
        if (sec) {
            const el = __dsAmountEl(sec);
            if (el && (el.textContent || '').trim()) { el.click(); return true; }
            return false;
        }
        sib = sib.nextElementSibling;
    }
    return false;
}
return false;
"""


def _wait_settle_section(driver, idx, timeout: float = 2.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if driver.execute_script(_SETTLE_READY_JS, idx):
                return True
        except Exception as exc:
            if _is_crash(exc):
                raise OrdersCollectionInterrupted(
                    f"Chrome died while waiting settle section idx={idx}: {exc}"
                ) from exc
        time.sleep(0.15)
    return False


# arguments[0]: data-index, arguments[1]: store_name, arguments[2]: iso
# 반환: [] 또는 [{...}, ...] (옵션별 분리)
_EXTRACT_SINGLE_ROW_JS = _DISCOUNT_SHEET_HELPERS_JS + r"""
const dataIdx = arguments[0];
const storeName = arguments[1];
const iso = arguments[2];

function cleanPrice(text) {
    if (!text) return '';
    return text.replace(/[,원\s()]/g, '').trim();
}

const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + dataIdx + '"]');
if (!row) return [];

const base = {
    collected_at: iso, store_name: storeName,
    주문상태: '', 주문번호: '', 주문시각: '', 광고상품: '', 캠페인ID: '',
    결제타입: '', 수령방법: '', 결제금액: '',
    상품금액: '', 즉시할인: '', 즉시할인_파트너부담: '', 즉시할인_배민지원: '',
    배민부담_쿠폰할인: '', 총결제금액: '',
    주문중개: '', 고객할인비용: '', 배달: '', 그외: '', 부가세: '',
    만나서결제금액: '', 입금예정금액: ''
};

for (const cell of row.querySelectorAll('td')) {
    const idx = cell.getAttribute('data-td-index');
    const text = cell.textContent.trim();
    if (idx === '1') {
        const badge = cell.querySelector('.Badge_b_r4ax_19agxiso, [data-atelier-component="Badge"] span');
        base.주문상태 = badge?.textContent.trim() || '';
        base.주문번호 = text.replace(base.주문상태, '').trim();
    } else if (idx === '2') { base.주문시각 = text.replace(/\s+/g, ' '); }
    else if (idx === '3') { base.광고상품 = text; }
    else if (idx === '4') { base.캠페인ID = text; }
    else if (idx === '6') { base.결제타입 = text; }
    else if (idx === '7') { base.수령방법 = text; }
    else if (idx === '8') { base.결제금액 = cleanPrice(text); }
}

// 상세 섹션 탐색 (다음 형제 TR)
let detailSection = null;
let sibling = row.nextElementSibling;
while (sibling?.tagName === 'TR') {
    const sec = sibling.querySelector('td[colspan="9"] .DetailInfo-module__pZYe');
    if (sec) { detailSection = sec; break; }
    sibling = sibling.nextElementSibling;
}

if (!detailSection) {
    return [{...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: ''}];
}

// 정산정보 / 주문정보 섹션 분리
const allSections = detailSection.querySelectorAll('section.DetailInfo-module__Sopx');
let orderSection = null, settleSection = null;
for (const sec of allSections) {
    const hdr = sec.querySelector('.DetailInfo-module__bKQt')?.textContent || '';
    if (hdr.includes('정산정보')) settleSection = sec;
    else if (hdr.includes('주문정보')) orderSection = sec;
}
if (!orderSection && allSections.length > 0) orderSection = allSections[0];

// 정산정보 추출
if (settleSection) {
    for (const item of settleSection.querySelectorAll('.FieldItem-module__gYJs')) {
        const label = item.querySelector('.FieldItem-module__YCcw')?.textContent.trim() || '';
        const value = cleanPrice(item.querySelector('.FieldItem-module__rb57')?.textContent || '');
        if (label.includes('(A)')) base.주문중개 = value;
        else if (label.includes('(B)')) base.배달 = value;
        else if (label.includes('(C)')) base.그외 = value;
        else if (label.includes('(D)')) base.부가세 = value;
        else if (label.includes('(E)') || label.includes('만나서결제금액')) base.만나서결제금액 = value;
        else if (label.includes('입금예정금액')) base.입금예정금액 = value;
    }
    for (const li of settleSection.querySelectorAll('.SettleContent-module__Bji5 li')) {
        if (li.textContent.includes('고객할인비용')) {
            base.고객할인비용 = cleanPrice(
                li.closest('p')?.querySelector('.SettleContent-module__E2ID')?.textContent || ''
            );
            break;
        }
    }
}

// 즉시할인 총액 (해시 클래스 회전 대비 텍스트 폴백 포함)
const discountEl = __dsAmountEl(detailSection);
if (discountEl) base.즉시할인 = cleanPrice(discountEl.textContent);

// 배민부담 쿠폰할인
const couponEl = detailSection.querySelector('.CouponDiscount-module__rTI9');
if (couponEl) {
    const parentLi = couponEl.closest('li');
    if (parentLi) {
        const valEl = parentLi.querySelector(
            '.TextListItem_b_r4ax_n197m77 span, .TextListItem_b_r4ax_n197m76 p'
        );
        base.배민부담_쿠폰할인 = cleanPrice(valEl?.textContent || '');
    }
}

// 총결제금액
const totalEl = orderSection?.querySelector('.DetailInfo-module__PmTR .FieldItem-module__LyiN');
if (totalEl) base.총결제금액 = cleanPrice(totalEl.textContent);

// 메뉴 블록 추출 (옵션별 행 분리)
const menuContainer = orderSection?.querySelector('.DetailInfo-module__j9yH');
const allMenuData = [];

if (menuContainer) {
    for (const menuBlock of menuContainer.querySelectorAll('.DetailInfo-module__pC_2')) {
        const menuInfoEl = menuBlock.querySelector('.DetailInfo-module__nV94');
        const menuName = menuInfoEl?.querySelector('span:first-child')?.textContent.trim() || '';
        const menuQty = menuInfoEl?.querySelector('.DetailInfo-module__QGJz')
            ?.textContent.replace(/[^\d]/g, '') || '1';
        const menuPrice = cleanPrice(menuBlock.querySelector('.FieldItem-module__rb57')?.textContent || '');

        const options = [];
        const optContainer = menuBlock.nextElementSibling;
        if (optContainer?.classList.contains('DetailInfo-module__J1rX')) {
            for (const optDiv of optContainer.querySelectorAll('.DetailInfo-module__n2Ro')) {
                const spans = optDiv.querySelectorAll('span');
                const optName = spans[0]?.textContent.trim() || '';
                let optPrice = '';
                const priceSpan = spans[1];
                if (priceSpan) {
                    const origEl = priceSpan.querySelector('.DetailInfo-module__t8S5');
                    if (origEl) {
                        const clone = priceSpan.cloneNode(true);
                        clone.querySelector('.DetailInfo-module__t8S5')?.remove();
                        optPrice = cleanPrice(clone.textContent);
                    } else {
                        optPrice = cleanPrice(priceSpan.textContent);
                    }
                }
                if (optName) options.push({name: optName, price: optPrice});
            }
        }
        if (options.length === 0) options.push({name: menuName, price: menuPrice});
        if (menuName) allMenuData.push({menuName, menuQty, menuPrice, options});
    }
}

const totalMenuPrice = allMenuData.reduce((s, m) => s + (parseInt(m.menuPrice) || 0), 0);
if (totalMenuPrice > 0) base.상품금액 = totalMenuPrice.toString();

if (allMenuData.length === 0) {
    return [{...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: ''}];
}

const orderSummary = allMenuData[0].menuName +
    (allMenuData.length > 1 ? ` 외 ${allMenuData.length - 1}건` : '');

const result = [];
let isFirst = true;
for (const menu of allMenuData) {
    for (let i = 0; i < menu.options.length; i++) {
        const opt = menu.options[i];
        const optName = (i === 0 && opt.name === '기본') ? menu.menuName : opt.name;
        result.push({
            ...base,
            주문내역: orderSummary, 주문수량: menu.menuQty,
            결제금액: isFirst ? base.결제금액 : '',
            상품금액: isFirst ? base.상품금액 : '',
            즉시할인: isFirst ? base.즉시할인 : '',
            즉시할인_파트너부담: '',
            즉시할인_배민지원: '',
            배민부담_쿠폰할인: isFirst ? base.배민부담_쿠폰할인 : '',
            총결제금액: isFirst ? base.총결제금액 : '',
            주문옵션상세: optName, 주문옵션금액: opt.price,
            주문중개: isFirst ? base.주문중개 : '',
            고객할인비용: isFirst ? base.고객할인비용 : '',
            배달: isFirst ? base.배달 : '',
            그외: isFirst ? base.그외 : '',
            부가세: isFirst ? base.부가세 : '',
            만나서결제금액: isFirst ? base.만나서결제금액 : '',
            입금예정금액: isFirst ? base.입금예정금액 : '',
        });
        isFirst = false;
    }
}
return result;
"""


# ---------------------------------------------------------------------------
# CSV 저장 (upsert by 주문번호)
# ---------------------------------------------------------------------------

_COLUMNS = [
    "collected_at", "store_name",
    "주문상태", "주문번호", "주문시각", "광고상품", "캠페인ID",
    "주문내역", "주문수량", "결제타입", "수령방법",
    "결제금액", "상품금액", "즉시할인", "즉시할인_파트너부담", "즉시할인_배민지원",
    "배민부담_쿠폰할인", "총결제금액",
    "주문옵션상세", "주문옵션금액",
    "주문중개", "고객할인비용", "배달", "그외", "부가세",
    "만나서결제금액", "입금예정금액",
]


def _record_reingest_dates(
    source: str,
    store: str,
    new_df: pd.DataFrame,
    new_dates: pd.Series,
    info: dict,
) -> None:
    date_values = new_dates.fillna("").astype(str).str.strip().reset_index(drop=True)
    for date in info.get("covered_dates", []):
        rows = int(date_values.eq(date).sum()) if len(date_values) == len(new_df) else 0
        record_manual_reingest_marker(
            source,
            store,
            date,
            {"rows": rows, "removed": int(info.get("removed", 0))},
        )


def _save_orders_csv(rows: list[dict], brand: str, store: str, target_date: str) -> Path:
    """주문시각 월별 파일로 저장하며 새 데이터의 주문일자 구간을 교체한다."""
    new_df = pd.DataFrame(rows, columns=_COLUMNS).astype(str)
    fallback_ym = target_date[:7]
    ym_series = order_ym(new_df["주문시각"])
    ym_series = ym_series.where(ym_series.ne(""), fallback_ym)

    saved: list[Path] = []
    for ym, group in new_df.groupby(ym_series, sort=True):
        group = group.reset_index(drop=True)
        stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}"
        existing = read_table(stem)
        group_dates = order_date(group["주문시각"])
        existing_dates = (
            order_date(existing["주문시각"])
            if existing is not None and not existing.empty
            else pd.Series(dtype=str)
        )
        combined, info = replace_covered_date_range(
            existing,
            group,
            existing_dates,
            group_dates,
        )
        if info["shrunk_dates"]:
            logger.warning(
                "재수집 구간 축소 의심: %s/%s ym=%s dates=%s",
                brand,
                store,
                ym,
                info["shrunk_dates"],
            )

        out_path = write_table(combined, stem)
        _record_reingest_dates("배민수동", store, group, group_dates, info)
        logger.info("저장 완료: %s (%d행)", out_path, len(combined))
        saved.append(out_path)

    if not saved:
        return (
            BAEMIN_ORDERS_DB
            / f"brand={brand}"
            / f"store={store}"
            / f"ym={fallback_ym}"
            / f"orders_{fallback_ym}.parquet"
        )
    return next((path for path in saved if f"ym={fallback_ym}" in str(path)), saved[0])
