"""쿠팡이츠 주문내역을 토더(ToOrder) 매출과 교차검증한다.

흐름:
  1. OneDrive toorder_daily_sales CSV → 쿠팡이츠 전체 합계 (도리당+나홀로 등 모든 브랜드)
  2. 쿠팡 orders DB → 수집된 매장 합계 = sum(매출액) - sum(취소금액)
  3. 총액 단위 비교 (브랜드별 매장명 불일치 문제 우회)
  4. 불일치 → 알림 (DAG 레이어에서 처리)
"""

import logging
import time
from datetime import datetime

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, COUPANG_ORDERS_DB

logger = logging.getLogger(__name__)

TOORDER_DAILY_DIR = ANALYTICS_DB / "toorder_daily_sales"


# ============================================================
# Step 1: ToOrder CSV → 쿠팡이츠 매장별 합계
# ============================================================

def _toorder_coupang_by_store(target_date: str) -> dict:
    """toorder_daily_sales CSV에서 쿠팡이츠 매장별 매출액 합계를 반환한다.

    Returns:
        {store_name: amount(int)}  파일 없으면 {}
    """
    date_str = target_date.replace("-", "")
    csv_path = TOORDER_DAILY_DIR / f"toorder_daily_sales_{date_str}.csv"
    if not csv_path.exists():
        logger.warning("ToOrder CSV 없음: %s", csv_path)
        return {}
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        mask = df["플랫폼명"] == "쿠팡이츠"
        by_store = (
            df.loc[mask]
            .groupby("매장명")["매출액"]
            .sum()
            .apply(int)
            .to_dict()
        )
        logger.info("ToOrder 쿠팡이츠 매장 %d개 (target_date=%s): %s", len(by_store), target_date, sorted(by_store))
        return by_store
    except Exception as exc:
        logger.error("ToOrder CSV 읽기 실패: %s / %s", csv_path, exc)
        return {}


# ============================================================
# Step 2: 쿠팡 orders DB → 매장별 실매출액
# ============================================================

def _coupang_orders_by_store(target_date: str, allowed_stores: set | None = None) -> dict:
    """쿠팡 orders DB에서 매장별 실매출액 = 정상주문 합계 - 취소주문 합계를 반환한다.

    order_date 형식: "2026.05.28 03:47" 또는 "2026.5.28 03:47"
    저장 경로: COUPANG_ORDERS_DB/brand=*/store=*/ym={ym}/orders_{ym}.csv

    Args:
        allowed_stores: 집계할 store 이름 집합. None이면 전체 스캔.

    Returns:
        {store_name: 실매출액(int)}
    """
    ym = target_date[:7]
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    date_patterns = [
        target_date.replace("-", "."),
        f"{dt.year}.{dt.month}.{dt.day}",
        f"{dt.year}.{dt.month:02d}.{dt.day:02d}",
    ]

    csv_paths = list(COUPANG_ORDERS_DB.glob(f"brand=*/store=*/ym={ym}/orders_{ym}.csv"))
    logger.info("쿠팡 orders CSV: %d개 (ym=%s)", len(csv_paths), ym)

    result = {}
    for csv_path in csv_paths:
        store = csv_path.parts[-3][len("store="):]
        if allowed_stores is not None and store not in allowed_stores:
            logger.info("validate 스킵 (수집 대상 외): %s", store)
            continue
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
            if df.empty:
                continue

            mask = df["order_date"].apply(
                lambda x: any(p in str(x) for p in date_patterns)
            )
            daily = df[mask]
            if daily.empty:
                continue

            all_price = (
                pd.to_numeric(
                    daily["total_price"].str.replace(",", "", regex=False),
                    errors="coerce",
                )
                .fillna(0)
            )
            cancel_mask = daily["is_cancelled"].astype(str).str.strip().str.upper() == "Y"
            매출액 = int(all_price[~cancel_mask].sum())
            취소금액 = int(all_price[cancel_mask].sum())
            실매출액 = 매출액 - 취소금액
            result[store] = 실매출액
            logger.info(
                "[%s] 실매출액=%d (매출액=%d, 취소금액=%d)",
                store, 실매출액, 매출액, 취소금액,
            )
        except Exception as exc:
            logger.warning("CSV 읽기 실패: %s / %s", csv_path, exc)

    logger.info("쿠팡 orders 집계 매장: %d개 (target_date=%s)", len(result), target_date)
    return result


# ============================================================
# 공개 오케스트레이션
# ============================================================

def validate_coupang_toorder(target_date: str, allowed_stores: set | None = None) -> dict:
    """ToOrder CSV 쿠팡이츠 전체 합계와 수집된 쿠팡 orders 합계를 비교한다.

    ToOrder에는 도리당·나홀로 등 브랜드별 매장명이 별도 행으로 존재하므로
    per-store 비교 대신 총액 단위로 비교한다.

    Returns:
        {
            "store_results":     {"__total__": {"toorder": int, "coupang": int, "matched": bool,
                                                "toorder_breakdown": {store: amount}}},
            "mismatched_stores": [],
            "toorder_gap_stores":[],
            "matched":           bool,
            "compared_count":    int,   # 0 or 1
        }
    """
    result: dict = {
        "store_results": {},
        "mismatched_stores": [],
        "toorder_gap_stores": [],
        "matched": False,
        "compared_count": 0,
    }

    toorder_by_store = _toorder_coupang_by_store(target_date)
    coupang_by_store = _coupang_orders_by_store(target_date, allowed_stores=allowed_stores)

    if not toorder_by_store:
        logger.warning("ToOrder CSV 없음 또는 쿠팡이츠 데이터 없음 — 검증 건너뜀")
        return result

    if not coupang_by_store:
        logger.warning("쿠팡 orders 없음 — 검증 건너뜀")
        return result

    # 총액 비교: ToOrder 쿠팡이츠 전체(도리당+나홀로 합산) vs 수집된 쿠팡 전체
    toorder_total = sum(toorder_by_store.values())
    coupang_total = sum(coupang_by_store.values())

    logger.info(
        "ToOrder 쿠팡이츠 총액=%d (%d개 매장) / 쿠팡 수집 총액=%d (%d개 매장)",
        toorder_total, len(toorder_by_store),
        coupang_total, len(coupang_by_store),
    )

    if toorder_total == 0 and coupang_total > 0:
        result["toorder_gap_stores"] = ["__total__"]
        result["store_results"]["__total__"] = {
            "toorder": toorder_total,
            "coupang": coupang_total,
            "matched": False,
            "toorder_gap": True,
            "toorder_breakdown": toorder_by_store,
        }
        result["compared_count"] = 1
        logger.warning("ToOrder 갭: ToOrder=0 / 쿠팡=%d", coupang_total)
        return result

    matched = toorder_total == coupang_total
    result["store_results"]["__total__"] = {
        "toorder": toorder_total,
        "coupang": coupang_total,
        "matched": matched,
        "toorder_gap": False,
        "toorder_breakdown": toorder_by_store,
    }
    result["compared_count"] = 1

    if not matched:
        result["mismatched_stores"] = ["__total__"]
        logger.warning(
            "총액 불일치: ToOrder=%d / 쿠팡=%d (차이=%d)",
            toorder_total, coupang_total, toorder_total - coupang_total,
        )
    else:
        logger.info("총액 일치: %d원", toorder_total)

    result["matched"] = matched
    return result


# ============================================================
# CMG vs ToOrder 검증
# ============================================================

CMG_MARKETING_DIR = ANALYTICS_DB / "coupang_marketing"


def _compare_cmg_once(target_date: str) -> dict:
    """CMG 파티션 전체매출 vs ToOrder 매출액(플랫폼명=쿠팡이츠) 1회 비교."""
    # ToOrder
    date_str = target_date.replace("-", "")
    toorder_csv = TOORDER_DAILY_DIR / f"toorder_daily_sales_{date_str}.csv"
    if not toorder_csv.exists():
        logger.warning("ToOrder CSV 없음: %s", toorder_csv)
        return {"matched": None, "reason": "toorder 파일 없음"}

    to_df = pd.read_csv(toorder_csv, dtype=str, encoding="utf-8-sig")
    to_df = to_df[to_df["플랫폼명"].str.strip() == "쿠팡이츠"]
    to_df["매출액"] = pd.to_numeric(to_df["매출액"], errors="coerce").fillna(0)
    toorder_by_store: dict = to_df.groupby("매장명")["매출액"].sum().apply(int).to_dict()

    # CMG parquet (ym 기준 파티션)
    ym = target_date[:7]
    cmg_files = list(CMG_MARKETING_DIR.glob(f"brand=*/store=*/ym={ym}/data.csv"))
    if not cmg_files:
        logger.warning("CMG 파티션 없음: ym=%s", ym)
        return {"matched": None, "reason": "CMG 파티션 없음"}

    cmg_df = pd.concat([pd.read_csv(f, dtype=str) for f in cmg_files], ignore_index=True)
    cmg_df = cmg_df[cmg_df.get("조회일자", pd.Series(dtype=str)).str.strip() == target_date]
    cmg_df["전체매출"] = pd.to_numeric(
        cmg_df["전체매출"].str.replace(",", "", regex=False), errors="coerce"
    ).fillna(0)
    cmg_by_store: dict = cmg_df.groupby("매장명")["전체매출"].sum().apply(int).to_dict()

    all_stores = sorted(set(toorder_by_store) | set(cmg_by_store))
    store_results = {}
    mismatches = []
    for store in all_stores:
        to_val = int(toorder_by_store.get(store, 0))
        cmg_val = int(cmg_by_store.get(store, 0))
        matched = to_val == cmg_val
        store_results[store] = {"toorder": to_val, "cmg": cmg_val, "matched": matched}
        if not matched:
            mismatches.append(store)

    toorder_total = int(sum(toorder_by_store.values()))
    cmg_total = int(sum(cmg_by_store.values()))
    logger.info(
        "CMG검증 %s: ToOrder=%d / CMG=%d / 불일치=%s",
        target_date, toorder_total, cmg_total, mismatches or "없음",
    )
    return {
        "matched": len(mismatches) == 0,
        "toorder_total": toorder_total,
        "cmg_total": cmg_total,
        "mismatches": mismatches,
        "store_results": store_results,
        "target_date": target_date,
    }


def validate_cmg_vs_toorder(target_date: str, retry: int = 1) -> dict:
    """CMG 파티션 전체매출 vs ToOrder(쿠팡이츠) 비교. 불일치 시 30초 후 1회 재시도."""
    for attempt in range(retry + 1):
        result = _compare_cmg_once(target_date)
        if result.get("matched") is not False or attempt >= retry:
            return result
        logger.warning("CMG vs ToOrder 불일치 → 30초 후 재시도 (%d/%d)", attempt + 1, retry)
        time.sleep(30)
    return result
