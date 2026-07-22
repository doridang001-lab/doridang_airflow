"""
unified_sales - ToOrder pipeline.

Input:
- ANALYTICS_DB/toorder_daily_store_platform/toorder_store_platform_daily.parquet
  columns: date, store, platform, price

Output:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet
"""

import logging
import re
from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, RAW_OKPOS_SALES
from modules.transform.utility.store_normalize import (
    normalize as _normalize_store_names,
    strip_brand as _strip_brand,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES,
    TOORDER_MANUAL_STORES,
    UNIFIED_COLUMNS,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _save_unified_daily,
    _to_int_series,
)

logger = logging.getLogger(__name__)

TOORDER_PARQUET = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
TOORDER_SOURCE = "toorder"
_MANUAL_DELIVERY_PLATFORMS = set().union(*DELIVERY_PLATFORM_FAMILIES.values())

_PLATFORM_MAP = {
    "\ubc30\ubbfc1": "\ubc30\ub2ec\uc758\ubbfc\uc871",
    "\ubc30\ub2ec\uc758\ubbfc\uc871": "\ubc30\ub2ec\uc758\ubbfc\uc871",
    "\ucfe0\ud321\uc774\uce20": "\ucfe0\ud321\uc774\uce20",
    "\uc694\uae30\uc694": "\uc694\uae30\uc694",
    "\ub561\uaca8\uc694": "\ub561\uaca8\uc694",
    "\ubc30\ubbfc": "\ubc30\ub2ec\uc758\ubbfc\uc871",
}

_ORDER_TYPE_MAP = {
    "\ubc30\ub2ec\uc758\ubbfc\uc871": "\uae30\ubcf8",
}

_BRAND_PREFIXES = {"\ub3c4\ub9ac\ub2f9", "\ud558\ud504\ub85c"}


@lru_cache(maxsize=1)
def _okpos_store_names() -> frozenset[str]:
    names: set[str] = set()
    base = RAW_OKPOS_SALES
    if not base.exists():
        return frozenset()

    for path in base.glob("brand=*/store=*"):
        if path.is_dir():
            store_name = path.name.split("store=", 1)[-1].strip()
            if store_name and store_name.endswith("\uc810"):
                names.add(store_name)
    return frozenset(names)


def _is_exact_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _extract_brand(normalized_name: str) -> str:
    parts = normalized_name.strip().split()
    return parts[0] if parts and parts[0] in _BRAND_PREFIXES else ""


def _load_parquet() -> pd.DataFrame:
    if not TOORDER_PARQUET.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(TOORDER_PARQUET).fillna("")
    except Exception as exc:
        logger.warning("toorder parquet load failed: %s", exc)
        return pd.DataFrame()
    if not {"date", "store", "platform", "price"}.issubset(df.columns):
        logger.warning("toorder parquet columns invalid: expected date, store, platform, price")
        return pd.DataFrame()
    return df


def _transform_df(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df = df[df["date"].astype(str).str.strip() == date_str].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df["price"] = _to_int_series(df["price"])
    df = df[df["price"] > 0].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df["_store_norm"] = _normalize_store_names(df["store"].fillna("").astype(str).str.strip())
    df["brand"] = df["_store_norm"].map(_extract_brand)
    df["store"] = _strip_brand(df["_store_norm"])
    df = df.drop(columns=["_store_norm"])

    okpos_stores = _okpos_store_names()
    if okpos_stores:
        before = len(df)
        df = df[~df["store"].astype(str).str.strip().isin(okpos_stores)].copy()
        dropped = before - len(df)
        if dropped:
            logger.info("toorder: okpos 매장 중복 제외 %d행 (stores=%s)", dropped, sorted(okpos_stores))
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df["platform"] = (
        df["platform"].fillna("").astype(str).str.strip().map(lambda v: _PLATFORM_MAP.get(v, v))
    )

    df = (
        df.groupby(["brand", "store", "platform"], as_index=False)
        .agg(price=("price", "sum"))
    )

    store_map = _load_store_map()
    df["_skey"] = df["store"].str.strip().str.split().str[-1]
    df["region"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    df["\ub2f4\ub2f9\uc790"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "\ub2f4\ub2f9\uc790"))
    df["\uc624\ud508\uc77c"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "\uc624\ud508\uc77c"))
    df = df.drop(columns=["_skey"])

    df["sale_date"] = date_str
    df["ym"] = date_str[:7]
    df["source"] = TOORDER_SOURCE
    df["order_type"] = df["platform"].map(lambda v: _ORDER_TYPE_MAP.get(v, "\ubc30\ub2ec"))

    store_part = df["store"].fillna("").astype(str).str.strip()
    platform_part = df["platform"].fillna("").astype(str).str.strip()
    df["order_id"] = (
        store_part.where(store_part != "", "unknown")
        + "_"
        + platform_part.where(platform_part != "", "unknown")
    )
    df["order_time"] = "00:00:00"
    df["menu_name"] = ""
    df["item_seq"] = "1"
    df["item_id"] = ""
    df["item_name"] = ""
    df["qty"] = "1"
    df["unit_price"] = df["price"]
    df["total_price"] = df["price"]
    df["discount_amount"] = 0
    df["sale_type"] = "\uc815\uc0c1"
    df["order_cnt"] = 1
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_pk"] = _make_unified_pk(df)

    return df.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def run_toorder(date_str: str, overwrite: bool = False) -> str:
    logger.info("toorder 채널 비활성화: unified_sales 적재 제외 (%s)", date_str)
    return f"SKIP: toorder 채널 비활성화 (unified_sales 제외) {date_str}"


def run_lookback_toorder(days: int = 7) -> str:
    return "SKIP: toorder 채널 비활성화 (unified_sales 제외)"


def backfill_toorder() -> str:
    return "SKIP: toorder 채널 비활성화 (unified_sales 제외)"


def _resolve_manual_stores(stores=None) -> list[str]:
    base = {str(store).strip() for store in TOORDER_MANUAL_STORES if str(store).strip()}
    if stores is None:
        return sorted(base)
    req = {str(store).strip() for store in stores if str(store).strip()}
    return sorted(base & req)


def run_toorder_manual_stores(date_str: str, stores=None, overwrite: bool = False) -> str:
    """POS 없는 수동매장의 배민·쿠팡 제외 채널을 toorder로 적재."""
    target = _resolve_manual_stores(stores)
    if not target:
        return f"SKIP: toorder 수동매장 대상 없음 ({date_str})"

    df = _transform_df(_load_parquet(), date_str)
    if df.empty:
        return f"SKIP: toorder {date_str} 해당일 데이터 없음"

    store = df["store"].fillna("").astype(str).str.strip()
    platform = df["platform"].fillna("").astype(str).str.strip()
    df = df[store.isin(target) & ~platform.isin(_MANUAL_DELIVERY_PLATFORMS)].copy()
    if df.empty:
        return f"SKIP: toorder {date_str} 대상 매장 비배민·쿠팡 채널 없음"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite, replace_stores=target)
    return f"toorder(수동매장) {date_str}: {saved}행 저장 (stores={target})"


def run_lookback_toorder_manual_stores(days: int = 14, stores=None) -> str:
    today = datetime.now()
    total = 0
    for i in range(days):
        date_str = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            result = run_toorder_manual_stores(date_str, stores=stores, overwrite=False)
            logger.info(result)
            if result.startswith("toorder(수동매장)"):
                try:
                    total += int(result.split(":", 1)[1].strip().split("행", 1)[0].strip())
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("toorder(수동매장) lookback error: %s | %s", date_str, exc)
    return f"toorder(수동매장) lookback({days}일): {total}행 저장"


def backfill_toorder_manual_stores(stores=None) -> str:
    target = _resolve_manual_stores(stores)
    if not target:
        return "SKIP: toorder 수동매장 대상 없음"

    df_all = _load_parquet()
    if df_all.empty:
        return "SKIP: toorder parquet 없음"

    pattern = "|".join(re.escape(store) for store in target)
    store_col = df_all["store"].fillna("").astype(str)
    sub = df_all[store_col.str.contains(pattern, na=False)]
    dates = sorted({str(date).strip() for date in sub["date"].dropna().unique() if str(date).strip()})
    if not dates:
        return "SKIP: 대상 매장 유효 date 없음"

    total = 0
    for date_str in dates:
        try:
            result = run_toorder_manual_stores(date_str, stores=stores, overwrite=True)
            logger.info(result)
            if result.startswith("toorder(수동매장)"):
                try:
                    total += int(result.split(":", 1)[1].strip().split("행", 1)[0].strip())
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("toorder(수동매장) backfill 실패: %s | %s", date_str, exc)
    return f"toorder(수동매장) backfill 완료: {len(dates)}일 / {total}행 저장"
