"""
unified_sales - ToOrder pipeline.

Input:
- ANALYTICS_DB/toorder_daily_store_platform/toorder_store_platform_daily.parquet
  columns: date, store, platform, price

Output:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet
"""

import logging
from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, RAW_OKPOS_SALES
from modules.transform.utility.store_normalize import (
    normalize as _normalize_store_names,
    strip_brand as _strip_brand,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
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
