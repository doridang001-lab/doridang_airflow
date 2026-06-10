"""
unified_sales - ToOrder pipeline.

Input:
- ANALYTICS_DB/toorder_daily_store_platform/toorder_store_platform_daily.csv
  columns: date, store, platform, price

Output:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet

Behavior:
- One row per store/platform/date becomes one unified_sales row
- Range tokens such as `YYYY-MM-DD~YYYY-MM-DD` are skipped in backfill
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB
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

TOORDER_CSV = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.csv"
TOORDER_SOURCE = "toorder"

_PLATFORM_MAP = {
    "배민1": "배달의민족",
    "배달의민족": "배달의민족",
    "쿠팡이츠": "쿠팡이츠",
    "요기요": "요기요",
    "땡겨요": "땡겨요",
    "배민": "배달의민족",
}

_ORDER_TYPE_MAP = {
    "배달의민족": "기본",
}

_BRAND_PREFIXES = {"도리당", "나홀로"}


def _is_exact_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _extract_brand(normalized_name: str) -> str:
    parts = normalized_name.strip().split()
    return parts[0] if parts and parts[0] in _BRAND_PREFIXES else ""


def _load_csv() -> pd.DataFrame:
    if not TOORDER_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(TOORDER_CSV, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("toorder CSV load failed: %s", exc)
        return pd.DataFrame()
    if not {"date", "store", "platform", "price"}.issubset(df.columns):
        logger.warning("toorder CSV columns invalid: expected date, store, platform, price")
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
    df["담당자"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    df["실오픈일"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    df = df.drop(columns=["_skey"])

    df["sale_date"] = date_str
    df["ym"] = date_str[:7]
    df["source"] = TOORDER_SOURCE
    df["order_type"] = df["platform"].map(lambda v: _ORDER_TYPE_MAP.get(v, "배달"))

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
    df["sale_type"] = "정상"
    df["order_cnt"] = 1
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_pk"] = _make_unified_pk(df)

    return df.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def run_toorder(date_str: str, overwrite: bool = False) -> str:
    df_all = _load_csv()
    if df_all.empty:
        raise FileNotFoundError(f"toorder CSV not found: {TOORDER_CSV}")

    df = _transform_df(df_all, date_str)
    if df.empty:
        return f"SKIP: toorder {date_str} no matching rows"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    return f"toorder {date_str}: {saved} rows"


def run_lookback_toorder(days: int = 7) -> str:
    df_all = _load_csv()
    if df_all.empty:
        return "SKIP: toorder CSV not found"

    total = 0
    today = datetime.now()
    for i in range(days):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            result = run_toorder(d, overwrite=True)
            logger.info(result)
            try:
                total += int(result.split(":")[1].strip().split()[0])
            except Exception:
                pass
        except FileNotFoundError:
            logger.info("toorder lookback skip: %s", d)
        except Exception as exc:
            logger.warning("toorder lookback error: %s | %s", d, exc)

    return f"toorder lookback({days} days): {total} rows"


def backfill_toorder() -> str:
    df_all = _load_csv()
    if df_all.empty:
        return "SKIP: toorder CSV not found"

    raw_dates = sorted({str(d).strip() for d in df_all["date"].dropna().unique() if str(d).strip()})
    dates: list[str] = []
    skipped_ranges: list[str] = []
    for raw in raw_dates:
        if _is_exact_date(raw):
            dates.append(raw)
            continue
        if "~" in raw:
            skipped_ranges.append(raw)
            continue
        logger.warning("toorder backfill skip invalid date token: %s", raw)

    if not dates:
        return "SKIP: no valid dates"
    if skipped_ranges:
        logger.warning("toorder backfill skip range tokens: %s", skipped_ranges[:10])

    total = 0
    for date_str in dates:
        try:
            result = run_toorder(date_str, overwrite=True)
            logger.info(result)
            try:
                total += int(result.split(":")[1].strip().split()[0])
            except Exception:
                pass
        except Exception as exc:
            logger.warning("toorder backfill failed: %s | %s", date_str, exc)

    return f"toorder backfill complete: {len(dates)} days / {total} rows"
