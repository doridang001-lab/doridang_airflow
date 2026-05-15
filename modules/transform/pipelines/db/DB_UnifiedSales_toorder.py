"""
unified_sales - ToOrder 채널 전용 모듈.

입력:
- ANALYTICS_DB/toorder_daily_store_platform/toorder_store_platform_daily.csv
  컬럼: date, store, platform, price

출력:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet

처리 방식:
- 일별 플랫폼 집계 1행 → unified_sales 1행 (order_cnt=1, total_price=price)
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
    "배민1":        "배달의민족",
    "배달의민족":   "배달의민족",
    "쿠팽이츠":     "쿠팡이츠",
    "쿠팡이츠":     "쿠팡이츠",
    "땡겨요":       "땡겨요",
    "요기요":       "요기요",
    "네이버":       "네이버주문",
    "포장":         "포장",
    "배민 포장":    "배민 포장",
}

# 플랫폼별 order_type 분류 (미등재 시 기본값 "배달")
_ORDER_TYPE_MAP = {
    "네이버주문":   "기타",
    "배민 포장":    "배달_포장",
}

_BRAND_PREFIXES = {"도리당", "나홀로"}


def _extract_brand(normalized_name: str) -> str:
    parts = normalized_name.strip().split()
    return parts[0] if parts and parts[0] in _BRAND_PREFIXES else ""


def _load_csv() -> pd.DataFrame:
    if not TOORDER_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(TOORDER_CSV, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("toorder CSV 로드 실패: %s", e)
        return pd.DataFrame()
    if not {"date", "store", "platform", "price"}.issubset(df.columns):
        logger.warning("toorder CSV 컬럼 오류 (필요: date, store, platform, price)")
        return pd.DataFrame()
    return df


def _transform_df(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df = df[df["date"].str.strip() == date_str].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df["price"] = _to_int_series(df["price"])
    df = df[df["price"] > 0].copy()  # 매출 없는 행(price=0/빈값) 제외
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df["_store_norm"] = _normalize_store_names(df["store"].fillna("").astype(str).str.strip())
    df["brand"] = df["_store_norm"].map(_extract_brand)
    df["store"] = _strip_brand(df["_store_norm"])
    df = df.drop(columns=["_store_norm"])

    df["platform"] = (
        df["platform"].fillna("").astype(str).str.strip()
        .map(lambda v: _PLATFORM_MAP.get(v, v))
    )

    # 같은 (store, platform) 조합 합산 (배민1 + 배달의민족 → 배달의민족 1행)
    agg_keys = ["brand", "store", "platform"]
    df = (
        df.groupby(agg_keys, as_index=False)
        .agg(price=("price", "sum"))
    )

    store_map = _load_store_map()
    df["_skey"]  = df["store"].str.strip().str.split().str[-1]
    df["담당자"]  = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    df["region"]  = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    df["실오픈일"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    df = df.drop(columns=["_skey"])

    df["sale_date"]       = date_str
    df["ym"]              = date_str[:7]
    df["source"]          = TOORDER_SOURCE
    df["order_type"]      = df["platform"].map(lambda v: _ORDER_TYPE_MAP.get(v, "배달"))
    store_part            = df["store"].fillna("").str.strip()
    platform_part         = df["platform"].fillna("").str.strip()
    df["order_id"]        = (
        store_part.where(store_part != "", "unknown")
        + "_"
        + platform_part.where(platform_part != "", "unknown")
    )
    df["order_time"]      = "00:00:00"
    df["menu_name"]       = ""
    df["item_seq"]        = "1"
    df["item_id"]         = ""
    df["item_name"]       = ""
    df["qty"]             = "1"
    df["unit_price"]      = df["price"]
    df["total_price"]     = df["price"]
    df["discount_amount"] = 0
    df["sale_type"]       = "정상"
    df["order_cnt"]       = 1
    df["collected_at"]    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_pk"]             = _make_unified_pk(df)

    return df.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def run_toorder(date_str: str, overwrite: bool = False) -> str:
    df_all = _load_csv()
    if df_all.empty:
        raise FileNotFoundError(f"toorder CSV 없음: {TOORDER_CSV}")
    df = _transform_df(df_all, date_str)
    if df.empty:
        return f"SKIP: toorder {date_str} 해당일 데이터 없음"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    return f"toorder {date_str}: {saved}행 저장"


def run_lookback_toorder(days: int = 7) -> str:
    df_all = _load_csv()
    if df_all.empty:
        return "SKIP: toorder CSV 없음"
    total = 0
    today = datetime.now()
    for i in range(days):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            result = run_toorder(d, overwrite=True)
            logger.info(result)
            try:
                total += int(result.split(":")[1].strip().split("행")[0].strip())
            except Exception:
                pass
        except FileNotFoundError:
            logger.info("toorder lookback skip: %s", d)
        except Exception as e:
            logger.warning("toorder lookback error: %s | %s", d, e)
    return f"toorder lookback({days}일): {total}행 저장"


def backfill_toorder() -> str:
    df_all = _load_csv()
    if df_all.empty:
        return "SKIP: toorder CSV 없음"
    dates = sorted({str(d).strip() for d in df_all["date"].dropna().unique() if str(d).strip()})
    if not dates:
        return "SKIP: 유효한 date 없음"
    total = 0
    for date_str in dates:
        try:
            result = run_toorder(date_str, overwrite=True)
            logger.info(result)
            try:
                total += int(result.split(":")[1].strip().split("행")[0].strip())
            except Exception:
                pass
        except Exception as e:
            logger.warning("toorder backfill 실패: %s | %s", date_str, e)
    return f"toorder backfill 완료: {len(dates)}일 / {total}행 저장"
