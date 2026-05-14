"""
unified_sales - 홀용 시각화 전용 누적 테이블 생성/갱신.

산출 컬럼:
  - order_type
  - sale_date
  - 주문수_t   : count(distinct order_id)
  - 총매출_t   : sum(unit_price)
  - 객단가_t   : 총매출_t / 주문수_t

저장:
  - 1개의 누적 parquet 파일로 계속 갱신(upsert)되는 구조
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT, _unified_daily_path
from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

HALL_VIZ_DIR = MART_DB / "unified_sales_hall_viz"
HALL_VIZ_PATH = HALL_VIZ_DIR / "hall_viz.parquet"

HALL_VIZ_COLUMNS = ["order_type", "sale_date", "주문수_t", "총매출_t", "객단가_t"]

HALL_ORDER_TYPES = {"홀_테이블", "홀_포장"}


def _aggregate_one_day(date_str: str) -> pd.DataFrame:
    path = _unified_daily_path(date_str)
    if not path.exists():
        return pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.warning("unified_sales parquet 로드 실패, 스킵: %s | %s", path, exc)
        return pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    if df.empty:
        return pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    # 필수 컬럼 방어 (구버전 parquet 포함)
    # - total_price가 없으면 unit_price로 fallback
    for col in ("sale_date", "order_type", "order_id", "unit_price"):
        if col not in df.columns:
            logger.warning("필수 컬럼 누락(%s), 스킵: %s", col, path)
            return pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    if "sale_type" not in df.columns and "구분" in df.columns:
        df = df.copy()
        df["sale_type"] = df["구분"]

    # 홀 전용 + 정상만
    df["order_type"] = df["order_type"].fillna("").astype(str).str.strip()
    df["sale_date"] = df["sale_date"].fillna("").astype(str).str.strip()
    df["order_id"] = df["order_id"].fillna("").astype(str).str.strip()
    df["sale_type"] = df.get("sale_type", "").astype(str).str.strip()

    df = df[df["order_type"].isin(HALL_ORDER_TYPES)].copy()
    df = df[df["sale_type"].ne("취소")].copy()
    df = df[df["order_id"].ne("")].copy()
    if df.empty:
        return pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    if "total_price" in df.columns:
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0).astype(int)
    amt_col = "total_price" if "total_price" in df.columns else "unit_price"

    agg = (
        df.groupby(["sale_date", "order_type"], dropna=False)
        .agg(주문수_t=("order_id", "nunique"), 총매출_t=(amt_col, "sum"))
        .reset_index()
    )
    agg["객단가_t"] = (agg["총매출_t"] / agg["주문수_t"]).replace([float("inf")], 0).fillna(0).round(2)
    agg = agg[["order_type", "sale_date", "주문수_t", "총매출_t", "객단가_t"]]
    return agg.reindex(columns=HALL_VIZ_COLUMNS, fill_value=0)


def _upsert_rows(new_rows: pd.DataFrame) -> int:
    if new_rows is None or new_rows.empty:
        return 0

    HALL_VIZ_DIR.mkdir(parents=True, exist_ok=True)

    if HALL_VIZ_PATH.exists():
        try:
            existing = pd.read_parquet(HALL_VIZ_PATH)
        except Exception as exc:
            logger.warning("hall_viz parquet 로드 실패(새로 생성): %s | %s", HALL_VIZ_PATH, exc)
            existing = pd.DataFrame(columns=HALL_VIZ_COLUMNS)
    else:
        existing = pd.DataFrame(columns=HALL_VIZ_COLUMNS)

    existing = existing.reindex(columns=HALL_VIZ_COLUMNS, fill_value=0)
    new_rows = new_rows.reindex(columns=HALL_VIZ_COLUMNS, fill_value=0)

    # 동일 (sale_date, order_type) 는 새 데이터로 교체
    key_cols = ["sale_date", "order_type"]
    existing["_key"] = existing[key_cols].astype(str).agg("|".join, axis=1)
    new_rows["_key"] = new_rows[key_cols].astype(str).agg("|".join, axis=1)

    keep = ~existing["_key"].isin(set(new_rows["_key"].tolist()))
    merged = pd.concat([existing.loc[keep].drop(columns=["_key"]), new_rows.drop(columns=["_key"])], ignore_index=True)

    merged["sale_date"] = merged["sale_date"].astype(str).str.strip()
    merged["order_type"] = merged["order_type"].astype(str).str.strip()
    merged["주문수_t"] = pd.to_numeric(merged["주문수_t"], errors="coerce").fillna(0).astype(int)
    merged["총매출_t"] = pd.to_numeric(merged["총매출_t"], errors="coerce").fillna(0).astype(int)
    merged["객단가_t"] = pd.to_numeric(merged["객단가_t"], errors="coerce").fillna(0.0).astype(float)

    merged = merged.sort_values(["sale_date", "order_type"]).reset_index(drop=True)
    merged.to_parquet(HALL_VIZ_PATH, index=False, engine="pyarrow")
    return len(new_rows)


def run_hall_viz(date_str: str) -> str:
    """특정 sale_date 1일치 집계 후 누적 파일에 upsert."""
    new_rows = _aggregate_one_day(date_str)
    upserted = _upsert_rows(new_rows)
    result = f"hall_viz upsert | {date_str} | {upserted} rows"
    logger.info(result)
    return result


def run_lookback_hall_viz(days: int = 7) -> str:
    """최근 N일치를 집계 후 누적 파일에 upsert."""
    try:
        import pendulum  # type: ignore
    except ModuleNotFoundError:
        from datetime import timedelta
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("Asia/Seoul"))
        date_iter = ((now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1))
    else:
        kst = pendulum.timezone("Asia/Seoul")
        date_iter = (pendulum.now(kst).subtract(days=i).format("YYYY-MM-DD") for i in range(1, days + 1))

    total = 0
    for date_str in date_iter:
        new_rows = _aggregate_one_day(date_str)
        total += _upsert_rows(new_rows)
    result = f"hall_viz lookback({days}d) | upsert {total} rows"
    logger.info(result)
    return result


def backfill_hall_viz() -> str:
    """UNIFIED_ROOT의 모든 unified_sales_*.parquet 기준으로 hall_viz를 재생성(overwrite)."""
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
    if not files:
        msg = f"hall_viz backfill 스킵 (unified_sales parquet 없음) | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    parts: list[pd.DataFrame] = []
    for p in files:
        # unified_sales_YYMMDD.parquet -> YYYY-MM-DD
        name = p.name
        try:
            yymmdd = name.split("unified_sales_", 1)[1].split(".", 1)[0]
            date_str = f"{2000 + int(yymmdd[:2])}-{yymmdd[2:4]}-{yymmdd[4:6]}"
        except Exception:
            continue
        part = _aggregate_one_day(date_str)
        if not part.empty:
            parts.append(part)

    if not parts:
        msg = "hall_viz backfill 완료(집계 데이터 없음)"
        logger.info(msg)
        return msg

    df = pd.concat(parts, ignore_index=True)
    df = df.groupby(["sale_date", "order_type"], dropna=False, as_index=False).agg(
        주문수_t=("주문수_t", "sum"),
        총매출_t=("총매출_t", "sum"),
    )
    df["객단가_t"] = (df["총매출_t"] / df["주문수_t"]).replace([float("inf")], 0).fillna(0).round(2)
    df = df.reindex(columns=HALL_VIZ_COLUMNS, fill_value=0).sort_values(["sale_date", "order_type"]).reset_index(drop=True)

    HALL_VIZ_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(HALL_VIZ_PATH, index=False, engine="pyarrow")
    result = f"hall_viz backfill 완료 | dates={df['sale_date'].nunique()} rows={len(df)}"
    logger.info(result)
    return result
