"""Build Baemin NOW unified mart parquet."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable

import pandas as pd

from modules.transform.pipelines.db.DB_BaeminManual_load import normalize_baemin_now_schema
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files
from modules.transform.utility.paths import (
    BAEMIN_METRICS_DB,
    BAEMIN_NOW_GRP_PARQUET,
)

logger = logging.getLogger(__name__)

_PARTITION_COLUMNS = ("brand", "store")
_SORT_COLUMNS = ["date", "brand", "store", "collected_at", "store_id"]


def _partition_value(path: Path, key: str) -> str:
    token = f"{key}="
    for part in path.parts:
        if part.startswith(token):
            return part[len(token) :].strip()
    return ""


def _read_now_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    for col in _PARTITION_COLUMNS:
        partition = _partition_value(path, col)
        if not partition:
            continue
        if col not in df.columns:
            df[col] = partition
            continue
        values = df[col].fillna("").astype(str).str.strip()
        df[col] = values
        df.loc[values.eq(""), col] = partition
    return normalize_baemin_now_schema(df)


def _dedupe_now_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.fillna("").astype(str).copy()
    for col in ("date", "brand", "store", "brand_store", "store_id", "collected_at"):
        if col not in out.columns:
            out[col] = ""

    out["_dedupe_brand_store"] = out["brand_store"].where(
        out["brand_store"].str.strip().ne(""),
        out["brand"].str.strip() + "|" + out["store"].str.strip(),
    )
    out["_dedupe_store_key"] = out["_dedupe_brand_store"].where(
        out["_dedupe_brand_store"].str.strip().ne("|"),
        out["store_id"].str.strip(),
    )
    out["_collected_at_sort"] = pd.to_datetime(out["collected_at"], errors="coerce", utc=True)
    out["_source_order"] = range(len(out))

    key_cols = ["date", "_dedupe_store_key"]
    out = out.sort_values(["date", "_dedupe_store_key", "_collected_at_sort", "_source_order"])
    out = out.drop_duplicates(subset=key_cols, keep="last")
    out = out.drop(columns=["_dedupe_brand_store", "_dedupe_store_key", "_collected_at_sort", "_source_order"])
    return normalize_baemin_now_schema(out)


def _unified_file_date(path: Path) -> str:
    stem = path.stem
    prefix = "unified_sales_"
    if not stem.startswith(prefix):
        return ""
    ymd = stem[len(prefix) :]
    if len(ymd) != 6 or not ymd.isdigit():
        return ""
    return f"20{ymd[:2]}-{ymd[2:4]}-{ymd[4:6]}"


def _read_store_sales_totals(
    unified_sales_files: Iterable[Path] | None,
    *,
    min_date: str,
    max_date: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    files = list(unified_sales_files) if unified_sales_files is not None else iter_unified_sales_files()

    for path in files:
        file_date = _unified_file_date(Path(path))
        if file_date and (file_date < min_date or file_date > max_date):
            continue
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "total_price"])
        except Exception as exc:
            logger.warning("UnifiedSales 매출 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["date", "store", "sales_total"])

    sales = pd.concat(frames, ignore_index=True)
    sales["date"] = sales["sale_date"].fillna("").astype(str).str.strip()
    sales["store"] = sales["store"].fillna("").astype(str).str.strip()
    amount = sales["total_price"].fillna("").astype(str).str.replace(",", "", regex=False).str.strip()
    sales["sales_total"] = pd.to_numeric(amount, errors="coerce").fillna(0)
    sales = sales[sales["date"].between(min_date, max_date) & sales["store"].ne("")]
    if sales.empty:
        return pd.DataFrame(columns=["date", "store", "sales_total"])

    grouped = (
        sales.groupby(["date", "store"], as_index=False)["sales_total"]
        .sum()
        .round({"sales_total": 0})
    )
    grouped["sales_total"] = grouped["sales_total"].astype(int)
    return grouped


def _attach_store_sales_total(
    df: pd.DataFrame,
    unified_sales_files: Iterable[Path] | None = None,
) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["sales_total"] = pd.Series(dtype="int64")
        return normalize_baemin_now_schema(out)

    out["date"] = out["date"].fillna("").astype(str).str.strip() if "date" in out.columns else ""
    out["store"] = out["store"].fillna("").astype(str).str.strip() if "store" in out.columns else ""
    valid_dates = out["date"][out["date"].ne("")]
    if valid_dates.empty:
        out["sales_total"] = 0
        return normalize_baemin_now_schema(out)

    sales = _read_store_sales_totals(
        unified_sales_files,
        min_date=str(valid_dates.min()),
        max_date=str(valid_dates.max()),
    )
    out = out.merge(sales, on=["date", "store"], how="left")
    out["sales_total"] = pd.to_numeric(out["sales_total"], errors="coerce").fillna(0).astype(int)
    return normalize_baemin_now_schema(out)


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def build_baemin_now_grp(
    source_root: Path = BAEMIN_METRICS_DB,
    output_path: Path = BAEMIN_NOW_GRP_PARQUET,
    unified_sales_files: Iterable[Path] | None = None,
) -> str:
    """Merge stored Baemin NOW CSV files into one daily stacked parquet mart."""
    files = sorted(source_root.glob("brand=*/store=*/ym=*/baemin_now.csv"))
    parts: list[pd.DataFrame] = []
    errors: list[str] = []

    for path in files:
        try:
            df = _read_now_csv(path)
        except Exception as exc:
            logger.warning("배민 NOW CSV 로드 실패, 스킵: %s | %s", path, exc)
            errors.append(str(path))
            continue
        if not df.empty:
            parts.append(df)

    if parts:
        combined = pd.concat(parts, ignore_index=True)
        combined = _dedupe_now_rows(combined)
    else:
        combined = normalize_baemin_now_schema(pd.DataFrame())

    combined = _attach_store_sales_total(combined, unified_sales_files=unified_sales_files)

    sort_cols = [col for col in _SORT_COLUMNS if col in combined.columns]
    if sort_cols and not combined.empty:
        combined = combined.sort_values(sort_cols).reset_index(drop=True)

    _write_parquet_atomic(combined, output_path)
    message = (
        f"배민 NOW 통합 mart 저장 완료: files={len(files)}, rows={len(combined)}, "
        f"errors={len(errors)}, path={output_path}"
    )
    logger.info(message)
    return message
