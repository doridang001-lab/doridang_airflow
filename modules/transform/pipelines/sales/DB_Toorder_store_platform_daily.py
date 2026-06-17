"""
ToOrder datedetail store/platform daily sales collection pipeline.

Downloads or ingests monthly ToOrder datedetail workbooks, parses every
store/platform/date sheet, and upserts rows into a Parquet dataset.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum

from modules.extract.crawling_toorder_sales_report import run_crawling_datedetail_months
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR

logger = logging.getLogger(__name__)

_TOORDER_ID = os.getenv("TOORDER_ID", "doridang15")
_TOORDER_PW = os.getenv("TOORDER_PW", "ehfl5233!")
_DEFAULT_DEST = ANALYTICS_DB / "toorder_daily_store_platform"
_PARQUET_NAME = "toorder_store_platform_daily.parquet"
_DATEDETAIL_PREFIX = "\uc885\ud569\ubcf4\uace0\uc11c_\uc77c\ubcc4\uc0c1\uc138_\ub9e4\ucd9c\ubcf4\uace0\uc11c"
_TOTAL_SHEET = "\uc885\ud569"
_TOTAL_MARKER = "\ud569\uacc4"
_TEST_STORE = "\ud14c\uc2a4\ud2b8\ub9e4\uc7a5"
_STORE_COL_START = 5
_STORE_COL_STRIDE = 4
_DATA_ROW_START = 6
_OUTPUT_COLUMNS = ["date", "store", "platform", "price", "receipts_num"]


def run_toorder_store_platform_daily(
    *,
    target_date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sale_dates: list[str] | None = None,
    dest_dir: str | Path | None = None,
    toorder_id: str | None = None,
    toorder_pw: str | None = None,
    manual_dir: str | Path | None = None,
    log_prefix: str = "",
    **_: object,
) -> str:
    """Collect datedetail data and upsert it into Parquet.

    Backward-compatible arguments such as ``target_date`` and ``sale_dates`` are
    accepted, but collection is executed by month spans.
    """
    resolved_from, resolved_to = _resolve_date_bounds(
        target_date=target_date,
        date_from=date_from,
        date_to=date_to,
        sale_dates=sale_dates,
    )
    resolved_dest = Path(dest_dir) if dest_dir else _DEFAULT_DEST
    resolved_manual = Path(manual_dir) if manual_dir else DOWN_DIR
    parquet_path = resolved_dest / _PARQUET_NAME
    resolved_dest.mkdir(parents=True, exist_ok=True)
    resolved_manual.mkdir(parents=True, exist_ok=True)

    month_spans = _iter_month_spans(resolved_from, resolved_to)
    pending_by_month: dict[str, Path] = {}
    missing_spans: list[tuple[str, str]] = []
    for month_start, month_end in month_spans:
        month_token = month_start[:7]
        xlsx_path = _find_pending_datedetail_file(resolved_manual, month_token)
        if xlsx_path is None:
            missing_spans.append((month_start, month_end))
        else:
            pending_by_month[month_token] = xlsx_path
            logger.info("%sUsing pending datedetail workbook: %s", log_prefix, xlsx_path.name)

    downloaded_by_month: dict[str, Path] = {}
    if missing_spans:
        logger.info(
            "%sDownloading ToOrder datedetail months: %s",
            log_prefix,
            ", ".join(month_start[:7] for month_start, _month_end in missing_spans),
        )
        results = run_crawling_datedetail_months(
            toorder_id=toorder_id or _TOORDER_ID,
            toorder_pw=toorder_pw or _TOORDER_PW,
            month_spans=missing_spans,
            download_dir=resolved_manual,
        )
        for result in results:
            month_token = str(result.get("month") or "")
            if result.get("success") and result.get("file"):
                downloaded_by_month[month_token] = Path(str(result["file"]))
            else:
                logger.warning(
                    "%sDatedetail download failed: %s (%s)",
                    log_prefix,
                    month_token,
                    result.get("error"),
                )

    all_dfs: list[pd.DataFrame] = []
    for month_start, month_end in month_spans:
        month_token = month_start[:7]
        xlsx_path = pending_by_month.get(month_token) or downloaded_by_month.get(month_token)
        if xlsx_path is None:
            logger.warning("%sSkipping ToOrder datedetail month without workbook: %s", log_prefix, month_token)
            continue

        month_df = _parse_datedetail_xlsx(xlsx_path)
        if month_df.empty:
            logger.warning("%sDatedetail workbook parsed empty: %s", log_prefix, xlsx_path.name)
        else:
            month_df = month_df[
                (month_df["date"].astype(str) >= month_start)
                & (month_df["date"].astype(str) <= month_end)
            ].copy()
            if not month_df.empty:
                all_dfs.append(month_df)
                logger.info("%sParsed datedetail %s: %d rows", log_prefix, month_token, len(month_df))
        _rename_to_raw(xlsx_path)

    if all_dfs:
        new_df = pd.concat(all_dfs, ignore_index=True)
        _upsert_parquet(new_df, parquet_path)
    else:
        logger.warning("%sNo ToOrder datedetail rows collected: %s~%s", log_prefix, resolved_from, resolved_to)

    return str(parquet_path)


def _resolve_date_bounds(
    *,
    target_date: str | None,
    date_from: str | None,
    date_to: str | None,
    sale_dates: list[str] | None,
) -> tuple[str, str]:
    if sale_dates:
        normalized = sorted(_parse_sale_date(d, "sale_dates").strftime("%Y-%m-%d") for d in sale_dates)
        return normalized[0], normalized[-1]
    if target_date and (date_from or date_to):
        raise ValueError("target_date and date_from/date_to cannot be used together.")
    if target_date:
        d = _parse_sale_date(target_date, "target_date").strftime("%Y-%m-%d")
        return d, d
    if date_from or date_to:
        if not (date_from and date_to):
            raise ValueError("date_from and date_to are both required.")
        start = _parse_sale_date(date_from, "date_from")
        end = _parse_sale_date(date_to, "date_to")
        if start > end:
            raise ValueError(f"date_from({date_from}) is after date_to({date_to}).")
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    return "2025-07-01", pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")


def _parse_sale_date(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format: {value}") from exc


def _iter_month_spans(date_from: str, date_to: str) -> list[tuple[str, str]]:
    start = _parse_sale_date(date_from, "date_from")
    end = _parse_sale_date(date_to, "date_to")
    if start > end:
        raise ValueError(f"date_from({date_from}) is after date_to({date_to}).")

    spans: list[tuple[str, str]] = []
    cur = start.replace(day=1)
    while cur <= end:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        span_start = max(start, cur)
        span_end = min(end, next_month - timedelta(days=1))
        spans.append((span_start.strftime("%Y-%m-%d"), span_end.strftime("%Y-%m-%d")))
        cur = next_month
    return spans


def _find_pending_datedetail_file(manual_dir: Path, month_token: str) -> Path | None:
    candidates = []
    for xlsx in sorted(manual_dir.glob("*.xlsx")):
        if not _is_pending_datedetail_xlsx(xlsx):
            continue
        if month_token in xlsx.stem:
            candidates.append(xlsx)
    return candidates[-1] if candidates else None


def _is_pending_datedetail_xlsx(path: Path) -> bool:
    if not path.is_file() or path.suffix.lower() != ".xlsx":
        return False
    if path.name.startswith("~$"):
        return False
    if "_raw" in path.stem or "_bak_" in path.stem:
        return False
    return path.stem.startswith(_DATEDETAIL_PREFIX)


def _parse_datedetail_xlsx(xlsx_path: str | Path) -> pd.DataFrame:
    empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
    try:
        sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, engine="openpyxl")
    except Exception as exc:
        logger.warning("Failed to read datedetail Excel (%s): %s", Path(xlsx_path).name, exc)
        return empty_df

    rows: list[dict[str, object]] = []
    for sheet_name, df in sheets.items():
        date_str = str(sheet_name).strip()
        if date_str == _TOTAL_SHEET:
            continue
        if not _looks_like_yyyy_mm_dd(date_str):
            logger.info("Skipping non-date sheet: %s", date_str)
            continue
        if df.shape[0] <= _DATA_ROW_START or df.shape[1] <= _STORE_COL_START:
            logger.warning("Datedetail sheet too small: %s shape=%s", date_str, df.shape)
            continue

        stores: list[tuple[int, str]] = []
        store_header = df.iloc[2]
        for col in range(_STORE_COL_START, df.shape[1], _STORE_COL_STRIDE):
            name = store_header.iloc[col]
            store = str(name).strip() if not pd.isna(name) else ""
            if not store or store == _TEST_STORE:
                continue
            stores.append((col, store))

        for row_idx in range(_DATA_ROW_START, df.shape[0]):
            channel_value = df.iloc[row_idx, 0]
            if pd.isna(channel_value):
                continue
            platform = str(channel_value).strip()
            if not platform:
                continue
            if platform == _TOTAL_MARKER:
                break
            for col, store in stores:
                price = _coerce_int(df.iloc[row_idx, col])
                receipts = _coerce_int(df.iloc[row_idx, col + 3]) if col + 3 < df.shape[1] else 0
                if price == 0 and receipts == 0:
                    continue
                rows.append(
                    {
                        "date": date_str,
                        "store": store,
                        "platform": platform,
                        "price": price,
                        "receipts_num": receipts,
                    }
                )

    result = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    logger.info("Datedetail Excel parsed: %s -> %d rows", Path(xlsx_path).name, len(result))
    return result


def _looks_like_yyyy_mm_dd(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _coerce_int(value: object) -> int:
    if pd.isna(value):
        return 0
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _rename_to_raw(xlsx_path: Path) -> None:
    raw_path = xlsx_path.with_name(f"{xlsx_path.stem}_raw{xlsx_path.suffix}")
    if raw_path.exists():
        backup_path = raw_path.with_name(
            f"{raw_path.stem}_bak_{pendulum.now('Asia/Seoul').format('HHmmss')}{raw_path.suffix}"
        )
        raw_path.rename(backup_path)
    xlsx_path.rename(raw_path)
    logger.info("Datedetail file archived: %s", raw_path.name)


def cleanup_datedetail_xlsx(download_dir: str | Path | None = None) -> str:
    resolved_dir = Path(download_dir) if download_dir else DOWN_DIR
    if not resolved_dir.exists():
        logger.info("Datedetail cleanup skipped; directory not found: %s", resolved_dir)
        return "deleted=0"

    deleted = 0
    for xlsx_path in sorted(resolved_dir.glob(f"{_DATEDETAIL_PREFIX}*.xlsx")):
        if not xlsx_path.is_file() or xlsx_path.name.startswith("~$"):
            continue
        try:
            xlsx_path.unlink()
            deleted += 1
            logger.info("Datedetail cleanup deleted: %s", xlsx_path.name)
        except Exception as exc:
            logger.warning("Datedetail cleanup failed: %s (%s)", xlsx_path.name, exc)

    logger.info("Datedetail cleanup complete: deleted=%d dir=%s", deleted, resolved_dir)
    return f"deleted={deleted}"


def _upsert_parquet(new_df: pd.DataFrame, parquet_path: Path) -> None:
    key_cols = ["date", "store", "platform"]
    numeric_cols = ["price", "receipts_num"]

    merged_new = new_df.copy()
    for col in _OUTPUT_COLUMNS:
        if col not in merged_new.columns:
            merged_new[col] = 0 if col in numeric_cols else ""
    for col in key_cols:
        merged_new[col] = merged_new[col].astype(str).str.strip()
    for col in numeric_cols:
        merged_new[col] = pd.to_numeric(merged_new[col], errors="coerce").fillna(0).astype(int)
    merged_new = merged_new[_OUTPUT_COLUMNS]

    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path).fillna("")
        for col in _OUTPUT_COLUMNS:
            if col not in existing.columns:
                existing[col] = 0 if col in numeric_cols else ""
        existing = existing[_OUTPUT_COLUMNS]
        for col in key_cols:
            existing[col] = existing[col].astype(str).str.strip()
        for col in numeric_cols:
            existing[col] = pd.to_numeric(existing[col], errors="coerce").fillna(0).astype(int)
        new_keys = merged_new[key_cols].drop_duplicates()
        existing = existing.merge(new_keys.assign(_new=1), on=key_cols, how="left")
        existing = existing.loc[existing["_new"].isna()].drop(columns=["_new"])
        merged = pd.concat([existing, merged_new], ignore_index=True)
    else:
        merged = merged_new

    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info("Parquet upsert complete: %s (%d rows)", parquet_path.name, len(merged))
