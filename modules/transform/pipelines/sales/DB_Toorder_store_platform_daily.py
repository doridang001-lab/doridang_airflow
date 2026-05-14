"""
ToOrder channel daily sales collection pipeline.

This pipeline merges manual daily sales report workbooks with API-collected
channel sales data and upserts the consolidated result into a single CSV.
Processed manual workbooks are renamed to `_raw.xlsx` to avoid reprocessing.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum

from modules.extract.crawling_toorder_sales_report_daily_date import run_crawling_single_date
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR

logger = logging.getLogger(__name__)

_TOORDER_ID = os.getenv("TOORDER_ID", "doridang15")
_TOORDER_PW = os.getenv("TOORDER_PW", "ehfl5233!")
_DEFAULT_DEST = ANALYTICS_DB / "toorder_daily_store_platform"
_CSV_NAME = "toorder_store_platform_daily.csv"
_MANUAL_REPORT_PREFIX = "\uc885\ud569\ubcf4\uace0\uc11c_\uc77c\ubcc4\ub9e4\ucd9c\ubcf4\uace0\uc11c"
_DEFAULT_STORE_NAME = "\ub3c4\ub9ac\ub2f9 \ud574\uc6b4\ub300\uc911\ub3d9\uc810"
_TOTAL_MARKER = "\ud569\uacc4"
_DEFAULT_PLATFORM_SUM = "\ud569\uacc4"

# Manual Excel channel columns
_PLATFORMS = [
    "\ud3ec\uc7a5",
    "\ud3ec\uc7a5 \uc0ac\uc7a5",
    "\ubc30\ub2ec\uc758\ubbfc\uc871",
    "\ubc30\ubbfc \uc0ac\uc7a5",
    "\ubc30\ubbfc1",
    "\ucfe0\ud33d\uc774\uce20",
    "\uc694\uae30\uc694",
]
_PRICE_COLS = list(range(5, 12))
_RECEIPTS_COLS = list(range(41, 48))
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
    store_name: str = _DEFAULT_STORE_NAME,
    manual_dir: str | Path | None = None,
    log_prefix: str = "",
) -> str:
    """
    Parameters:
        target_date: single sale date in YYYY-MM-DD.
        date_from/date_to: inclusive date range in YYYY-MM-DD.
        sale_dates: pre-resolved date list. When provided, it overrides the
            other date arguments.
        dest_dir: destination folder for the upserted CSV.
        manual_dir: folder to scan for pending manual xlsx files.

    Returns:
        Saved CSV path as string.
    """
    resolved_sale_dates = sale_dates or _resolve_sale_dates(
        target_date=target_date,
        date_from=date_from,
        date_to=date_to,
    )
    resolved_dest = Path(dest_dir) if dest_dir else _DEFAULT_DEST
    resolved_manual = Path(manual_dir) if manual_dir else DOWN_DIR
    csv_path = resolved_dest / _CSV_NAME
    resolved_dest.mkdir(parents=True, exist_ok=True)

    all_dfs: list[pd.DataFrame] = []

    manual_dfs = _ingest_manual_files(resolved_manual, fallback_store_name=store_name)
    all_dfs.extend(manual_dfs)

    auto_df = _collect_for_dates(
        sale_dates=resolved_sale_dates,
        toorder_id=toorder_id or _TOORDER_ID,
        toorder_pw=toorder_pw or _TOORDER_PW,
        store_name=store_name,
        download_dir=resolved_manual,
        log_prefix=log_prefix,
    )
    if not auto_df.empty:
        all_dfs.append(auto_df)
        logger.info(
            "%sDownloaded workbook collection complete: %d rows (dates=%s, store=%s)",
            log_prefix,
            len(auto_df),
            ",".join(resolved_sale_dates),
            store_name,
        )

    if not all_dfs:
        logger.warning("%sNo rows collected (dates=%s)", log_prefix, ",".join(resolved_sale_dates))
        return str(csv_path)

    new_df = pd.concat(all_dfs, ignore_index=True)
    _upsert_csv(new_df, csv_path)
    return str(csv_path)


def _resolve_sale_dates(
    target_date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    if target_date and (date_from or date_to):
        raise ValueError("sale_date and sale_date_from/sale_date_to cannot be used together.")

    if date_from or date_to:
        if not (date_from and date_to):
            raise ValueError("sale_date_from and sale_date_to are both required.")
        start = _parse_sale_date(date_from, "sale_date_from")
        end = _parse_sale_date(date_to, "sale_date_to")
        if start > end:
            raise ValueError(f"sale_date_from({date_from}) is after sale_date_to({date_to}).")
        delta = (end - start).days + 1
        return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta)]

    if target_date:
        return [_parse_sale_date(target_date, "sale_date").strftime("%Y-%m-%d")]

    return [pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")]


def _parse_sale_date(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format: {value}") from exc


def _collect_for_dates(
    *,
    sale_dates: list[str],
    toorder_id: str,
    toorder_pw: str,
    store_name: str,
    download_dir: Path,
    log_prefix: str = "",
) -> pd.DataFrame:
    daily_dfs: list[pd.DataFrame] = []

    for sale_date in sale_dates:
        logger.info("%sCollecting date by workbook download: %s", log_prefix, sale_date)
        result = run_crawling_single_date(
            toorder_id=toorder_id,
            toorder_pw=toorder_pw,
            target_date=sale_date,
            download_dir=download_dir,
        )
        if not result.get("success") or not result.get("file"):
            logger.warning("%sDownload failed: %s (%s)", log_prefix, sale_date, result.get("error"))
            continue

        downloaded_path = Path(result["file"])
        daily_df = _parse_manual_xlsx(downloaded_path, fallback_store_name=store_name)
        if daily_df.empty:
            logger.warning("%sDownloaded workbook parsed empty: %s", log_prefix, downloaded_path.name)
            _rename_to_raw(downloaded_path)
            continue

        filtered_df = _filter_store_rows(daily_df, store_name)
        if filtered_df.empty:
            logger.warning(
                "%sDownloaded workbook has no rows for store=%s date=%s",
                log_prefix,
                store_name,
                sale_date,
            )
            _rename_to_raw(downloaded_path)
            continue

        daily_dfs.append(filtered_df)
        logger.info("%sDate complete: %s (%d rows)", log_prefix, sale_date, len(filtered_df))
        _rename_to_raw(downloaded_path)

    if not daily_dfs:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    return pd.concat(daily_dfs, ignore_index=True)


def _is_pending_manual_xlsx(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() != ".xlsx":
        return False
    if path.name.startswith("~$"):
        return False
    if "_raw" in path.stem or "_bak_" in path.stem:
        return False
    return path.stem.startswith(_MANUAL_REPORT_PREFIX)


def _ingest_manual_files(manual_dir: Path, fallback_store_name: str) -> list[pd.DataFrame]:
    """Parse pending manual xlsx files and rename them to `_raw.xlsx`."""
    dfs: list[pd.DataFrame] = []
    for xlsx in sorted(manual_dir.glob("*.xlsx")):
        if not _is_pending_manual_xlsx(xlsx):
            continue
        logger.info("Processing manual file: %s", xlsx.name)
        df = _parse_manual_xlsx(xlsx, fallback_store_name=fallback_store_name)
        if not df.empty:
            dfs.append(df)
        _rename_to_raw(xlsx)
    return dfs


def _normalize_manual_store(value: object, fallback_store_name: str) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return fallback_store_name
    return text


def _filter_store_rows(df: pd.DataFrame, store_name: str) -> pd.DataFrame:
    normalized_target = str(store_name or "").strip()
    if not normalized_target:
        return df.copy()

    normalized_store = df["store"].astype(str).str.strip()
    exact_match = df.loc[normalized_store == normalized_target].copy()
    if not exact_match.empty:
        return exact_match

    return df.loc[normalized_store.str.contains(normalized_target, regex=False)].copy()


def _coerce_int(value: object) -> int:
    if pd.isna(value):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _normalize_date_str(value: object) -> str:
    """날짜 셀 값을 YYYY-MM-DD 형식으로 정규화. datetime/Timestamp 및 2026-5-1 형식 모두 처리."""
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    text = str(value or "").replace(" ", "")
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return text


def _parse_manual_xlsx(xlsx_path: Path, fallback_store_name: str) -> pd.DataFrame:
    """
    Parse ToOrder daily sales report Excel into long format.

    Format A (48+ cols):
        per-platform values exist in columns 5-11 and 41-47.
    Format B (20+ cols):
        only total values exist in columns 4 and 19.
    """
    empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)

    try:
        df = pd.read_excel(xlsx_path, sheet_name=0, header=None, engine="openpyxl")
    except Exception as exc:
        logger.warning("Failed to read manual Excel (%s): %s", xlsx_path.name, exc)
        return empty_df

    if df.shape[0] < 4:
        logger.warning("Manual Excel too small (%s): shape=%s", xlsx_path.name, df.shape)
        return empty_df

    date_range = _normalize_date_str(df.iloc[1, 0])
    rows = []

    if df.shape[1] >= 48:
        for row_idx in range(3, df.shape[0]):
            if _TOTAL_MARKER in str(df.iloc[row_idx, 0]):
                continue
            if pd.isna(df.iloc[row_idx, 1]):
                continue
            store_name = _normalize_manual_store(df.iloc[row_idx, 1], fallback_store_name)
            for i, platform in enumerate(_PLATFORMS):
                price = _coerce_int(df.iloc[row_idx, _PRICE_COLS[i]])
                receipts = _coerce_int(df.iloc[row_idx, _RECEIPTS_COLS[i]])
                if price == 0 and receipts == 0:
                    continue
                rows.append(
                    {
                        "date": date_range,
                        "store": store_name,
                        "platform": platform,
                        "price": price,
                        "receipts_num": receipts,
                    }
                )
    elif df.shape[1] >= 20:
        for row_idx in range(3, df.shape[0]):
            if _TOTAL_MARKER in str(df.iloc[row_idx, 0]):
                continue
            if pd.isna(df.iloc[row_idx, 1]):
                continue
            store_name = _normalize_manual_store(df.iloc[row_idx, 1], fallback_store_name)
            price = _coerce_int(df.iloc[row_idx, 4])
            receipts = _coerce_int(df.iloc[row_idx, 19])
            if price == 0 and receipts == 0:
                continue
            rows.append(
                {
                    "date": date_range,
                    "store": store_name,
                    "platform": _DEFAULT_PLATFORM_SUM,
                    "price": price,
                    "receipts_num": receipts,
                }
            )
    else:
        logger.info("Unsupported manual Excel shape (%s): shape=%s", xlsx_path.name, df.shape)
        return empty_df

    result = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    logger.info("Manual Excel parsed: %s -> %d rows", xlsx_path.name, len(result))
    return result


def _rename_to_raw(xlsx_path: Path) -> None:
    raw_path = xlsx_path.with_name(f"{xlsx_path.stem}_raw{xlsx_path.suffix}")
    if raw_path.exists():
        backup_path = raw_path.with_name(
            f"{raw_path.stem}_bak_{pendulum.now('Asia/Seoul').format('HHmmss')}{raw_path.suffix}"
        )
        raw_path.rename(backup_path)
    xlsx_path.rename(raw_path)
    logger.info("Manual file archived: %s", raw_path.name)


def _upsert_csv(new_df: pd.DataFrame, csv_path: Path) -> None:
    """Upsert rows into CSV by (date, store, platform), keeping the last row."""
    key_cols = ["date", "store", "platform"]
    numeric_cols = ["price", "receipts_num"]

    merged_new = new_df.copy()
    for col in key_cols:
        merged_new[col] = merged_new[col].astype(str).str.strip()
    for col in numeric_cols:
        merged_new[col] = pd.to_numeric(merged_new[col], errors="coerce").fillna(0).astype(int).astype(str)

    if csv_path.exists():
        existing = pd.read_csv(csv_path, dtype=str)
        for col in _OUTPUT_COLUMNS:
            if col not in existing.columns:
                existing[col] = ""
        existing = existing[_OUTPUT_COLUMNS]
        existing["date"] = existing["date"].apply(_normalize_date_str)
        for col in key_cols:
            existing[col] = existing[col].astype(str).str.strip()
        new_keys = merged_new[key_cols].drop_duplicates()
        existing = existing.merge(new_keys.assign(_new=1), on=key_cols, how="left")
        existing = existing.loc[existing["_new"].isna()].drop(columns=["_new"])
        merged = pd.concat([existing, merged_new[_OUTPUT_COLUMNS]], ignore_index=True)
    else:
        merged = merged_new[_OUTPUT_COLUMNS]

    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    merged.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("CSV upsert complete: %s (%d rows)", csv_path.name, len(merged))
