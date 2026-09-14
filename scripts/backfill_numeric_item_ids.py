"""Backfill numeric item_id for manual/no-code unified_sales sources.

Default mode is dry-run. Pass --apply to write fin_product_grp.csv and parquet files.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    MANUAL_SOURCES,
    allocate_manual_item_ids,
    canonical_source,
    normalize_item_key,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    iter_unified_sales_files,
    _make_unified_pk,
)


_MD5_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)


def _backup_file(path: Path) -> None:
    backup_dir = path.parent / "_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"backup_{path.stem}_{datetime.now():%Y%m%d_%H%M%S}{path.suffix}"
    backup.write_bytes(path.read_bytes())


def _manual_mask(df: pd.DataFrame) -> pd.Series:
    if "source" not in df.columns:
        return pd.Series(False, index=df.index)
    source = df["source"].fillna("").astype(str).str.strip()
    return source.isin(MANUAL_SOURCES)


def _collect_manual_items() -> pd.DataFrame:
    rows = []
    for path in iter_unified_sales_files():
        try:
            df = pd.read_parquet(path, columns=["source", "brand", "store", "item_name", "unit_price"])
        except Exception:
            df = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        mask = _manual_mask(df)
        if not mask.any():
            continue
        part = df.loc[mask, ["source", "brand", "store", "item_name", "unit_price"]].copy()
        for col in part.columns:
            part[col] = part[col].fillna("").astype(str).str.strip()
        part = part[part["item_name"] != ""]
        if not part.empty:
            rows.append(part)
    if not rows:
        return pd.DataFrame(columns=["source", "brand", "store", "item_name", "unit_price"])
    return (
        pd.concat(rows, ignore_index=True)
        .drop_duplicates(subset=["source", "brand", "store", "item_name"], keep="last")
        .reset_index(drop=True)
    )


def _manual_key_frame(df: pd.DataFrame) -> pd.Series:
    source = df["source"].fillna("").astype(str).str.strip().map(canonical_source)
    brand = df["brand"].fillna("").astype(str).str.strip()
    store = df["store"].fillna("").astype(str).str.strip()
    item_key = df["item_name"].fillna("").astype(str).map(normalize_item_key)
    return pd.Series(list(zip(source, brand, store, item_key)), index=df.index)


def _build_code_map(items: pd.DataFrame, apply: bool) -> dict[tuple[str, str, str, str], str]:
    if items.empty:
        return {}
    codes = allocate_manual_item_ids(items, persist=apply)
    keyed = items.copy()
    keyed["_key"] = _manual_key_frame(keyed)
    keyed["_code"] = codes.fillna("").astype(str).str.strip()
    return dict(zip(keyed["_key"], keyed["_code"]))


def _backfill_parquet(apply: bool, code_map: dict[tuple[str, str, str, str], str]) -> tuple[int, int, int]:
    changed_files = 0
    changed_rows = 0
    non_numeric_rows = 0
    for path in iter_unified_sales_files():
        df = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        mask = _manual_mask(df)
        if not mask.any():
            continue

        current = df.loc[mask, "item_id"].fillna("").astype(str).str.strip()
        numeric_current = pd.to_numeric(current[current != ""], errors="coerce")
        non_numeric_current = pd.Series(False, index=current.index)
        non_numeric_current.loc[current[current != ""].index] = numeric_current.isna()

        keys = _manual_key_frame(df.loc[mask, ["source", "brand", "store", "item_name"]])
        mapped = keys.map(code_map).fillna("").astype(str)
        needs_update = mapped.ne("") & current.ne(mapped)
        target_mask = pd.Series(False, index=df.index)
        target_mask.loc[mask] = needs_update
        if not target_mask.any():
            continue

        before_sum = pd.to_numeric(df.get("total_price", 0), errors="coerce").fillna(0).sum()
        target_keys = _manual_key_frame(df.loc[target_mask, ["source", "brand", "store", "item_name"]])
        new_codes = target_keys.map(code_map).fillna("").astype(str)
        if new_codes.eq("").any():
            raise RuntimeError(f"item_id 매핑 실패: {path} rows={int(new_codes.eq('').sum())}")
        df.loc[target_mask, "item_id"] = new_codes.values
        df.loc[target_mask, "_pk"] = _make_unified_pk(df.loc[target_mask])
        after_sum = pd.to_numeric(df.get("total_price", 0), errors="coerce").fillna(0).sum()
        if int(before_sum) != int(after_sum):
            raise RuntimeError(f"매출 합계 변경 감지: {path} before={before_sum} after={after_sum}")

        changed_files += 1
        changed_rows += int(target_mask.sum())
        non_numeric_rows += int(non_numeric_current.sum())
        if apply:
            _backup_file(path)
            df.to_parquet(path, index=False)
    return changed_files, changed_rows, non_numeric_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 fin_product_grp.csv/parquet 파일을 수정합니다.")
    args = parser.parse_args()

    items = _collect_manual_items()
    print(f"manual item keys: {len(items)}")
    code_map = _build_code_map(items, apply=args.apply)
    print(f"allocated/reused codes: {len(set(code_map.values()))}")

    changed_files, changed_rows, non_numeric_rows = _backfill_parquet(apply=args.apply, code_map=code_map)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"{mode}: files={changed_files} rows={changed_rows} non_numeric_rows={non_numeric_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
