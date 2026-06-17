"""Migrate Baemin CSV tables to parquet(zstd)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.beamin_store_io import PARQUET_COMPRESSION
from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB,
    BAEMIN_SHOP_CHANGE_DB,
    BAEMIN_SHOP_OPERATION_DB,
)


def _targets(include_operation: bool) -> list[tuple[str, Path, str]]:
    items = [
        ("orders", BAEMIN_ORDERS_DB, "brand=*/store=*/ym=*/orders_*.csv"),
        ("shop_change", BAEMIN_SHOP_CHANGE_DB, "brand=*/store=*/ym=*/shop_change.csv"),
    ]
    if include_operation:
        items.append(
            ("shop_operation", BAEMIN_SHOP_OPERATION_DB, "brand=*/store=*/ym=*/shop_operation.csv")
        )
    return items


def _convert_one(csv_path: Path, apply: bool, keep_csv: bool) -> tuple[bool, str]:
    pq_path = csv_path.with_suffix(".parquet")
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").fillna("").astype(str)
    except Exception as exc:
        return False, f"read_failed: {csv_path.name} / {exc}"

    csv_kb = csv_path.stat().st_size / 1024
    if not apply:
        return True, f"DRY {csv_path.name}: {len(df)} rows, {csv_kb:.0f}KB"

    try:
        df.to_parquet(pq_path, engine="pyarrow", compression=PARQUET_COMPRESSION, index=False)
        check = pd.read_parquet(pq_path).astype(str)
    except Exception as exc:
        return False, f"write_failed: {csv_path.name} / {exc}"

    if len(check) != len(df):
        return False, f"verify_failed: {csv_path.name} expected={len(df)} actual={len(check)}"

    pq_kb = pq_path.stat().st_size / 1024
    msg = f"OK {csv_path.name}: {len(df)} rows, {csv_kb:.0f}KB -> {pq_kb:.0f}KB"
    if not keep_csv:
        try:
            csv_path.unlink()
        except OSError as exc:
            msg += f" (csv_remove_failed: {exc})"
    return True, msg


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write parquet (dry-run by default)")
    ap.add_argument("--keep-csv", action="store_true", help="keep source csv")
    ap.add_argument("--include-operation", action="store_true", help="also migrate shop_operation")
    args = ap.parse_args()

    n_ok = 0
    n_fail = 0
    total_pq_kb = 0.0

    for label, base, pattern in _targets(args.include_operation):
        csv_files = sorted(base.glob(pattern))
        print(f"\n=== {label}: CSV {len(csv_files)} (base={base}) ===")
        for csv_path in csv_files:
            ok, msg = _convert_one(csv_path, args.apply, args.keep_csv)
            print(("  " if ok else "  !! ") + msg)
            if ok:
                n_ok += 1
                if args.apply:
                    pq = csv_path.with_suffix(".parquet")
                    if pq.exists():
                        total_pq_kb += pq.stat().st_size / 1024
            else:
                n_fail += 1

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"\n[{mode}] ok={n_ok} fail={n_fail}")
    if args.apply:
        print(f"parquet_total_mb: {total_pq_kb / 1024:.1f} MB")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
