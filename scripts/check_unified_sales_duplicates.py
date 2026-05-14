"""
Scan unified_sales parquet files for duplication / source variants.

Usage:
  python -X utf8 scripts\\check_unified_sales_duplicates.py
  python -X utf8 scripts\\check_unified_sales_duplicates.py --limit 30
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.transform.utility.paths import MART_DB


def _scan_one(path: Path) -> dict:
    df = pd.read_parquet(path, columns=["_pk", "source", "sale_date"])
    dup_pk = int(df["_pk"].duplicated().sum()) if "_pk" in df.columns else 0

    sources = df["source"].fillna("").astype(str)
    blank_source = int((sources.str.strip() == "").sum())

    unique_sources = set(sources.tolist())
    norm = {}
    for s in unique_sources:
        k = s.strip().lower()
        norm.setdefault(k, set()).add(s)
    variants = {k: sorted(list(v)) for k, v in norm.items() if len(v) > 1}

    multi_sale_date = 0
    if "sale_date" in df.columns:
        multi_sale_date = int(df["sale_date"].nunique() > 1)

    return {
        "file": path.name,
        "rows": int(len(df)),
        "dup_pk": dup_pk,
        "blank_source": blank_source,
        "multi_sale_date": multi_sale_date,
        "source_variants": variants,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="scan newest N files only (0=all)")
    args = ap.parse_args()

    root = Path(MART_DB) / "unified_sales_grp"
    files = sorted(root.glob("unified_sales_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if args.limit and args.limit > 0:
        files = files[: args.limit]

    print(f"dir={root}")
    print(f"files={len(files)}")

    issues = []
    for p in files:
        r = _scan_one(p)
        if r["dup_pk"] or r["blank_source"] or r["multi_sale_date"] or r["source_variants"]:
            issues.append(r)

    print(f"issue_files={len(issues)}")
    for r in issues[:50]:
        print(
            f"- {r['file']}: rows={r['rows']} dup_pk={r['dup_pk']} blank_source={r['blank_source']} "
            f"multi_sale_date={r['multi_sale_date']} variants={r['source_variants']}"
        )

    return 0 if not issues else 2


if __name__ == "__main__":
    raise SystemExit(main())
