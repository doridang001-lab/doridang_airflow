"""테스트 매장 배달 플랫폼/source 잔존 상태를 read-only로 진단한다."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_MANUAL_TEST_STORES,
    DELIVERY_PLATFORM_FAMILIES,
    PLATFORM_TO_MANUAL_SOURCE,
    UNIFIED_ROOT,
)


def main() -> int:
    stores = {str(store).strip() for store in DELIVERY_MANUAL_TEST_STORES if str(store).strip()}
    platforms = set(PLATFORM_TO_MANUAL_SOURCE)
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []

    parts: list[pd.DataFrame] = []
    failures: list[str] = []
    for path in files:
        try:
            df = pd.read_parquet(
                path,
                columns=["sale_date", "store", "platform", "source", "total_price"],
            )
        except Exception as exc:
            failures.append(f"{path.name}: {exc}")
            continue

        df = df.copy()
        df["store"] = df["store"].fillna("").astype(str).str.strip()
        df["platform"] = df["platform"].fillna("").astype(str).str.strip()
        df["source"] = df["source"].fillna("").astype(str).str.strip()
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
        sub = df[df["store"].isin(stores) & df["platform"].isin(platforms)].copy()
        if not sub.empty:
            parts.append(sub)

    print(f"UNIFIED_ROOT={UNIFIED_ROOT}")
    print(f"FILES={len(files)} READ_FAILURES={len(failures)} MATCHED_PARTS={len(parts)}")
    print(f"DELIVERY_PLATFORM_FAMILIES={DELIVERY_PLATFORM_FAMILIES}")
    if failures:
        print("\n[READ_FAILURES]")
        for failure in failures:
            print(failure)

    if not parts:
        print("\n[SUMMARY]\n대상 행 없음")
        print("\n[BAD]\nBAD_ROWS=0 BAD_TOTAL=0")
        return 0

    all_df = pd.concat(parts, ignore_index=True)
    summary = (
        all_df.groupby(["platform", "source"], dropna=False)
        .agg(rows=("source", "size"), total_price=("total_price", "sum"))
        .reset_index()
        .sort_values(["platform", "source"])
    )
    summary["total_price"] = summary["total_price"].round().astype(int)
    print("\n[SUMMARY]")
    print(summary.to_string(index=False))

    expected = all_df["platform"].map(PLATFORM_TO_MANUAL_SOURCE)
    bad = all_df[expected.notna() & all_df["source"].ne(expected)].copy()
    bad_total = int(bad["total_price"].sum()) if not bad.empty else 0
    print("\n[BAD]")
    print(f"BAD_ROWS={len(bad)} BAD_TOTAL={bad_total}")
    if not bad.empty:
        detail = (
            bad.groupby(["sale_date", "store", "platform", "source"], dropna=False)
            .agg(rows=("source", "size"), total_price=("total_price", "sum"))
            .reset_index()
            .sort_values(["sale_date", "store", "platform", "source"])
        )
        detail["total_price"] = detail["total_price"].round().astype(int)
        print(detail.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
