"""Compare unified_sales totals against toorder daily store-platform totals.

Read-only validation. Backup unified_sales parquet files are excluded through
iter_unified_sales_files().
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files  # noqa: E402
from modules.transform.utility.paths import ANALYTICS_DB  # noqa: E402


TOORDER_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"

PLATFORM_MAP = {
    "배민1": "배달의민족",
    "배민 포장": "배달의민족",
    "배달의민족": "배달의민족",
    "쿠팡 포장": "쿠팡이츠",
    "쿠팡이츠": "쿠팡이츠",
    "요기배달": "요기요",
    "요기요 포장": "요기요",
    "요기요": "요기요",
    "땡겨요 포장": "땡겨요",
    "땡겨요": "땡겨요",
    "먹깨비 포장": "먹깨비",
    "먹깨비": "먹깨비",
    "배달e음": "인천이음",
    "인천이음": "인천이음",
    "네이버": "네이버주문",
    "네이버주문": "네이버주문",
    "홀 포장": "홀",
    "홀 배달": "홀",
    "홀": "홀",
}


def _norm_platform(value: object) -> str:
    text = "" if value is None else str(value).strip()
    return PLATFORM_MAP.get(text, text)


def _load_unified(date_from: str | None, date_to: str | None) -> pd.DataFrame:
    frames = []
    for path in iter_unified_sales_files():
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "total_price"])
        except Exception as exc:
            print(f"warn=unified_read_failed file={path.name} error={exc}")
            continue
        if date_from:
            df = df[df["sale_date"].astype(str) >= date_from]
        if date_to:
            df = df[df["sale_date"].astype(str) <= date_to]
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "store", "platform", "unified_price"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = out["sale_date"].astype(str)
    out["store"] = out["store"].astype(str).str.strip()
    out["platform"] = out["platform"].map(_norm_platform)
    out["unified_price"] = pd.to_numeric(out["total_price"], errors="coerce").fillna(0)
    return (
        out.groupby(["date", "store", "platform"], as_index=False)["unified_price"]
        .sum()
    )


def _load_toorder(date_from: str | None, date_to: str | None) -> pd.DataFrame:
    df = pd.read_parquet(TOORDER_PATH)
    df["date"] = df["date"].astype(str)
    if date_from:
        df = df[df["date"] >= date_from]
    if date_to:
        df = df[df["date"] <= date_to]
    df["store"] = df["store"].astype(str).str.strip()
    df["platform"] = df["platform"].map(_norm_platform)
    df["toorder_price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    return (
        df.groupby(["date", "store", "platform"], as_index=False)["toorder_price"]
        .sum()
    )


def _pct_error(unified: float, toorder: float) -> float:
    if toorder == 0:
        return 0.0 if unified == 0 else 100.0
    return abs(unified - toorder) / abs(toorder) * 100


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--threshold", type=float, default=5.0)
    parser.add_argument("--top", type=int, default=30)
    args = parser.parse_args()

    unified = _load_unified(args.date_from, args.date_to)
    toorder = _load_toorder(args.date_from, args.date_to)
    merged = unified.merge(toorder, on=["date", "store", "platform"], how="outer").fillna(0)
    merged["diff"] = merged["unified_price"] - merged["toorder_price"]
    merged["error_pct"] = [
        _pct_error(u, t) for u, t in zip(merged["unified_price"], merged["toorder_price"])
    ]

    total_unified = float(merged["unified_price"].sum())
    total_toorder = float(merged["toorder_price"].sum())
    total_error = _pct_error(total_unified, total_toorder)
    status = "OK" if total_error <= args.threshold else "FAIL"

    print(f"date_from={args.date_from or ''} date_to={args.date_to or ''}")
    print(f"unified_rows={len(unified)} toorder_rows={len(toorder)} merged_rows={len(merged)}")
    print(f"unified_total={int(total_unified)}")
    print(f"toorder_total={int(total_toorder)}")
    print(f"diff={int(total_unified - total_toorder)}")
    print(f"error_pct={total_error:.2f}")
    print(f"threshold={args.threshold:.2f}")
    print(f"status={status}")

    platform = (
        merged.groupby("platform", as_index=False)[["unified_price", "toorder_price"]]
        .sum()
    )
    platform["diff"] = platform["unified_price"] - platform["toorder_price"]
    platform["error_pct"] = [
        _pct_error(u, t) for u, t in zip(platform["unified_price"], platform["toorder_price"])
    ]
    platform = platform.sort_values("diff", key=lambda s: s.abs(), ascending=False)
    print("\nplatform_diff_top")
    print(platform.head(args.top).to_string(index=False))

    worst = merged.sort_values("diff", key=lambda s: s.abs(), ascending=False)
    print("\nstore_platform_diff_top")
    print(worst.head(args.top).to_string(index=False))
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
