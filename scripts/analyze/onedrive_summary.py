"""
OneDrive baemin_marketing 파티션 분석 스크립트.
결과를 scripts/output/onedrive_summary_{timestamp}.json 으로 저장.

사용법:
    python scripts/analyze/onedrive_summary.py --ym 2026-03
    python scripts/analyze/onedrive_summary.py --brand 도리당 --ym 2026-03
    python scripts/analyze/onedrive_summary.py --brand 도리당 --store 강원영월점
"""
import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts._base import run_script, save_summary
from modules.transform.utility.paths import BAEMIN_MARKETING_DB
from modules.transform.utility.analytics import read_analytics_partition

logger = logging.getLogger(__name__)
SCRIPT_NAME = "onedrive_summary"
KST = timezone(timedelta(hours=9))


def _build_label(brand: str | None, store: str | None, ym: str | None) -> str:
    parts = ["baemin_marketing"]
    if brand:
        parts.append(f"brand={brand}")
    if store:
        parts.append(f"store={store}")
    if ym:
        parts.append(f"ym={ym}")
    return " / ".join(parts)


def _null_rate(df) -> dict:
    import pandas as pd
    rate = (df.isnull().sum() / len(df) * 100).round(2)
    return {col: float(v) for col, v in rate.items() if v > 0}


def main(brand: str | None, store: str | None, ym: str | None) -> dict:
    import pandas as pd

    df = read_analytics_partition(BAEMIN_MARKETING_DB, brand=brand, store=store, ym=ym)

    brands = df["brand"].unique().tolist() if "brand" in df.columns else []
    stores = df["store"].unique().tolist() if "store" in df.columns else []
    yms = sorted(df["ym"].unique().tolist()) if "ym" in df.columns else []

    by_store = {}
    if "store" in df.columns:
        for s, grp in df.groupby("store"):
            by_store[s] = {"rows": len(grp), "columns": len(grp.columns)}

    date_range: list[str] = []
    for col in df.columns:
        if "date" in col.lower() or "날짜" in col:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce").dropna()
                if not parsed.empty:
                    date_range = [str(parsed.min().date()), str(parsed.max().date())]
                break
            except Exception:
                pass

    return {
        "meta": {
            "script": f"analyze/{SCRIPT_NAME}.py",
            "run_at": datetime.now(KST).isoformat(),
            "status": "ok",
        },
        "summary": {
            "label": _build_label(brand, store, ym),
            "total_rows": len(df),
            "files_loaded": len(df["_file"].unique()) if "_file" in df.columns else None,
            "partitions": {
                "brands": len(brands),
                "stores": len(stores),
                "yms": yms,
            },
            "result": "PASS",
        },
        "stats": {
            "by_store": by_store,
            "date_range": date_range,
            "null_rate": _null_rate(df),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OneDrive baemin_marketing 파티션 분석")
    parser.add_argument("--brand", default=None, help="brand= 파티션 값")
    parser.add_argument("--store", default=None, help="store= 파티션 값")
    parser.add_argument("--ym", default=None, help="ym= 파티션 값 (예: 2026-03)")
    args = parser.parse_args()

    run_script(lambda: main(args.brand, args.store, args.ym), SCRIPT_NAME)
