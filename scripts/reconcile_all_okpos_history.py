"""Jan~May 전 기간 okpos_daily vs okpos_order/okpos_order_item 실매출액 검증 및 자동 재수집."""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from scripts._base import run_script
from modules.transform.utility.paths import RAW_OKPOS_SALES
from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    reconcile_against_daily_summary,
    STORES,
)

logger = logging.getLogger(__name__)


class _MockTI:
    def __init__(self, sale_dates: list[str]) -> None:
        self._dates = sale_dates
        self._pushed: dict = {}

    def xcom_pull(self, task_ids=None, key=None):
        if key == "sale_dates":
            return self._dates
        return None

    def xcom_push(self, key, value):
        self._pushed[key] = value
        logger.info("[xcom_push] %s: %s", key, str(value)[:200])


class _MockDagRun:
    conf: dict = {}


def _collect_all_daily_dates() -> list[str]:
    """okpos_daily.csv 전체에서 중복 없는 날짜 목록 수집."""
    base = RAW_OKPOS_SALES / "brand=도리당"
    dates: set[str] = set()
    for store_info in STORES:
        store_short = store_info["name"].replace("도리당 ", "", 1)
        store_dir = base / f"store={store_short}"
        if not store_dir.exists():
            continue
        for ym_dir in store_dir.iterdir():
            csv_path = ym_dir / "okpos_daily.csv"
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                continue
            try:
                df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(csv_path, dtype=str)
            if "sale_date" in df.columns:
                dates.update(df["sale_date"].dropna().str.strip().tolist())
    return sorted(dates)


def main() -> dict:
    all_dates = _collect_all_daily_dates()
    if not all_dates:
        return {
            "meta": {"total_dates": 0, "stores": len(STORES)},
            "summary": {"result": "날짜 없음 — okpos_daily CSV 없음"},
        }

    logger.info("전체 날짜: %d일  (%s ~ %s)", len(all_dates), all_dates[0], all_dates[-1])

    mock_ti = _MockTI(all_dates)
    result = reconcile_against_daily_summary(ti=mock_ti, dag_run=_MockDagRun())
    logger.info("reconcile 결과: %s", result)

    return {
        "meta": {
            "total_dates": len(all_dates),
            "date_range": f"{all_dates[0]} ~ {all_dates[-1]}",
            "stores": len(STORES),
        },
        "summary": {"result": result},
        "xcom": mock_ti._pushed,
    }


if __name__ == "__main__":
    run_script(main, "reconcile_all_okpos_history")
