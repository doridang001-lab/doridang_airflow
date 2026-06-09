"""
홀 주간보고 Excel을 Windows 로컬에서 직접 생성하는 스크립트.

목적:
  Airflow worker(Docker)가 OneDrive 마운트 폴더에 파일을 쓰면 OneDrive가
  online-only placeholder 로 두거나 동기화 중 잠그는 경우가 있어, Windows
  Excel에서 바로 열리지 않을 수 있다. 이 스크립트는 동일한 파이프라인을
  로컬 .venv 로 실행해 OneDrive 로컬 디스크에 직접 파일을 생성한다.

실행 (Windows PowerShell):
  C:\\airflow\\.venv\\Scripts\\python.exe scripts\\run_hall_weekly_report_local.py
  또는 scripts\\run_hall_weekly_report_local.bat 더블클릭

전제:
  - DB_Hall_Sales_Target_Dags 가 한 번 실행되어 hall_sale_target.csv /
    hall_marketing_target.csv 가 MART_DB 에 존재해야 한다 (이 스크립트는
    CSV → Excel 단계만 재생성한다).
"""

import os
import sys
from datetime import date

from _base import logger, run_script

from modules.transform.utility.paths import MART_DB
from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
from modules.transform.pipelines.db.DB_Hall_Sales_Target import build_hall_sales_target
from modules.transform.pipelines.db.DB_Hall_Sales_Excel import build_weekly_report_excel
from modules.transform.pipelines.db.DB_Hall_Daily_Excel import build_daily_tracking_excel

XLSX_DIR = MART_DB / "hall_sales_target"


def _open_in_excel(path) -> None:
    """생성된 파일을 OS 기본 앱(Excel)으로 자동 열기 (Windows 전용)."""
    if not path.exists():
        logger.warning("열 파일이 없습니다: %s", path)
        return
    if sys.platform.startswith("win"):
        os.startfile(str(path))  # type: ignore[attr-defined]
        logger.info("Excel 자동 실행: %s", path)
    else:
        logger.info("자동 열기 생략 (non-Windows): %s", path)


def main() -> dict:
    monthly_targets, marketing_monthly_targets, daily_tracking_target = build_targets()

    csv_msg = build_hall_sales_target(monthly_targets=monthly_targets)
    logger.info("매출 CSV: %s", csv_msg)

    weekly_msg = build_weekly_report_excel(
        monthly_targets=monthly_targets,
        marketing_monthly_targets=marketing_monthly_targets,
    )
    logger.info("주간보고: %s", weekly_msg)

    daily_msg = build_daily_tracking_excel(
        monthly_targets=monthly_targets,
        marketing_monthly_targets=marketing_monthly_targets,
        daily_target=daily_tracking_target,
    )
    logger.info("일간트래킹: %s", daily_msg)

    # 생성 후 주간보고만 자동 실행
    weekly_path = XLSX_DIR / f"hall_weekly_report_{date.today():%y%m%d}.xlsx"
    _open_in_excel(weekly_path)

    return {
        "meta": {"script": "run_hall_weekly_report_local", "status": "ok"},
        "summary": {"weekly": weekly_msg, "daily": daily_msg},
        "stats": {},
    }


if __name__ == "__main__":
    run_script(main, "run_hall_weekly_report_local")
