"""
ToOrder daily store report collection DAG.

- Collects the daily sales report from ToOrder
- Splits each xlsx into per-day parquet files
- Also processes manually dropped xlsx files in the target folder

날짜 override:
- conf {"sale_date": "YYYY-MM-DD"} → 해당 날짜만 수집
- LOOKBACK_DAYS: conf 미지정 시 최근 N일 중 parquet 없는 날만 수집
"""

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.DB_ai_daily_collection_01_collect import (
    run_ai_daily_collection_to_daily_parquet_dir,
)
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.schedule import SMD_TOORDER_SALES_REPORT_TIME


TARGET_DEST_DIR = ANALYTICS_DB / "toorder_daily_store"

# LOOKBACK_DAYS: conf 미지정 시 최근 N일 중 parquet 없는 날만 수집
# None → 어제 1일만, int → 최근 N일 중 누락분만
LOOKBACK_DAYS: int | None = 3


def _parquet_exists(dest_dir: Path, date_str: str) -> bool:
    fname = f"toorder_daily_store_{date_str.replace('-', '')}.parquet"
    return (dest_dir / fname).exists()


def collect_toorder_daily_store(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("sale_date"):
        target_dates = [conf["sale_date"]]
    elif LOOKBACK_DAYS:
        today = pendulum.now("Asia/Seoul")
        candidates = [today.subtract(days=i).format("YYYY-MM-DD") for i in range(1, LOOKBACK_DAYS + 1)]
        target_dates = [d for d in candidates if not _parquet_exists(TARGET_DEST_DIR, d)]
        if not target_dates:
            yesterday = today.subtract(days=1).format("YYYY-MM-DD")
            target_dates = [yesterday]
    else:
        target_dates = [pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")]

    results = []
    for target_date in target_dates:
        saved = run_ai_daily_collection_to_daily_parquet_dir(
            dest_dir=TARGET_DEST_DIR,
            target_date=target_date,
        )
        results.extend(saved)
    return "\n".join(results) if results else "수집 없음"


with DAG(
    dag_id=Path(__file__).stem,
    schedule=SMD_TOORDER_SALES_REPORT_TIME,
    start_date=pendulum.datetime(2026, 5, 8, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["db", "toorder", "daily_store", "excel"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:
    PythonOperator(
        task_id="collect_toorder_daily_store",
        python_callable=collect_toorder_daily_store,
    )
