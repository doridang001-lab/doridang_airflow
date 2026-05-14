"""
ToOrder daily store report collection DAG.

- Collects the daily sales report from ToOrder
- Splits each xlsx into per-day parquet files
- Also processes manually dropped xlsx files in the target folder
"""

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.DB_ai_daily_collection_01_collect import (
    run_ai_daily_collection_to_daily_parquet_dir,
)
from modules.transform.utility.schedule import SMD_TOORDER_SALES_REPORT_TIME


TARGET_DEST_DIR = Path(
    r"C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\toorder_daily_store"
)


def collect_toorder_daily_store(**context) -> str:
    target_date = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
    saved = run_ai_daily_collection_to_daily_parquet_dir(
        dest_dir=TARGET_DEST_DIR,
        target_date=target_date,
    )
    return "\n".join(saved)


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
