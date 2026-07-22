"""직영점 일일 매출 감시 리포트 mart DAG."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Daily_Corporate_Store_Report import build_report_mart_task
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_DAILY_CORPORATE_STORE_REPORT_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_DAILY_CORPORATE_STORE_REPORT_TIME,
    start_date=pendulum.datetime(2026, 7, 7, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "mart", "daily_report", "songpa", "llm"],
) as dag:
    build_report = PythonOperator(
        task_id="build_report_mart",
        python_callable=build_report_mart_task,
        execution_timeout=timedelta(minutes=20),
    )
