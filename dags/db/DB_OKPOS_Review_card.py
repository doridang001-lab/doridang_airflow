"""
OKPOS card approval test DAG.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_OKPOS_Card_Test import (
    build_okpos_customer_mart,
    download_okpos_card_test,
    ingest_manual_okpos_card_files,
    save_okpos_card_test_csv,
    validate_okpos_card_lookback,
)
from modules.transform.utility.notifier import on_failure_callback

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
    schedule="5 6 * * *", # "매일 오전 6시 5분" -> schedule="5 6 * * *"
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "card", "test", "selenium"],
) as dag:
    t0 = PythonOperator(
        task_id="ingest_manual_okpos_card_files",
        python_callable=ingest_manual_okpos_card_files,
    )

    t1 = PythonOperator(
        task_id="download_okpos_card_test",
        python_callable=download_okpos_card_test,
        execution_timeout=timedelta(minutes=40),
    )

    t2 = PythonOperator(
        task_id="save_okpos_card_test_csv",
        python_callable=save_okpos_card_test_csv,
    )

    t3 = PythonOperator(
        task_id="build_okpos_customer_mart",
        python_callable=build_okpos_customer_mart,
    )

    t4 = PythonOperator(
        task_id="validate_okpos_card_lookback",
        python_callable=validate_okpos_card_lookback,
        op_kwargs={"lookback_days": 7, "min_date": "2026-04-17"},
    )

    t0 >> t1 >> t2 >> t3 >> t4
