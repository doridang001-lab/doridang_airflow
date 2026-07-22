"""OKPOS 당일 카드 승인 수집 DAG."""

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
from modules.transform.utility.schedule import DB_OKPOS_REVIEW_CARD_TODAY_TIME

dag_id = Path(__file__).stem


def download_okpos_card_today(**context) -> str:
    """기존 카드 승인 파이프라인을 KST 당일 기준으로 실행한다."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if not isinstance(conf, dict):
        conf = {}
        if dag_run is not None:
            dag_run.conf = conf
    conf.setdefault("sale_date", pendulum.now("Asia/Seoul").format("YYYY-MM-DD"))
    return download_okpos_card_test(**context)


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
    schedule=DB_OKPOS_REVIEW_CARD_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "card", "today", "selenium"],
) as dag:
    t0 = PythonOperator(
        task_id="ingest_manual_okpos_card_files",
        python_callable=ingest_manual_okpos_card_files,
    )

    t1 = PythonOperator(
        task_id="download_okpos_card_test",
        python_callable=download_okpos_card_today,
        pool="selenium_pool",
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
