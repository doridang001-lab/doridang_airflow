"""Delivery commission mart DAG."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission
from modules.transform.utility.schedule import DB_DELIVERY_COMMISSION_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_DELIVERY_COMMISSION_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "dashboard", "delivery_commission", "powerbi"],
) as dag:
    build_commission = PythonOperator(
        task_id="build_delivery_commission",
        python_callable=build_delivery_commission,
    )
