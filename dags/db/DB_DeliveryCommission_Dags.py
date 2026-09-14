"""Delivery commission mart DAG."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_DeliveryCommission import (
    build_delivery_commission,
    build_delivery_revenue,
    build_store_cost_allocation,
    monitor_baemin_settlement_missing,
    trigger_baemin_orders_only_recollect,
)
from modules.transform.utility.schedule import DB_DELIVERY_COMMISSION_TIME
from modules.transform.utility.notifier import on_failure_callback

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
    monitor_settlement = PythonOperator(
        task_id="monitor_baemin_settlement_missing",
        python_callable=monitor_baemin_settlement_missing,
        retries=0,
        on_failure_callback=on_failure_callback,
    )

    trigger_recollect = PythonOperator(
        task_id="trigger_baemin_orders_only_recollect",
        python_callable=trigger_baemin_orders_only_recollect,
        retries=0,
        on_failure_callback=on_failure_callback,
    )

    build_commission = PythonOperator(
        task_id="build_delivery_commission",
        python_callable=build_delivery_commission,
    )

    build_revenue = PythonOperator(
        task_id="build_delivery_revenue",
        python_callable=build_delivery_revenue,
    )

    build_cost = PythonOperator(
        task_id="build_store_cost_allocation",
        python_callable=build_store_cost_allocation,
    )

    monitor_settlement >> trigger_recollect >> build_commission >> build_revenue >> build_cost
