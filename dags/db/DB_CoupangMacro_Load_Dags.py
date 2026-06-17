from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_CoupangMacro_load import load_coupang_macro_partition
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_COUPANG_MACRO_TIME


dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_COUPANG_MACRO_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "coupang_macro", "partition"],
) as dag:

    PythonOperator(
        task_id="load_coupang_macro_partition",
        python_callable=load_coupang_macro_partition,
    )

