"""Baemin NOW unified mart DAG."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Baemin_now_grp import build_baemin_now_grp
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_BAEMIN_NOW_GRP_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_BAEMIN_NOW_GRP_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "mart", "baemin", "now", "powerbi"],
) as dag:
    build_now_grp = PythonOperator(
        task_id="build_baemin_now_grp",
        python_callable=build_baemin_now_grp,
    )
