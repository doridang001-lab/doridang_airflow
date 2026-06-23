"""Collection comparison mart DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
from modules.transform.utility.schedule import DB_COLLECTION_COMPARE_TIME

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
    schedule=DB_COLLECTION_COMPARE_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "dashboard", "collection_compare", "powerbi"],
) as dag:
    ingest_baemin = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_orders,
    )

    cleanup_baemin = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_orders,
    )

    ensure_coupang_macro_loaded = TriggerDagRunOperator(
        task_id="ensure_coupang_macro_loaded",
        trigger_dag_id="DB_CoupangMacro_Load_Dags",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    build_compare = PythonOperator(
        task_id="build_collection_compare",
        python_callable=build_collection_compare,
    )

    ingest_baemin >> cleanup_baemin
    [cleanup_baemin, ensure_coupang_macro_loaded] >> build_compare
