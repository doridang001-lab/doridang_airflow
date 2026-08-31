"""Collection comparison mart DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_files,
    cleanup_manual_baemin_files,
)
from modules.transform.pipelines.db.DB_CoupangMacro_load import (
    move_coupang_down_to_collect,
    load_coupang_macro_partition,
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
        python_callable=load_manual_baemin_files,
    )

    cleanup_baemin = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_files,
    )

    move_coupang = PythonOperator(
        task_id="move_coupang_to_collect",
        python_callable=move_coupang_down_to_collect,
    )

    load_coupang = PythonOperator(
        task_id="load_coupang_macro_partition",
        python_callable=load_coupang_macro_partition,
    )

    build_compare = PythonOperator(
        task_id="build_collection_compare",
        python_callable=build_collection_compare,
    )

    ingest_baemin >> cleanup_baemin
    move_coupang >> load_coupang
    [cleanup_baemin, load_coupang] >> build_compare
