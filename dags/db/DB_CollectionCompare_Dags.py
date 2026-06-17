"""Collection comparison mart DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
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

    build_compare = PythonOperator(
        task_id="build_collection_compare",
        python_callable=build_collection_compare,
    )
