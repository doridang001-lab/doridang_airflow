"""Collection comparison mart DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import ingest_baemin_pc2_inbox
from modules.transform.utility.schedule import DB_COLLECTION_COMPARE_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def _latest_macro_execution_date(dt, **kwargs):
    """가장 최근 성공한 CoupangMacro run의 execution_date 반환."""
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState

    runs = DagRun.find(
        dag_id="DB_CoupangMacro_Load_Dags",
        state=DagRunState.SUCCESS,
    )
    eligible = sorted(
        [run for run in runs if run.execution_date <= dt],
        key=lambda run: run.execution_date,
        reverse=True,
    )
    return eligible[0].execution_date if eligible else dt


def ingest_pc2(**context) -> str:
    return ingest_baemin_pc2_inbox(**context)


with DAG(
    dag_id=dag_id,
    schedule=DB_COLLECTION_COMPARE_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "dashboard", "collection_compare", "powerbi"],
) as dag:
    ingest_pc2_inbox = PythonOperator(
        task_id="ingest_baemin_pc2_inbox",
        python_callable=ingest_pc2,
    )

    ingest_baemin = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_orders,
    )

    cleanup_baemin = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_orders,
    )

    ensure_coupang_macro_loaded = ExternalTaskSensor(
        task_id="ensure_coupang_macro_loaded",
        external_dag_id="DB_CoupangMacro_Load_Dags",
        external_task_id=None,
        execution_date_fn=_latest_macro_execution_date,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=60,
        timeout=3600,
        mode="poke",
    )

    build_compare = PythonOperator(
        task_id="build_collection_compare",
        python_callable=build_collection_compare,
    )

    ingest_pc2_inbox >> ingest_baemin >> cleanup_baemin
    [cleanup_baemin, ensure_coupang_macro_loaded] >> build_compare
