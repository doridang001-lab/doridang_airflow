"""카카오톡 가맹점 대화 TXT -> kakao_store_raw.parquet 적재 DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_KakaoStoreRaw_load import run as run_kakao_store_raw_load
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_KAKAO_STORE_RAW_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


def task_load_kakao_store_raw(**context) -> str:
    return run_kakao_store_raw_load()


with DAG(
    dag_id=dag_id,
    description="카카오톡 가맹점 대화 TXT 스냅샷을 파싱해 kakao_store_raw.parquet에 멱등 누적",
    schedule=DB_KAKAO_STORE_RAW_TIME,
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    default_args=default_args,
    tags=["db", "kakao", "raw"],
) as dag:
    load_kakao_store_raw = PythonOperator(
        task_id="load_kakao_store_raw",
        python_callable=task_load_kakao_store_raw,
    )
