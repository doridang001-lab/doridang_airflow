"""Storage cleanup DAG: prune Airflow logs, temp files, and stale Chrome profiles."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_StorageCleanup import cleanup_storage
from modules.transform.utility.schedule import DB_STORAGE_CLEANUP_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # 전체 순회가 Windows 바인드 마운트 위에서 5분 남짓 걸린다(2026-07-30 실측 322초).
    # 마운트가 멈추면 태스크가 무한정 붙잡히므로 상한을 둔다.
    "execution_timeout": timedelta(minutes=45),
}


with DAG(
    dag_id=dag_id,
    schedule=DB_STORAGE_CLEANUP_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "maintenance", "storage"],
    doc_md=__doc__,
) as dag:
    cleanup = PythonOperator(
        task_id="cleanup_storage",
        python_callable=cleanup_storage,
    )
