"""
ETL_StoreMigration_Dags.py — store= 파티션 폴더명 정규화 마이그레이션 DAG

수동 트리거 전용. STORE_NAME_MAP 기준으로 구버전 store= 폴더를 감지하고
공식명 폴더로 머지/이동한 뒤 구버전 폴더를 삭제한다.

트리거 방법:
  - conf 없음 → 실제 마이그레이션 실행
  - conf {"dry_run": true} → 변환 계획만 출력 (파일 변경 없음)

대상 채널:
  - analytics/posfeed_sales
  - analytics/posfeed_sales_detail
  - analytics/baemin_marketing
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.etl.DB_ETL_StoreMigration import (
    scan_legacy_partitions,
    migrate_partitions,
)
from modules.transform.utility.notifier import on_failure_callback

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


def _run_migrate(**context):
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    dry_run = conf.get("dry_run", False) if isinstance(conf, dict) else False
    if dry_run:
        logger.info("dry_run=true → 실제 변경 없이 계획만 출력")
    return migrate_partitions(dry_run=dry_run, **context)


with DAG(
    dag_id=dag_id,
    schedule=None,  # 수동 트리거 전용
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "migration", "manual"],
) as dag:

    scan_task = PythonOperator(
        task_id="scan_legacy_partitions",
        python_callable=scan_legacy_partitions,
    )

    migrate_task = PythonOperator(
        task_id="migrate_partitions",
        python_callable=_run_migrate,
    )

    scan_task >> migrate_task
