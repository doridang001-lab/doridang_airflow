"""Central Baemin macro upload DAG (적재 전용).

이 DAG은 중앙 PC에서만 unpause한다. 수집 PC는 _baemin_upload_inbox까지만
내보내고, analytics/baemin_macro 적재는 이 DAG이 담당한다.
검증(validate_*)은 DB_Beamin_Macro_Upload_Validate_Dags로 분리되어 있다.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import (
    _safe_run_id_part,
    has_ingested_or_manual_files,
    ingest,
)
from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_TIME

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem
VALIDATE_DAG_ID = "DB_Beamin_Macro_Upload_Validate_Dags"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


def trigger_validate(**context) -> str:
    ti = context["ti"]
    handoff_path = ti.xcom_pull(task_ids="ingest", key="handoff_path")
    if not handoff_path:
        logger.warning("배민 upload validate 트리거 스킵: handoff_path 없음")
        return "validate 트리거 스킵: handoff_path 없음"

    dag_run = context.get("dag_run")
    source_run_id = getattr(dag_run, "run_id", context.get("run_id", "manual"))
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    run_id = f"upload__{_safe_run_id_part(str(source_run_id))}"
    trigger_conf = {
        "handoff_path": str(handoff_path),
        "folder_pattern": conf.get("folder_pattern", "manual__top__*"),
        "source": "upload_ingest",
        "source_run_id": source_run_id,
    }
    for key in ("target_date", "target_dates", "manual_baemin_dir"):
        if conf.get(key):
            trigger_conf[key] = conf[key]

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    try:
        trigger_dag(dag_id=VALIDATE_DAG_ID, run_id=run_id, conf=trigger_conf)
    except DagRunAlreadyExists:
        logger.info("배민 upload validate DAG run 이미 존재: %s", run_id)
        return f"validate DAG run 이미 존재: {run_id}"
    logger.info("배민 upload validate DAG 트리거 완료: run_id=%s", run_id)
    return f"validate DAG 트리거 완료: {run_id}"


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_UPLOAD_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "upload"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        op_kwargs={"folder_pattern": "{{ dag_run.conf.get('folder_pattern', 'manual__top__*') }}"},
        execution_timeout=timedelta(minutes=30),
    )

    t_gate = ShortCircuitOperator(
        task_id="has_pending",
        python_callable=has_ingested_or_manual_files,
    )

    t_trigger_validate = PythonOperator(
        task_id="trigger_validate",
        python_callable=trigger_validate,
    )

    t_ingest >> t_gate >> t_trigger_validate
