"""DB_UnifiedSales 08:37 스케줄 미생성 감시 및 1회 보정 트리거."""

import logging
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.utility.dag_schedule_guard import (
    build_guard_recovery_run_id,
    collect_today_dagruns,
    has_daily_unified_sales_run,
    kst_day_bounds,
)
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIFIED_SALES_GUARD_TIME

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem
TARGET_DAG_ID = "DB_UnifiedSales"
RECOVERY_RUN_PREFIX = "schedule_guard__"


def _on_failure_callback(context):
    ti = context.get("task_instance")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {ti.execution_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


def guard_unified_sales_schedule(**context) -> str:
    day_start_utc, day_end_utc, target_date = kst_day_bounds()
    with create_session() as session:
        runs = collect_today_dagruns(
            session,
            DagRun,
            dag_id=TARGET_DAG_ID,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
        )

    if has_daily_unified_sales_run(runs, recovery_prefix=RECOVERY_RUN_PREFIX):
        run_ids = ", ".join(str(getattr(run, "run_id", "")) for run in runs[:5])
        return f"{TARGET_DAG_ID} 당일 실행 확인: {run_ids}"

    run_id = build_guard_recovery_run_id(target_date=target_date, parent_run_id=context.get("run_id") or "")
    body = (
        "[Airflow 스케줄 미생성 보정]\n"
        f"dag_id={TARGET_DAG_ID}\n"
        f"target_date={target_date}\n"
        f"expected_schedule=08:37 KST\n"
        f"recovery_run_id={run_id}"
    )
    send_telegram(body)

    try:
        trigger_dag(dag_id=TARGET_DAG_ID, run_id=run_id, conf={})
    except DagRunAlreadyExists:
        logger.info("%s 보정 DagRun 이미 존재: %s", TARGET_DAG_ID, run_id)
        return f"{TARGET_DAG_ID} 보정 DagRun 이미 존재: {run_id}"

    logger.warning("%s 08:37 스케줄 미생성 감지 후 보정 트리거: %s", TARGET_DAG_ID, run_id)
    return f"{TARGET_DAG_ID} 보정 트리거 완료: {run_id}"


default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_SALES_GUARD_TIME,
    start_date=pendulum.datetime(2026, 7, 8, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unified_sales", "guard"],
) as dag:
    t1 = PythonOperator(
        task_id="guard_unified_sales_schedule",
        python_callable=guard_unified_sales_schedule,
    )
