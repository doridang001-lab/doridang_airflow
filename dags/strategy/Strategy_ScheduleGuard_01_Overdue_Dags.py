"""Airflow scheduled DagRun creation delay guard."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.utility.dag_schedule_guard import (
    build_overdue_recovery_run_id,
    collect_overdue_schedules,
    format_overdue_schedule_alert,
)
from modules.transform.utility.schedule import AIRFLOW_SCHEDULE_GUARD_TIME

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem
GRACE_MINUTES = 10


def _to_utc_datetime(value):
    if isinstance(value, pendulum.DateTime):
        return value.in_timezone("UTC")
    if isinstance(value, str):
        return pendulum.parse(value).in_timezone("UTC")
    if hasattr(value, "to_pydatetime"):
        return pendulum.instance(value.to_pydatetime()).in_timezone("UTC")
    return pendulum.instance(value).in_timezone("UTC")


def trigger_recovery_runs(overdue, *, parent_run_id: str | None = None) -> tuple[list[str], list[str]]:
    triggered: list[str] = []
    skipped: list[str] = []

    for item in overdue:
        if getattr(item, "blocked", False):
            logger.warning(
                "%s 스케줄 봉쇄(max_active_runs 의심), 자동 트리거 생략: next_dagrun=%s lag=%d분",
                item.dag_id,
                item.next_dagrun,
                item.lag_minutes,
            )
            skipped.append(f"{item.dag_id}:blocked")
            continue

        logical_date = _to_utc_datetime(item.next_dagrun)
        recovery_run_id = build_overdue_recovery_run_id(
            dag_id=item.dag_id,
            logical_date=logical_date,
        )
        try:
            trigger_dag(
                dag_id=item.dag_id,
                run_id=recovery_run_id,
                conf={
                    "recovered_by": dag_id,
                    "parent_run_id": parent_run_id,
                    "scheduled_logical_date": logical_date.to_iso8601_string(),
                },
                execution_date=logical_date,
            )
        except DagRunAlreadyExists:
            logger.info("%s 보정 DagRun 이미 존재: %s", item.dag_id, recovery_run_id)
            skipped.append(f"{item.dag_id}:{recovery_run_id}")
            continue

        logger.warning("%s 스케줄 누락 보정 트리거: %s", item.dag_id, recovery_run_id)
        triggered.append(f"{item.dag_id}:{recovery_run_id}")

    return triggered, skipped


def check_overdue_schedules(**context) -> str:
    with create_session() as session:
        overdue = collect_overdue_schedules(
            session,
            DagModel,
            dag_run_model=DagRun,
            grace_minutes=GRACE_MINUTES,
            exclude_dag_ids={dag_id},
            skip_existing_logical_runs=True,
        )

    if not overdue:
        return f"스케줄 생성 지연 없음 (grace={GRACE_MINUTES}분)"

    body = format_overdue_schedule_alert(overdue, grace_minutes=GRACE_MINUTES)
    logger.warning(body)
    triggered, skipped = trigger_recovery_runs(overdue, parent_run_id=context.get("run_id"))
    return (
        f"{body}\n"
        f"스케줄 생성 지연 보정: triggered={len(triggered)} skipped={len(skipped)} "
        f"(grace={GRACE_MINUTES}분)"
    )


with DAG(
    dag_id=dag_id,
    description="Airflow scheduled DagRun 생성 지연을 10분마다 점검",
    schedule=AIRFLOW_SCHEDULE_GUARD_TIME,
    start_date=pendulum.datetime(2026, 7, 30, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        # 자정 스택 재시작 창에서 큐 적체로 미실행 실패하면 복귀 후 스스로 재시도한다.
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["strategy", "monitoring", "schedule_guard"],
) as dag:
    check_task = PythonOperator(
        task_id="check_overdue_schedules",
        python_callable=check_overdue_schedules,
        execution_timeout=pendulum.duration(minutes=5),
    )
