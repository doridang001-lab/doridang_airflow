"""Airflow scheduled DagRun creation delay guard."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.jobs.job import Job
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.utility.dag_schedule_guard import (
    STALE_BLOCKING_RUN_HOURS,
    build_overdue_recovery_run_id,
    collect_overdue_schedules,
    expire_stale_blocking_runs,
    find_stale_scheduler,
    find_zeroed_pools,
    format_overdue_schedule_alert,
)
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.schedule import AIRFLOW_SCHEDULE_GUARD_TIME
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem
GRACE_MINUTES = 10
STALE_RUN_HOURS = STALE_BLOCKING_RUN_HOURS


def _to_utc_datetime(value):
    if isinstance(value, pendulum.DateTime):
        return value.in_timezone("UTC")
    if isinstance(value, str):
        return pendulum.parse(value).in_timezone("UTC")
    if hasattr(value, "to_pydatetime"):
        return pendulum.instance(value.to_pydatetime()).in_timezone("UTC")
    return pendulum.instance(value).in_timezone("UTC")


def trigger_recovery_runs(
    overdue,
    *,
    parent_run_id: str | None = None,
    unblocked_dag_ids: set[str] | None = None,
) -> tuple[list[str], list[str]]:
    triggered: list[str] = []
    skipped: list[str] = []
    unblocked = unblocked_dag_ids or set()

    for item in overdue:
        # 좀비 DagRun을 방금 마감해 봉쇄가 풀린 DAG는 그대로 보정 트리거한다.
        if getattr(item, "blocked", False) and item.dag_id not in unblocked:
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
        # 풀 용량이 0이면 큐잉 자체가 멈춘다. 봉쇄 DAG가 있든 없든 매번 확인한다
        # (2026-09-11: 원인 불명으로 풀 4개가 전부 0이 되어 신규 태스크가
        # 하나도 시작하지 못한 사고가 이 점검 없이 지나갔다).
        zeroed_pools = find_zeroed_pools(session, Pool)
        if zeroed_pools:
            names = ", ".join(p["pool"] for p in zeroed_pools)
            message = f"[Airflow 풀 슬롯 0 경고] 신규 태스크가 시작되지 않습니다: {names}"
            logger.error(message)
            try:
                send_telegram(message)
            except Exception as exc:
                logger.warning("풀 슬롯 경고 알림 실패: %s", exc)

        # 스케줄러 하트비트가 멎으면 트리거된 DAG가 스케줄러 자체가 죽은 줄도
        # 모른 채 그냥 대기한다(2026-09-11: 7분간 무응답, 워치독이 뒤늦게 감지).
        stale_scheduler = find_stale_scheduler(session, Job)
        if stale_scheduler:
            message = (
                f"[Airflow 스케줄러 하트비트 경고] {stale_scheduler['age_minutes']}분간 무응답 "
                f"(host={stale_scheduler['hostname']})"
            )
            logger.error(message)
            try:
                send_telegram(message)
            except Exception as exc:
                logger.warning("스케줄러 하트비트 경고 알림 실패: %s", exc)

        overdue = collect_overdue_schedules(
            session,
            DagModel,
            dag_run_model=DagRun,
            grace_minutes=GRACE_MINUTES,
            exclude_dag_ids={dag_id},
            skip_existing_logical_runs=True,
        )

        if not overdue:
            notes = []
            if zeroed_pools:
                notes.append(f"풀 슬롯 0 경고: {names}")
            if stale_scheduler:
                notes.append(f"스케줄러 하트비트 {stale_scheduler['age_minutes']}분 무응답")
            if notes:
                return f"스케줄 생성 지연 없음, " + ", ".join(notes)
            return f"스케줄 생성 지연 없음 (grace={GRACE_MINUTES}분)"

        # 감지만 하고 넘어가면 좀비 DagRun이 max_active_runs=1 DAG의 다음 스케줄을
        # 영원히 막는다(2026-09-11 장애). 오래된 봉쇄 런은 여기서 직접 마감한다.
        unblocked = expire_stale_blocking_runs(
            session,
            DagRun,
            TaskInstance,
            overdue,
            stale_hours=STALE_RUN_HOURS,
        )
        if unblocked:
            session.commit()
            logger.warning(
                "%d시간 이상 running으로 남은 봉쇄 DagRun 마감: %s",
                STALE_RUN_HOURS,
                ", ".join(unblocked),
            )

    body = format_overdue_schedule_alert(overdue, grace_minutes=GRACE_MINUTES)
    logger.warning(body)
    triggered, skipped = trigger_recovery_runs(
        overdue,
        parent_run_id=context.get("run_id"),
        unblocked_dag_ids=set(unblocked),
    )
    return (
        f"{body}\n"
        f"스케줄 생성 지연 보정: triggered={len(triggered)} skipped={len(skipped)} "
        f"unblocked={len(unblocked)} (grace={GRACE_MINUTES}분, stale={STALE_RUN_HOURS}시간)"
    )


with DAG(
    dag_id=dag_id,
    description="Airflow scheduled DagRun 생성 지연을 10분마다 점검",
    schedule=AIRFLOW_SCHEDULE_GUARD_TIME,
    start_date=pendulum.datetime(2026, 7, 30, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
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
