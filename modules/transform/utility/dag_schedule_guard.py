"""Helpers for detecting and repairing missing scheduled DagRuns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

import pendulum

KST = pendulum.timezone("Asia/Seoul")
READY_STATES = {"queued", "running", "success"}

# max_active_runs=1 DAG가 좀비 DagRun에 막히면 다음 스케줄이 영원히 생성되지 않는다.
# 이 시간을 넘긴 running DagRun은 실행 중이 아니라 박제된 것으로 본다.
# (2026-09-11 장애: Docker 백엔드 붕괴로 43개 DagRun이 이틀간 running으로 남아 전체 스케줄 봉쇄)
STALE_BLOCKING_RUN_HOURS = 6

# DagRun을 마감할 때 되돌려야 하는 미완료 TaskInstance 상태.
# None으로 비워야 다음 DagRun에서 새로 스케줄된다.
INCOMPLETE_TI_STATES = ("scheduled", "queued", "running", "up_for_retry", "restarting")

# 하나라도 이 상태면 DagRun은 살아 있는 것이다. 오래 걸리는 수집 런을 가드가
# 죽이면 버그보다 더 나쁘므로, 시간만 보고 마감하지 않는다.
# scheduled는 의도적으로 제외한다 - 2026-09-11 장애에서 좀비의 특징이 바로
# "scheduled인 채 executor로 넘어가지 않음" 이었다.
ACTIVE_TI_STATES = ("running", "queued", "deferred", "up_for_reschedule", "restarting")

# SchedulerJob 하트비트가 이 시간을 넘기면 프로세스가 멈춘 것으로 본다.
# 기본 heartrate는 5초 안팎이라 5분이면 충분히 넉넉한 임계치다.
# (2026-09-11: 스케줄러가 05:17:10부터 7분간 하트비트 없이 멈췄고, 그 사이
# 트리거된 DAG가 아무 것도 진행되지 않았다. Airflow 자체 워치독이 뒤늦게
# 감지해 새 프로세스로 교체했지만, 그 7분은 아무도 모르고 지나갔다.)
SCHEDULER_HEARTBEAT_STALE_MINUTES = 5


@dataclass(frozen=True)
class DagRunSnapshot:
    run_id: str
    state: str | None
    start_date: Any = None
    run_type: str | None = None


@dataclass(frozen=True)
class OverdueSchedule:
    dag_id: str
    next_dagrun: Any
    next_dagrun_create_after: Any
    lag_minutes: int
    blocked: bool = False
    blocking_run_id: str | None = None
    blocking_run_start: Any = None


def as_snapshot(run: Any) -> DagRunSnapshot:
    if isinstance(run, dict):
        return DagRunSnapshot(
            run_id=str(run.get("run_id") or ""),
            state=run.get("state"),
            start_date=run.get("start_date"),
            run_type=run.get("run_type"),
        )
    return DagRunSnapshot(
        run_id=str(getattr(run, "run_id", "") or ""),
        state=getattr(run, "state", None),
        start_date=getattr(run, "start_date", None),
        run_type=getattr(run, "run_type", None),
    )


def _to_pendulum_datetime(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pendulum.DateTime):
        return value
    if isinstance(value, str):
        return pendulum.parse(value)
    if hasattr(value, "to_pydatetime"):
        return pendulum.instance(value.to_pydatetime())
    return pendulum.instance(value)


def is_daily_unified_sales_run(run: Any, *, recovery_prefix: str) -> bool:
    snapshot = as_snapshot(run)
    if snapshot.state not in READY_STATES:
        return False
    if snapshot.run_id.startswith("today__"):
        return False
    if snapshot.run_id.startswith(recovery_prefix):
        return True
    return True


def has_daily_unified_sales_run(runs: Iterable[Any], *, recovery_prefix: str) -> bool:
    return any(is_daily_unified_sales_run(run, recovery_prefix=recovery_prefix) for run in runs)


def kst_day_bounds(now: Any | None = None) -> tuple[Any, Any, str]:
    current = now or pendulum.now(KST)
    current = pendulum.instance(current).in_timezone(KST)
    start = current.start_of("day")
    end = start.add(days=1)
    return start.in_timezone("UTC"), end.in_timezone("UTC"), start.format("YYYYMMDD")


def build_guard_recovery_run_id(*, target_date: str, parent_run_id: str) -> str:
    safe_parent = re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(parent_run_id or "scheduled")).strip("_")
    return f"schedule_guard__{target_date}__{safe_parent[:80]}"


def build_overdue_recovery_run_id(*, dag_id: str, logical_date: Any) -> str:
    logical_dt = _to_pendulum_datetime(logical_date)
    logical_text = logical_dt.in_timezone("UTC").format("YYYYMMDDTHHmmss") if logical_dt else "unknown"
    safe_dag_id = re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(dag_id or "dag")).strip("_")
    return f"schedule_guard__{logical_text}__{safe_dag_id[:80]}"


def has_dagrun_for_logical_date(
    session: Any,
    dag_run_model: Any,
    *,
    dag_id: str,
    logical_date: Any,
) -> bool:
    logical_dt = _to_pendulum_datetime(logical_date)
    if logical_dt is None:
        return False
    return (
        session.query(dag_run_model)
        .filter(dag_run_model.dag_id == dag_id)
        .filter(dag_run_model.execution_date == logical_dt)
        .first()
        is not None
    )


def collect_today_dagruns(session: Any, dag_run_model: Any, *, dag_id: str, day_start_utc: Any, day_end_utc: Any) -> list[Any]:
    return (
        session.query(dag_run_model)
        .filter(dag_run_model.dag_id == dag_id)
        .filter(dag_run_model.start_date.isnot(None))
        .filter(dag_run_model.start_date >= day_start_utc)
        .filter(dag_run_model.start_date < day_end_utc)
        .order_by(dag_run_model.start_date.desc())
        .all()
    )


def find_stale_scheduler(
    session: Any,
    job_model: Any,
    *,
    stale_minutes: int = SCHEDULER_HEARTBEAT_STALE_MINUTES,
    now: Any | None = None,
) -> dict | None:
    """가장 최근 SchedulerJob의 하트비트가 오래됐으면 그 정보를 반환한다."""
    latest = (
        session.query(job_model)
        .filter(job_model.job_type == "SchedulerJob")
        .filter(job_model.state == "running")
        .order_by(job_model.latest_heartbeat.desc())
        .first()
    )
    if latest is None or latest.latest_heartbeat is None:
        return None
    heartbeat = _to_pendulum_datetime(latest.latest_heartbeat)
    current = pendulum.instance(now or pendulum.now("UTC")).in_timezone("UTC")
    age_minutes = (current - heartbeat.in_timezone("UTC")).total_seconds() / 60
    if age_minutes < stale_minutes:
        return None
    return {"hostname": getattr(latest, "hostname", None), "age_minutes": round(age_minutes, 1)}


def find_zeroed_pools(session: Any, pool_model: Any) -> list[dict]:
    """슬롯이 0인 풀을 찾는다.

    풀 슬롯은 태스크가 scheduled -> queued 로 넘어가는 관문이다. 0이면
    이미 실행 중이던 태스크만 계속 돌고 새 태스크는 전혀 시작되지 않는다.
    2026-09-11: 4개 풀(default_pool 포함) 전체가 원인 불명으로 0이 되어
    신규 트리거·스케줄 태스크가 아무것도 시작하지 못한 사고가 있었다.
    """
    zeroed = []
    for pool in session.query(pool_model).all():
        if getattr(pool, "slots", None) == 0:
            zeroed.append({"pool": pool.pool, "description": getattr(pool, "description", None)})
    return zeroed


def find_oldest_running_dagrun(session: Any, dag_run_model: Any, *, dag_id: str) -> Any | None:
    """해당 DAG에서 가장 오래된 running DagRun. max_active_runs 봉쇄의 주범을 찾는다."""
    return (
        session.query(dag_run_model)
        .filter(dag_run_model.dag_id == dag_id)
        .filter(dag_run_model.state == "running")
        .order_by(dag_run_model.start_date)
        .first()
    )


def collect_overdue_schedules(
    session: Any,
    dag_model: Any,
    *,
    dag_run_model: Any | None = None,
    now: Any | None = None,
    grace_minutes: int = 10,
    exclude_dag_ids: Iterable[str] | None = None,
    skip_existing_logical_runs: bool = False,
    limit: int = 50,
) -> list[OverdueSchedule]:
    """Return unpaused DAGs whose scheduled DagRun creation is late."""
    current = pendulum.instance(now or pendulum.now("UTC")).in_timezone("UTC")
    threshold = current.subtract(minutes=grace_minutes)
    excludes = [dag_id for dag_id in (exclude_dag_ids or []) if dag_id]

    query = (
        session.query(dag_model)
        .filter(dag_model.is_active == True)  # noqa: E712
        .filter(dag_model.is_paused == False)  # noqa: E712
        .filter(dag_model.next_dagrun.isnot(None))
        .order_by(dag_model.next_dagrun)
    )
    if excludes:
        query = query.filter(~dag_model.dag_id.in_(excludes))

    overdue: list[OverdueSchedule] = []
    for model in query.all():
        if skip_existing_logical_runs and dag_run_model is not None:
            if has_dagrun_for_logical_date(
                session,
                dag_run_model,
                dag_id=str(model.dag_id),
                logical_date=model.next_dagrun,
            ):
                continue
        create_after = _to_pendulum_datetime(model.next_dagrun_create_after)
        blocked = create_after is None
        anchor = _to_pendulum_datetime(model.next_dagrun) if blocked else create_after
        if anchor is None:
            continue
        anchor = anchor.in_timezone("UTC")
        if anchor > threshold:
            continue
        lag_minutes = max(0, int((current - anchor).total_seconds() // 60))
        blocking_run = None
        if blocked and dag_run_model is not None:
            # 봉쇄된 DAG는 어떤 DagRun이 막고 있는지 함께 실어 보낸다.
            # 이 정보가 있어야 좀비인지(오래된 running) 정상 실행 중인지 구분할 수 있다.
            blocking_run = find_oldest_running_dagrun(
                session,
                dag_run_model,
                dag_id=str(model.dag_id),
            )
        blocking_snapshot = as_snapshot(blocking_run) if blocking_run is not None else None
        overdue.append(
            OverdueSchedule(
                dag_id=str(model.dag_id),
                next_dagrun=model.next_dagrun,
                next_dagrun_create_after=model.next_dagrun_create_after,
                lag_minutes=lag_minutes,
                blocked=blocked,
                blocking_run_id=blocking_snapshot.run_id if blocking_snapshot else None,
                blocking_run_start=blocking_snapshot.start_date if blocking_snapshot else None,
            )
        )
        if len(overdue) >= limit:
            break
    return overdue


def is_stale_blocking_run(
    item: OverdueSchedule,
    *,
    stale_hours: int = STALE_BLOCKING_RUN_HOURS,
    now: Any | None = None,
) -> bool:
    """봉쇄 중인 DagRun이 좀비로 볼 만큼 오래됐는지."""
    if not item.blocked or not item.blocking_run_id:
        return False
    started = _to_pendulum_datetime(item.blocking_run_start)
    if started is None:
        # start_date가 없으면 실행 이력을 판단할 수 없다. 함부로 죽이지 않는다.
        return False
    current = pendulum.instance(now or pendulum.now("UTC")).in_timezone("UTC")
    return started.in_timezone("UTC") <= current.subtract(hours=stale_hours)


def blocking_run_is_idle(
    session: Any,
    task_instance_model: Any,
    *,
    dag_id: str,
    run_id: str,
    idle_hours: int = STALE_BLOCKING_RUN_HOURS,
    now: Any | None = None,
) -> bool:
    """봉쇄 런이 실제로 멈춰 있는지 확인한다.

    살아 있는 TaskInstance가 하나라도 있거나, 최근에 끝난 TaskInstance가 있으면
    아직 진행 중인 런이므로 건드리지 않는다.
    """
    active = (
        session.query(task_instance_model)
        .filter(task_instance_model.dag_id == dag_id)
        .filter(task_instance_model.run_id == run_id)
        .filter(task_instance_model.state.in_(ACTIVE_TI_STATES))
        .first()
    )
    if active is not None:
        return False

    current = pendulum.instance(now or pendulum.now("UTC")).in_timezone("UTC")
    cutoff = current.subtract(hours=idle_hours)
    recent = (
        session.query(task_instance_model)
        .filter(task_instance_model.dag_id == dag_id)
        .filter(task_instance_model.run_id == run_id)
        .filter(task_instance_model.end_date.isnot(None))
        .filter(task_instance_model.end_date > cutoff)
        .first()
    )
    return recent is None


def expire_blocking_run(
    session: Any,
    dag_run_model: Any,
    task_instance_model: Any,
    *,
    dag_id: str,
    run_id: str,
    now: Any | None = None,
) -> int:
    """좀비 DagRun을 failed로 마감하고 미완료 TaskInstance를 비운다.

    TaskInstance를 None으로 되돌려야 다음 DagRun에서 새로 스케줄된다.
    비운 TaskInstance 개수를 반환한다.
    """
    current = pendulum.instance(now or pendulum.now("UTC")).in_timezone("UTC")

    cleared = (
        session.query(task_instance_model)
        .filter(task_instance_model.dag_id == dag_id)
        .filter(task_instance_model.run_id == run_id)
        .filter(task_instance_model.state.in_(INCOMPLETE_TI_STATES))
        .update({"state": None}, synchronize_session=False)
    )
    (
        session.query(dag_run_model)
        .filter(dag_run_model.dag_id == dag_id)
        .filter(dag_run_model.run_id == run_id)
        .update({"state": "failed", "end_date": current}, synchronize_session=False)
    )
    return int(cleared or 0)


def expire_stale_blocking_runs(
    session: Any,
    dag_run_model: Any,
    task_instance_model: Any,
    overdue: Iterable[OverdueSchedule],
    *,
    stale_hours: int = STALE_BLOCKING_RUN_HOURS,
    now: Any | None = None,
) -> list[str]:
    """좀비 DagRun에 막힌 DAG들을 풀어준다. 해소한 dag_id 목록을 반환."""
    unblocked: list[str] = []
    for item in overdue:
        if not is_stale_blocking_run(item, stale_hours=stale_hours, now=now):
            continue
        if not blocking_run_is_idle(
            session,
            task_instance_model,
            dag_id=item.dag_id,
            run_id=item.blocking_run_id,
            idle_hours=stale_hours,
            now=now,
        ):
            # 오래됐지만 아직 움직이는 런. 오래 걸리는 수집일 수 있으므로 살려둔다.
            continue
        expire_blocking_run(
            session,
            dag_run_model,
            task_instance_model,
            dag_id=item.dag_id,
            run_id=item.blocking_run_id,
            now=now,
        )
        unblocked.append(item.dag_id)
    return unblocked


def format_overdue_schedule_alert(overdue: Iterable[OverdueSchedule], *, grace_minutes: int) -> str:
    lines = [
        "[Airflow 스케줄 생성 지연]",
        f"grace_minutes={grace_minutes}",
    ]
    for item in overdue:
        create_after_dt = _to_pendulum_datetime(item.next_dagrun_create_after)
        logical_dt = _to_pendulum_datetime(item.next_dagrun)
        create_after_kst = create_after_dt.in_timezone(KST) if create_after_dt is not None else None
        logical_kst = logical_dt.in_timezone(KST) if logical_dt is not None else None
        create_after_text = create_after_kst.format("YYYY-MM-DD HH:mm:ss") if create_after_kst is not None else "unknown"
        logical_text = logical_kst.format("YYYY-MM-DD HH:mm:ss") if logical_kst is not None else "unknown"
        status_text = "max_active_runs 봉쇄 의심" if item.blocked else "생성 지연"
        blocker_text = f", blocked_by={item.blocking_run_id}" if item.blocking_run_id else ""
        lines.append(
            f"- {item.dag_id}: status={status_text}, create_after={create_after_text} KST, "
            f"logical={logical_text} KST, "
            f"lag={item.lag_minutes}분{blocker_text}"
        )
    return "\n".join(lines)
