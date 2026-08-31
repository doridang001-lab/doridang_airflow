"""Helpers for detecting and repairing missing scheduled DagRuns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

import pendulum

KST = pendulum.timezone("Asia/Seoul")
READY_STATES = {"queued", "running", "success"}


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
        overdue.append(
            OverdueSchedule(
                dag_id=str(model.dag_id),
                next_dagrun=model.next_dagrun,
                next_dagrun_create_after=model.next_dagrun_create_after,
                lag_minutes=lag_minutes,
                blocked=blocked,
            )
        )
        if len(overdue) >= limit:
            break
    return overdue


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
        lines.append(
            f"- {item.dag_id}: status={status_text}, create_after={create_after_text} KST, "
            f"logical={logical_text} KST, "
            f"lag={item.lag_minutes}분"
        )
    return "\n".join(lines)
