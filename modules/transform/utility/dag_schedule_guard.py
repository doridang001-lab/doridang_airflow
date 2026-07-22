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
