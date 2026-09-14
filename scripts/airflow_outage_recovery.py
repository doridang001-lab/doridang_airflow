"""Outage reconciliation against serialized timetables and actual runs.

Host invocation writes a stable incident inventory; subsequent invocations refresh
the same time window. Read-only unless --ensure-dag is explicitly supplied.
Existing scheduled runs are never duplicated or cleared.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def inspect_runs(start: str, end: str, ensure_dag: str | None = None) -> dict:
    import pendulum
    from airflow.models import DagModel, DagRun, TaskInstance
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType

    begin, finish = pendulum.parse(start), pendulum.parse(end)
    results = []
    with create_session() as session:
        models = session.query(DagModel).filter(DagModel.is_active.is_(True), DagModel.is_paused.is_(False)).all()
        for model in models:
            serialized = session.query(SerializedDagModel).filter_by(dag_id=model.dag_id).first()
            if serialized is None:
                results.append(dict(dag_id=model.dag_id, status="serialization_missing"))
                continue
            dag = serialized.dag
            timetable = dag.timetable
            if not timetable.can_be_scheduled:
                continue
            if not hasattr(timetable, "_get_prev"):
                results.append(dict(dag_id=dag.dag_id, status="unsupported_timetable"))
                continue
            due = timetable._get_prev(finish + timedelta(microseconds=1))
            if due < begin:
                continue
            # One representative run per DAG covers polling ticks without replaying
            # historical snapshots. Record its actual interval for human review.
            runs = session.query(DagRun).filter(
                DagRun.dag_id == dag.dag_id, DagRun.run_type == "scheduled",
                DagRun.data_interval_end >= begin, DagRun.data_interval_end <= finish,
            ).order_by(DagRun.data_interval_end.desc()).all()
            run = runs[0] if runs else None
            if run is None and ensure_dag == dag.dag_id:
                interval = timetable.infer_manual_data_interval(run_after=due)
                run_id = DagRun.generate_run_id(DagRunType.SCHEDULED, interval.start)
                run = session.query(DagRun).filter_by(dag_id=dag.dag_id, run_id=run_id).first()
                if run is None:
                    run = dag.create_dagrun(
                        run_id=run_id, execution_date=interval.start,
                        data_interval=interval, run_type=DagRunType.SCHEDULED,
                        state=DagRunState.QUEUED, external_trigger=False, session=session,
                    )
            row = dict(dag_id=dag.dag_id, expected_latest=due.isoformat(),
                       status=run.state if run else "missing", schedule=timetable.summary)
            if run:
                tasks = session.query(TaskInstance).filter_by(dag_id=dag.dag_id, run_id=run.run_id).all()
                row.update(run_id=run.run_id, interval_end=run.data_interval_end.isoformat(),
                           tasks=dict(Counter(task.state or "pending" for task in tasks)),
                           failed_tasks=[task.task_id for task in tasks if task.state in ("failed", "upstream_failed")])
            results.append(row)
        interrupted = session.query(DagRun).filter(
            DagRun.state.in_(["running", "queued"]), DagRun.start_date < begin).all()
        return dict(start=start, end=end, counts=dict(Counter(row["status"] for row in results)),
                    dags=sorted(results, key=lambda row: row["dag_id"]),
                    prior_active=[dict(dag_id=run.dag_id, run_id=run.run_id, state=run.state) for run in interrupted])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--ensure-dag", help="Explicitly create only this DAG's missing scheduled run")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / ".tmp" / "outage_20260908.json")
    args = parser.parse_args()
    if args.inside:
        print("OUTAGE_JSON=" + json.dumps(inspect_runs(args.start, args.end, args.ensure_dag), ensure_ascii=False))
        return 0
    from airflow_scheduler_watchdog import run_cmd
    command = ["docker", "exec", "airflow-airflow-scheduler-1", "python",
                      "/opt/airflow/scripts/airflow_outage_recovery.py", "--inside",
                      "--start", args.start, "--end", args.end]
    if args.ensure_dag:
        command.extend(["--ensure-dag", args.ensure_dag])
    result = run_cmd(command, timeout=90)
    if result.returncode:
        logger.error("Inventory failed: %s", result.stderr)
        return 1
    line = next((line for line in result.stdout.splitlines() if line.startswith("OUTAGE_JSON=")), None)
    if line is None:
        logger.error("No inventory returned")
        return 1
    payload = json.loads(line.partition("=")[2])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.warning("Recovery inventory: %s; output=%s", payload["counts"], args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
