"""수동 날짜 범위 기반 수집 복구 DAG."""

import logging
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.utility.date_collection_recovery import (
    ACTIVE_RECOVERY_STATES,
    build_recovery_plan,
    format_plan_summary,
    parse_target_dag_ids,
    parse_target_groups,
    resolve_schedule_date_range,
    truthy,
)
from modules.transform.utility.notifier import on_failure_callback, send_telegram

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem


def _conf(context: dict[str, Any]) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


def _existing_active_recovery_run(session, item) -> str | None:
    row = (
        session.query(DagRun)
        .filter(DagRun.dag_id == item.target.dag_id)
        .filter(DagRun.run_id.like(f"{item.duplicate_prefix}%"))
        .filter(DagRun.state.in_(ACTIVE_RECOVERY_STATES))
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    if row is None:
        return None
    return f"{row.run_id}:{row.state}"


def recover_date_collections(**context) -> str:
    conf = _conf(context)
    execute = truthy(conf.get("execute"))
    force = truthy(conf.get("force"))
    schedule_date_from, schedule_date_to = resolve_schedule_date_range(conf)
    target_dag_ids = parse_target_dag_ids(conf.get("target_dag_ids"))
    target_groups = parse_target_groups(conf.get("target_groups"))
    parent_run_id = str(context.get("run_id") or "manual")

    plan = build_recovery_plan(
        schedule_date_from=schedule_date_from,
        schedule_date_to=schedule_date_to,
        parent_dag_id=dag_id,
        parent_run_id=parent_run_id,
        target_dag_ids=target_dag_ids,
        target_groups=target_groups,
    )

    triggered: list[str] = []
    skipped: list[str] = []
    executable_items = []

    with create_session() as session:
        for item in plan:
            existing = None if force else _existing_active_recovery_run(session, item)
            if existing:
                skipped.append(f"{item.target.dag_id} {item.sale_date_from}~{item.sale_date_to}: 기존 복구 run 존재({existing})")
                continue
            executable_items.append(item)

    if execute:
        for item in executable_items:
            try:
                trigger_dag(
                    dag_id=item.target.dag_id,
                    run_id=item.run_id,
                    conf=item.conf,
                )
            except DagRunAlreadyExists:
                logger.info("%s 복구 DagRun 이미 존재: %s", item.target.dag_id, item.run_id)
                skipped.append(f"{item.target.dag_id}: run_id 이미 존재({item.run_id})")
                continue
            triggered.append(f"{item.target.dag_id}:{item.run_id}")
            logger.warning("날짜 수집 복구 트리거: %s %s", item.target.dag_id, item.run_id)

    summary = format_plan_summary(executable_items, execute=execute, skipped=skipped)
    result = (
        f"{summary}\n"
        f"schedule_date={schedule_date_from}~{schedule_date_to}\n"
        f"target_groups={target_groups or ['source']}\n"
        f"planned={len(plan)} executable={len(executable_items)} "
        f"triggered={len(triggered)} skipped={len(skipped)} force={force}"
    )

    if execute and triggered:
        send_telegram("[Airflow 날짜 수집 복구 실행]\n" + result)
    else:
        logger.info(result)
    return result


default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    description="Airflow 장애로 지나간 날짜 기반 수집 DAG를 수동 복구 트리거",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "recovery", "manual", "collection"],
) as dag:
    recover = PythonOperator(
        task_id="recover_date_collections",
        python_callable=recover_date_collections,
        execution_timeout=pendulum.duration(minutes=10),
    )
