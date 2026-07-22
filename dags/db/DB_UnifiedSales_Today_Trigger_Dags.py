"""Today POS 수집 이후 기존 DB_UnifiedSales DAG를 today_mode로 트리거한다."""

import logging
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import (
    DB_UNIFIED_SALES_TODAY_TRIGGER_1915_TIME,
    DB_UNIFIED_SALES_TODAY_TRIGGER_TIME,
)

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

SOURCE_TODAY_DAGS = [
    "DB_OKPOS_Sales_Today_Dags",
    "DB_EasyPOS_Sales_Today_Dags",
    "DB_UnionPOS_Receipt_Today_Dags",
    "DB_Posfeed_Sales_Today_Dags",
]


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


def trigger_unified_sales_today(**context) -> str:
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    conf = (getattr(context.get("dag_run"), "conf", None) or {})
    sale_date = conf.get("sale_date") or pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    skip_source_wait = str(conf.get("skip_source_wait", "")).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    if not skip_source_wait:
        _assert_source_runs_ready(context)

    run_clock = pendulum.now("Asia/Seoul").format("HHmm")
    parent_run_id = re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(context.get("run_id") or "scheduled"))
    run_id = f"today__{sale_date.replace('-', '')}__{run_clock}__{parent_run_id[:80]}"
    trigger_conf = {"sale_date": sale_date, "today_mode": True}

    try:
        trigger_dag(dag_id="DB_UnifiedSales", run_id=run_id, conf=trigger_conf)
    except DagRunAlreadyExists:
        logger.info("DB_UnifiedSales Today run already exists: %s", run_id)
        return f"이미 트리거됨: {run_id}"

    logger.info("DB_UnifiedSales Today 트리거 완료: %s | conf=%s", run_id, trigger_conf)
    return f"DB_UnifiedSales Today 트리거 완료: {run_id}"


def _source_slot_start_utc(context) -> pendulum.DateTime:
    """현재 trigger run의 원천 수집 슬롯 시작시각을 UTC로 반환한다."""
    dag_run = context.get("dag_run")
    ref = getattr(dag_run, "start_date", None) or pendulum.now("UTC")
    ref_kst = pendulum.instance(ref).in_timezone("Asia/Seoul")
    slot_start_kst = ref_kst.replace(minute=0, second=0, microsecond=0)
    return slot_start_kst.in_timezone("UTC")


def _assert_source_runs_ready(context) -> None:
    """현재 수집 슬롯 이후 끝난 Today 원천 DAG success를 요구한다."""
    min_end_date = _source_slot_start_utc(context)

    missing = []
    with create_session() as session:
        for source_dag_id in SOURCE_TODAY_DAGS:
            latest = (
                session.query(DagRun)
                .filter(DagRun.dag_id == source_dag_id)
                .filter(DagRun.state == State.SUCCESS)
                .filter(DagRun.end_date.isnot(None))
                .filter(DagRun.end_date >= min_end_date)
                .order_by(DagRun.end_date.desc())
                .first()
            )
            if latest is None:
                missing.append(source_dag_id)
            else:
                logger.info(
                    "Today source ready: %s | run_id=%s | end_date=%s | slot_start=%s",
                    source_dag_id,
                    latest.run_id,
                    latest.end_date,
                    min_end_date,
                )

    if missing:
        raise AirflowException(
            "Today 원천 수집 DAG 완료 대기 중: "
            + ", ".join(missing)
            + f" | 기준시각={min_end_date}"
        )


default_args = {
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_SALES_TODAY_TRIGGER_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unified_sales", "today", "trigger"],
) as dag:
    t1 = PythonOperator(
        task_id="trigger_unified_sales_today",
        python_callable=trigger_unified_sales_today,
    )


with DAG(
    dag_id=f"{dag_id}_1915",
    schedule=DB_UNIFIED_SALES_TODAY_TRIGGER_1915_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unified_sales", "today", "trigger"],
) as dag_1915:
    t1_1915 = PythonOperator(
        task_id="trigger_unified_sales_today",
        python_callable=trigger_unified_sales_today,
    )
