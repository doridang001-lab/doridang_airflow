"""
unified_review 데이터마트 생성 DAG.

처리 흐름:
1. conf 해석 → mode 결정 (XCom push)
2. toorder_voc_*.parquet 집계 → unified_review.parquet 저장

conf 파라미터:
- {"backfill": true}           → ALL 모드 (전체 재집계)
- {"sale_date": "YYYY-MM-DD"}  → 정정 모드 (해당 날짜 1일치)
- conf 없음                    → Lookback 30일 모드 (기본)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIFIED_REVIEW_TIME
from modules.transform.pipelines.db.DB_UnifiedReview import (
    run_review as pipeline_run_review,
    backfill_review as pipeline_backfill_review,
    build_daily_review_count as pipeline_count_reviews,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

_ALERT_EMAILS = ["a17019@kakao.com"]

LOOKBACK_DAYS = 30


def _resolve_sale_date(conf: dict) -> str | None:
    for key in ("sale_date", "target_date", "date"):
        value = conf.get(key)
        if value:
            return str(value).strip()
    return None


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
    except Exception as e:
        logger.error("알림 발송 실패: %s", e)


def _on_failure_callback(context):
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {ti.log_url}"
    )
    _send_alert(subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}", body=body)
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def resolve_mode(**context) -> str:
    """conf 해석 → mode XCom push."""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    sale_date = _resolve_sale_date(conf)
    if conf.get("backfill"):
        mode = "all"
    elif sale_date:
        mode = "date"
    else:
        mode = "lookback"

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="mode", value=mode)
        ti.xcom_push(key="sale_date", value=sale_date)

    logger.info("모드 결정: %s", mode)
    return f"모드: {mode}"


def build_review_mart(**context) -> str:
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    override_sale_date = _resolve_sale_date(conf)

    if conf.get("backfill"):
        return pipeline_backfill_review()

    mode      = context["ti"].xcom_pull(task_ids="resolve_mode", key="mode") or "lookback"
    sale_date = override_sale_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="sale_date")

    return pipeline_run_review(mode=mode, days=LOOKBACK_DAYS, target_date=sale_date)


def count_daily_reviews(**context) -> str:
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    override_sale_date = _resolve_sale_date(conf)

    if conf.get("backfill"):
        mode, sale_date = "all", None
    else:
        mode      = context["ti"].xcom_pull(task_ids="resolve_mode", key="mode") or "lookback"
        sale_date = override_sale_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="sale_date")

    return pipeline_count_reviews(mode=mode, days=LOOKBACK_DAYS, target_date=sale_date)


with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_REVIEW_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "review", "toorder", "unified_review"],
) as dag:
    wait_for_toorder_review = ExternalTaskSensor(
        task_id="wait_for_toorder_review",
        external_dag_id="Sales_ToOrder_Review_Dags",
        external_task_id=None,
        allowed_states=["success"],
        execution_delta=timedelta(0),
        timeout=60 * 60 * 6,
        poke_interval=60,
        mode="reschedule",
    )

    t1 = PythonOperator(
        task_id="resolve_mode",
        python_callable=resolve_mode,
    )

    t2 = PythonOperator(
        task_id="build_review_mart",
        python_callable=build_review_mart,
    )

    t3 = PythonOperator(
        task_id="count_daily_reviews",
        python_callable=count_daily_reviews,
    )

    wait_for_toorder_review >> t1 >> t2 >> t3
