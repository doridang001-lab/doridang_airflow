"""
unified_review 데이터마트 생성 DAG.

처리 흐름:
1. conf 해석 → mode 결정 (XCom push)
2. toorder_voc_*.parquet 집계 → unified_review.parquet 저장

conf 파라미터:
- {"backfill": true}           → ALL 모드 (전체 재집계)
- {"start_date": "YYYY-MM-DD",
   "end_date": "YYYY-MM-DD"}   → 범위 백필 모드
- {"sale_date": "YYYY-MM-DD"}  → 정정 모드 (해당 날짜 1일치)
- conf 없음                    → Lookback 30일 모드 (기본)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.schedule import DB_UNIFIED_REVIEW_TIME
from modules.transform.pipelines.db.DB_UnifiedReview import (
    run_review as pipeline_run_review,
    backfill_review as pipeline_backfill_review,
    build_daily_review_count as pipeline_count_reviews,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

_ALERT_EMAILS = [MAIL_CMJ_PM]

LOOKBACK_DAYS = 30


def _latest_toorder_review_execution_date(dt, **context):
    """Sales_ToOrder_Review_Dags.t4_validate의 최신 성공 execution_date를 최신순으로 조회한다."""
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI

    session = settings.Session()
    try:
        row = (
            session.query(DagRun.execution_date)
            .join(
                TI,
                (TI.dag_id == DagRun.dag_id) & (TI.run_id == DagRun.run_id),
            )
            .filter(
                TI.dag_id == "Sales_ToOrder_Review_Dags",
                TI.task_id == "t4_validate",
                TI.state == "success",
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        if not row:
            logger.info("[sensor] Sales_ToOrder_Review_Dags.t4_validate 성공 이력 없음. fallback=%s", dt)
            return dt

        latest_execution_date = row[0]
        # DB 실행 기준으로 너무 오래된 완료 건은 무시한다.
        now = pendulum.now("Asia/Seoul")
        if latest_execution_date >= dt - timedelta(hours=12) and latest_execution_date <= now:
            logger.info(
                "[sensor] Sales_ToOrder_Review_Dags.t4_validate 최신 성공 시각: %s",
                latest_execution_date,
            )
            return latest_execution_date

        logger.info(
            "[sensor] 최신 성공(%s)이 현재 기준(%s)에서 너무 과거/미래함. fallback=%s",
            latest_execution_date,
            dt,
            dt,
        )
    except Exception as exc:
        logger.warning("[sensor] Sales_ToOrder_Review_Dags.t4_validate 조회 실패: %s", exc)
    finally:
        session.close()

    return dt


def _resolve_sale_date(conf: dict) -> str | None:
    for key in ("sale_date", "target_date", "date"):
        value = conf.get(key)
        if value:
            return str(value).strip()
    return None


def _resolve_date_range(conf: dict) -> tuple[str | None, str | None]:
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")
    if start_date or end_date:
        return (
            str(start_date).strip() if start_date else None,
            str(end_date).strip() if end_date else None,
        )
    return None, None


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
    start_date, end_date = _resolve_date_range(conf)
    if conf.get("backfill"):
        mode = "all"
    elif start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("start_date + end_date 모두 필요합니다.")
        mode = "range"
    elif sale_date:
        mode = "date"
    else:
        mode = "lookback"

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="mode", value=mode)
        ti.xcom_push(key="sale_date", value=sale_date)
        ti.xcom_push(key="start_date", value=start_date)
        ti.xcom_push(key="end_date", value=end_date)

    logger.info("모드 결정: %s", mode)
    return f"모드: {mode}"


def build_review_mart(**context) -> str:
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    override_sale_date = _resolve_sale_date(conf)
    override_start_date, override_end_date = _resolve_date_range(conf)

    if conf.get("backfill"):
        return pipeline_backfill_review()

    mode      = context["ti"].xcom_pull(task_ids="resolve_mode", key="mode") or "lookback"
    sale_date = override_sale_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="sale_date")
    start_date = override_start_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="start_date")
    end_date = override_end_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="end_date")

    return pipeline_run_review(
        mode=mode,
        days=LOOKBACK_DAYS,
        target_date=sale_date,
        start_date=start_date,
        end_date=end_date,
    )


def count_daily_reviews(**context) -> str:
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    override_sale_date = _resolve_sale_date(conf)
    override_start_date, override_end_date = _resolve_date_range(conf)

    if conf.get("backfill"):
        mode, sale_date, start_date, end_date = "all", None, None, None
    else:
        mode      = context["ti"].xcom_pull(task_ids="resolve_mode", key="mode") or "lookback"
        sale_date = override_sale_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="sale_date")
        start_date = override_start_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="start_date")
        end_date = override_end_date or context["ti"].xcom_pull(task_ids="resolve_mode", key="end_date")

    return pipeline_count_reviews(
        mode=mode,
        days=LOOKBACK_DAYS,
        target_date=sale_date,
        start_date=start_date,
        end_date=end_date,
    )


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
        external_task_id="t4_validate",
        execution_date_fn=_latest_toorder_review_execution_date,
        allowed_states=["success"],
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
