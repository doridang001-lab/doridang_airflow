"""
OKPOS 매출 원본파일 2차 보정 DAG

처리 흐름:
1. 1차 OKPOS 수집 DAG가 아직 실행 중이면 대기
2. 실행 날짜 범위 결정
3. 이미 수집된 데이터는 스킵하고 누락분만 다운로드
4. daily 0원/order-daily 불일치 보정
5. log.parquet 실행 이력 기록

수동 실행 전용 (DB_OKPOS_SALES_RECHECK_TIME)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG, settings
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_OKPOS_SALES_RECHECK_TIME
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    check_and_fill_missing_today,
    ingest_manual_daily_xlsx,
    plan_okpos_download_batches,
    prune_preopen_data,
    purge_okpos_daily,
    report_missing_daily,
    download_okpos_batch,
    merge_okpos_download_batches,
    resolve_sale_dates,
    save_to_raw,
    write_log,
)


def _reload_and_call(fn_name: str, **context):
    """Long-running worker에서 코드 변경이 retry에 반영되도록 모듈을 reload 후 실행."""
    import importlib

    from modules.transform.pipelines.db import DB_OKPOS_Sales as mod

    importlib.reload(mod)
    fn = getattr(mod, fn_name)
    return fn(**context)


logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

MANUAL_DATE_RANGE = None
LOOKBACK_DAYS: int | None = 14
PRIMARY_DAG_ID = "DB_OKPOS_Sales_Dags"
_ALERT_EMAILS = [MAIL_CMJ_PM]


def _on_failure_callback(context):
    """Task 실패 시 이메일/텔레그램 알림 발송."""
    from modules.transform.utility.mailer import send_email, text_to_html

    ti = context.get("task_instance")
    dag_id_ = ti.dag_id
    task_id = ti.task_id
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "예외 없음")
    log_url = ti.log_url

    subject = f"[Airflow 실패] {dag_id_} / {task_id}"
    body = (
        f"DAG: {dag_id_}\n"
        f"Task: {task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {log_url}"
    )
    try:
        send_email(
            subject=subject,
            html_content=text_to_html(body),
            to_emails=_ALERT_EMAILS,
        )
        logger.info("실패 알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("실패 알림 발송 실패: %s", e)
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


def wait_for_primary_okpos_idle(**context) -> bool:
    """1차 OKPOS DAG가 아직 실행 중이면 2차 보정 시작을 늦춘다."""
    cutoff = pendulum.now("UTC") - timedelta(hours=4)
    session = settings.Session()
    try:
        active = (
            session.query(DagRun)
            .filter(DagRun.dag_id == PRIMARY_DAG_ID)
            .filter(DagRun.start_date >= cutoff)
            .filter(DagRun.state.in_(("queued", "running")))
            .order_by(DagRun.start_date.desc())
            .first()
        )
        if active is None:
            logger.info("1차 OKPOS DAG active run 없음 - recheck 진행")
            return True
        logger.info("1차 OKPOS DAG 대기 중: run_id=%s state=%s", active.run_id, active.state)
        return False
    finally:
        session.close()


with DAG(
    dag_id=dag_id,
    schedule=DB_OKPOS_SALES_RECHECK_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "excel", "selenium", "recheck"],
) as dag:

    t0 = PythonSensor(
        task_id="wait_for_primary_okpos_idle",
        python_callable=wait_for_primary_okpos_idle,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 2,
    )

    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_sale_dates,
        op_kwargs={"manual_date_range": MANUAL_DATE_RANGE, "lookback_days": LOOKBACK_DAYS},
    )

    t1b = PythonOperator(
        task_id="report_missing_daily",
        python_callable=report_missing_daily,
    )

    t1c = PythonOperator(
        task_id="prune_preopen_data",
        python_callable=prune_preopen_data,
    )

    t1d = PythonOperator(
        task_id="purge_okpos_daily",
        python_callable=purge_okpos_daily,
    )

    t1e = PythonOperator(
        task_id="repair_okpos_order_duplicates",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "repair_okpos_order_duplicates"},
        retries=0,
    )

    t2_plan = PythonOperator(
        task_id="plan_download_today",
        python_callable=plan_okpos_download_batches,
        op_kwargs={"page_type": "today"},
    )

    t2_batch = PythonOperator.partial(
        task_id="download_today_batch",
        python_callable=download_okpos_batch,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    ).expand(op_kwargs=t2_plan.output)

    t2 = PythonOperator(
        task_id="download_today",
        python_callable=merge_okpos_download_batches,
        op_kwargs={"page_type": "today", "batch_task_id": "download_today_batch"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    t3_plan = PythonOperator(
        task_id="plan_download_receipt",
        python_callable=plan_okpos_download_batches,
        op_kwargs={"page_type": "receipt"},
    )

    t3_batch = PythonOperator.partial(
        task_id="download_receipt_batch",
        python_callable=download_okpos_batch,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    ).expand(op_kwargs=t3_plan.output)

    t3 = PythonOperator(
        task_id="download_receipt",
        python_callable=merge_okpos_download_batches,
        op_kwargs={"page_type": "receipt", "batch_task_id": "download_receipt_batch"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    t3b_plan = PythonOperator(
        task_id="plan_download_daily",
        python_callable=plan_okpos_download_batches,
        op_kwargs={"page_type": "daily"},
    )

    t3b_batch = PythonOperator.partial(
        task_id="download_daily_batch",
        python_callable=download_okpos_batch,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    ).expand(op_kwargs=t3b_plan.output)

    t3b = PythonOperator(
        task_id="download_daily",
        python_callable=merge_okpos_download_batches,
        op_kwargs={"page_type": "daily", "batch_task_id": "download_daily_batch"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    t4 = PythonOperator(
        task_id="save_to_raw",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "save_to_raw"},
    )

    t4b = PythonOperator(
        task_id="ingest_manual_daily_xlsx",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "ingest_manual_daily_xlsx"},
        retries=0,
    )

    t5 = PythonOperator(
        task_id="check_and_fill_missing_today",
        python_callable=check_and_fill_missing_today,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t5b = PythonOperator(
        task_id="reconcile_zero_daily_against_sales_detail",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "reconcile_zero_daily_against_sales_detail"},
        retries=0,
    )

    t6c = PythonOperator(
        task_id="reconcile_against_daily_summary",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "reconcile_against_daily_summary"},
        retries=0,
    )

    t6a = PythonOperator(
        task_id="reconcile_today_vs_receipt",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "reconcile_today_vs_receipt"},
        retries=0,
        retry_delay=timedelta(minutes=3),
    )

    t6b = PythonOperator(
        task_id="validate_today_vs_receipt",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "validate_today_vs_receipt"},
    )

    t6 = PythonOperator(
        task_id="write_log",
        python_callable=write_log,
    )

    t0 >> t1 >> t1b >> t1c >> t1d >> t1e >> t2_plan >> t2_batch >> t2
    t2 >> t3_plan >> t3_batch >> t3
    t3 >> t3b_plan >> t3b_batch >> t3b
    t3b >> t4 >> t4b >> t5 >> t5b >> t6c >> t6a >> t6b >> t6
