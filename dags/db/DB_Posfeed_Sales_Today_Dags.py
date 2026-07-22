"""Posfeed 당일 주문 수집 DAG."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Posfeed_Sales import collect_posfeed_today, partition_to_onedrive
from modules.transform.pipelines.db.DB_Posfeed_Sales_Detail import (
    extract_today_order_codes,
    scrape_today_details,
)
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_POSFEED_SALES_TODAY_TIME

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
_ALERT_EMAILS = [MAIL_CMJ_PM]


def _on_failure_callback(context):
    from modules.transform.utility.mailer import send_email, text_to_html

    ti = context.get("task_instance")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {ti.execution_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    try:
        send_email(
            subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}",
            html_content=text_to_html(body),
            to_emails=_ALERT_EMAILS,
        )
    except Exception as e:
        logger.error("실패 알림 발송 실패: %s", e)
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


def resolve_today(**context) -> str:
    conf = (getattr(context.get("dag_run"), "conf", None) or {})
    sale_date = conf.get("sale_date") or pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    context["ti"].xcom_push(key="sale_date", value=sale_date)
    context["ti"].xcom_push(key="sale_dates", value=[sale_date])
    return f"Today Posfeed 수집 날짜: {sale_date}"


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_POSFEED_SALES_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "posfeed", "today", "selenium"],
) as dag:
    t1 = PythonOperator(task_id="resolve_today", python_callable=resolve_today)
    t2 = PythonOperator(
        task_id="move_to_storage",
        python_callable=collect_posfeed_today,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    t3 = PythonOperator(task_id="partition_to_onedrive", python_callable=partition_to_onedrive)
    t4 = PythonOperator(task_id="extract_order_codes", python_callable=extract_today_order_codes)
    t5 = PythonOperator(
        task_id="scrape_order_details",
        python_callable=scrape_today_details,
        pool="selenium_pool",
        execution_timeout=timedelta(hours=2),
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
    )

    t1 >> t2 >> t3 >> t4 >> t5
