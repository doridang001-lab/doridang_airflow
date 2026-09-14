"""UnionPOS 당일 영수증 수집 DAG."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_UnionPOS_Receipt import collect_and_save, write_log
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIONPOS_RECEIPT_TODAY_TIME

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
    return f"Today UnionPOS 수집 날짜: {sale_date}"


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
    schedule=DB_UNIONPOS_RECEIPT_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unionpos", "today", "playwright"],
) as dag:
    t1 = PythonOperator(task_id="resolve_today", python_callable=resolve_today)
    t2 = PythonOperator(
        task_id="collect_and_save",
        python_callable=collect_and_save,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    t3 = PythonOperator(task_id="write_log", python_callable=write_log)

    t1 >> t2 >> t3
