"""OKPOS 당일 매출 수집 DAG."""

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    download_okpos_batch,
    merge_okpos_download_batches,
    plan_okpos_download_batches,
    save_to_raw,
)
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_OKPOS_SALES_TODAY_TIME

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
    # Today DAG의 sale_date는 요청 힌트다.
    # 실제 저장 sale_date는 OKPOS 화면 조회일자 입력값을 읽어 DB_OKPOS_Sales에서 확정한다.
    sale_date = conf.get("sale_date") or pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    context["ti"].xcom_push(key="sale_date", value=sale_date)
    context["ti"].xcom_push(key="sale_dates", value=[sale_date])
    return f"Today OKPOS 수집 날짜: {sale_date}"


def _force_today_conf(context) -> None:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if isinstance(conf, dict):
        conf["force_redownload"] = True
        conf["replace_by_date"] = True


def _with_okpos_today_force(fn, **context) -> str:
    old_today = os.environ.get("OKPOS_FORCE_REDOWNLOAD_TODAY")
    old_receipt = os.environ.get("OKPOS_FORCE_REDOWNLOAD_RECEIPT")
    old_replace = os.environ.get("OKPOS_REPLACE_BY_DATE")
    old_default_date = os.environ.get("OKPOS_USE_DEFAULT_QUERY_DATE")
    try:
        os.environ["OKPOS_FORCE_REDOWNLOAD_TODAY"] = "1"
        os.environ["OKPOS_FORCE_REDOWNLOAD_RECEIPT"] = "1"
        os.environ["OKPOS_REPLACE_BY_DATE"] = "1"
        os.environ["OKPOS_USE_DEFAULT_QUERY_DATE"] = "1"
        _force_today_conf(context)
        return fn(**context)
    finally:
        for key, old_value in {
            "OKPOS_FORCE_REDOWNLOAD_TODAY": old_today,
            "OKPOS_FORCE_REDOWNLOAD_RECEIPT": old_receipt,
            "OKPOS_REPLACE_BY_DATE": old_replace,
            "OKPOS_USE_DEFAULT_QUERY_DATE": old_default_date,
        }.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def save_to_raw_replace(**context) -> str:
    return _with_okpos_today_force(save_to_raw, **context)


def plan_download_today_force(page_type: str, **context) -> list[dict]:
    return _with_okpos_today_force(lambda **ctx: plan_okpos_download_batches(page_type, **ctx), **context)


def download_okpos_batch_force(page_type: str, batch_id: int, pending_keys: list[dict], **context) -> dict:
    return _with_okpos_today_force(
        lambda **ctx: download_okpos_batch(page_type, batch_id, pending_keys, **ctx),
        **context,
    )


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
    schedule=DB_OKPOS_SALES_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "today", "selenium"],
) as dag:
    t1 = PythonOperator(task_id="resolve_today", python_callable=resolve_today)
    t2_plan = PythonOperator(
        task_id="plan_download_today",
        python_callable=plan_download_today_force,
        op_kwargs={"page_type": "today"},
    )
    t2_batch = PythonOperator.partial(
        task_id="download_today_batch",
        python_callable=download_okpos_batch_force,
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
        python_callable=plan_download_today_force,
        op_kwargs={"page_type": "receipt"},
    )
    t3_batch = PythonOperator.partial(
        task_id="download_receipt_batch",
        python_callable=download_okpos_batch_force,
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
    t4 = PythonOperator(task_id="save_to_raw", python_callable=save_to_raw_replace)

    t1 >> t2_plan >> t2_batch >> t2
    t2 >> t3_plan >> t3_batch >> t3 >> t4
