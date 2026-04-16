"""
이지포스(EasyPOS) 매출 영수증 수집 DAG

처리 흐름:
1. 실행 날짜 결정 (conf['sale_date'] 또는 어제)
2. Selenium으로 EasyPOS 당일매출내역 → 영수증별 상품내역 수집
3. OneDrive easypos_sales_raw/ym={YYYY-MM}/receipts.csv 적재

스케줄: 매일 09:50 (DB_EASYPOS_SALES_TIME)
수집 경로: ANALYTICS_DB/easypos_sales_raw/ym=YYYY-MM/receipts.csv
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_EASYPOS_SALES_TIME
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (
    resolve_sale_dates,
    collect_receipts,
    save_to_raw,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

_ALERT_EMAILS = ["a17019@kakao.com"]


def _on_failure_callback(context):
    """Task 실패 시 이메일 알림"""
    from modules.transform.utility.mailer import send_email, text_to_html

    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")

    subject = f"[Airflow 실패] {ti.dag_id} / {ti.task_id}"
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {ti.log_url}"
    )
    try:
        send_email(
            subject=subject,
            html_content=text_to_html(body),
            to_emails=_ALERT_EMAILS,
        )
        logger.info(f"실패 알림 발송 완료: {_ALERT_EMAILS}")
    except Exception as e:
        logger.error(f"실패 알림 발송 실패: {e}")


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
    schedule=DB_EASYPOS_SALES_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "easypos", "selenium", "receipts"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_sale_dates,
    )

    t2 = PythonOperator(
        task_id="collect_receipts",
        python_callable=collect_receipts,
    )

    t3 = PythonOperator(
        task_id="save_to_raw",
        python_callable=save_to_raw,
    )

    t1 >> t2 >> t3
