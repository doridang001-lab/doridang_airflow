"""
이지포스(EasyPOS) 매출 영수증 수집 DAG

처리 흐름:
1. 실행 날짜 결정 (conf['date_from'/'date_to'] 또는 conf['sale_date'] 또는 어제)
2. Playwright로 EasyPOS 당일매출내역 → 영수증별 상품내역 수집
3. OneDrive easypos_sales_raw/ym={YYYY-MM}/receipts.csv 적재

날짜 범위 설정:
- DATE_FROM / DATE_TO 상수(코드)로 기본값 설정 가능 (None이면 어제 1일)
- 런타임 conf {"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"} 로 override
- conf {"sale_date": "YYYY-MM-DD"} (단일 날짜) 도 호환

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
    verify_missing,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# 날짜 범위 설정 (None이면 어제만, 설정하면 해당 기간 수집)
# 런타임 conf(date_from/date_to)가 있으면 conf 우선
DATE_FROM = None   # 예: "2026-04-01"
DATE_TO   = None   # 예: "2026-04-03"

_ALERT_EMAILS = ["a17019@kakao.com"]


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info(f"알림 발송 완료: {_ALERT_EMAILS}")
    except Exception as e:
        logger.error(f"알림 발송 실패: {e}")


def _on_failure_callback(context):
    """Task 최종 실패 시 이메일 알림"""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")

    _send_alert(
        subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"실행일시: {execution_date}\n"
            f"에러: {exception}\n"
            f"로그: {ti.log_url}"
        ),
    )


def _on_retry_callback(context):
    """Task 재시도 시 이메일 알림"""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")
    retry_number   = ti.try_number - 1

    _send_alert(
        subject=f"[Airflow 재시도 {retry_number}회] {ti.dag_id} / {ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"재시도: {retry_number}회차\n"
            f"실행일시: {execution_date}\n"
            f"에러: {exception}\n"
            f"로그: {ti.log_url}"
        ),
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback": _on_retry_callback,
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
        op_kwargs={"dag_date_from": DATE_FROM, "dag_date_to": DATE_TO},
    )

    t2 = PythonOperator(
        task_id="collect_receipts",
        python_callable=collect_receipts,
    )

    t3 = PythonOperator(
        task_id="save_to_raw",
        python_callable=save_to_raw,
    )

    t4 = PythonOperator(
        task_id="verify_missing",
        python_callable=verify_missing,
    )

    t1 >> t2 >> t3 >> t4
