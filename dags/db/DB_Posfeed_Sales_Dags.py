"""
Posfeed 주문 데이터 자동 다운로드 DAG

📋 처리 흐름:
1. Selenium으로 Posfeed 관리자 사이트 로그인
2. 주문 목록 검색 후 엑셀 다운로드
3. 다운로드 파일을 LOCAL_DB/posfeed/ 로 이동

🔐 인증 정보: 환경변수 POSFEED_USERNAME / POSFEED_PASSWORD
📅 스케줄: 매주 월요일 08:00 (DB_POSFEED_SALES_TIME)
"""

import os
import logging
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from modules.transform.utility.schedule import DB_POSFEED_SALES_TIME
from modules.transform.pipelines.db.DB_Posfeed_Sales import (
    download_posfeed_excel,
    move_to_storage,
    partition_to_onedrive,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

_ALERT_EMAILS = ["a17019@kakao.com"]


def _on_failure_callback(context):
    """Task 실패 시 이메일 알림 발송"""
    from modules.transform.utility.mailer import send_email, text_to_html

    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
    log_url = ti.log_url

    subject = f"[Airflow 실패] {dag_id} / {task_id}"
    body = (
        f"DAG: {dag_id}\n"
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
        logger.info(f"실패 알림 발송 완료: {_ALERT_EMAILS}")
    except Exception as e:
        logger.error(f"실패 알림 발송 실패: {e}")


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': _on_failure_callback,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_POSFEED_SALES_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['db', 'posfeed', 'excel', 'selenium'],
) as dag:

    # ============================================================
    # 1️⃣ Selenium 로그인 + 엑셀 다운로드
    # ============================================================
    download_task = PythonOperator(
        task_id='download_excel',
        python_callable=download_posfeed_excel,
    )

    # ============================================================
    # 2️⃣ 다운로드 파일 → LOCAL_DB/posfeed/ 이동
    # ============================================================
    move_task = PythonOperator(
        task_id='move_to_storage',
        python_callable=move_to_storage,
    )

    # ============================================================
    # 3️⃣ 브랜드/지점/월 파티션으로 OneDrive CSV 저장
    # ============================================================
    onedrive_task = PythonOperator(
        task_id='partition_to_onedrive',
        python_callable=partition_to_onedrive,
    )

    # ============================================================
    # Task 의존성
    # ============================================================
    download_task >> move_task >> onedrive_task
