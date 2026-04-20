"""
OKPOS 매출 원본파일 자동 수집 DAG

처리 흐름:
1. 실행 날짜 범위 결정 (conf 또는 yesterday)
2. today 페이지 매장별 엑셀 다운로드 (당일매출상세내역, 4개 매장)
3. receipt/details 페이지 매장별 엑셀 다운로드 (영수증별매출상세현황, 4개 매장)
4. 합계 제거 + 매장명 추가 → brand=도리당/store={매장}/ym={}/{page_type}.csv 저장
5. log.parquet 실행 이력 기록

스케줄: 매일 09:30 (DB_OKPOS_SALES_TIME)
인증: OKPOS ID/PW (파이프라인 상수)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_OKPOS_SALES_TIME
from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    resolve_sale_dates,
    download_today_stores,
    download_receipt_stores,
    save_to_raw,
    check_and_fill_missing_today,
    write_log,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# ── 수집 날짜 범위 설정 ──────────────────────────────────────────────────────
# None    → 어제 1일만 수집 (기본 스케줄 동작)
# 기간 지정 → ("2026-03-01", "2026-03-02") 형식으로 입력하면 1일씩 순차 수집
# 예) MANUAL_DATE_RANGE = ("2026-03-01", "2026-03-31")  # 3월 전체
# MANUAL_DATE_RANGE: tuple | None = None
MANUAL_DATE_RANGE = ("2026-01-01", "2026-03-31")  # 3월 전체
_ALERT_EMAILS = ["a17019@kakao.com"]


def _on_failure_callback(context):
    """Task 실패 시 이메일 알림 발송"""
    from modules.transform.utility.mailer import send_email, text_to_html

    ti = context.get("task_instance")
    dag_id_ = ti.dag_id
    task_id = ti.task_id
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
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
    schedule=DB_OKPOS_SALES_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "excel", "selenium"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_sale_dates,
        op_kwargs={"manual_date_range": MANUAL_DATE_RANGE},
    )

    # Selenium 태스크: Python 레벨(3회) + Airflow 레벨(2회, 3분) 2계층 재시도
    t2 = PythonOperator(
        task_id="download_today",
        python_callable=download_today_stores,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t3 = PythonOperator(
        task_id="download_receipt",
        python_callable=download_receipt_stores,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t4 = PythonOperator(
        task_id="save_to_raw",
        python_callable=save_to_raw,
    )

    t5 = PythonOperator(
        task_id="check_and_fill_missing_today",
        python_callable=check_and_fill_missing_today,
        retries=1,
        retry_delay=timedelta(minutes=3),
    )

    t6 = PythonOperator(
        task_id="write_log",
        python_callable=write_log,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
