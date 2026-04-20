"""
UnionPOS 영수증 원본 자동 수집 DAG

처리 흐름:
1. 실행 날짜 범위 결정 (conf 또는 yesterday)
2. 로그인 → 영수증 목록 + 상세품목 수집 → 파티션 CSV 저장
3. log.parquet 실행 이력 기록

저장 경로: RAW_OKPOS_SALES / brand=도리당 / store={매장} / ym={} /
  - unionpos_receipt_list.csv  : 영수증 헤더
  - unionpos_receipt_items.csv : 영수증 상세품목

스케줄: 매일 09:55 (DB_UNIONPOS_RECEIPT_TIME)
인증: UnionPOS ID/PW (파이프라인 상수)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_UNIONPOS_RECEIPT_TIME
from modules.transform.pipelines.db.DB_UnionPOS_Receipt import (
    resolve_sale_dates,
    collect_and_save,
    write_log,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# None    → 어제 1일만 수집 (기본 스케줄 동작)
# 기간 지정 → ("2026-04-01", "2026-04-19") 형식으로 입력하면 1일씩 순차 수집
MANUAL_DATE_RANGE: tuple | None  = ("2026-01-01", "2026-02-28")

_ALERT_EMAILS = ["a17019@kakao.com"]


def _on_failure_callback(context):
    from modules.transform.utility.mailer import send_email, text_to_html

    ti = context.get("task_instance")
    subject = f"[Airflow 실패] {ti.dag_id} / {ti.task_id}"
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {ti.execution_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
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
    schedule=DB_UNIONPOS_RECEIPT_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unionpos", "receipt", "playwright"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_sale_dates,
        op_kwargs={"manual_date_range": MANUAL_DATE_RANGE},
    )

    t2 = PythonOperator(
        task_id="collect_and_save",
        python_callable=collect_and_save,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t3 = PythonOperator(
        task_id="write_log",
        python_callable=write_log,
    )

    t1 >> t2 >> t3
