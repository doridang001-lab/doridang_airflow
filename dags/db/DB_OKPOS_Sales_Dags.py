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
    prune_preopen_data,
    purge_okpos_daily,
    download_today_stores,
    download_receipt_stores,
    download_daily_stores,
    report_missing_daily,
    ingest_manual_daily_xlsx,
    save_to_raw,
    check_and_fill_missing_today,
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

# ── 수집 날짜 범위 설정 ──────────────────────────────────────────────────────
# None    → 어제 1일만 수집 (기본 스케줄 동작)
# 기간 지정 → ("2026-03-01", "2026-03-02") 형식으로 입력하면 1일씩 순차 수집
# 예) MANUAL_DATE_RANGE = ("2026-03-01", "2026-03-31")  # 3월 전체
# Backfill 팁:
# - 누락 체크(로컬): `python -X utf8 scripts/okpos_sales_missing_report.py 2023-12-05 2026-05-05 --brand 도리당 --pages daily --emit_airflow_confs`
# - Airflow UI에서 DAG Run Conf로 월별/기간별 재수집:
#   {"sale_date_from":"2025-01-01","sale_date_to":"2025-01-31"}
#   (기존 파일이 있으면 자동으로 skip, 강제 재다운로드는 conf에 force_redownload / force_redownload_daily 등을 사용)
# MANUAL_DATE_RANGE: tuple | None = None
MANUAL_DATE_RANGE = None
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

    t3b = PythonOperator(
        task_id="download_daily",
        python_callable=download_daily_stores,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t4 = PythonOperator(
        task_id="save_to_raw",
        python_callable=save_to_raw,
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
        # today/receipt 불일치(한쪽만 저장/NO_DATA 마킹 등) 자동 보정용
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t6 = PythonOperator(
        task_id="write_log",
        python_callable=write_log,
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

    t6c = PythonOperator(
        task_id="reconcile_against_daily_summary",
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "reconcile_against_daily_summary"},
        retries=0,
    )

    t1 >> t1b >> t1c >> t1d >> t2 >> t3 >> t3b >> t4 >> t4b >> t5 >> t6c >> t6a >> t6b >> t6
