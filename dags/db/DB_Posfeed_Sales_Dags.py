"""
Posfeed 주문 수집 통합 DAG

처리 흐름:
1. [Orders] Selenium으로 Posfeed 관리자 사이트 로그인 → 엑셀 다운로드
2. [Orders] 다운로드 파일 → UTF-8 CSV 변환 후 DOWN_DIR 저장
3. [Orders] 브랜드/지점/월 파티션으로 OneDrive CSV 저장
4. [Orders] 누락 날짜 감지 및 재수집 (ALL_DONE)
5. [Detail] 수집된 주문 코드 추출
6. [Detail] 주문 상세 페이지 크롤링 → OneDrive 저장
7. [Detail] 미수집 코드 재수집 (ALL_DONE)

📅 스케줄: 매일 07:15 (DB_POSFEED_SALES_TIME)
"""

import logging
from datetime import timedelta
from pathlib import Path
from pickle import NONE

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_POSFEED_SALES_TIME
from modules.transform.pipelines.db.DB_Posfeed_Sales import (
    reset_posfeed_partitions,
    check_monthly_collection,
    download_posfeed_excel,
    move_to_storage,
    partition_to_onedrive,
    collect_missing_dates,
    ingest_manual_csvs,
)
from modules.transform.pipelines.db.DB_Posfeed_Sales_Detail import (
    extract_order_codes,
    check_undetailed_orders,
    scrape_order_details,
    scrape_missing_order_details,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# ============================================================
# 수집 모드 — 여기서만 수정
# None             : 최신 등록날짜 기준 자동 감지 (기본, 매일 스케줄)
# "backfill_missing"      : 전체 파티션 스캔 → 상세 누락 주문 전부 수집
# "2026-03-01"            : 특정 날짜 단건
# "2026-04-01~2026-04-22" : 날짜 범위
# ============================================================
                                                                                                    
# 1월부터 2월까지만이면:                                                                                                                                                                                                                           
# COLLECT_MODE = "2026-01-01~2026-02-28"                                                                                                                                                                                                                  
# 2월만이면:                                                                                                                                                                                                                                        
# COLLECT_MODE = "2026-03-01~2026-04-30"

COLLECT_MODE = None # None으로 두면 최신 등록날짜 기준으로 자동 감지 (매일 스케줄에 적합)

# ============================================================
# 옵션 — 여기서만 수정
# - True : OneDrive 파티션 기준 최근 30일 누락 날짜 자동 재수집
# - False: 누락 날짜 체크/재수집 끔 (30일치 3월부터 다운받는 현상 방지)
# ============================================================
ENABLE_MISSING_DATES_BACKFILL = False


def _is_date_mode(collect_mode: str) -> bool:
    mode = str(collect_mode or "").strip()
    return mode not in ("", "yesterday", "backfill_missing")


logger.info("DB_Posfeed_Sales_Dags 설정 | COLLECT_MODE=%s | ENABLE_MISSING_DATES_BACKFILL=%s", COLLECT_MODE, ENABLE_MISSING_DATES_BACKFILL)

_ALERT_EMAILS = ["a17019@kakao.com"]


def _on_failure_callback(context):
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
        logger.info("실패 알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("실패 알림 발송 실패: %s", e)
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


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

    # ── Orders ───────────────────────────────────────────────
    reset_task = PythonOperator(
        task_id="reset_posfeed_partitions",
        python_callable=reset_posfeed_partitions,
    )

    check_monthly_task = PythonOperator(
        task_id="check_monthly_collection",
        python_callable=check_monthly_collection,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(hours=2),
    )

    download_task = PythonOperator(
        task_id='download_excel',
        python_callable=download_posfeed_excel,
        op_kwargs={'collect_mode': COLLECT_MODE},
    )

    move_task = PythonOperator(
        task_id='move_to_storage',
        python_callable=move_to_storage,
    )

    onedrive_task = PythonOperator(
        task_id='partition_to_onedrive',
        python_callable=partition_to_onedrive,
    )

    missing_task = PythonOperator(
        task_id='collect_missing_dates',
        python_callable=collect_missing_dates,
        op_kwargs={
            "enable_missing_backfill": ENABLE_MISSING_DATES_BACKFILL or _is_date_mode(COLLECT_MODE),
            "collect_mode": COLLECT_MODE,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── 수동 today 파일 인제스트 (DOWN_DIR의 order-info-list_매장명.csv/xlsx) ──
    ingest_manual_task = PythonOperator(
        task_id="ingest_manual_files",
        python_callable=ingest_manual_csvs,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # ── Detail ───────────────────────────────────────────────
    extract_task = PythonOperator(
        task_id='extract_order_codes',
        python_callable=extract_order_codes,
        op_kwargs={'collect_mode': COLLECT_MODE},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cross_check_task = PythonOperator(
        task_id='check_undetailed_orders',
        python_callable=check_undetailed_orders,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    scrape_task = PythonOperator(
        task_id='scrape_order_details',
        python_callable=scrape_order_details,
        execution_timeout=timedelta(hours=8),
        retries=6,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
    )

    scrape_missing_task = PythonOperator(
        task_id='scrape_missing_order_details',
        python_callable=scrape_missing_order_details,
        op_kwargs={'collect_mode': COLLECT_MODE},
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(hours=4),
        retries=3,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
    )

    # ── 의존성: orders → detail ───────────────────────────────
    # ingest_manual_task를 download_task 앞에 두어 수동 파일이 먼저 처리·이동된 뒤 자동 다운로드가 시작됨
    reset_task >> check_monthly_task >> ingest_manual_task >> download_task >> move_task >> onedrive_task
    onedrive_task >> missing_task >> extract_task >> cross_check_task >> scrape_task >> scrape_missing_task
