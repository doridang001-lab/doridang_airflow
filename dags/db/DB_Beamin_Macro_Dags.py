'''
Baemin macro DAG — 배달의민족 계정별 자동 수집

=== 수집 흐름 (계정 단위, 단일 브라우저 세션) ===
  load_accounts
      ↓
  collect_all
    ├─ 로그인
    ├─ now 수집        (우리가게NOW 현황 지표)
    ├─ 우가클 수집     (우리가게 클릭 현황 - 이번달 + 저번달)
    ├─ 변경이력 수집   (매장 변경이력 - history/change/shop)
    └─ 로그아웃

=== 저장 경로 ===
  now      : analytics/baemin_macro/metrics_now/
               brand={brand}/store={store}/ym={YYYY-MM}/baemin_now.csv
  우가클    : analytics/baemin_macro/metrics_our_store_clicks/
               brand={brand}/store={store}/ym={YYYY-MM}/woori_shop_click.csv
  변경이력  : analytics/baemin_macro/shop_change/
               brand={brand}/store={store}/ym={YYYY-MM}/shop_change.csv

=== 수집 월 ===
  우가클: 이번달 + 저번달 (덮어쓰기)
'''

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori as pipeline_collect_all,
)
from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_TIME

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

KST = pendulum.timezone("Asia/Seoul")
_ALERT_EMAILS = ["a17019@kakao.com"]

TARGET_STORES = [
    "도리당 송파삼전점",
    "도리당 역삼점",
]  # exact match; empty list means all stores


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html

    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info("알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("알림 발송 실패: %s", e)


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logical_date = context.get("logical_date") or ti.execution_date
    execution_date = logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
    _send_alert(
        subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"실행일시(KST): {execution_date}\n"
            f"에러: {context.get('exception', '알 수 없음')}\n"
            f"로그: {ti.log_url}"
        ),
    )


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def load_accounts(**context) -> str:
    accounts = pipeline_load_accounts(target_stores=TARGET_STORES, exact=True)
    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개: {stores}"


def collect_all(**context) -> str:
    import random
    import time

    wait_sec = random.uniform(0, 60)
    logger.info("수집 시작 전 랜덤 대기: %.0f초", wait_sec)
    time.sleep(wait_sec)

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    return pipeline_collect_all(account_list)


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_COLLECT_TIME,
    start_date=pendulum.datetime(2026, 5, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "crawl"],
) as dag:

    t1 = PythonOperator(
        task_id="load_accounts",
        python_callable=load_accounts,
    )

    t2 = PythonOperator(
        task_id="collect_all",
        python_callable=collect_all,
        execution_timeout=timedelta(minutes=150),
    )

    t1 >> t2
