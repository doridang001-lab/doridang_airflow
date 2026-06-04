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
    ├─ 주문내역 수집   (orders/history - 어제)
    ├─ 광고 funnel 수집 (stat/advertisement - 어제)
    └─ 로그아웃

=== 저장 경로 ===
  now      : analytics/baemin_macro/metrics_now/
               brand={brand}/store={store}/ym={YYYY-MM}/baemin_now.csv
  우가클    : analytics/baemin_macro/metrics_our_store_clicks/
               brand={brand}/store={store}/ym={YYYY-MM}/woori_shop_click.csv
  변경이력  : analytics/baemin_macro/shop_change/
               brand={brand}/store={store}/ym={YYYY-MM}/shop_change.csv
  주문내역  : analytics/baemin_macro/orders/
  광고funnel: analytics/baemin_macro/ad_funnel/
               brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM-DD}.csv

=== 수집 월 ===
  우가클: 이번달 + 저번달 (덮어쓰기)
  주문내역: 어제 (upsert by 주문번호)
'''

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import send_telegram
from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori as pipeline_collect_all,
    retry_once_failed as pipeline_retry_failed,
)
from modules.transform.pipelines.db.DB_Beamin_03_shop_change import (
    collect_shop_change as pipeline_collect_shop_change,
)
from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_TIME
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

KST = pendulum.timezone("Asia/Seoul")
_ALERT_EMAILS = ["a17019@kakao.com"]

TARGET_STORES = [
    "도리당 역삼점",
    "도리당 송파삼전점",
    "도리당 연신내점"
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
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시(KST): {execution_date}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    _send_alert(subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}", body=body)
    send_telegram(body + "\n해결해라")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")  # e.g. ["도리당 역삼점"] — 테스트·재수집 용
    target = stores_override if stores_override else TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)
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

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")  # "YYYY-MM-DD" → orders CSV 날짜 라벨 override

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    result = pipeline_collect_all(account_list, target_date=target_date)
    context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
    context["ti"].xcom_push(key="store_info_per_account", value=result.get("store_info_per_account", []))
    return result["summary"]


def collect_shop_change(**context) -> str:
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    return pipeline_collect_shop_change(account_list)


def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")

    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or {}
    if not any(failed.get(k) for k in ("accounts", "stores", "orders")):
        logger.info("재시도 대상 없음")
        return "재시도 없음"

    return pipeline_retry_failed(failed, target_date=target_date)


def validate_orders(**context) -> str:
    validation = context["ti"].xcom_pull(task_ids="collect_all", key="validation") or []
    if not validation:
        logger.info("orders 검증 결과 없음")
        return "검증 없음"

    mismatches = [v for v in validation if v.get("matched") is False]
    matched = [v for v in validation if v.get("matched") is True]
    unknown = [v for v in validation if v.get("matched") is None]

    lines = [
        f"orders 검증: 총 {len(validation)}건 "
        f"(일치 {len(matched)}, 불일치 {len(mismatches)}, 미확인 {len(unknown)})"
    ]
    for v in mismatches:
        lines.append(
            f"  ❌ {v.get('store', '?')} [{v.get('status', '?')}] "
            f"수집={v.get('actual_count')}건/{v.get('actual_amount', 0):,}원 "
            f"기대={v.get('expected_count')}건/{v.get('expected_amount', 0):,}원 "
            f"(재시도 {v.get('retried', 0)}회)"
        )
    summary = "\n".join(lines)
    logger.info(summary)

    if mismatches:
        _send_alert(subject=f"[배민 orders 불일치] {len(mismatches)}건", body=summary)
        send_telegram(summary)

    return summary


def validate_ad_funnel(**context) -> str:
    from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    ad_stores = context["ti"].xcom_pull(task_ids="collect_all", key="ad_stores") or []
    if not ad_stores:
        logger.info("ad_funnel 점검 대상 없음")
        return "점검 없음"

    result = _validate_and_retry_ad_funnel(ad_stores, target_date)
    empty = result["empty_stores"]
    still = result["still_empty"]

    lines = [
        f"ad_funnel 빈값 점검: 총 {len(ad_stores)}매장 / 빈값 {len(empty)}건 / "
        f"재수집 후 잔여 {len(still)}건"
    ]
    for s in still:
        lines.append(f"  ❌ {s.get('store', '?')} 재수집 후에도 빈값 잔존")
    summary = "\n".join(lines)
    logger.info(summary)

    if still:
        _send_alert(subject=f"[배민 ad_funnel 빈값] {len(still)}건 잔존", body=summary)
        send_telegram(summary)

    return summary


def validate_toorder(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list") or []
    store_info_per_account = (
        context["ti"].xcom_pull(task_ids="collect_all", key="store_info_per_account") or []
    )

    result = validate_toorder_orders(
        account_list, store_info_per_account, target_date
    )

    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    gap_stores = result.get("toorder_gap_stores", [])

    # 요약 헤더
    summary_lines = [
        f"토더 교차검증 [{target_date}]: "
        f"비교 {compared}개 매장 / "
        f"{'✅전체일치' if matched else f'❌불일치 {len(mismatched)}개'}"
    ]
    # 불일치 매장 상세
    for store, info in store_results.items():
        if not info.get("toorder_gap"):
            if not info["matched"]:
                retry_mark = " (재수집후)" if store in retried else ""
                summary_lines.append(
                    f"  [{store}] ToOrder={info['toorder']:,} / 배민={info['baemin']:,}{retry_mark}"
                )
    # ToOrder 갭 매장 (계정연결 문제) 별도 표기
    if gap_stores:
        summary_lines.append(f"⚠️ ToOrder 갭 의심(계정연결?): {', '.join(gap_stores)}")
        for store in gap_stores:
            info = store_results.get(store, {})
            summary_lines.append(
                f"  [{store}] ToOrder=0 / 배민={info.get('baemin', 0):,}"
            )
    summary = "\n".join(summary_lines)
    logger.info(summary)

    if not matched or gap_stores:
        _send_alert(subject=f"[배민↔토더 불일치] {target_date}", body=summary)

    return summary


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_COLLECT_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
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
        execution_timeout=timedelta(minutes=200),
    )

    t2a = PythonOperator(
        task_id="collect_shop_change",
        python_callable=collect_shop_change,
        execution_timeout=timedelta(minutes=120),
    )

    t3 = PythonOperator(
        task_id="retry_failed",
        python_callable=retry_failed,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=120),
    )

    t4 = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule="all_done",
    )

    t5 = PythonOperator(
        task_id="validate_ad_funnel",
        python_callable=validate_ad_funnel,
        trigger_rule="all_done",
    )

    t6 = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
    )

    t1 >> t2 >> t2a >> t3 >> [t4, t5, t6]
