"""
OKPOS 매출 원본파일 자동 수집 DAG

처리 흐름:
1. 실행 날짜 범위 결정 (conf 또는 yesterday)
2. today 페이지 매장별 엑셀 다운로드 (당일매출상세내역, 4개 매장)
3. receipt/details 페이지 매장별 엑셀 다운로드 (영수증별매출상세현황, 4개 매장)
4. 합계 제거 + 매장명 추가 후 brand=도리당/store={매장}/ym={YYYY-MM}/{page_type}.csv 저장
5. log.parquet 실행 이력 기록

매일 06:00 KST 실행 (DB_OKPOS_SALES_TIME)
인증: OKPOS ID/PW (파이프라인 상수)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
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

# 수집 날짜 범위 설정
# None이면 어제 1일만 수집 (기본 스케줄 동작)
# 기간 지정은 ("2026-03-01", "2026-03-02") 형식으로 입력하면 1일씩 순차 수집
# 예: MANUAL_DATE_RANGE = ("2026-03-01", "2026-03-31")  # 3월 전체
# Backfill 참고
# - 누락 체크(로컬): `python -X utf8 scripts/okpos_sales_missing_report.py 2023-12-05 2026-05-05 --brand 도리당 --pages daily --emit_airflow_confs`
# - Airflow UI에서 DAG Run Conf로 월별/기간별 재수집
#   {"sale_date_from":"2025-01-01","sale_date_to":"2025-01-31"}
#   (기존 파일이 있으면 자동으로 skip, 강제 재다운로드는 conf의 force_redownload / force_redownload_daily 사용)
# MANUAL_DATE_RANGE: tuple | None = None
MANUAL_DATE_RANGE = None
LOOKBACK_DAYS: int | None = 120 # None or int형


def _on_failure_callback(context):
    """Task 실패 시 Telegram 알림 발송."""
    ti = context.get("task_instance")
    dag_id_ = ti.dag_id
    task_id = ti.task_id
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "예외 없음")
    log_url = ti.log_url

    body = (
        f"DAG: {dag_id_}\n"
        f"Task: {task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {log_url}"
    )
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def trigger_unified_sales_after_okpos(**context) -> str:
    """수동 단일 날짜 OKPOS 재수집 후 UnifiedSales 정정 실행을 선택적으로 트리거."""
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if not _truthy(conf.get("trigger_unified_sales")):
        return "trigger_unified_sales=false - 스킵"

    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    sale_dates = [str(d).strip() for d in sale_dates if str(d).strip()]
    if len(sale_dates) != 1:
        logger.warning("UnifiedSales 자동 트리거 스킵: 단일 날짜가 아님 (%s)", sale_dates)
        return f"UnifiedSales 자동 트리거 스킵: 단일 날짜 아님 ({len(sale_dates)}일)"

    sale_date = sale_dates[0]
    import re
    parent_run_id = str(context.get("run_id") or "manual")
    safe_parent_run_id = re.sub(r"[^A-Za-z0-9_.~-]+", "_", parent_run_id).strip("_")[:120]
    run_id = f"manual__okpos_after_unified_sales__{sale_date.replace('-', '')}__{safe_parent_run_id}"
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    trigger_conf = {"sale_date": sale_date}
    if _truthy(conf.get("wait_unified_upstreams")):
        trigger_conf["wait_upstreams"] = True

    try:
        trigger_dag(
            dag_id="DB_UnifiedSales",
            run_id=run_id,
            conf=trigger_conf,
        )
    except DagRunAlreadyExists:
        logger.info("DB_UnifiedSales 이미 트리거됨: %s", run_id)
        return f"DB_UnifiedSales 이미 트리거됨: {run_id}"

    logger.info("DB_UnifiedSales 트리거 완료: %s | conf=%s", run_id, trigger_conf)
    return f"DB_UnifiedSales 트리거 완료: {run_id}"


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
        op_kwargs={"manual_date_range": MANUAL_DATE_RANGE, "lookback_days": LOOKBACK_DAYS},
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

    # Selenium task: Python-level retry plus Airflow retry.
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
        python_callable=_reload_and_call,
        op_kwargs={"fn_name": "save_to_raw"},
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
        # today/receipt 불일치 한쪽만 있을 때 NO_DATA 마킹 및 자동 보정
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    t6 = PythonOperator(
        task_id="write_log",
        python_callable=write_log,
    )

    t7 = PythonOperator(
        task_id="trigger_unified_sales_after_okpos",
        python_callable=trigger_unified_sales_after_okpos,
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

    t1 >> t1b >> t1c >> t1d >> t2 >> t3 >> t3b >> t4 >> t4b >> t5 >> t6c >> t6a >> t6b >> t6 >> t7
