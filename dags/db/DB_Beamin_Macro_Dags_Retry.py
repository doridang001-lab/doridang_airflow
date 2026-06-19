import logging
import random
import re
import time
import uuid
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import DagRunAlreadyExists
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_retry import (
    retry_needed,
    retry_collect_from_conf,
    sanitize_retry_payload,
    validate_retry_ad_funnel,
    validate_retry_toorder,
)
from modules.transform.utility.notifier import send_telegram

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
MAX_ATTEMPTS = 10


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    return (getattr(dag_run, "conf", None) or {}) if dag_run else {}


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def load_failed_and_accounts(**context) -> str:
    conf = _conf(context)
    attempt = int(conf.get("attempt", 1))
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    context["ti"].xcom_push(key="attempt", value=attempt)
    context["ti"].xcom_push(key="target_date", value=target_date)
    return (
        f"attempt={attempt} target_date={target_date} "
        f"accounts={len(conf.get('failed_account_ids') or [])}"
    )


def retry_collect(**context) -> str:
    conf = _conf(context)
    has_retry_target = any(
        conf.get(key)
        for key in (
            "failed_account_ids",
            "failed_accounts_ids_only",
            "failed_stores",
            "failed_orders",
            "failed_ads",
        )
    )
    if not has_retry_target:
        payload = sanitize_retry_payload(
            {
                "target_date": conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD"),
                "retry_result": "재시도 대상 없음",
                "store_info_per_account": [],
                "ad_store_infos": [],
            }
        )
        context["ti"].xcom_push(key="retry_payload", value=payload)
        context["ti"].xcom_push(key="retry_result", value=payload["retry_result"])
        logger.info("Retry DAG 종료 대상: conf에 실패 대상 없음")
        return payload["retry_result"]

    wait_sec = random.uniform(180, 900)
    logger.info("재시도 전 랜덤 대기: %.0f초 (%.1f분)", wait_sec, wait_sec / 60)
    time.sleep(wait_sec)

    payload = sanitize_retry_payload(retry_collect_from_conf(conf))
    context["ti"].xcom_push(key="retry_payload", value=payload)
    context["ti"].xcom_push(key="retry_result", value=payload.get("retry_result", ""))
    return str(payload.get("retry_result") or "재시도 완료")


def validate_toorder(**context) -> str:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="retry_collect", key="retry_payload")
    if not payload:
        result = {
            "store_results": {},
            "mismatched_stores": [],
            "missing_brand_stores": [],
            "retried_stores": [],
            "matched": True,
            "compared_count": 0,
            "toorder_gap_stores": [],
        }
        context["ti"].xcom_push(key="toorder_result", value=result)
        logger.info("Retry ToOrder 검증 스킵: retry_payload 없음")
        return "Retry ToOrder 검증 스킵: retry_payload 없음"

    if not payload.get("store_info_per_account"):
        result = {
            "store_results": {},
            "mismatched_stores": [],
            "missing_brand_stores": [],
            "retried_stores": [],
            "matched": True,
            "compared_count": 0,
            "toorder_gap_stores": [],
        }
        context["ti"].xcom_push(key="toorder_result", value=result)
        logger.info("Retry ToOrder 검증 스킵: 수집 매장 정보 없음")
        return "Retry ToOrder 검증 스킵: 수집 매장 정보 없음"

    toorder_result = validate_retry_toorder(payload)
    context["ti"].xcom_push(key="toorder_result", value=toorder_result)

    target_date = payload.get("target_date") or ti.xcom_pull(task_ids="load_failed_and_accounts", key="target_date")
    matched = toorder_result.get("matched", False)
    compared = toorder_result.get("compared_count", 0)
    mismatched = toorder_result.get("mismatched_stores", [])
    gap_stores = toorder_result.get("toorder_gap_stores", [])
    summary = (
        f"[Retry 교차검증 {target_date}] 비교 {compared}개 / "
        f"{'완전일치' if matched else f'불일치 {len(mismatched)}개'}"
    )
    if gap_stores:
        summary += f"\nToOrder 갭: {', '.join(gap_stores)}"
    logger.info(summary)
    return summary


def validate_ad_funnel(**context) -> str:
    payload = context["ti"].xcom_pull(task_ids="retry_collect", key="retry_payload")
    if not payload:
        result = {"empty_stores": [], "retried": [], "still_empty": []}
        context["ti"].xcom_push(key="ad_funnel_result", value=result)
        logger.info("Retry ad_funnel 검증 스킵: retry_payload 없음")
        return "Retry ad_funnel 검증 스킵: retry_payload 없음"

    result = validate_retry_ad_funnel(payload)
    context["ti"].xcom_push(key="ad_funnel_result", value=result)

    empty = result.get("empty_stores") or []
    still = result.get("still_empty") or []
    summary = f"[Retry ad_funnel] 빈값 {len(empty)}개 / 재수집 후 잔존 {len(still)}개"
    if still:
        summary += "\n" + "\n".join(f"  - {item.get('store', '?')}" for item in still)
    logger.info(summary)
    return summary


def notify_and_trigger_next(**context) -> str:
    ti = context["ti"]
    conf = _conf(context)
    attempt = int(ti.xcom_pull(task_ids="load_failed_and_accounts", key="attempt") or conf.get("attempt", 1))
    target_date = ti.xcom_pull(task_ids="load_failed_and_accounts", key="target_date") or conf.get("target_date")
    max_attempts = int(conf.get("max_attempts", MAX_ATTEMPTS))
    retry_result = ti.xcom_pull(task_ids="retry_collect", key="retry_result") or ""
    retry_payload = ti.xcom_pull(task_ids="retry_collect", key="retry_payload")
    if not retry_payload:
        msg = f"[배민 Retry {attempt}/{max_attempts}회] {target_date}\nRetry 종료: retry_payload 없음"
        logger.warning(msg)
        send_telegram(msg)
        return msg

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result") or {}
    needs_next = (
        True
        if toorder_result is None
        else retry_needed(toorder_result, ad_funnel_result, None)
    )

    toorder_result = toorder_result or {}
    mismatched = toorder_result.get("mismatched_stores") or []
    gaps = toorder_result.get("toorder_gap_stores") or []
    still_ads = ad_funnel_result.get("still_empty") or []
    summary_lines = [
        f"[배민 Retry {attempt}/{max_attempts}회] {target_date}",
        retry_result,
        f"ToOrder 불일치={len(mismatched)} / ToOrder 갭={len(gaps)} / ad 잔존={len(still_ads)}",
    ]
    summary = "\n".join(line for line in summary_lines if line)

    if not needs_next:
        msg = f"{summary}\nRetry 종료: 추가 재시도 불필요"
        logger.info(msg)
        send_telegram(msg)
        return msg

    if attempt >= max_attempts:
        msg = f"[배민 Retry 최종실패] {max_attempts}회 재시도 후에도 누락 잔존\n{summary}"
        logger.warning(msg)
        send_telegram(msg)
        return msg

    next_conf = dict(conf)
    next_conf["attempt"] = attempt + 1
    next_conf["max_attempts"] = max_attempts

    parent = _safe_run_id_part(str(context.get("run_id") or "manual"))
    run_id = f"retry__{str(target_date).replace('-', '')}__attempt_{attempt + 1}__{parent}__{uuid.uuid4().hex[:8]}"
    from airflow.api.common.trigger_dag import trigger_dag

    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=next_conf)
    except DagRunAlreadyExists:
        logger.info("다음 Retry DAG run 이미 존재: %s", run_id)

    msg = f"{summary}\nRetry DAG attempt {attempt + 1} 트리거: {run_id}"
    logger.info(msg)
    send_telegram(msg)
    return msg


with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["db", "baemin", "retry"],
) as dag:
    t1 = PythonOperator(
        task_id="load_failed_and_accounts",
        python_callable=load_failed_and_accounts,
        execution_timeout=timedelta(minutes=5),
    )

    t2 = PythonOperator(
        task_id="retry_collect",
        python_callable=retry_collect,
        execution_timeout=timedelta(minutes=180),
    )

    t3 = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )

    t4 = PythonOperator(
        task_id="validate_ad_funnel",
        python_callable=validate_ad_funnel,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )

    t5 = PythonOperator(
        task_id="notify_and_trigger_next",
        python_callable=notify_and_trigger_next,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4 >> t5
