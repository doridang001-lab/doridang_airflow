import logging
import random
import re
import time
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import DagRunAlreadyExists
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_retry import (
    build_next_retry_conf,
    retry_needed,
    retry_collect_from_conf,
    sanitize_retry_payload,
    validate_retry_ad_funnel,
    validate_retry_toorder,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_upload import build_final_notification_message
from modules.transform.utility.notifier import send_telegram

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
MAX_ATTEMPTS = 3


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    return (getattr(dag_run, "conf", None) or {}) if dag_run else {}


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def _store_id_check_summary(payload: dict) -> str:
    total = 0
    missing: list[str] = []
    for item in payload.get("store_info_per_account") or []:
        account_id = str(item.get("account_id") or "?")
        for store in item.get("stores") or []:
            total += 1
            store_id = str((store or {}).get("store_id") or "").strip()
            if not store_id:
                store_name = f"{store.get('brand', '')} {store.get('store', '')}".strip()
                missing.append(f"{account_id}/{store_name or '?'}")

    if total == 0:
        return "store_id 확인: 대상 없음"
    if missing:
        sample = ", ".join(missing[:10])
        return f"store_id 확인: {total - len(missing)}/{total}개 확인, 누락 {len(missing)}개 ({sample})"
    return f"store_id 확인: {total}/{total}개 확인"


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

    wait_override = conf.get("retry_wait_sec")
    wait_sec = float(wait_override) if wait_override is not None else random.uniform(180, 900)
    logger.info("재시도 전 랜덤 대기: %.0f초 (%.1f분)", wait_sec, wait_sec / 60)
    if wait_sec > 0:
        time.sleep(wait_sec)

    payload = sanitize_retry_payload(retry_collect_from_conf(conf))
    store_id_summary = _store_id_check_summary(payload)
    if "누락" in store_id_summary:
        logger.warning(store_id_summary)
    else:
        logger.info(store_id_summary)

    retry_result = str(payload.get("retry_result") or "재시도 완료")
    retry_result = f"{retry_result}\n{store_id_summary}"
    payload["retry_result"] = retry_result
    context["ti"].xcom_push(key="retry_payload", value=payload)
    context["ti"].xcom_push(key="retry_result", value=retry_result)
    return retry_result


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
    notification_context = conf.get("notification_context") or {
        "source_dag_id": conf.get("source_dag_id") or dag_id,
        "source_run_id": conf.get("source_run_id") or context.get("run_id") or "?",
        "target_date": target_date,
        "total_accounts": len(set(conf.get("failed_account_ids") or [])),
        "orders": {},
        "ad_funnel": {},
        "toorder": {},
        "residual_failed": {},
        "hard_failures": [],
    }
    retry_result = ti.xcom_pull(task_ids="retry_collect", key="retry_result") or ""
    retry_payload = ti.xcom_pull(task_ids="retry_collect", key="retry_payload")
    if not retry_payload:
        msg = build_final_notification_message(
            notification_context,
            attempt=attempt,
            max_attempts=max_attempts,
            hard_failure="retry_collect: retry_payload 없음",
        )
        logger.warning(msg)
        send_telegram(msg)
        raise RuntimeError(msg)

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result") or {}
    residual_failed = retry_payload.get("residual_failed") or {}
    needs_next = retry_needed(toorder_result, ad_funnel_result, residual_failed)
    dag_run = context.get("dag_run")
    hard_failures = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        hard_failures = [
            task.task_id
            for task in dag_run.get_task_instances()
            if task.task_id != "notify_and_trigger_next"
            and getattr(task, "state", None) in {"failed", "upstream_failed"}
        ]

    def send_final(*, hard_failure: str | None = None) -> str:
        failure_text = hard_failure
        if hard_failures:
            failure_text = ", ".join(sorted(set(hard_failures)))
        message = build_final_notification_message(
            notification_context,
            final_toorder_result=toorder_result,
            final_ad_funnel_result=ad_funnel_result,
            residual_failed=residual_failed,
            attempt=attempt,
            max_attempts=max_attempts,
            hard_failure=failure_text,
        )
        send_telegram(message)
        return message

    toorder_result = toorder_result or {}
    mismatched = toorder_result.get("mismatched_stores") or []
    gaps = toorder_result.get("toorder_gap_stores") or []
    still_ads = ad_funnel_result.get("still_empty") or []
    residual_count = sum(len(residual_failed.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))
    summary_lines = [
        f"[배민 Retry {attempt}/{max_attempts}회] {target_date}",
        retry_result,
        f"잔여실패={residual_count} / ToOrder 불일치={len(mismatched)} / ToOrder 갭={len(gaps)} / ad 잔존={len(still_ads)}",
    ]
    summary = "\n".join(line for line in summary_lines if line)

    if not needs_next:
        msg = send_final()
        logger.info(msg)
        return msg

    if attempt >= max_attempts:
        msg = send_final()
        logger.warning(msg)
        return msg

    next_conf = build_next_retry_conf(
        previous_conf=conf,
        retry_payload=retry_payload,
        toorder_result=toorder_result,
        ad_funnel_result=ad_funnel_result,
        attempt=attempt + 1,
        max_attempts=max_attempts,
    )
    residual_accounts = residual_failed.get("accounts") or []
    residual_account_ids = [
        str(account.get("account_id") or "").strip()
        for account in residual_accounts
        if str(account.get("account_id") or "").strip()
    ]
    if residual_account_ids:
        next_conf["failed_account_ids"] = sorted(set((next_conf.get("failed_account_ids") or []) + residual_account_ids))
        next_conf["failed_accounts_ids_only"] = sorted(set((next_conf.get("failed_accounts_ids_only") or []) + residual_account_ids))
    next_target_count = sum(
        len(next_conf.get(key) or [])
        for key in ("failed_account_ids", "failed_stores", "failed_orders", "failed_ads")
    )
    if next_target_count == 0:
        msg = send_final()
        logger.info(msg)
        return msg

    root = _safe_run_id_part(str(conf.get("source_run_id") or context.get("run_id") or "manual"))
    run_id = f"retry__{str(target_date).replace('-', '')}__attempt_{attempt + 1}__{root}"
    from airflow.api.common.trigger_dag import trigger_dag

    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=next_conf)
    except DagRunAlreadyExists:
        logger.info("다음 Retry DAG run 이미 존재: %s", run_id)

    msg = (
        f"{summary}\n"
        f"다음 재시도 대상: accounts={len(next_conf.get('failed_account_ids') or [])}, "
        f"stores={len(next_conf.get('failed_stores') or [])}, "
        f"orders={len(next_conf.get('failed_orders') or [])}, "
        f"ads={len(next_conf.get('failed_ads') or [])}\n"
        f"Retry DAG attempt {attempt + 1} 트리거: {run_id}"
    )
    logger.info(msg)
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
        pool="selenium_pool",
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
