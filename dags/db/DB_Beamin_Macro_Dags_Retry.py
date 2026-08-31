import logging
import random
import re
import time
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, DagRunAlreadyExists
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_retry import (
    build_next_retry_conf,
    merge_retry_payloads as merge_retry_payloads_from_lanes,
    retry_needed,
    retry_collect_from_conf,
    sanitize_retry_payload,
    split_retry_conf_by_lane,
    validate_retry_ad_funnel,
    validate_retry_toorder,
)
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    cleanup_manual_baemin_files,
    load_manual_baemin_files,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_upload import build_final_notification_message
from modules.transform.utility.notifier import send_telegram

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
MAX_ATTEMPTS = 3
RETRY_LANES: int = 2
BAEMIN_SELENIUM_POOL = "baemin_selenium_pool"
_LANE_OFFSET_RANGE: tuple[float, float] = (15.0, 20.0)
_RETRY_COLLECT_BUDGET_MINUTES = 60
_RETRY_AD_FUNNEL_BUDGET_MINUTES = 15


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    return (getattr(dag_run, "conf", None) or {}) if dag_run else {}


def _is_delivery_commission_recollect(conf: dict, context: dict) -> bool:
    dag_run = context.get("dag_run")
    run_id = str(getattr(dag_run, "run_id", "") or context.get("run_id") or "")
    source_dag_id = str(conf.get("source_dag_id") or "")
    source_run_id = str(conf.get("source_run_id") or "")
    return (
        source_dag_id == "DB_DeliveryCommission_Dags"
        or "delivery_commission" in source_run_id
        or "delivery_commission" in run_id
    )


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


def _fallback_residual_from_conf(conf: dict) -> dict:
    residual = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    for account_id in conf.get("failed_accounts_ids_only") or []:
        text = str(account_id or "").strip()
        if text:
            residual["accounts"].append({"account_id": text})
    for key, output_key in (
        ("failed_stores", "stores"),
        ("failed_orders", "orders"),
        ("failed_ads", "ads"),
        ("failed_stages", "stages"),
    ):
        residual[output_key].extend(conf.get(key) or [])
    return residual


def _store_identity(account_id: str, store: dict | None) -> tuple[str, str, str]:
    store = store or {}
    return (
        str(account_id or "").strip(),
        str(store.get("store_id") or "").strip(),
        str(store.get("store") or "").strip(),
    )


def _stores_from_residual_item(item: dict, payload_key: str) -> list[dict]:
    stores = item.get(payload_key)
    if isinstance(stores, dict):
        return [stores]
    if isinstance(stores, list):
        return [store for store in stores if isinstance(store, dict)]
    return []


def _collected_order_store_keys(retry_payload: dict | None) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    for item in (retry_payload or {}).get("store_info_per_account") or []:
        if not isinstance(item, dict):
            continue
        account_id = str(item.get("account_id") or "").strip()
        for store in item.get("stores") or []:
            if isinstance(store, dict):
                keys.add(_store_identity(account_id, store))
    return keys


def _collected_account_ids(retry_payload: dict | None) -> set[str]:
    return {
        str(item.get("account_id") or "").strip()
        for item in (retry_payload or {}).get("store_info_per_account") or []
        if isinstance(item, dict) and str(item.get("account_id") or "").strip()
    }


def _target_date_text(target_date: str | None) -> str:
    if not target_date:
        return ""
    try:
        parsed = datetime.strptime(str(target_date), "%Y-%m-%d")
    except ValueError:
        return str(target_date)
    return parsed.strftime("%Y. %m. %d")


def _split_brand_store(store_name: str | None) -> tuple[str, str]:
    parts = str(store_name or "").strip().split(maxsplit=1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", str(store_name or "").strip()


def _orders_output_has_target_rows(account: dict, target_date: str | None) -> bool:
    brand, store = _split_brand_store(account.get("store_name"))
    date_text = _target_date_text(target_date)
    if not brand or not store or not date_text:
        return False
    try:
        import pandas as pd
        from modules.transform.utility.paths import BAEMIN_ORDERS_DB

        ym = str(target_date)[:7]
        path = Path(BAEMIN_ORDERS_DB) / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}.parquet"
        if not path.exists():
            return False
        df = pd.read_parquet(path, columns=["주문시각"])
        return bool(df["주문시각"].astype(str).str.contains(date_text, regex=False, na=False).any())
    except Exception as exc:
        logger.warning("orders residual 산출물 확인 실패: account_id=%s error=%s", account.get("account_id"), exc)
        return False


def _reconcile_account_residuals(
    residual_accounts: list,
    retry_payload: dict | None,
    conf: dict | None = None,
) -> list:
    collected_ids = _collected_account_ids(retry_payload)
    target_date = (retry_payload or {}).get("target_date") or (conf or {}).get("target_date")
    if not collected_ids:
        collected_ids = set()
    return [
        account
        for account in residual_accounts or []
        if not isinstance(account, dict)
        or (
            str(account.get("account_id") or "").strip() not in collected_ids
            and not (_is_orders_only_retry(conf) and _orders_output_has_target_rows(account, target_date))
        )
    ]


def _reconcile_order_residuals(residual_orders: list, retry_payload: dict | None) -> list:
    collected_keys = _collected_order_store_keys(retry_payload)
    if not collected_keys:
        return list(residual_orders or [])

    reconciled: list = []
    for item in residual_orders or []:
        if not isinstance(item, dict):
            reconciled.append(item)
            continue
        account_id = str((item.get("account") or {}).get("account_id") or "").strip()
        remaining_stores = [
            store
            for store in _stores_from_residual_item(item, "stores")
            if _store_identity(account_id, store) not in collected_keys
        ]
        if remaining_stores:
            reconciled.append({**item, "stores": remaining_stores})
    return reconciled


def _is_orders_only_retry(conf: dict | None) -> bool:
    text = " ".join(
        str(value or "")
        for value in (
            (conf or {}).get("source_run_id"),
            (conf or {}).get("collect_range"),
            (conf or {}).get("stability_profile"),
        )
    ).lower()
    return "orders_only" in text or "orders-only" in text


def _reconcile_residual_failed_after_validation(
    residual_failed: dict,
    ad_funnel_result: dict | None,
    retry_payload: dict | None = None,
    conf: dict | None = None,
) -> dict:
    reconciled = {
        key: list((residual_failed or {}).get(key) or [])
        for key in ("accounts", "stores", "orders", "ads", "stages")
    }
    reconciled["accounts"] = _reconcile_account_residuals(reconciled["accounts"], retry_payload, conf)
    reconciled["orders"] = _reconcile_order_residuals(reconciled["orders"], retry_payload)
    if ad_funnel_result and "still_empty" in ad_funnel_result and not (ad_funnel_result.get("still_empty") or []):
        reconciled["ads"] = []
    if _is_orders_only_retry(conf):
        reconciled["ads"] = []
        reconciled["stages"] = []
    return reconciled


def _append_residual_to_next_conf(next_conf: dict, residual_failed: dict) -> None:
    account_ids: list[str] = []
    residual_accounts = residual_failed.get("accounts") or []
    for account in residual_accounts:
        account_id = str((account or {}).get("account_id") or "").strip()
        if account_id:
            account_ids.append(account_id)
    if account_ids:
        next_conf["failed_account_ids"] = sorted(set((next_conf.get("failed_account_ids") or []) + account_ids))
        next_conf["failed_accounts_ids_only"] = sorted(
            set((next_conf.get("failed_accounts_ids_only") or []) + account_ids)
        )

    def append_store_items(residual_key: str, conf_key: str, payload_key: str) -> None:
        ids: list[str] = []
        items: list[dict] = []
        for item in residual_failed.get(residual_key) or []:
            if not isinstance(item, dict):
                continue
            account = item.get("account") or {}
            account_id = str(account.get("account_id") or "").strip()
            payload_value = item.get(payload_key)
            if not account_id or not payload_value:
                continue
            items.append({"account_id": account_id, payload_key: payload_value})
            ids.append(account_id)
        if items:
            next_conf[conf_key] = [*(next_conf.get(conf_key) or []), *items]
            next_conf["failed_account_ids"] = sorted(
                set((next_conf.get("failed_account_ids") or []) + ids)
            )

    append_store_items("stores", "failed_stores", "store")
    append_store_items("orders", "failed_orders", "stores")
    append_store_items("ads", "failed_ads", "stores")

    stage_items: list[dict] = []
    stage_account_ids: list[str] = []
    for item in residual_failed.get("stages") or []:
        if not isinstance(item, dict):
            continue
        account = item.get("account") or {}
        account_id = str(account.get("account_id") or "").strip()
        store = item.get("store") or {}
        stage = str(item.get("stage") or "").strip()
        if not account_id or not store or not stage:
            continue
        stage_items.append({"account_id": account_id, "store": store, "stage": stage})
        stage_account_ids.append(account_id)
    if stage_items:
        next_conf["failed_stages"] = [*(next_conf.get("failed_stages") or []), *stage_items]
        next_conf["failed_account_ids"] = sorted(
            set((next_conf.get("failed_account_ids") or []) + stage_account_ids)
        )


def load_failed_and_accounts(**context) -> str:
    conf = _conf(context)
    attempt = int(conf.get("attempt", 1))
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    context["ti"].xcom_push(key="attempt", value=attempt)
    context["ti"].xcom_push(key="target_date", value=target_date)
    return (
        f"attempt={attempt} target_date={target_date} "
        f"collect_range={conf.get('collect_range') or '-'} "
        f"allowed_accounts={len(conf.get('allowed_account_ids') or [])} "
        f"accounts={len(conf.get('failed_account_ids') or [])} "
        f"stages={len(conf.get('failed_stages') or [])}"
    )


def retry_collect(
    *,
    lane_index: int = 0,
    lanes: int = 1,
    lane_offset: bool = False,
    **context,
) -> str:
    conf = _conf(context)
    lane_confs = split_retry_conf_by_lane(conf, lanes)
    conf = lane_confs[lane_index] if lane_index < len(lane_confs) else {}
    has_retry_target = any(
        conf.get(key)
        for key in (
            "failed_account_ids",
            "failed_accounts_ids_only",
            "failed_stores",
            "failed_orders",
            "failed_ads",
            "failed_stages",
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

    deadline_at = time.monotonic() + (_RETRY_COLLECT_BUDGET_MINUTES * 60)
    skip_wait = _is_delivery_commission_recollect(conf, context)
    if lane_offset and not skip_wait:
        offset_sec = random.uniform(*_LANE_OFFSET_RANGE)
        logger.info("Retry 레인 B 시작 오프셋 %.0f초", offset_sec)
        time.sleep(offset_sec)

    wait_override = conf.get("retry_wait_sec")
    wait_sec = 0.0 if skip_wait else (
        float(wait_override) if wait_override is not None else random.uniform(180, 900)
    )
    remaining_before_wait = max(0.0, deadline_at - time.monotonic())
    wait_sec = min(wait_sec, max(0.0, remaining_before_wait - 60.0))
    if skip_wait:
        logger.info("delivery_commission 재수집 retry 대기 생략")
    else:
        logger.info("재시도 전 랜덤 대기: %.0f초 (%.1f분)", wait_sec, wait_sec / 60)
    if wait_sec > 0:
        time.sleep(wait_sec)

    partial_payload: dict = {}

    def push_partial(raw_payload: dict) -> None:
        nonlocal partial_payload
        partial_payload = sanitize_retry_payload(raw_payload)
        context["ti"].xcom_push(key="retry_payload", value=partial_payload)
        context["ti"].xcom_push(
            key="retry_result",
            value=str(partial_payload.get("retry_result") or "재시도 부분 결과 저장"),
        )

    try:
        try:
            raw_payload = retry_collect_from_conf(
                conf,
                deadline_at=deadline_at,
                partial_result_callback=push_partial,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            raw_payload = retry_collect_from_conf(conf)
        payload = sanitize_retry_payload(raw_payload)
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
    except Exception:
        if not partial_payload:
            partial_payload = sanitize_retry_payload(
                {
                    "target_date": conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD"),
                    "retry_result": "재시도 중 예외 발생: 부분 결과 없음",
                    "store_info_per_account": [],
                    "ad_store_infos": [],
                    "residual_failed": _fallback_residual_from_conf(conf),
                }
            )
        context["ti"].xcom_push(key="retry_payload", value=partial_payload)
        context["ti"].xcom_push(
            key="retry_result",
            value=str(partial_payload.get("retry_result") or "재시도 중 예외 발생"),
        )
        raise


def merge_retry_payloads(**context) -> str:
    ti = context["ti"]
    task_ids = [f"retry_collect_{index}" for index in range(1, RETRY_LANES + 1)]
    payload = merge_retry_payloads_from_lanes(
        *(ti.xcom_pull(task_ids=task_id, key="retry_payload") for task_id in task_ids)
    )
    retry_result = str(payload.get("retry_result") or "재시도 대상 없음")
    ti.xcom_push(key="retry_payload", value=payload)
    ti.xcom_push(key="retry_result", value=retry_result)
    return retry_result


def validate_toorder(**context) -> str:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="merge_retry_payloads", key="retry_payload")
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
    payload = context["ti"].xcom_pull(task_ids="merge_retry_payloads", key="retry_payload")
    if not payload:
        result = {"empty_stores": [], "retried": [], "still_empty": []}
        context["ti"].xcom_push(key="ad_funnel_result", value=result)
        logger.info("Retry ad_funnel 검증 스킵: retry_payload 없음")
        return "Retry ad_funnel 검증 스킵: retry_payload 없음"

    deadline_at = time.monotonic() + (_RETRY_AD_FUNNEL_BUDGET_MINUTES * 60)
    result = validate_retry_ad_funnel(payload, deadline_at=deadline_at)
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
    retry_result = ti.xcom_pull(task_ids="merge_retry_payloads", key="retry_result") or ""
    retry_payload = ti.xcom_pull(task_ids="merge_retry_payloads", key="retry_payload")
    if not retry_payload:
        msg = build_final_notification_message(
            notification_context,
            attempt=attempt,
            max_attempts=max_attempts,
            hard_failure="retry_collect: retry_payload 없음",
        )
        logger.warning(msg)
        send_telegram(msg)
        raise AirflowException(msg)

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result") or {}
    if _is_orders_only_retry(conf):
        ad_funnel_result = {**ad_funnel_result, "still_empty": []}
    residual_failed = _reconcile_residual_failed_after_validation(
        retry_payload.get("residual_failed") or {},
        ad_funnel_result,
        retry_payload,
        conf,
    )
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
    residual_count = sum(
        len(residual_failed.get(key) or [])
        for key in ("accounts", "stores", "orders", "ads", "stages")
    )
    summary_lines = [
        f"[배민 Retry {attempt}/{max_attempts}회] {target_date}",
        retry_result,
        f"잔여실패={residual_count} / ToOrder 불일치={len(mismatched)} / ToOrder 갭={len(gaps)} / ad 잔존={len(still_ads)}",
    ]
    summary = "\n".join(line for line in summary_lines if line)

    if not needs_next:
        msg = send_final()
        if hard_failures:
            logger.warning(msg)
            raise AirflowException(msg)
        logger.info(msg)
        return msg

    if attempt >= max_attempts:
        msg = send_final()
        logger.warning(msg)
        raise AirflowException(msg)

    next_conf = build_next_retry_conf(
        previous_conf=conf,
        retry_payload=retry_payload,
        toorder_result=toorder_result,
        ad_funnel_result=ad_funnel_result,
        attempt=attempt + 1,
        max_attempts=max_attempts,
    )
    _append_residual_to_next_conf(next_conf, residual_failed)

    next_target_count = sum(
        len(next_conf.get(key) or [])
        for key in ("failed_account_ids", "failed_stores", "failed_orders", "failed_ads", "failed_stages")
    )
    if next_target_count == 0:
        msg = send_final()
        logger.warning(msg)
        raise AirflowException(msg)

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
        f"ads={len(next_conf.get('failed_ads') or [])}, "
        f"stages={len(next_conf.get('failed_stages') or [])}\n"
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
    max_active_tasks=2 if RETRY_LANES == 2 else 1,
    default_args={
        "retries": 0,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["db", "baemin", "retry"],
) as dag:
    t_ingest_manual = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_files,
        execution_timeout=timedelta(minutes=15),
    )

    t1 = PythonOperator(
        task_id="load_failed_and_accounts",
        python_callable=load_failed_and_accounts,
        execution_timeout=timedelta(minutes=5),
    )

    retry_tasks = [
        PythonOperator(
            task_id=f"retry_collect_{index + 1}",
            python_callable=retry_collect,
            op_kwargs={
                "lane_index": index,
                "lanes": RETRY_LANES,
                "lane_offset": RETRY_LANES == 2 and index == 1,
            },
            pool=BAEMIN_SELENIUM_POOL,
            execution_timeout=timedelta(minutes=_RETRY_COLLECT_BUDGET_MINUTES + 30),
        )
        for index in range(RETRY_LANES)
    ]

    t_merge = PythonOperator(
        task_id="merge_retry_payloads",
        python_callable=merge_retry_payloads,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=5),
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

    t_cleanup_manual = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_files,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ingest_manual >> t1 >> retry_tasks >> t_merge >> t3 >> t4 >> t5 >> t_cleanup_manual
