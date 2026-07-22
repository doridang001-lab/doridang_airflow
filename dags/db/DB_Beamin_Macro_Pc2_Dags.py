"""Baemin macro PC2 collection DAG.

PC2 writes collected baemin_macro files to a Collect_Data inbox only. The main
PC ingests that inbox into analytics to avoid OneDrive analytics conflicts.
"""

from __future__ import annotations

import importlib
import logging
import os
import random
import re
import shutil
import time
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori as pipeline_collect_all,
    retry_once_failed as pipeline_retry_failed,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES
from modules.transform.pipelines.db.beamin_staging import (
    export_staging_to_inbox,
    init_empty_staging,
    local_stage_paths,
    patch_baemin_staging_paths,
    restore_baemin_staging_paths,
)
from modules.transform.pipelines.db.beamin_stability import resolve_stability_profile
from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB
from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_TIME

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
BAEMIN_SCHEDULE = os.getenv("BAEMIN_PC2_COLLECT_SCHEDULE", SMD_BAEMIN_COLLECT_TIME)

warnings.filterwarnings(
    "ignore",
    message="This process .* is multi-threaded, use of fork\\(\\) may lead to deadlocks in the child\\.",
    category=DeprecationWarning,
)

KST = pendulum.timezone("Asia/Seoul")
TARGET_STORES = []
COLLECT_RANGE: str | None = "하위"
PC2_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"

_MISSING = object()
_BAEMIN_STAGE_ATTRS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("modules.transform.pipelines.db.DB_Beamin_01_now", "BAEMIN_METRICS_DB", ("metrics_now",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_02_woori_shop_click",
        "BAEMIN_OUR_STORE_CLICKS_DB",
        ("metrics_our_store_clicks",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_03_shop_change", "BAEMIN_SHOP_CHANGE_DB", ("shop_change",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_03_shop_change",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_04_orders", "BAEMIN_ORDERS_DB", ("orders",)),
    ("modules.transform.pipelines.db.DB_Beamin_05_ad_funnel", "BAEMIN_AD_FUNNEL_DB", ("ad_funnel",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_MONTHLY_OPERATION_DB",
        ("monthly_operation",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_monthly_operation", "BAEMIN_SHOP_CHANGE_DB", ("shop_change",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
)


def _split_accounts_by_range(accounts: list[dict], collect_range: str | None) -> list[dict]:
    if not accounts:
        return accounts

    ordered = sorted(accounts, key=lambda a: str(a.get("store_name", "")))
    if collect_range == "상위":
        mid = len(ordered) // 2
        selected = ordered[:mid]
    elif collect_range == "하위":
        mid = len(ordered) // 2
        selected = ordered[mid:]
    else:
        if collect_range is not None:
            logger.warning("알 수 없는 COLLECT_RANGE=%r", collect_range)
        selected = ordered

    selected = sorted(
        selected,
        key=lambda account: account.get("store_name") not in DELIVERY_MANUAL_TEST_STORES,
    )

    if collect_range in {"상위", "하위"}:
        logger.info(
            "PC2 매장 분할 [%s]: 전체 %d개 -> %d개 (%s ~ %s)",
            collect_range,
            len(ordered),
            len(selected),
            selected[0]["store_name"] if selected else "-",
            selected[-1]["store_name"] if selected else "-",
        )
    return selected


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")
    target = stores_override if stores_override else TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)

    if not stores_override:
        collect_range = conf.get("collect_range", COLLECT_RANGE)
        accounts = _split_accounts_by_range(accounts, collect_range)

    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("PC2 계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"PC2 계정 {len(accounts)}개, {stores}"


def _init_empty_staging(local_baemin: Path) -> None:
    local_root = LOCAL_DB.resolve()
    target = local_baemin.resolve()
    if local_baemin.exists():
        if not target.is_relative_to(local_root):
            raise RuntimeError(f"staging 삭제 범위 오류: {local_baemin}")
        shutil.rmtree(local_baemin)
    local_baemin.mkdir(parents=True, exist_ok=True)
    logger.info("PC2 빈 staging 생성: %s", local_baemin)


def _patch_baemin_staging_paths(local_analytics: Path) -> tuple[str | None, list[tuple[Any, str, Any]]]:
    analytics_original = os.environ.get("ANALYTICS_DB")
    os.environ["ANALYTICS_DB"] = str(local_analytics)

    local_baemin = local_analytics / "baemin_macro"
    originals: list[tuple[Any, str, Any]] = []
    for module_name, attr_name, relative_parts in _BAEMIN_STAGE_ATTRS:
        module = importlib.import_module(module_name)
        originals.append((module, attr_name, getattr(module, attr_name, _MISSING)))
        setattr(module, attr_name, local_baemin.joinpath(*relative_parts))

    logger.info("PC2 staging 모드 전환: %s", local_analytics)
    return analytics_original, originals


def _restore_baemin_staging_paths(
    analytics_original: str | None,
    originals: list[tuple[Any, str, Any]],
) -> None:
    for module, attr_name, original_value in reversed(originals):
        if original_value is _MISSING:
            try:
                delattr(module, attr_name)
            except AttributeError:
                pass
        else:
            setattr(module, attr_name, original_value)

    if analytics_original is None:
        os.environ.pop("ANALYTICS_DB", None)
    else:
        os.environ["ANALYTICS_DB"] = analytics_original
    logger.info("PC2 staging 모드 해제")


def _safe_run_id_part(value: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def _local_stage_paths() -> tuple[Path, Path]:
    local_analytics = LOCAL_DB / "analytics_stage_pc2"
    return local_analytics, local_analytics / "baemin_macro"


def _export_staging_to_inbox(local_baemin: Path, run_id: str | None) -> Path:
    if not local_baemin.exists():
        raise RuntimeError(f"staging 경로 없음: {local_baemin}")

    run_part = _safe_run_id_part(run_id)
    inbox_run = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox" / f"manual__{run_part}"
    tmp_run = inbox_run.with_name(f"_tmp__{inbox_run.name}")
    if tmp_run.exists():
        shutil.rmtree(tmp_run)
    if inbox_run.exists():
        shutil.rmtree(inbox_run)

    dst_root = tmp_run / "baemin_macro"
    file_count = 0
    for src in local_baemin.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(local_baemin)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        file_count += 1

    tmp_run.mkdir(parents=True, exist_ok=True)
    tmp_run.rename(inbox_run)
    logger.info("PC2 inbox export 완료: %s (%d files)", inbox_run, file_count)
    return inbox_run


def collect_all(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    profile = resolve_stability_profile(conf.get("stability_profile") or "safe_memory")
    wait_sec = random.uniform(*profile["initial_stagger_range"])
    logger.info("PC2 수집 시작 전 계정 지터 대기 %.0f초", wait_sec)
    time.sleep(wait_sec)

    target_date = conf.get("target_date")
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("PC2 수집 대상 계정 없음")
        return "PC2 수집 대상 없음"

    local_analytics, local_baemin = local_stage_paths("analytics_stage_pc2")
    init_empty_staging(local_baemin)
    analytics_original, patched_paths = patch_baemin_staging_paths(local_analytics)

    try:
        result = pipeline_collect_all(
            account_list,
            target_date=target_date,
            stability_profile=profile["name"],
        )
        context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
        context["ti"].xcom_push(key="validation", value=result.get("validation", []))
        context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
        context["ti"].xcom_push(
            key="store_info_per_account",
            value=result.get("store_info_per_account", []),
        )
        final_run = export_staging_to_inbox(
            local_baemin,
            getattr(dag_run, "run_id", None) if dag_run else None,
            inbox_dir=PC2_INBOX_DIR,
            replace_existing=True,
        )
        return f"{result.get('summary', 'PC2 수집 완료')}\n[inbox] {final_run}"
    finally:
        restore_baemin_staging_paths(analytics_original, patched_paths)


def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or {}
    failed_count = _count_failed_items(failed)
    if failed_count == 0:
        context["ti"].xcom_push(key="residual_failed", value={"accounts": [], "stores": [], "orders": [], "ads": []})
        return "PC2 retry skipped: no failed items"

    target_date = conf.get("target_date")
    local_analytics, local_baemin = local_stage_paths("analytics_stage_pc2")
    analytics_original, patched_paths = patch_baemin_staging_paths(local_analytics)
    try:
        result = pipeline_retry_failed(failed, target_date=target_date)
        final_run = export_staging_to_inbox(
            local_baemin,
            getattr(dag_run, "run_id", None) if dag_run else None,
            inbox_dir=PC2_INBOX_DIR,
            replace_existing=True,
        )
        summary = result.get("summary") if isinstance(result, dict) else str(result)
        residual_failed = (
            result.get("residual_failed")
            if isinstance(result, dict)
            else {"accounts": [], "stores": [], "orders": [], "ads": []}
        )
        context["ti"].xcom_push(key="residual_failed", value=residual_failed or {"accounts": [], "stores": [], "orders": [], "ads": []})
        return f"[retry] {summary}\n[inbox] {final_run}"
    finally:
        restore_baemin_staging_paths(analytics_original, patched_paths)


def _count_failed_items(failed: dict | None) -> int:
    if not failed:
        return 0
    return sum(len(failed.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))


def notify_collection_result(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "manual") if dag_run else "manual"
    collect_summary = ti.xcom_pull(task_ids="collect_all", key="return_value") or ""
    retry_summary = ti.xcom_pull(task_ids="retry_failed", key="return_value") or ""
    original_failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}
    residual_failed = ti.xcom_pull(task_ids="retry_failed", key="residual_failed")
    final_failed = residual_failed if residual_failed is not None else original_failed
    original_failed_count = _count_failed_items(original_failed)
    failed_count = _count_failed_items(final_failed)
    collect_ti = dag_run.get_task_instance("collect_all") if dag_run else None
    collect_state = getattr(collect_ti, "state", None)
    status = "부분성공" if failed_count else "성공"
    if collect_state in {"failed", "upstream_failed"}:
        status = "실패"

    body = "\n".join(
        [
            f"[배민 PC2 수집 결과] {status}",
            f"DAG: {dag_id}",
            f"Run: {run_id}",
            f"실행시각(KST): {pendulum.now(KST).format('YYYY-MM-DD HH:mm:ss')}",
            "",
            str(collect_summary) or f"collect_all summary 없음 (state={collect_state})",
            f"원본 실패 건수: {original_failed_count}",
            f"최종 잔여 실패 건수: {failed_count}",
            "",
            str(retry_summary),
        ]
    )[:4000]
    logger.info(body)
    return f"[배민 PC2 수집 결과 기록] {status}"


def finalize_run_state(**context) -> str:
    dag_run = context.get("dag_run")
    if not dag_run:
        return "no dag_run"

    failed_tasks = [
        ti.task_id
        for ti in dag_run.get_task_instances()
        if ti.task_id != "finalize_run_state" and ti.state in {"failed", "upstream_failed"}
    ]
    if failed_tasks:
        raise RuntimeError(f"PC2 failed tasks: {failed_tasks}")
    return "PC2 DAG 완료"


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


with DAG(
    dag_id=dag_id,
    schedule=BAEMIN_SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["db", "baemin", "crawl", "pc2"],
) as dag:
    t1 = PythonOperator(
        task_id="load_accounts",
        python_callable=load_accounts,
    )

    t2 = PythonOperator(
        task_id="collect_all",
        python_callable=collect_all,
        pool="selenium_pool",
        execution_timeout=timedelta(minutes=300),
    )

    t3 = PythonOperator(
        task_id="retry_failed",
        python_callable=retry_failed,
        pool="selenium_pool",
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120),
    )

    t4 = PythonOperator(
        task_id="notify_collection_result",
        python_callable=notify_collection_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t5 = PythonOperator(
        task_id="finalize_run_state",
        python_callable=finalize_run_state,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4 >> t5
