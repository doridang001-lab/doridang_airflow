"""Baemin macro PC2 collection DAG.

PC2 writes collected baemin_macro files to a Collect_Data inbox only. The main
PC ingests that inbox into analytics to avoid OneDrive analytics conflicts.
"""

import importlib
import json
import logging
import os
import random
import re
import shutil
import hashlib
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
)
from modules.transform.pipelines.db.beamin_stability import resolve_stability_profile
from modules.transform.utility.notifier import send_telegram
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

_MISSING = object()
_BAEMIN_STAGE_ATTRS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "modules.transform.pipelines.db.DB_Beamin_01_now",
        "BAEMIN_METRICS_DB",
        ("metrics_now",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_02_woori_shop_click",
        "BAEMIN_OUR_STORE_CLICKS_DB",
        ("metrics_our_store_clicks",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_03_shop_change",
        "BAEMIN_SHOP_CHANGE_DB",
        ("shop_change",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_03_shop_change",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_04_orders",
        "BAEMIN_ORDERS_DB",
        ("orders",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_05_ad_funnel",
        "BAEMIN_AD_FUNNEL_DB",
        ("ad_funnel",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_MONTHLY_OPERATION_DB",
        ("monthly_operation",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_SHOP_CHANGE_DB",
        ("shop_change",),
    ),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
)


def _split_accounts_by_range(accounts: list[dict], collect_range: str | None) -> list[dict]:
    if not collect_range or not accounts:
        return accounts

    ordered = sorted(accounts, key=lambda a: str(a.get("store_name", "")))
    mid = len(ordered) // 2
    if collect_range == "상위":
        selected = ordered[:mid]
    elif collect_range == "하위":
        selected = ordered[mid:]
    else:
        logger.warning("알 수 없는 COLLECT_RANGE=%r", collect_range)
        return accounts

    logger.info(
        "매장 분할 [%s]: 전체 %d개 -> %d개 (%s ~ %s)",
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
    import modules.transform.utility.paths as _paths

    analytics_original = os.environ.get("ANALYTICS_DB")
    os.environ["ANALYTICS_DB"] = str(local_analytics)
    _paths._cache.clear()

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
    import modules.transform.utility.paths as _paths

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
    _paths._cache.clear()
    logger.info("PC2 staging 모드 해제")


def _safe_run_id_part(value: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def _flat_export_name(run_part: str, rel: str) -> str:
    dataset = rel.split("/", 1)[0]
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:12]
    basename = re.sub(r"[^A-Za-z0-9_.=-]+", "_", Path(rel).name).strip("_")
    dataset = re.sub(r"[^A-Za-z0-9_.=-]+", "_", dataset).strip("_")
    return f"baemin_pc2__{run_part}__{dataset}__{digest}__{basename}"


def _export_staging_to_inbox(local_baemin: Path, run_id: str | None) -> Path:
    if not local_baemin.exists():
        raise RuntimeError(f"staging 경로 없음: {local_baemin}")

    inbox = COLLECT_DB
    inbox.mkdir(parents=True, exist_ok=True)
    ts = pendulum.now(KST).format("YYYYMMDD_HHmmss")
    run_part = f"{_safe_run_id_part(run_id)}_{ts}"
    tmp_manifest = inbox / f"_tmp_baemin_pc2_manifest__{run_part}.json"
    final_manifest = inbox / f"baemin_pc2_manifest__{run_part}.json"

    manifest: dict[str, dict[str, str | int]] = {}
    for p in local_baemin.rglob("*"):
        if p.is_file():
            rel = Path("baemin_macro") / p.relative_to(local_baemin)
            rel_text = str(rel).replace("\\", "/")
            final_name = _flat_export_name(run_part, rel_text)
            tmp_name = f"_tmp_{final_name}"
            tmp_path = inbox / tmp_name
            final_path = inbox / final_name
            if tmp_path.exists():
                tmp_path.unlink()
            shutil.copy2(p, tmp_path)
            tmp_path.rename(final_path)
            manifest[final_name] = {
                "rel": rel_text,
                "size": final_path.stat().st_size,
            }

    if tmp_manifest.exists():
        tmp_manifest.unlink()
    tmp_manifest.write_text(
        json.dumps(
            {"run": run_part, "files": manifest},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    tmp_manifest.rename(final_manifest)
    logger.info("PC2 flat inbox export 완료: %s (%d files)", final_manifest, len(manifest))
    return final_manifest


def collect_all(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    profile = resolve_stability_profile(conf.get("stability_profile"))
    wait_sec = random.uniform(*profile["initial_stagger_range"])
    logger.info("PC2 수집 시작 전 계정 지터 대기 %.0f초", wait_sec)
    time.sleep(wait_sec)

    target_date = conf.get("target_date")
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("PC2 수집 대상 계정 없음")
        return "PC2 수집 대상 없음"

    local_analytics = LOCAL_DB / "analytics_stage_pc2"
    local_baemin = local_analytics / "baemin_macro"
    _init_empty_staging(local_baemin)
    analytics_original, patched_paths = _patch_baemin_staging_paths(local_analytics)

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
        final_run = _export_staging_to_inbox(
            local_baemin,
            getattr(dag_run, "run_id", None) if dag_run else None,
        )
        return f"{result.get('summary', 'PC2 수집 완료')}\n[inbox] {final_run}"
    finally:
        _restore_baemin_staging_paths(analytics_original, patched_paths)


def notify_collection_result(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "manual") if dag_run else "manual"
    collect_summary = ti.xcom_pull(task_ids="collect_all", key="return_value") or ""
    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}
    failed_count = sum(len(failed.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))
    status = "부분성공" if failed_count else "성공"
    body = "\n".join(
        [
            f"[배민 PC2 수집 결과] {status}",
            f"DAG: {dag_id}",
            f"Run: {run_id}",
            f"실행시각(KST): {pendulum.now(KST).format('YYYY-MM-DD HH:mm:ss')}",
            "",
            str(collect_summary),
            f"실패 대상 수: {failed_count}",
        ]
    )[:4000]
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    return f"[배민 PC2 수집 결과] {status}"


def _on_failure_callback(context):
    ti = context.get("task_instance")
    exception = context.get("exception", "알 수 없음")
    body = (
        f"DAG: {getattr(ti, 'dag_id', dag_id)}\n"
        f"Task: {getattr(ti, 'task_id', '?')}\n"
        f"에러: {exception}\n"
        f"로그: {getattr(ti, 'log_url', '?')}"
    )
    send_telegram(body + "\n해결해라")


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
    schedule=BAEMIN_SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
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
        execution_timeout=timedelta(minutes=300),
    )

    t3 = PythonOperator(
        task_id="notify_collection_result",
        python_callable=notify_collection_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3
