"""UnifiedSales 과거 수동배달 재수집 마커 처리 DAG."""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.pipelines.db.DB_UnifiedSales import DELIVERY_MANUAL_TEST_STORES
from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (
    reconcile_baemin_for_test_stores,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    filter_manual_reingest_dates_outside_recent_window,
    list_manual_reingest_dates,
)
from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
    reconcile_coupang_for_test_stores,
)
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

BAEMIN_SOURCE = "배민수동"
COUPANG_SOURCE = "쿠팡수동"
DEFAULT_SOURCES = ("baemin", "coupang")
RECENT_PROTECT_DAYS = 9
BLOCKING_DAG_IDS = (
    "DB_UnifiedSales",
    "DB_UnifiedSales_Today_Dags",
)
BLOCKING_STATES = ("running", "queued")


def _on_failure_callback(context):
    ti = context.get("task_instance")
    if not ti:
        return
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {ti.log_url}"
    )
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(part).strip() for part in value if str(part).strip()]
    raise ValueError("문자열 또는 리스트 형식만 지원합니다.")


def _selected_sources(context) -> set[str]:
    raw = _conf(context).get("sources", _conf(context).get("source"))
    requested = _as_list(raw) or list(DEFAULT_SOURCES)
    selected: set[str] = set()
    for item in requested:
        normalized = item.strip().lower()
        if normalized in {"baemin", "배민", "배민수동"}:
            selected.add("baemin")
        elif normalized in {"coupang", "쿠팡", "쿠팡수동", "coupangeats"}:
            selected.add("coupang")
        elif normalized in {"both", "all", "전체"}:
            selected.update(DEFAULT_SOURCES)
        else:
            raise ValueError(f"지원하지 않는 source: {item}")
    return selected


def _target_stores(context) -> list[str]:
    stores = _as_list(_conf(context).get("stores"))
    if stores:
        return stores
    return [store for store in DELIVERY_MANUAL_TEST_STORES if str(store).strip()]


def _sale_date(context) -> str | None:
    value = _conf(context).get("sale_date")
    if value is None:
        return None
    sale_date = str(value).strip()
    if not sale_date:
        return None
    return pendulum.parse(sale_date, strict=False).format("YYYY-MM-DD")


def _recent_cutoff_date() -> str:
    return pendulum.now("Asia/Seoul").subtract(days=RECENT_PROTECT_DAYS).format("YYYY-MM-DD")


def _marker_dates(source: str, store: str) -> list[str]:
    return filter_manual_reingest_dates_outside_recent_window(
        list_manual_reingest_dates(source, [store]),
        recent_days=RECENT_PROTECT_DAYS,
    )


def _count_marker_dates(source: str, stores: list[str]) -> int:
    return sum(len(_marker_dates(source, store)) for store in stores)


def _assert_no_blocking_runs() -> None:
    with create_session() as session:
        rows = (
            session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
            .filter(DagRun.dag_id.in_(BLOCKING_DAG_IDS))
            .filter(DagRun.state.in_(BLOCKING_STATES))
            .order_by(DagRun.execution_date.desc())
            .all()
        )
    if not rows:
        return
    active = ", ".join(f"{dag_id}:{run_id}:{state}" for dag_id, run_id, state in rows)
    raise AirflowSkipException(
        "DB_UnifiedSales 또는 DB_UnifiedSales_Today_Dags 실행 중이어서 "
        f"Total Macro 재처리를 건너뜁니다. active_runs={active}"
    )


def resolve_options(**context) -> str:
    _assert_no_blocking_runs()
    stores = _target_stores(context)
    sources = sorted(_selected_sources(context))
    sale_date = _sale_date(context)
    baemin_markers = _count_marker_dates(BAEMIN_SOURCE, stores)
    coupang_markers = _count_marker_dates(COUPANG_SOURCE, stores)
    return (
        f"sources={sources} stores={len(stores)} sale_date={sale_date or '-'} "
        f"최근{RECENT_PROTECT_DAYS}일제외 cutoff<={_recent_cutoff_date()} "
        f"markers=배민{baemin_markers}/쿠팡{coupang_markers}"
    )


def _run_marker_dates(
    source_key: str,
    source_name: str,
    reconcile_func,
    context,
) -> str:
    if source_key not in _selected_sources(context):
        return f"{source_name} Total Macro 스킵: source 미선택"

    stores = _target_stores(context)
    sale_date = _sale_date(context)
    if sale_date:
        return reconcile_func(stores, sale_date=sale_date)

    results = []
    processed = 0
    for store in stores:
        for marker_date in _marker_dates(source_name, store):
            processed += 1
            results.append(reconcile_func([store], sale_date=marker_date))

    if processed == 0:
        return (
            f"{source_name} Total Macro 스킵 | 최근{RECENT_PROTECT_DAYS}일 제외 후 "
            "처리할 재수집 마커 없음"
        )
    return f"{source_name} Total Macro 완료 | marker_dates={processed} | " + " | ".join(results)


def run_baemin_total(**context) -> str:
    return _run_marker_dates("baemin", BAEMIN_SOURCE, reconcile_baemin_for_test_stores, context)


def run_coupang_total(**context) -> str:
    return _run_marker_dates("coupang", COUPANG_SOURCE, reconcile_coupang_for_test_stores, context)


with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unified_sales", "manual_delivery", "total_macro"],
) as dag:
    t_resolve = PythonOperator(task_id="resolve_options", python_callable=resolve_options)
    t_baemin = PythonOperator(
        task_id="reconcile_baemin_total_macro",
        python_callable=run_baemin_total,
        execution_timeout=timedelta(minutes=600),
    )
    t_coupang = PythonOperator(
        task_id="reconcile_coupang_total_macro",
        python_callable=run_coupang_total,
        execution_timeout=timedelta(minutes=600),
    )

    t_resolve >> t_baemin >> t_coupang
