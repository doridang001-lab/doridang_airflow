"""Today 원천 수집 이후 UnifiedSales 최근 2일 통합 DAG."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State

from modules.transform.pipelines.db.DB_UnifiedSales import (
    ADD_TEST_STORES,
    DELIVERY_MANUAL_TEST_STORES,
    enforce_manual_delivery_sources_for_test_stores as pipeline_enforce_manual_delivery_sources,
    quarantine_conflict_copies as pipeline_quarantine_conflict_copies,
    refresh_store_meta_in_unified_sales as pipeline_refresh_store_meta,
    run_toorder_manual_stores as pipeline_run_toorder,
    upsert_fin_product_grp_from_unionpos as pipeline_upsert_unionpos_products,
)
from modules.transform.pipelines.db.DB_UnifiedSales_easypos import (
    run_easypos as pipeline_run_easypos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_okpos import (
    run_okpos as pipeline_run_okpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import (
    run_posfeed as pipeline_run_posfeed,
)
from modules.transform.pipelines.db.DB_UnifiedSales_unionpos import (
    run_unionpos as pipeline_run_unionpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
    COUPANG_ORDERS_DB,
    reconcile_coupang_for_test_stores,
)
from modules.transform.pipelines.db.DB_UnifiedSales_validate import (
    build_daily_summary as pipeline_build_daily_summary,
)
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIFIED_SALES_TODAY_TIME

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

TODAY_LOOKBACK_DAYS = 2
ALLOWED_SLOTS = {"08:45", "12:45", "14:45", "16:45", "19:15", "21:45"}
BLOCKING_DAG_IDS = (
    "DB_UnifiedSales",
    "DB_UnifiedSales_Total_Macro_Dags",
    "DB_UnifiedSales_Total_Dags",
)
BLOCKING_STATES = ("running", "queued")
SOURCE_TODAY_DAGS = [
    "DB_OKPOS_Sales_Today_Dags",
    "DB_EasyPOS_Sales_Today_Dags",
    "DB_UnionPOS_Receipt_Today_Dags",
    "DB_Posfeed_Sales_Today_Dags",
]
AUTO_ENFORCE_TEST_STORES = [store for store in ADD_TEST_STORES if str(store).strip()]
COUPANG_TODAY_STORES = [store for store in DELIVERY_MANUAL_TEST_STORES if str(store).strip()]


def _on_failure_callback(context):
    ti = context.get("task_instance")
    if not ti:
        return
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {ti.execution_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


default_args = {
    "retries": 12,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_manual_run(context) -> bool:
    dag_run = context.get("dag_run")
    return str(getattr(dag_run, "run_type", "")).lower() == "manual"


def _scheduled_slot(context) -> str:
    dag_run = context.get("dag_run")
    ref = (
        getattr(dag_run, "data_interval_end", None)
        or getattr(dag_run, "logical_date", None)
        or pendulum.now("Asia/Seoul")
    )
    return pendulum.instance(ref).in_timezone("Asia/Seoul").format("HH:mm")


def _assert_allowed_slot(context) -> None:
    if _is_manual_run(context):
        return
    slot = _scheduled_slot(context)
    if slot not in ALLOWED_SLOTS:
        raise AirflowSkipException(f"Today 통합 허용 슬롯 아님: {slot}")


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
    raise AirflowException(
        "UnifiedSales 정규/Total 실행 중이어서 Today 통합을 대기합니다. "
        f"active_runs={active}"
    )


def _source_slot_start_utc(context) -> pendulum.DateTime:
    dag_run = context.get("dag_run")
    now_kst = pendulum.now("Asia/Seoul")
    fallback = now_kst.replace(minute=0, second=0, microsecond=0)

    if dag_run is None:
        logger.warning("dag_run이 없어 fallback 슬롯 기준 사용: %s", fallback)
        return fallback.in_timezone("UTC")

    candidate_attrs = ("data_interval_start", "logical_date", "start_date")
    ref = None
    used_attr = ""
    for attr in candidate_attrs:
        value = getattr(dag_run, attr, None)
        if value:
            ref = value
            used_attr = attr
            break

    if ref is None:
        logger.warning("dag_run 슬롯 산정값이 없어 fallback 슬롯 기준 사용")
        return fallback.in_timezone("UTC")

    try:
        slot_start_kst = pendulum.instance(ref).in_timezone("Asia/Seoul").replace(
            minute=0,
            second=0,
            microsecond=0,
        )
    except Exception:
        logger.exception("슬롯 산정 실패(source=%s), fallback 사용", used_attr)
        return fallback.in_timezone("UTC")

    if slot_start_kst > now_kst or (now_kst - slot_start_kst).in_days() > 2:
        logger.warning("슬롯 기준 시각 비정상: %s, fallback 사용", slot_start_kst)
        return fallback.in_timezone("UTC")

    return slot_start_kst.in_timezone("UTC")


def _assert_source_runs_ready(context) -> None:
    min_end_date = _source_slot_start_utc(context)
    not_ready = []
    ready = []
    with create_session() as session:
        for source_dag_id in SOURCE_TODAY_DAGS:
            latest_any = (
                session.query(DagRun)
                .filter(DagRun.dag_id == source_dag_id)
                .order_by(DagRun.start_date.desc())
                .first()
            )
            latest_success = (
                session.query(DagRun)
                .filter(DagRun.dag_id == source_dag_id)
                .filter(DagRun.state == State.SUCCESS)
                .filter(DagRun.end_date.isnot(None))
                .order_by(DagRun.end_date.desc())
                .first()
            )
            if latest_success is None:
                not_ready.append(
                    f"{source_dag_id}: latest={getattr(latest_any, 'run_id', None)} "
                    f"state={getattr(latest_any, 'state', None)}"
                )
            elif latest_success.end_date >= min_end_date:
                ready.append(source_dag_id)
                logger.info(
                    "Today source ready: %s | run_id=%s | end_date=%s | slot_start=%s",
                    source_dag_id,
                    latest_success.run_id,
                    latest_success.end_date,
                    min_end_date,
                )
            else:
                not_ready.append(
                    f"{source_dag_id}: latest_success_end={latest_success.end_date} < {min_end_date}"
                )

    if not_ready:
        raise AirflowException(
            "Today 원천 수집 DAG 완료 대기 중: "
            + ", ".join(not_ready)
            + f" | 완료된 DAG={(', '.join(ready) if ready else '(none)')}"
        )


def wait_for_safe_window(**context) -> str:
    _assert_allowed_slot(context)
    _assert_no_blocking_runs()
    if not _truthy(_conf(context).get("skip_source_wait")):
        _assert_source_runs_ready(context)
    return "Today 통합 실행 조건 확인 완료"


def resolve_dates(**context) -> str:
    conf = _conf(context)
    sale_date = str(conf.get("sale_date") or "").strip()
    if sale_date:
        dates = [pendulum.parse(sale_date, strict=False).format("YYYY-MM-DD")]
    else:
        today = pendulum.now("Asia/Seoul")
        dates = [today.subtract(days=i).format("YYYY-MM-DD") for i in range(TODAY_LOOKBACK_DAYS)]

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="target_dates", value=dates)
    return f"Today lookback={TODAY_LOOKBACK_DAYS} 대상 날짜: {dates}"


def _target_dates(context) -> list[str]:
    dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="target_dates") or []
    return [str(date).strip() for date in dates if str(date).strip()]


def _run_for_dates(context, label: str, func) -> str:
    results = []
    for sale_date in _target_dates(context):
        results.append(f"{sale_date}: {func(sale_date)}")
    return f"{label} 완료 | " + " | ".join(results)


def quarantine_conflicts(**context) -> str:
    return pipeline_quarantine_conflict_copies()


def build_okpos(**context) -> str:
    return _run_for_dates(
        context,
        "OKPOS Today",
        lambda sale_date: pipeline_run_okpos(sale_date, overwrite=True),
    )


def sync_unionpos_products(**context) -> str:
    yms = sorted({date[:7] for date in _target_dates(context)})
    results = [pipeline_upsert_unionpos_products(ym=ym, dry_run=False) for ym in yms]
    return "UnionPOS 상품 마스터 Today 선반영 완료 | " + " | ".join(results)


def build_unionpos(**context) -> str:
    return _run_for_dates(
        context,
        "UnionPOS Today",
        lambda sale_date: pipeline_run_unionpos(sale_date, overwrite=True),
    )


def build_easypos(**context) -> str:
    return _run_for_dates(
        context,
        "EasyPOS Today",
        lambda sale_date: pipeline_run_easypos(sale_date, overwrite=True),
    )


def build_posfeed(**context) -> str:
    return _run_for_dates(
        context,
        "Posfeed Today",
        lambda sale_date: pipeline_run_posfeed(
            sale_date,
            overwrite=True,
            persist_item_ids=False,
        ),
    )


def build_toorder(**context) -> str:
    return _run_for_dates(
        context,
        "ToOrder Today",
        lambda sale_date: pipeline_run_toorder(sale_date, overwrite=True),
    )


def _has_coupang_source_for_date(store: str, sale_date: str) -> bool:
    ym = sale_date[:7]
    date_dot = sale_date.replace("-", ".")
    for path in COUPANG_ORDERS_DB.glob(f"brand=*/store={store}/ym={ym}/orders_{ym}.parquet"):
        try:
            df = pd.read_parquet(path, columns=["order_date"])
        except Exception as exc:
            logger.warning("쿠팡 Today 원천 확인 실패, 스킵: %s | %s", path, exc)
            continue
        order_date = df["order_date"].fillna("").astype(str).str.strip()
        if order_date.str.startswith(sale_date).any() or order_date.str.startswith(date_dot).any():
            return True
    return False


def reconcile_coupang_today(**context) -> str:
    results = []
    for sale_date in _target_dates(context):
        stores = [store for store in COUPANG_TODAY_STORES if _has_coupang_source_for_date(store, sale_date)]
        if not stores:
            results.append(f"{sale_date}: 쿠팡수동 원천 없음 - 기존 데이터 보존")
            continue
        results.append(
            f"{sale_date}: "
            + reconcile_coupang_for_test_stores(
                stores=stores,
                sale_date=sale_date,
                lookback_days=0,
            )
        )
    return "쿠팡수동 Today 완료 | " + " | ".join(results)


def enforce_manual_delivery_sources(**context) -> str:
    if AUTO_ENFORCE_TEST_STORES:
        return pipeline_enforce_manual_delivery_sources(stores=AUTO_ENFORCE_TEST_STORES)
    return "Today 수동 배달 source enforce 대상 없음"


def refresh_store_meta(**context) -> str:
    return pipeline_refresh_store_meta()


def build_daily_summary(**context) -> str:
    return pipeline_build_daily_summary()


def send_success_alert(**context) -> str:
    dates = _target_dates(context)
    completed_at = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", "")
    body = (
        "[도리당] Today UnifiedSales 완료\n"
        f"DAG: {dag_id}\n"
        f"target_dates: {', '.join(dates) if dates else '-'}\n"
        f"run_id: {run_id}\n"
        f"완료시각: {completed_at} KST"
    )
    send_telegram(body)
    return f"Today UnifiedSales 성공 알림 발송: {dates or '-'}"


with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_SALES_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "unified_sales", "today"],
) as dag:
    t_wait = PythonOperator(task_id="wait_for_safe_window", python_callable=wait_for_safe_window)
    t_resolve = PythonOperator(task_id="resolve_dates", python_callable=resolve_dates)
    t_quarantine = PythonOperator(task_id="quarantine_conflicts", python_callable=quarantine_conflicts)
    t_okpos = PythonOperator(task_id="build_okpos", python_callable=build_okpos)
    t_union_products = PythonOperator(task_id="sync_unionpos_products", python_callable=sync_unionpos_products)
    t_unionpos = PythonOperator(task_id="build_unionpos", python_callable=build_unionpos)
    t_easypos = PythonOperator(task_id="build_easypos", python_callable=build_easypos)
    t_posfeed = PythonOperator(task_id="build_posfeed", python_callable=build_posfeed)
    t_toorder = PythonOperator(task_id="build_toorder", python_callable=build_toorder)
    t_coupang = PythonOperator(task_id="reconcile_coupang_today", python_callable=reconcile_coupang_today)
    t_enforce = PythonOperator(
        task_id="enforce_manual_delivery_sources",
        python_callable=enforce_manual_delivery_sources,
    )
    t_meta = PythonOperator(task_id="refresh_store_meta", python_callable=refresh_store_meta)
    t_summary = PythonOperator(task_id="build_daily_summary", python_callable=build_daily_summary)
    t_alert = PythonOperator(task_id="send_success_alert", python_callable=send_success_alert)

    (
        t_wait
        >> t_resolve
        >> t_quarantine
        >> t_okpos
        >> t_union_products
        >> t_unionpos
        >> t_easypos
        >> t_posfeed
        >> t_toorder
        >> t_coupang
        >> t_enforce
        >> t_meta
        >> t_summary
        >> t_alert
    )
