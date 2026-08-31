"""
Food Guide order history collection DAG.

Default run collects yesterday in KST and archives the downloaded source file
under LOCAL_DB/temp/food_guide_orders. OneDrive-backed analytics storage should
be enabled only after an explicit approval and sample schema confirmation.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.Food_Guide_orders_collect import (
    download_food_guide_orders,
    save_food_guide_orders,
)
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_FOOD_GUIDE_ORDERS_TIME


def _parse_date(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format: {value}") from exc


def resolve_dates(**context) -> str:
    conf = context["dag_run"].conf or {}
    sale_date = (conf.get("sale_date") or "").strip()
    date_from = (conf.get("date_from") or conf.get("sale_date_from") or "").strip()
    date_to = (conf.get("date_to") or conf.get("sale_date_to") or "").strip()
    backfill = bool(conf.get("backfill"))

    if sale_date and (date_from or date_to or backfill):
        raise ValueError("sale_date cannot be used with date_from/date_to or backfill.")
    if backfill and (date_from or date_to):
        raise ValueError("backfill cannot be used with date_from/date_to.")

    if sale_date:
        resolved_from = resolved_to = _parse_date(sale_date, "sale_date").strftime("%Y-%m-%d")
    elif date_from or date_to:
        if not (date_from and date_to):
            raise ValueError("date_from and date_to are both required.")
        start = _parse_date(date_from, "date_from")
        end = _parse_date(date_to, "date_to")
        if start > end:
            raise ValueError(f"date_from({date_from}) is after date_to({date_to}).")
        resolved_from = start.strftime("%Y-%m-%d")
        resolved_to = end.strftime("%Y-%m-%d")
    elif backfill:
        yesterday = pendulum.now("Asia/Seoul").subtract(days=1)
        resolved_from = yesterday.start_of("month").format("YYYY-MM-DD")
        resolved_to = yesterday.format("YYYY-MM-DD")
    else:
        yesterday = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
        resolved_from = resolved_to = yesterday

    context["ti"].xcom_push(key="date_from", value=resolved_from)
    context["ti"].xcom_push(key="date_to", value=resolved_to)
    return f"date_from={resolved_from}, date_to={resolved_to}"


def collect_food_guide_orders(**context) -> str:
    date_from = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_from")
    date_to = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_to")
    if not date_from or not date_to:
        raise ValueError("date_from/date_to XCom values are required.")
    result = download_food_guide_orders(date_from=date_from, date_to=date_to)
    no_data = bool(result.get("no_data"))
    downloaded_path = str(result.get("downloaded_path") or "")
    context["ti"].xcom_push(key="no_data", value=no_data)
    context["ti"].xcom_push(key="downloaded_path", value=downloaded_path)
    if no_data:
        return f"no_data: date_from={date_from}, date_to={date_to}"
    return downloaded_path


def save_food_guide_orders_task(**context) -> str:
    date_from = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_from")
    date_to = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_to")
    downloaded_path = context["ti"].xcom_pull(task_ids="collect_food_guide_orders", key="downloaded_path")
    no_data = bool(context["ti"].xcom_pull(task_ids="collect_food_guide_orders", key="no_data"))
    if no_data:
        context["ti"].xcom_push(key="parquet_files", value=[])
        context["ti"].xcom_push(key="deleted_sources", value=[])
        return f"parquet_files=0, rows=0, no_data=True, date_from={date_from}, date_to={date_to}"
    if not downloaded_path:
        raise ValueError("collect_food_guide_orders XCom(downloaded_path) is empty.")
    result = save_food_guide_orders(
        downloaded_path=downloaded_path,
        date_from=date_from,
        date_to=date_to,
    )
    parquet_files = result.get("parquet_files") or []
    context["ti"].xcom_push(key="parquet_files", value=parquet_files)
    context["ti"].xcom_push(key="deleted_sources", value=result.get("deleted_sources") or [])
    return f"parquet_files={len(parquet_files)}, rows={result.get('rows')}"


with DAG(
    dag_id=Path(__file__).stem,
    description="Food Guide order history Excel source collection",
    schedule=DB_FOOD_GUIDE_ORDERS_TIME,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": on_failure_callback,
    },
    tags=["db", "food_guide", "orders", "selenium", "excel"],
) as dag:
    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_dates,
    )

    t2 = PythonOperator(
        task_id="collect_food_guide_orders",
        python_callable=collect_food_guide_orders,
        pool="selenium_pool",
    )

    t3 = PythonOperator(
        task_id="save_food_guide_orders",
        python_callable=save_food_guide_orders_task,
    )

    t1 >> t2 >> t3
