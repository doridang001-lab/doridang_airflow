"""
ToOrder datedetail monthly recollection DAG.

- Default: recollect previous month start through yesterday in KST by month spans.
- conf ``month``: collect that month only, for example {"month": "2025-07"}.
- conf ``sale_date_from`` and ``sale_date_to``: collect the inclusive range.
- conf ``backfill``: recollect 2025-07-01 through yesterday in KST.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.DB_Toorder_store_platform_daily import (
    cleanup_datedetail_xlsx,
    run_toorder_store_platform_daily,
)
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.schedule import DB_TOORDER_STORE_PLATFORM_TIME
from modules.transform.utility.notifier import on_failure_callback

PARQUET_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
DEFAULT_DATE_FROM = "2025-07-01"


def resolve_dates(**context) -> str:
    conf = context["dag_run"].conf or {}
    month = conf.get("month")
    sale_date_from = conf.get("sale_date_from")
    sale_date_to = conf.get("sale_date_to")
    backfill = conf.get("backfill")

    if month and (sale_date_from or sale_date_to):
        raise ValueError("month and sale_date_from/sale_date_to cannot be used together.")
    if backfill and (month or sale_date_from or sale_date_to):
        raise ValueError("backfill cannot be used with month or sale_date_from/sale_date_to.")

    if month:
        start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
        next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month - timedelta(days=1)
        date_from = start.strftime("%Y-%m-%d")
        date_to = end.strftime("%Y-%m-%d")
    elif sale_date_from or sale_date_to:
        if not (sale_date_from and sale_date_to):
            raise ValueError("sale_date_from and sale_date_to are both required.")
        start = datetime.strptime(sale_date_from, "%Y-%m-%d")
        end = datetime.strptime(sale_date_to, "%Y-%m-%d")
        if start > end:
            raise ValueError(f"sale_date_from({sale_date_from}) is after sale_date_to({sale_date_to}).")
        date_from = sale_date_from
        date_to = sale_date_to
    elif backfill:
        date_from = DEFAULT_DATE_FROM
        date_to = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
    else:
        today = pendulum.now("Asia/Seoul")
        date_from = today.start_of("month").subtract(months=1).format("YYYY-MM-DD")
        date_to = today.subtract(days=1).format("YYYY-MM-DD")

    context["ti"].xcom_push(key="date_from", value=date_from)
    context["ti"].xcom_push(key="date_to", value=date_to)
    return f"date_from={date_from}, date_to={date_to}, parquet={PARQUET_PATH}"


def collect_and_save(**context) -> str:
    date_from = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_from")
    date_to = context["ti"].xcom_pull(task_ids="resolve_dates", key="date_to")
    if not date_from or not date_to:
        raise ValueError("date_from/date_to XCom values are required.")
    return run_toorder_store_platform_daily(
        date_from=date_from,
        date_to=date_to,
        log_prefix="[1st] ",
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder datedetail monthly recollection to Parquet",
    schedule=DB_TOORDER_STORE_PLATFORM_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "toorder", "platform", "daily", "parquet"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
) as dag:
    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_dates,
    )

    t2 = PythonOperator(
        task_id="collect_and_save",
        python_callable=collect_and_save,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_datedetail_xlsx",
        python_callable=cleanup_datedetail_xlsx,
    )

    t1 >> t2 >> cleanup_task
