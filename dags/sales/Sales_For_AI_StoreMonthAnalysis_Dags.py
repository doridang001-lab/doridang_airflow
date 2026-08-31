"""For_AI 매장·월 압축 분석 JSON 생성 DAG."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from modules.transform.pipelines.sales.For_AI_store_month_analysis import (
    build_for_ai_store_month_analysis,
)
from modules.transform.utility.notifier import on_failure_callback

SMD_FOR_AI_STORE_MONTH_TIME = "40 9 * * *"
LOOKBACK = None  # None이면 저장된 주문건 전체, 3이면 저장된 주문건 기준 최근 3개월


def run_for_ai_store_month_analysis() -> str:
    context = get_current_context()
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    lookback = conf.get("lookback", LOOKBACK)
    return build_for_ai_store_month_analysis(
        brand=conf.get("brand"),
        ym=conf.get("ym"),
        store=conf.get("store"),
        lookback=_normalize_lookback(lookback),
        target_source="orders",
        write=True,
    )


def _normalize_lookback(value):
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() in {"none", "null"}:
        return None
    return int(value)


with DAG(
    dag_id=Path(__file__).stem,
    description="For_AI 매장·월 압축 분석 JSON 생성",
    schedule=SMD_FOR_AI_STORE_MONTH_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
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
    tags=["sales", "analysis", "for_ai", "json"],
) as dag:
    build_json = PythonOperator(
        task_id="build_for_ai_store_month_json",
        python_callable=run_for_ai_store_month_analysis,
    )
