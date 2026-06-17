"""
매장별 매출 달성 분석 자동화 DAG

매일 11:00 실행 (POS 수집 완료 후)

처리 흐름:
1. unified_sales parquet → 일별 실적 집계 → daily_actuals.csv (append)
2. daily_actuals + target.csv → 달성률/구간/LLM 진단 → sales_analysis.csv (append)

출력:
- analytics/store_sales_target/daily_actuals.csv
- analytics/store_sales_target/sales_analysis.csv
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import SMD_STORE_SALES_TIME
from modules.transform.pipelines.sales.SMD_store_sales_daily_actuals import run_daily_actuals
from modules.transform.pipelines.sales.SMD_store_sales_analysis import run_analysis
from modules.transform.utility.notifier import on_failure_callback

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace(".py", ""),
    description="매장 매출 달성 분석 (일별 실적 집계 + LLM 진단)",
    schedule=SMD_STORE_SALES_TIME,
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
    tags=["sales", "analysis", "매출분석", "LLM"],
) as dag:

    task_daily_actuals = PythonOperator(
        task_id="aggregate_daily_actuals",
        python_callable=run_daily_actuals,
    )

    task_analysis = PythonOperator(
        task_id="run_sales_analysis",
        python_callable=run_analysis,
    )

    task_daily_actuals >> task_analysis
