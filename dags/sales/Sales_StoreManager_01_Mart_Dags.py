"""
매장 담당자/지역 + 전월 매출 스냅샷 마트 DAG

매일 02:35 실행 (Sales_Employee_Extract_Dags 02:30 수집 완료 5분 후)

출력: OneDrive data/mart/Store_Manager/store_manager.csv
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import SMD_STORE_MANAGER_MART_TIME
from modules.transform.pipelines.sales.SMD_store_manager_mart import run_store_manager_mart
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace(".py", ""),
    description="매장 담당자/지역 마스터 + 전월 매출 스냅샷 마트 (store_manager.csv)",
    schedule=SMD_STORE_MANAGER_MART_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": on_failure_callback,
    },
    tags=["sales", "mart", "store_manager"],
) as dag:

    task_build_store_manager_mart = PythonOperator(
        task_id="build_store_manager_mart",
        python_callable=run_store_manager_mart,
    )
