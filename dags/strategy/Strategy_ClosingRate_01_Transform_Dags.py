"""
월별 폐점율 KPI 스냅샷 DAG
GSheet 직원관리 → 매장×월 스냅샷 + 월별 KPI CSV 저장 (MART_DB/closing_rate/)
"""

import os
import pendulum
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)

from modules.transform.pipelines.strategy.SMP_closing_rate_snapshot import (
    extract_employee,
    transform_and_save,
)
from modules.transform.utility.schedule import SMP_CLOSING_RATE_TIME
from modules.transform.utility.notifier import on_failure_callback

with DAG(
    dag_id=Path(__file__).stem,
    description="GSheet 직원관리 → 월별 폐점율 KPI 스냅샷 CSV 저장",
    schedule=SMP_CLOSING_RATE_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
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
    tags=["strategy", "monthly", "kpi", "closing_rate"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_employee",
        python_callable=extract_employee,
    )

    t2 = PythonOperator(
        task_id="transform_and_save",
        python_callable=transform_and_save,
    )

    t1 >> t2
