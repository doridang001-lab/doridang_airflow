"""
당근 광고 CSV 통합 DAG.

Collect_Data/마케팅_수집/daangn_ads_*.csv를 매일 12:00에
analytics/Daangn_ads/daangn_ads.csv 단일 파일로 병합 저장한다.
"""

import json
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.BSP_DaangnAds_CSV import load_daangn_ads_csv
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import BSP_DAANGN_ADS_TIME


def task_load_daangn_ads_csv() -> str:
    result_json = load_daangn_ads_csv()
    result = json.loads(result_json)
    if result.get("status") == "NO_SOURCE_FILES":
        raise AirflowSkipException("당근 광고 CSV와 기존 통합 파일이 없어 처리 중단")
    return result_json


with DAG(
    dag_id=Path(__file__).stem,
    schedule=BSP_DAANGN_ADS_TIME,
    start_date=pendulum.datetime(2026, 8, 26, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
    tags=["marketing", "daangn"],
) as dag:
    load_task = PythonOperator(
        task_id="load_daangn_ads_csv",
        python_callable=task_load_daangn_ads_csv,
    )
