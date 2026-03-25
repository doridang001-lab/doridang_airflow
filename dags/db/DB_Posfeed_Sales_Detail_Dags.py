"""
Posfeed 주문 상세 크롤링 DAG

처리 흐름:
1. extract_order_codes: 어제 주문 코드 목록 추출 (증분)
2. scrape_order_details: 상세 페이지 순회 크롤링 → OneDrive 저장
"""
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_POSFEED_SALES_DETAIL_TIME
from modules.transform.pipelines.db.DB_Posfeed_Sales_Detail import (
    extract_order_codes,
    scrape_order_details,
)

dag_id = Path(__file__).stem

default_args = {
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_POSFEED_SALES_DETAIL_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "posfeed", "detail", "selenium"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_order_codes",
        python_callable=extract_order_codes,
    )

    t2 = PythonOperator(
        task_id="scrape_order_details",
        python_callable=scrape_order_details,
        execution_timeout=timedelta(hours=3),
    )

    t1 >> t2
