"""
EasyPOS(이지포스) 상품조회 엑셀 자동 수집 DAG

처리 요약:
1) Playwright로 EasyPOS 로그인
2) 기초정보 → 상품조회 진입 후 조회/엑셀 다운로드
3) ANALYTICS_DB/easypos_product/상품조회.xlsx (덮어쓰기, 1개 파일만 저장)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_EasyPOS_Product import (
    download_easypos_product,
    save_easypos_product,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# 매일 10:25 (KST) 실행
SCHEDULE = "25 10 * * *"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=dag_id,
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "easypos", "product", "excel", "playwright"],
) as dag:

    t1 = PythonOperator(
        task_id="download_easypos_product",
        python_callable=download_easypos_product,
    )

    t2 = PythonOperator(
        task_id="save_easypos_product",
        python_callable=save_easypos_product,
    )

    t1 >> t2
