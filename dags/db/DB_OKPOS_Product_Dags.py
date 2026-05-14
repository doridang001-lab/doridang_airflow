"""
OKPOS 상품조회 엑셀 자동 수집 DAG

처리 요약:
1) OKPOS 로그인
2) https://my.okpos.co.kr/asp/base/prod/search 이동
3) 조회줄수(perPage)=5000 설정 후 조회
4) 엑셀다운(exportSheet) 클릭
5) ANALYTICS_DB/okpos_product/상품조회.xlsx 로 매번 덮어쓰기 저장

다운로드 기본 위치: DOWN_DIR (Windows 기본: E:/down)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_OKPOS_PRODUCT_TIME
from modules.transform.pipelines.db.DB_OKPOS_Product import (
    download_okpos_product,
    save_okpos_product,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id=dag_id,
    schedule=DB_OKPOS_PRODUCT_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "product", "excel", "selenium"],
) as dag:

    t1 = PythonOperator(
        task_id="download_okpos_product",
        python_callable=download_okpos_product,
    )

    t2 = PythonOperator(
        task_id="save_okpos_product",
        python_callable=save_okpos_product,
    )

    t1 >> t2

