"""
Posfeed 주문 데이터 자동 다운로드 DAG

📋 처리 흐름:
1. Selenium으로 Posfeed 관리자 사이트 로그인
2. 주문 목록 검색 후 엑셀 다운로드
3. 다운로드 파일을 LOCAL_DB/posfeed/ 로 이동

🔐 인증 정보: 환경변수 POSFEED_USERNAME / POSFEED_PASSWORD
📅 스케줄: 매주 월요일 08:00 (DB_POSFEED_SALES_TIME)
"""

import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from modules.transform.utility.schedule import DB_POSFEED_SALES_TIME
from modules.transform.pipelines.db.DB_Posfeed_Sales import (
    download_posfeed_excel,
    move_to_storage,
    partition_to_onedrive,
)

dag_id = Path(__file__).stem

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_POSFEED_SALES_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['db', 'posfeed', 'excel', 'selenium'],
) as dag:

    # ============================================================
    # 1️⃣ Selenium 로그인 + 엑셀 다운로드
    # ============================================================
    download_task = PythonOperator(
        task_id='download_excel',
        python_callable=download_posfeed_excel,
    )

    # ============================================================
    # 2️⃣ 다운로드 파일 → LOCAL_DB/posfeed/ 이동
    # ============================================================
    move_task = PythonOperator(
        task_id='move_to_storage',
        python_callable=move_to_storage,
    )

    # ============================================================
    # 3️⃣ 브랜드/지점/월 파티션으로 OneDrive CSV 저장
    # ============================================================
    onedrive_task = PythonOperator(
        task_id='partition_to_onedrive',
        python_callable=partition_to_onedrive,
    )

    # ============================================================
    # Task 의존성
    # ============================================================
    download_task >> move_task >> onedrive_task
