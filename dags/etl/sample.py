# dags/etl/load_coupang_coupon.py (또는 원하는 파일명)
"""
쿠팡이츠 쿠폰 데이터 로드 DAG

🔄 두 가지 모드:
1. 정기 실행 (월/수 10:45): 원본 데이터 새로 로드
2. 재업로드 모드: "업로드_temp" + "원드라이브"의 이전 파일들 재사용
"""
import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# 함수 import
from modules.transform.pipelines.strategic_coupang_coupon import (
    load_reupload_coupang_coupon
)

# ==================================================
# DAG 정의
# ==================================================
filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="45 10 * * 1,3",  # 매주 월, 수 10시 45분 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    load_coupang_coupon = PythonOperator(
        task_id='load_coupang_coupon',
        python_callable=load_reupload_coupang_coupon,
    )

load_coupang_coupon