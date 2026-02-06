# SMD_01_sales_orders_csv_Dags.py
"""
배민/쿠팡 일일 주문 데이터 ETL DAG

📋 처리 흐름:
1. 배민/쿠팡 주문 데이터 로드 + 전처리
   - 파일명 추적 (_source_file 컬럼 자동 추가)
   - 정산금액 검증 (결제금액은 있는데 정산금액 없으면 이메일 알림)
2. 담당자 테이블 조인
3. 최종 전처리 (sub_order_id 생성)
4. CSV 저장 (append 방식, 중복 제거)
"""

import pendulum
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.transform.utility.io3 import SMD_ORDERS_TIME, SMD_VISIT_LOG

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

# Import 분리 (명확성)
from modules.transform.pipelines.SMD_01_sales_orders_csv import (
    # 로드 함수
    load_baemin_data, 
    preprocess_load_baemin_data,
    load_coupang_data, 
    preprocess_load_coupang_data,
    load_employee_data,
    preprocess_load_employee_data,
    
    # 병합 및 전처리
    preprocess_join_orders_with_stores,
    preprocess_merged_daily_orders,
)

# CSV 저장 유틸
from modules.transform.utility.io3 import append_save_to_csv 
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMD_ORDERS_TIME,  # 매주 월,수 10:30 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 🔒 동시 실행 방지
    default_args={
        'retries': 1,  # 🔄 최대 1회 재시도
        'retry_delay': timedelta(minutes=5),  # ⏱️ 재시도 간격
        'depends_on_past': False,  # 🚫 이전 실행 실패해도 다음 실행 허용
        'email_on_failure': False,  # 📧 실패 시 이메일 발송 안 함 (중복 방지)
        'email_on_retry': False,  # 📧 재시도 시 이메일 발송 안 함
    },
    tags=['sales', 'daily', 'baemin', 'coupang', 'validation'],  # ⭐ validation 태그 추가
) as dag:
    
    # ============================================================
    # 1️⃣ 데이터 로드 (병렬)
    # ⭐ 각 파일에 _source_file 컬럼 자동 추가
    # ============================================================
    load_baemin_task = PythonOperator(
        task_id='load_baemin_data',
        python_callable=load_baemin_data,
    )
    
    load_coupang_task = PythonOperator(
        task_id='load_coupang_data',
        python_callable=load_coupang_data,
    )
    
    load_employee_task = PythonOperator(
        task_id='load_employee_data',
        python_callable=load_employee_data,
    )
    
    # ============================================================
    # 2️⃣ 전처리 (병렬)
    # ⭐ 정산금액 검증 + 문제 발견 시 자동 이메일 발송
    # ============================================================
    preprocess_baemin_task = PythonOperator(
        task_id='preprocess_baemin',
        python_callable=preprocess_load_baemin_data,
        op_kwargs={
            'input_task_id': 'load_baemin_data',
            'input_xcom_key': 'baemin_parquet_path',
            'output_xcom_key': 'baemin_processed_path'
        }
    )
    
    preprocess_coupang_task = PythonOperator(
        task_id='preprocess_coupang',
        python_callable=preprocess_load_coupang_data,
        op_kwargs={
            'input_task_id': 'load_coupang_data',
            'input_xcom_key': 'coupang_parquet_path',
            'output_xcom_key': 'coupang_processed_path'
        }
    )
    
    preprocess_employee_task = PythonOperator(
        task_id='preprocess_employee',
        python_callable=preprocess_load_employee_data,
        op_kwargs={
            'input_task_id': 'load_employee_data',
            'input_xcom_key': 'employee_parquet_path',
            'output_xcom_key': 'employee_processed_path'
        }
    )
    
    # ============================================================
    # 3️⃣ 주문 데이터 + 담당자 조인
    # ⭐ _source_file 컬럼 유지
    # ============================================================
    join_orders_task = PythonOperator(
        task_id='join_orders_with_stores',
        python_callable=preprocess_join_orders_with_stores,
        op_kwargs={
            'baemin_task_id': 'preprocess_baemin',
            'baemin_xcom_key': 'baemin_processed_path',
            'coupang_task_id': 'preprocess_coupang',
            'coupang_xcom_key': 'coupang_processed_path',
            'employee_task_id': 'preprocess_employee',
            'employee_xcom_key': 'employee_processed_path',
            'output_xcom_key': 'orders_joined_path'
        }
    )
    
    # ============================================================
    # 4️⃣ 최종 전처리 (중복 제거, 컬럼 정리)
    # ⭐ sub_order_id 생성, _source_file 유지
    # ============================================================
    final_preprocess_task = PythonOperator(
        task_id='preprocess_merged_orders',
        python_callable=preprocess_merged_daily_orders,
        op_kwargs={
            'input_task_id': 'join_orders_with_stores',
            'input_xcom_key': 'orders_joined_path',
            'output_xcom_key': 'orders_final_path'
        }
    )
    
    # ============================================================
    # 5️⃣ CSV 저장 (주문 데이터)
    # ⭐ _source_file 컬럼 포함된 데이터 저장
    # ⭐ append 방식으로 기존 데이터에 누적
    # ============================================================
    save_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=append_save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_merged_orders',
            'input_xcom_key': 'orders_final_path',
            'output_csv_path': str(LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders.csv'),  # ⬅️ str() 추가
            'dedup_key': ['sub_order_id'],  # 중복 체크 키
            # backup_dir 파라미터 제거! ⬅️ 제거
        }
    )
    

    # ============================================================
    # Task 의존성
    # ============================================================
    # 1. 로드 → 전처리 (검증 + 이메일 자동 발송)
    load_baemin_task >> preprocess_baemin_task
    load_coupang_task >> preprocess_coupang_task
    load_employee_task >> preprocess_employee_task
    
    # 2. 전처리 → 조인
    [preprocess_baemin_task, preprocess_coupang_task, preprocess_employee_task] >> join_orders_task
    
    # 3. 조인 → 최종 전처리 → CSV 저장
    join_orders_task >> final_preprocess_task >> save_csv_task
