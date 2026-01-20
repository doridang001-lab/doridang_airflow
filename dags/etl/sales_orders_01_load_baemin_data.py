"""
배민/쿠팡 일일 주문 데이터 ETL DAG

📋 처리 흐름:
1. 배민/쿠팡 주문 데이터 로드 + 전처리
2. 담당자 테이블 조인
3. 일별 매출 집계
4. 이상 감지 점수 계산
5. 알림 대상 필터링
6. 담당자별 이메일 발송
"""

import pendulum
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

# Import 분리 (명확성)
from modules.transform.pipelines.sales_daily_orders import (
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
    
    # 집계 및 점수 계산
    aggregate_daily_sales,
    calculate_scores,
    filter_alerts,
    
    # 이메일 발송
    send_alert_email,
    send_completion_email,
)

# CSV 저장 유틸
from modules.transform.utility.io import save_to_csv


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="30 10 * * *",  # 매일 10:30 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['sales', 'daily', 'baemin', 'coupang'],
) as dag:
    
    # ============================================================
    # 1️⃣ 데이터 로드 (병렬)
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
    # ============================================================
    save_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_merged_orders',
            'input_xcom_key': 'orders_final_path'
        }
    )
    
    # ============================================================
    # 6️⃣ 일별 매출 집계
    # ============================================================
    aggregate_task = PythonOperator(
        task_id='aggregate_daily_sales',
        python_callable=aggregate_daily_sales,
        op_kwargs={
            'input_task_id': 'preprocess_merged_orders',
            'input_xcom_key': 'orders_final_path',
            'output_xcom_key': 'daily_aggregated'
        }
    )
    
    # ============================================================
    # 7️⃣ 이상 감지 점수 계산
    # ============================================================
    calculate_scores_task = PythonOperator(
        task_id='calculate_scores',
        python_callable=calculate_scores,
        op_kwargs={
            'input_task_id': 'aggregate_daily_sales',
            'input_xcom_key': 'daily_aggregated',
            'output_xcom_key': 'scores_calculated'
        }
    )
    
    # ============================================================
    # 8️⃣ 알림 대상 필터링
    # ============================================================
    filter_alerts_task = PythonOperator(
        task_id='filter_alerts',
        python_callable=filter_alerts,
        op_kwargs={
            'input_task_id': 'calculate_scores',
            'input_xcom_key': 'scores_calculated',
            'output_xcom_key': 'alert_targets'
        }
    )
    
    # ============================================================
    # 9️⃣ 이메일 발송
    # ============================================================
    # 수집 완료 알림 (전체 담당자)
    send_completion_task = PythonOperator(
        task_id='send_completion_email',
        python_callable=send_completion_email,
    )
    
    # 매출 이상 알림 (알림 대상만)
    send_alert_task = PythonOperator(
        task_id='send_alert_email',
        python_callable=send_alert_email,
    )
    
    # ============================================================
    # Task 의존성
    # ============================================================
    # 1. 로드 → 전처리
    load_baemin_task >> preprocess_baemin_task
    load_coupang_task >> preprocess_coupang_task
    load_employee_task >> preprocess_employee_task
    
    # 2. 전처리 → 조인
    [preprocess_baemin_task, preprocess_coupang_task, preprocess_employee_task] >> join_orders_task
    
    # 3. 조인 → 최종 전처리 → CSV 저장
    join_orders_task >> final_preprocess_task >> save_csv_task
    
    # 4. CSV 저장 → 집계 → 점수 계산 → 필터링
    save_csv_task >> aggregate_task >> calculate_scores_task >> filter_alerts_task
    
    # 5. 필터링 → 이메일 발송 (병렬)
    filter_alerts_task >> [send_completion_task, send_alert_task]


# ============================================================
# DAG 플로우 요약
# ============================================================
"""
┌─────────────────────────────────────────────────────────────┐
│  데이터 로드 (병렬)                                          │
│  load_baemin → preprocess_baemin ─┐                         │
│  load_coupang → preprocess_coupang ┼─→ join_orders          │
│  load_employee → preprocess_employee ┘                      │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  데이터 병합 및 정제                                         │
│  join_orders → final_preprocess → save_to_csv              │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  집계 및 이상 감지                                           │
│  aggregate_daily_sales → calculate_scores → filter_alerts  │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  이메일 발송 (병렬)                                          │
│  send_completion_email (전체)                               │
│  send_alert_email (이상 감지)                               │
└─────────────────────────────────────────────────────────────┘

📌 주요 기능:
- 배민/쿠팡 주문 데이터 자동 수집
- 담당자 매핑 및 중복 제거
- 일별 매출 집계 및 이상 감지
- 담당자별 맞춤 알림 발송

⚙️ 실행 시각: 매일 06:30 (KST)
"""