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
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor  # ⭐ 추가
from modules.transform.utility.io3 import SMD_ORDERS_TIME

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

# Import 분리 (명확성)
from modules.transform.pipelines.SMD_03_sales_orders_transform import (
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
from modules.transform.utility.io3 import sales_cleanup_collected_csvs
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
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['sales', 'daily', 'baemin', 'coupang'],
) as dag:
    # ============================================================
    # ⭐ SMD_02 완료 대기
    # ============================================================
    wait_for_smd_02 = ExternalTaskSensor(
        task_id='wait_for_smd_02', #⭐ SMD_02 완료 대기
        external_dag_id='SMD_02_sales_orders_csv_review_Dags', # SMD_02 DAG ID
        external_task_id='fin_save_to_csv',  # SMD_02의 마지막 task
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        timeout=3600,
        poke_interval=60,
    )
    
    
    # ============================================================
    # 1️⃣  데이터 로드
    # ============================================================
    load_baemin_data_task = PythonOperator(
        task_id='load_baemin_data',
        python_callable=load_baemin_data,
    )
    
    # ============================================================
    # 6️⃣ 로드한 주문 데이터 일별 매출 집계
    # ============================================================
    aggregate_task = PythonOperator(
        task_id='aggregate_daily_sales',
        python_callable=aggregate_daily_sales,
        op_kwargs={
            'input_task_id': 'save_final_orders_csv',
            'input_xcom_key': 'final_orders_preprocessed',
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
    
    
    # OneDrive 수집 폴더 정리
    cleanup_task = PythonOperator(
        task_id='move_collected_files',
        python_callable=sales_cleanup_collected_csvs,
        op_kwargs={
            'patterns': [
                'baemin_orders_*.csv',
                'coupangeats_orders_*.csv'
            ],
            'source_dir': str(COLLECT_DB / '영업관리부_수집'), # 수집 원본 폴더
            'dest_dir': '/opt/airflow/download/업로드_temp' # 이동 목적지
        }
    )
    
    # ============================================================
    # Task 의존성
    # ============================================================
    # 1. 로드 → 집계 → 점수 계산 → 필터링
    wait_for_smd_02 >> load_baemin_data_task >> aggregate_task >> calculate_scores_task >> filter_alerts_task
    
    # 5. 필터링 → 이메일 발송 (병렬)
    filter_alerts_task >> [send_completion_task, send_alert_task] >> cleanup_task


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