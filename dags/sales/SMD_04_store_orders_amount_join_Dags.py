"""
매출 데이터 + 매장 성실지표 조인 DAG

📋 처리 흐름:
1. 데이터 로드 (스마트 로더 - 재사용/새로 로드 자동 감지)
   - 배민 우리가게 now (성실지표)
   - 배민 변경이력
   - 토더 리뷰
   - 매출 집계 데이터 (sales_daily_orders_alerts.csv)

2. 전처리 (각 데이터셋 정제)

3. 순차 LEFT JOIN
   - 매출 + 배민 우리가게 now
   - 위 결과 + 토더 리뷰
   - 위 결과 + 배민 변경이력

4. 최종 전처리 (전주/전월 비교 지표 추가)

5. 담당자별 점수 계산 및 조인

6. 검증 및 저장

7. 정리 (수집 파일 이동)

🔄 두 가지 모드:
1. 정기 실행 (월/수 10:45): 모든 원본 데이터 새로 로드 후 JOIN
2. 재업로드 모드: 이전의 배민/토더 계산 결과 재사용, 수정된 매출 데이터만 새로 JOIN
   - "업로드_temp" + "원드라이브"의 이전 파일들 활용
   - 누락된 데이터 보정 시 사용
"""

import pendulum
import os
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from modules.transform.utility.io import SMD_ORDERS_TIME

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

# 재업로드 함수 import (스마트 로딩)
from modules.transform.pipelines.sales.SMD_04_store_orders_amount_join import (
    # 재업로드 함수 (업로드_temp + 원드라이브 동시 검색)
    load_reupload_toorder_review,
    load_reupload_baemin_store_now,
    load_reupload_baemin_history,
    
    # 전처리 함수
    preprocess_toorder_review_df,
    preprocess_baemin_store_now_df,
    preprocess_baemin_history_df,
    
    # 매출 데이터 (항상 새로 로드)
    load_sales_daily_orders_alerts_df,
    
    # JOIN 함수
    left_join_orders_now,
    left_join_orders_now_toorder,
    left_join_orders_now_toorder_history,
    
    # 최종 전처리 (전주/전월 비교 지표)
    preprocess_add_main_left_join_df,
    
    # ⭐ 담당자 점수 계산 및 조인
    calculate_manager_scores,
    left_join_manager_scores,
    
    # CSV 저장
    fin_save_to_csv
)


from modules.load.load_df_glob import cleanup_collected_csvs, move_download_files, upload_final_csv
from modules.transform.utility.paths import LOCAL_DB, COLLECT_DB, DOWN_DIR


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMD_ORDERS_TIME,  # 매주 월, 수 10시 30분 실행
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
    tags=['02_sales', 'crawling', 'toorder', 'baemin'],
) as dag:
    
    # ============================================================
    # 1️⃣ 스마트 로딩 태스크 (자동 감지 방식)
    # 업로드_temp + 원드라이브에서 파일 찾기
    # 이전 파일 있으면 재사용, 없으면 원본 새로 로드
    # ============================================================

    # ⭐ SMD_03 완료 대기 - 최신 성공 실행 자동 검색
    wait_for_smd_03 = ExternalTaskSensor(
        task_id='wait_for_smd_03',
        external_dag_id='SMD_03_sales_orders_transform_Dags',
        external_task_id='move_collected_files',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=7200,
        poke_interval=60,
        soft_fail=True,
    )

    # 토더 리뷰
    load_toorder_review_task = PythonOperator(
        task_id='load_toorder_review',
        python_callable=load_reupload_toorder_review,
    )
    
    preprocess_toorder_review_task = PythonOperator(
        task_id='preprocess_toorder_review',
        python_callable=preprocess_toorder_review_df,
        op_kwargs={
            'input_task_id': 'load_toorder_review',
            'input_xcom_key': 'toorder_review_path',
            'output_xcom_key': 'preprocessed_toorder_review_path'
        }
    )
    
    # 배민 우리가게 now (성실지표)
    load_baemin_store_now_task = PythonOperator(
        task_id='load_baemin_store_now',
        python_callable=load_reupload_baemin_store_now,
    )
    
    preprocess_baemin_store_now_task = PythonOperator(
        task_id='preprocess_baemin_store_now',
        python_callable=preprocess_baemin_store_now_df
    )
    
    # 배민 변경이력
    load_baemin_history_task = PythonOperator(
        task_id='load_baemin_history',
        python_callable=load_reupload_baemin_history,
    )
    
    preprocess_baemin_history_task = PythonOperator(
        task_id='preprocess_baemin_history',
        python_callable=preprocess_baemin_history_df,
        op_kwargs={
            'input_task_id': 'load_baemin_history',
            'input_xcom_key': 'baemin_history_path',
            'output_xcom_key': 'preprocessed_baemin_history_path'
        }
    )
    
    # ============================================================
    # 2️⃣ 매출 집계 데이터 로드 (항상 새로 로드)
    # sales_daily_orders DAG에서 생성된 파일 사용
    # ============================================================
    load_sales_daily_orders_alerts_task = PythonOperator(
        task_id='load_sales_daily_orders_alerts',
        python_callable=load_sales_daily_orders_alerts_df,
    )
    
    # ============================================================
    # 3️⃣ LEFT JOIN 태스크들 (순차 조인)
    # ============================================================
    
    # JOIN 1: 매출 + 배민 우리가게 now
    left_join_orders_now_task = PythonOperator(
        task_id='left_join_orders_now',
        python_callable=left_join_orders_now,
        op_kwargs={
            'left_task': {
                'task_id': 'load_sales_daily_orders_alerts',
                'xcom_key': 'sales_daily_orders_alerts_path'
            },
            'right_task': {
                'task_id': 'preprocess_baemin_store_now',
                'xcom_key': 'processed_baemin_path'
            },
            'left_on': ['order_daily', '매장명'],
            'right_on': ['collected_date', 'stores_name'],
            'how': 'left',
            'drop_columns': ['collected_date', 'stores_name'],
            'output_xcom_key': 'joined_orders_now_path'
        }
    )
    
    # JOIN 2: (매출 + 우리가게 now) + 토더 리뷰
    left_join_orders_now_toorder_task = PythonOperator(
        task_id='left_join_orders_now_toorder',
        python_callable=left_join_orders_now_toorder,
        op_kwargs={
            'left_task': {
                'task_id': 'left_join_orders_now',
                'xcom_key': 'joined_orders_now_path'
            },
            'right_task': {
                'task_id': 'preprocess_toorder_review',
                'xcom_key': 'preprocessed_toorder_review_path'
            },
            'left_on': ['order_daily', '매장명'],
            'right_on': ['date', 'stores_name'],
            'how': 'left',
            'drop_columns': ['date', 'stores_name'],
            'output_xcom_key': 'joined_orders_now_toorder_path'
        }
    )
    
    # JOIN 3: (매출 + 우리가게 now + 토더) + 배민 변경이력
    left_join_orders_now_toorder_history_task = PythonOperator(
        task_id='left_join_orders_now_toorder_history',
        python_callable=left_join_orders_now_toorder_history,
        op_kwargs={
            'left_task': {
                'task_id': 'left_join_orders_now_toorder',
                'xcom_key': 'joined_orders_now_toorder_path'
            },
            'right_task': {
                'task_id': 'preprocess_baemin_history',
                'xcom_key': 'preprocessed_baemin_history_path'
            },
            'left_on': ['order_daily', '매장명'],
            'right_on': ['change_date', 'stores_name'],
            'how': 'left',
            'drop_columns': ['change_date', 'stores_name'],
            'output_xcom_key': 'joined_orders_now_toorder_history_path'
        }
    )
    
    # ============================================================
    # 4️⃣ 최종 전처리 (전주/전월 비교 지표 추가)
    # ============================================================
    preprocess_add_main_task = PythonOperator(
        task_id='preprocess_add_main_left_join',
        python_callable=preprocess_add_main_left_join_df,
        op_kwargs={
            'input_task_id': 'left_join_orders_now_toorder_history',
            'input_xcom_key': 'joined_orders_now_toorder_history_path',
            'output_xcom_key': 'final_preprocessed_path'
        }
    )
    
    # ============================================================
    # 5️⃣ 담당자별 점수 계산
    # ============================================================
    calculate_manager_scores_task = PythonOperator(
        task_id='calculate_manager_scores',
        python_callable=calculate_manager_scores,
        op_kwargs={
            'input_task_id': 'preprocess_add_main_left_join',
            'input_xcom_key': 'final_preprocessed_path',
            'output_xcom_key': 'manager_scores_path'
        }
    )
    
    # ============================================================
    # 6️⃣ 담당자 점수 조인
    # ============================================================
    left_join_manager_scores_task = PythonOperator(
        task_id='left_join_manager_scores',
        python_callable=left_join_manager_scores,
        op_kwargs={
            'left_task': {
                'task_id': 'preprocess_add_main_left_join',
                'xcom_key': 'final_preprocessed_path'
            },
            'right_task': {
                'task_id': 'calculate_manager_scores',
                'xcom_key': 'manager_scores_path'
            },
            'left_on': ['order_daily', '담당자'],
            'right_on': ['order_daily', '담당자'],
            'how': 'left',
            'output_xcom_key': 'final_preprocessed_with_scores_path'
        }
    )
    
    # ============================================================
    # 7️⃣ CSV 저장
    # ============================================================
    fin_save_to_csv_task = PythonOperator(
        task_id='fin_save_to_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'left_join_manager_scores',
            'input_xcom_key': 'final_preprocessed_with_scores_path',
            'output_filename': 'sales_daily_orders_upload.csv',
            'output_subdir': '영업관리부_DB',
            'dedup_key': ['order_daily', '매장명']
        }
    )
    
    # ============================================================
    # 8️⃣ 정리 및 업로드 (병렬 실행)
    # ============================================================
    
    # OneDrive 수집 폴더 정리
    cleanup_task = PythonOperator(
        task_id='move_collected_files',
        python_callable=cleanup_collected_csvs,
        op_kwargs={
            'patterns': [
                'baemin_change_history_*.csv',
                'baemin_metrics_*.csv',
                'toorder_review_*.csv',
                'toorder_review_*.xlsx'
            ],
            'source_dir': str(COLLECT_DB / '영업관리부_수집'),
            'dest_dir': '/opt/airflow/download/업로드_temp'
        }
    )
    
    # E:\down\업로드_temp로 복사
    upload_task = PythonOperator(
        task_id='upload_final_csv',
        python_callable=upload_final_csv,
        op_kwargs={
            'source_filename': 'sales_daily_orders_upload.csv',
            'source_subdir': '영업관리부_DB',
            'dest_dir': str(DOWN_DIR / '업로드_temp')  # ✅ 동적 경로 사용
        }
    )
    
    # ============================================================
    # Task 의존성 정의
    # ============================================================

    # 1. 로드 → 전처리
    wait_for_smd_03 >> load_toorder_review_task >> preprocess_toorder_review_task
    wait_for_smd_03 >> load_baemin_store_now_task >> preprocess_baemin_store_now_task
    wait_for_smd_03 >> load_baemin_history_task >> preprocess_baemin_history_task
    wait_for_smd_03 >> load_sales_daily_orders_alerts_task  # ⭐ 이 줄 추가!

    # 2. 조인 1: 주문 + 우리가게 now
    [load_sales_daily_orders_alerts_task, preprocess_baemin_store_now_task] >> left_join_orders_now_task

    # 3. 조인 2: (주문 + 우리가게 now) + 토더 리뷰
    [left_join_orders_now_task, preprocess_toorder_review_task] >> left_join_orders_now_toorder_task

    # 4. 조인 3: (주문 + 우리가게 now + 토더) + 변경이력
    [left_join_orders_now_toorder_task, preprocess_baemin_history_task] >> left_join_orders_now_toorder_history_task

    # 5. 최종 전처리
    left_join_orders_now_toorder_history_task >> preprocess_add_main_task

    # 6. 담당자 점수 계산 (전처리 완료 후)
    preprocess_add_main_task >> calculate_manager_scores_task

    # 7. 담당자 점수 조인 (전처리 + 담당자 점수 둘 다 필요)
    [preprocess_add_main_task, calculate_manager_scores_task] >> left_join_manager_scores_task

    # 8. CSV 저장
    left_join_manager_scores_task >> fin_save_to_csv_task

    # 9. 정리 및 업로드 (병렬)
    fin_save_to_csv_task >> [cleanup_task, upload_task]


# ============================================================
# DAG 플로우 요약
# ============================================================
"""
┌─────────────────────────────────────────────────────────────┐
│  데이터 로드 (스마트 로더 - 재사용/새로 로드 자동 감지)     │
│                                                              │
│  load_toorder_review → preprocess_toorder_review ──┐        │
│                                                      │        │
│  load_baemin_store_now → preprocess_baemin_store_now┼─┐     │
│                                                      │ │     │
│  load_baemin_history → preprocess_baemin_history ───┼─┼─┐   │
│                                                      │ │ │   │
│  load_sales_daily_orders_alerts ────────────────────┘ │ │   │
└─────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────┐
│  순차 LEFT JOIN                                              │
│                                                              │
│  left_join_orders_now (매출 + 우리가게 now)                 │
│          ↓                                                   │
│  left_join_orders_now_toorder (+ 토더 리뷰)                 │
│          ↓                                                   │
│  left_join_orders_now_toorder_history (+ 변경이력)          │
└─────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────┐
│  최종 전처리 및 담당자 점수                                  │
│                                                              │
│  preprocess_add_main_left_join (전주/전월 비교 지표 추가)   │
│          ├──────────────────────┐                           │
│          ↓                       ↓                           │
│  calculate_manager_scores   (원본 유지)                     │
│          └──────────────────────┘                           │
│                   ↓                                          │
│  left_join_manager_scores (담당자 점수 조인)                │
│          ↓                                                   │
│  fin_save_to_csv (CSV 저장)                                 │
└─────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────┐
│  정리 및 업로드 (병렬)                                       │
│                                                              │
│  cleanup_task (OneDrive 정리)                               │
│  upload_task (E:\\down 복사)                                │
└─────────────────────────────────────────────────────────────┘

📌 주요 기능:
- 매출 데이터에 매장 성실지표 조인
- 전주/전월 비교 지표 자동 계산
- 담당자별 점수 계산 및 조인
- 스마트 로더로 파일 재사용 (성능 향상)
- 수집 파일 자동 정리

⚙️ 실행 시각: 매주 월/수 10:45 (KST)
"""