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
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.utils.state import State
from airflow import settings
from sqlalchemy import text
from modules.transform.utility.io import SMD_ORDERS_TIME


# ============================================================
# ExternalTaskSensor용 최신 성공 execution_date 검색
# ============================================================
def _latest_smd02_execution_date(dt, **context):
    """SMD_02의 fin_save_to_csv 태스크가 성공한 최신 execution_date를 찾는다."""
    session = settings.Session()
    try:
        row = session.execute(
            text(
                """
                SELECT execution_date
                FROM task_instance
                WHERE dag_id = :dag_id
                  AND task_id = :task_id
                  AND state = :state
                ORDER BY execution_date DESC
                LIMIT 1
                """
            ),
            {
                'dag_id': 'Sales_Orders_02_Review_Dags',
                'task_id': 'fin_save_to_csv',
                'state': 'success',
            },
        ).first()

        if row:
            execution_date = row[0]
            print(f"[sensor] SMD_02 fin_save_to_csv 성공 찾음: {execution_date}")
            return execution_date
    except Exception as e:
        print(f"[sensor] TaskInstance 조회 실패: {e}")
    finally:
        session.close()
    
    print(f"[sensor] 성공한 fin_save_to_csv 없음, 기본값 사용: {dt}")
    return dt

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

# Import 분리 (명확성)
from modules.transform.pipelines.sales.SMD_03_sales_orders_transform import (
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
    
    # 이메일 발송은 SMD_07로 이동됨
)

# CSV 저장 유틸
from modules.transform.utility.io import save_to_csv
from modules.transform.utility.io import sales_cleanup_collected_csvs
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
        external_dag_id='Sales_Orders_02_Review_Dags',
        external_task_id='fin_save_to_csv',  # SMD_02의 마지막 task
        execution_date_fn=_latest_smd02_execution_date,  # ⭐ 최신 성공 실행 찾기
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',  # poke → reschedule (작업 슬롯 반환)
        timeout=3600,
        poke_interval=60,
        soft_fail=True,  # ⭐ timeout 시에도 계속 진행
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
            'input_task_id': 'load_baemin_data',
            'input_xcom_key': 'baemin_parquet_path',
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
    # 9️⃣ 이메일 발송은 SMD_07로 이동
    # ============================================================
    # 수집 완료 알림 (전체 담당자) - SMD_07로 이동됨
    # 매출 이상 알림 (알림 대상만) - SMD_07로 이동됨
    
    
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
    # 1. 로드 → 집계 → 점수 계산 → 필터링 → 정리
    chain(
        wait_for_smd_02,
        load_baemin_data_task,
        aggregate_task,
        calculate_scores_task,
        filter_alerts_task,
        cleanup_task,
    )
    
    # 💡 이메일 발송은 SMD_07_store_ordesr_alert DAG에서 처리
    # SMD_07이 calculate_scores 완료를 감지하고 자동 실행됨


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
│  정리 작업                                                   │
│  move_collected_files → 수집 파일 정리                      │
└─────────────────────────────────────────────────────────────┘

                        ⚡ (별도 DAG)
┌─────────────────────────────────────────────────────────────┐
│  📧 SMD_07: 알림 전용 DAG                                    │
│  calculate_scores 완료 감지 → 이메일 발송                  │
│  • send_completion_email (전체)                             │
│  • send_alert_email (이상 감지)                             │
└─────────────────────────────────────────────────────────────┘

📌 주요 기능:
- 배민/쿠팡 주문 데이터 자동 수집
- 담당자 매핑 및 중복 제거
- 일별 매출 집계 및 이상 감지
- 점수 계산 및 알림 대상 필터링
- CSV 저장 및 XCom 전달 (SMD_07용)

⚙️ 실행 시각: 매일 06:30 (KST)
💡 이메일 발송: SMD_07 DAG에서 자동 처리
"""