# dags/etl/SMD_02_sales_orders_csv_review.py (또는 원하는 파일명)
"""
쿠팡이츠 쿠폰 데이터 로드 DAG

🔄 두 가지 모드:
1. 정기 실행 (월/수 10:40): 원본 데이터 새로 로드
2. 재업로드 모드: "업로드_temp" + "원드라이브"의 이전 파일들 재사용
"""
import pendulum
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor  # ⭐ 추가
from modules.transform.utility.io3 import SMD_ORDERS_TIME

# 함수 import
from modules.transform.pipelines.SMD_02_sales_orders_csv_review import (
    load_sales_orders_daily_csv,
    clear_sales_orders_daily_csv,
    fin_save_to_csv

)

# ==================================================
# DAG 정의
# ==================================================
filename = os.path.basename(__file__)

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
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    # ⭐ SMD_01 완료 대기
    wait_for_smd_01 = ExternalTaskSensor(
        task_id='wait_for_smd_01_completion',
        external_dag_id='SMD_01_sales_orders_csv_Dags',  # SMD_01 DAG ID
        external_task_id='save_to_csv',  # SMD_01의 마지막 task
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        timeout=3600,  # 1시간 타임아웃
        poke_interval=60,  # 1분마다 체크
    )
    
    # 데이터 로드
    load_sales_orders_daily_csv = PythonOperator(
        task_id='load_sales_orders_daily_csv',
        python_callable=load_sales_orders_daily_csv,
    )
    
    
    # 기존 데이터 삭제 <<
    clear_sales_orders_daily_csv = PythonOperator(
        task_id='clear_sales_orders_daily_csv',
        python_callable=clear_sales_orders_daily_csv,
    )
    
    # 최종 저장
    fin_save_to_csv = PythonOperator(
        task_id='fin_save_to_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'clear_sales_orders_daily_csv',
            'input_xcom_key': 'sales_orders_daily_processed_path',
            'output_filename': 'sales_daily_orders.csv',
            'output_subdir': '영업관리부_DB'
        }
    )
    
    
    wait_for_smd_01 >> load_sales_orders_daily_csv  >> clear_sales_orders_daily_csv >> fin_save_to_csv
