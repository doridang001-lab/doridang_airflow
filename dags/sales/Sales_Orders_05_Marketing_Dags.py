import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
from pathlib import Path
import datetime as dt
from airflow.sensors.external_task import ExternalTaskSensor
from modules.transform.utility.io import SMD_ORDERS_TIME




from modules.transform.pipelines.sales.SMD_05_store_ordesr_marketing import (
    load_beamin_ad_change_history_df,
    baemin_marketing_store_click_df,
    coupangeats_cmg_df,
    load_orders_upload_df,
    preprocess_beamin_ad_change_history_df,
    preprocess_coupangeats_cmg_df,
    preprocess_orders_upload_df,
    preprocess_store_click_df,
    left_join_orders_history,
    left_join_orders_history_click,
    left_join_orders_history_click_coupang,
    preprocess_fin_df,
    fin_save_to_csv
)

from modules.transform.utility.io import join_dataframes, sales_cleanup_collected_csvs
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, DOWN_DIR
from modules.transform.utility.notifier import on_failure_callback

filename = os.path.basename(__file__)

# 1. 주문 로직 정의
def merge_orders_history_df(left_df, right_df):
    """주문 데이터 + 변경 이력 병합"""
    left_df["order_daily_day"] = (
        pd.to_datetime(
            left_df["order_daily"].astype(str),
            format="mixed",
            errors="coerce"
        ).dt.normalize()
    )
    
    merged_df = pd.merge(
        left_df,
        right_df,
        left_on=["order_daily_day", "매장명"],
        right_on=["join_date", "store_names"],
        how="left",
    )
    
    return merged_df


def join_orders_history(**context):
    """주문 데이터 + 변경 이력 조인"""
    return join_dataframes(
        left_xcom_key='',
        right_xcom_key='',
        output_xcom_key='',
        join_func=merge_orders_history_df,
        **context
    )


with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMD_ORDERS_TIME,  # 매주 월,수 10:30 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 🔒 동시 실행 방지
    default_args={
        'retries': 1,
        'retry_delay': dt.timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    "on_failure_callback": on_failure_callback,
    },
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    # ============================================================
    # 0️⃣ SMD_04 완료 대기
    # ============================================================
    wait_for_smd_04 = ExternalTaskSensor(
        task_id='wait_for_smd_04',
        external_dag_id='Sales_Orders_04_Join_Dags',
        external_task_id='upload_final_csv',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',  # 작업 슬롯 반환
        timeout=3600,  # 60분 대기
        poke_interval=300,  # 5분마다 체크
        check_existence=False,  # task 없으면 skip
        soft_fail=True,  # timeout 시에도 계속 진행
    )
    
    # ============================================================
    # 1️⃣ 데이터 로드 및 전처리
    # ============================================================
    task_load_beamin_ad_change_history = PythonOperator(
        task_id='load_beamin_ad_change_history_df',
        python_callable=load_beamin_ad_change_history_df,
    )
    
    task_preprocess_beamin_ad_change_history = PythonOperator(
        task_id='preprocess_beamin_ad_change_history_df',
        python_callable=preprocess_beamin_ad_change_history_df,
    )
    
    task_load_baemin_marketing_store_click = PythonOperator(
        task_id='baemin_marketing_store_click_df',
        python_callable=baemin_marketing_store_click_df,
    )
    
    task_preprocess_store_click = PythonOperator(
        task_id='preprocess_store_click_df',
        python_callable=preprocess_store_click_df,
    )
    
    task_load_coupangeats_cmg = PythonOperator(
        task_id='coupangeats_cmg_df',
        python_callable=coupangeats_cmg_df,
    )
    
    task_preprocess_coupangeats_cmg = PythonOperator(
        task_id='preprocess_coupangeats_cmg_df',
        python_callable=preprocess_coupangeats_cmg_df,
    )   
    
    task_load_orders_upload = PythonOperator(
        task_id='load_orders_upload_df',
        python_callable=load_orders_upload_df,
    )
    
    task_preprocess_orders_upload = PythonOperator(
        task_id='preprocess_orders_upload_df',
        python_callable=preprocess_orders_upload_df,
    )   
    
    # ============================================================
    # 2️⃣ JOIN 태스크들
    # ============================================================
    task_left_orders_history = PythonOperator(
        task_id='left_join_orders_history',
        python_callable=left_join_orders_history,
        op_kwargs={
            'left_task': {
                'task_id': 'preprocess_orders_upload_df',
                'xcom_key': 'sales_daily_orders_upload_processed_path'
            },
            'right_task': {
                'task_id': 'preprocess_beamin_ad_change_history_df',
                'xcom_key': 'baemin_ad_change_history_processed_path'
            },
            'left_on': ['order_daily_day', '매장명'], 
            'right_on': ['join_date', 'store_names'],
            'how': 'left',
            'drop_right_keys': False,
            'output_xcom_key': 'joined_orders_history_path',
        }
    )
    
    task_left_orders_history_click = PythonOperator(
        task_id='left_join_orders_history_click',
        python_callable=left_join_orders_history_click,
        op_kwargs={
            'left_task': {
                'task_id': 'left_join_orders_history',
                'xcom_key': 'joined_orders_history_path'
            },
            'right_task': {
                'task_id': 'preprocess_store_click_df',
                'xcom_key': 'baemin_marketing_store_click_processed_path'
            },
            'left_on': ['order_daily_day', '매장명'], 
            'right_on': ['join_date', 'store_names'],
            'how': 'left',
            'drop_right_keys': False,
            'output_xcom_key': 'joined_orders_history_click_path',
        }
    )
    
    task_left_orders_history_click_coupang = PythonOperator(
        task_id='left_join_orders_history_click_coupang',
        python_callable=left_join_orders_history_click_coupang,
        op_kwargs={
            'left_task': {
                'task_id': 'left_join_orders_history_click',
                'xcom_key': 'joined_orders_history_click_path'
            },
            'right_task': {
                'task_id': 'preprocess_coupangeats_cmg_df',
                'xcom_key': 'coupangeats_cmg_processed_path'
            },
            'left_on': ['order_daily_day', '매장명'], 
            'right_on': ['조회일자', 'store_names'],
            'how': 'left',
            'drop_right_keys': False,
            'output_xcom_key': 'joined_orders_history_click_coupang_path',
        }
    )
    
    task_preprocess_fin_df = PythonOperator(
        task_id='preprocess_fin_df',
        python_callable=preprocess_fin_df,
    )
    
    task_fin_save_to_csv = PythonOperator(
        task_id='fin_save_to_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_fin_df',
            'input_xcom_key': 'final_processed_path',
            'dedup_key': ['order_daily', '매장명']
        },
    )
    
    cleanup_task = PythonOperator(
        task_id='move_collected_files',
        python_callable=sales_cleanup_collected_csvs,
        op_kwargs={
            'patterns': [
                'baemin_ad_change_history_*.csv',
                'baemin_change_history_*.csv',
                'baemin_marketing_*.csv',
                'coupangeats_cmg_*.csv'
            ],
            'source_dir': str(COLLECT_DB / '영업관리부_수집'),
            'dest_dir': str(DOWN_DIR / '업로드_temp')
        }
    )
    
    # ============================================================
    # 의존성 정의 (⭐ 핵심 수정)
    # ============================================================
    
    # 0. SMD_04 완료 후 모든 로드 시작
    wait_for_smd_04 >> [
        task_load_beamin_ad_change_history,
        task_load_baemin_marketing_store_click,
        task_load_coupangeats_cmg,
        task_load_orders_upload
    ]
    
    # 1. 개별 전처리
    task_load_beamin_ad_change_history >> task_preprocess_beamin_ad_change_history
    task_load_baemin_marketing_store_click >> task_preprocess_store_click
    task_load_coupangeats_cmg >> task_preprocess_coupangeats_cmg
    task_load_orders_upload >> task_preprocess_orders_upload
    
    # 2. JOIN 체인
    [task_preprocess_orders_upload, task_preprocess_beamin_ad_change_history] >> task_left_orders_history
    [task_left_orders_history, task_preprocess_store_click] >> task_left_orders_history_click
    [task_left_orders_history_click, task_preprocess_coupangeats_cmg] >> task_left_orders_history_click_coupang
    
    # 3. 최종 처리
    task_left_orders_history_click_coupang >> task_preprocess_fin_df >> task_fin_save_to_csv >> cleanup_task
