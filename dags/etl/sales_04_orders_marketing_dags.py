import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
from pathlib import Path
import datetime as dt

# sales_operation_now_dags.py 파일 상단
from modules.transform.pipelines.sales_04_orders_marketing import (
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
    preprocess_fin_df, # 전처리
    fin_save_to_csv # 저장
)

from modules.transform.utility.io3 import join_dataframes

filename = os.path.basename(__file__)

# 1. 주문 로직 정의
def merge_orders_history_df(left_df, right_df):
    """주문 데이터 + 변경 이력 병합"""
    
    # 1. 날짜 전처리 (merge 전에)
    left_df["order_daily_day"] = (
        pd.to_datetime(
            left_df["order_daily"].astype(str),
            format="mixed",
            errors="coerce"
        )
        .dt.normalize()  # 00:00:00으로 정규화
    )
    
    # 2. 병합
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
    schedule="0 11 * * 1,3",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    # 변경이력 수집
    task_load_beamin_ad_change_history = PythonOperator(
        task_id='load_beamin_ad_change_history_df',
        python_callable=load_beamin_ad_change_history_df,
    )
    
    # 변경이력 전처리
    task_preprocess_beamin_ad_change_history = PythonOperator(
        task_id='preprocess_beamin_ad_change_history_df',
        python_callable=preprocess_beamin_ad_change_history_df,
    )
    
    
    # store_click_df 수집
    task_load_baemin_marketing_store_click = PythonOperator(
        task_id='baemin_marketing_store_click_df',
        python_callable=baemin_marketing_store_click_df,
    )
    
    # store_click_df 전처리
    task_preprocess_store_click = PythonOperator(
        task_id='preprocess_store_click_df',
        python_callable=preprocess_store_click_df,
    )
    
    
    
    # 쿠팡광고 수집
    task_load_coupangeats_cmg = PythonOperator(
        task_id='coupangeats_cmg_df',
        python_callable=coupangeats_cmg_df,
    )
    
    # 쿠팡광고 전처리
    task_preprocess_coupangeats_cmg = PythonOperator(
        task_id='preprocess_coupangeats_cmg_df',
        python_callable=preprocess_coupangeats_cmg_df,
    )   
        
    
    # 주문데이터 수집
    task_load_orders_upload = PythonOperator(
        task_id='load_orders_upload_df',
        python_callable=load_orders_upload_df,
    )
    
    # 주문데이터 전처리
    task_preprocess_orders_upload = PythonOperator(
        task_id='preprocess_orders_upload_df',
        python_callable=preprocess_orders_upload_df,
    )   

    # 병합
    # 주문 + 변경이력 left 조인
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
    
    ## 주문 + 변경이력 + 우가클 left 조인
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
    
    ## 주문 + 변경이력 + 우가클 left 조인 + 쿠팡광고
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
    
    # 최종 전처리
    task_preprocess_fin_df = PythonOperator(
        task_id='preprocess_fin_df',
        python_callable=preprocess_fin_df,
        op_kwargs={
            'input_xcom_key': 'joined_orders_history_click_coupang_path',
            'output_xcom_key': 'final_processed_path',
        },
    )
    
    # 2. 최종 저장 태스크
    task_fin_save_to_csv = PythonOperator(
        task_id='fin_save_to_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_fin_df',  # 전처리 태스크 ID
            'input_xcom_key': 'final_processed_path' # 저장된 XCom 키
        },
    )



# 1. 개별 수집 및 전처리 흐름
task_load_beamin_ad_change_history >> task_preprocess_beamin_ad_change_history
task_load_baemin_marketing_store_click >> task_preprocess_store_click
task_load_coupangeats_cmg >> task_preprocess_coupangeats_cmg
task_load_orders_upload >> task_preprocess_orders_upload

# 2. 병합 흐름 (파이프라인)
# 주문 데이터 전처리가 완료되면 -> 변경이력 병합 -> 그 결과에 우리가게클릭 병합
task_preprocess_orders_upload >> task_left_orders_history

# 변경이력 전처리가 완료되어야 병합이 가능함
task_preprocess_beamin_ad_change_history >> task_left_orders_history

# 우리가게클릭 전처리가 완료되어야 병합이 가능함
task_preprocess_store_click >> task_left_orders_history_click

# (주문+변경이력) 결과와 (우리가게클릭) 전처리가 완료되면 최종 병합
[task_left_orders_history, task_preprocess_store_click] >> task_left_orders_history_click

# (주문+변경이력+우리가게클릭) 결과와 (쿠팡광고) 전처리가 완료되면 최종 병합
[task_left_orders_history_click, task_preprocess_coupangeats_cmg] >> task_left_orders_history_click_coupang >> task_preprocess_fin_df >> task_fin_save_to_csv