import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db_file_merge_pipeline import (
    load_df as load_reupload_toorder_review,
    preprocess_toorder_review_df,
    fin_save_to_csv,
    load_baemin_ad_change_history_df,
    preprocess_baemin_ad_change_history_df,
    baemin_ad_change_history_save_to_csv
)

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 11 * * 1,3",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    task_load_reupload = PythonOperator(
        task_id='load_reupload_toorder_review',
        python_callable=load_reupload_toorder_review,
    )
    
    task_preprocess = PythonOperator(
        task_id='preprocess_toorder_review',
        python_callable=preprocess_toorder_review_df,
        op_kwargs={
            'input_task_id': 'load_reupload_toorder_review',
            'input_xcom_key': 'toorder_review_path',
            'output_xcom_key': 'processed_toorder_review_path',
        }
    )
    
    task_save_csv = PythonOperator(
        task_id='save_toorder_review_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_toorder_review',
            'input_xcom_key': 'processed_toorder_review_path',
            'output_filename': 'toorder_review_doridang.csv',
        }
    )
    
    task_load_baemin_ad_change_history = PythonOperator(
        task_id='load_baemin_ad_change_history',
        python_callable=load_baemin_ad_change_history_df,
    )
    
    task_preprocess_baemin_ad_change_history = PythonOperator(
        task_id='preprocess_baemin_ad_change_history',
        python_callable=preprocess_baemin_ad_change_history_df,
        op_kwargs={
            'input_task_id': 'load_baemin_ad_change_history',
            'input_xcom_key': 'baemin_ad_change_history_path',
            'output_xcom_key': 'processed_baemin_ad_change_history_path',
        }
    )
    
    task_baemin_ad_change_history_save_csv = PythonOperator(
        task_id='baemin_ad_change_history_save_to_csv',
        python_callable=baemin_ad_change_history_save_to_csv,
        op_kwargs={
            'input_task_id': 'preprocess_baemin_ad_change_history',
            'input_xcom_key': 'processed_baemin_ad_change_history_path',
        }
    )

    task_load_reupload >> task_preprocess >> task_save_csv
    task_load_baemin_ad_change_history >> task_preprocess_baemin_ad_change_history >> task_baemin_ad_change_history_save_csv
