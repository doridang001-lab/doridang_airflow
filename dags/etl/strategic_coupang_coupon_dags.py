import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.strategic_coupang_coupon import (
    load_coupang_coupon_df,
    preprocess_coupang_coupon_df,
    coupon_fin_save_to_csv
)

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['02_sales', 'coupang'],
) as dag:
    
    task_load = PythonOperator(
        task_id='load_coupang_coupon',
        python_callable=load_coupang_coupon_df,
    )
    
    task_preprocess = PythonOperator(
        task_id='preprocess_coupang_coupon',
        python_callable=preprocess_coupang_coupon_df,
    )
    
    task_save = PythonOperator(
        task_id='save_coupang_coupon_csv',
        python_callable=coupon_fin_save_to_csv,
    )

    task_load >> task_preprocess >> task_save
