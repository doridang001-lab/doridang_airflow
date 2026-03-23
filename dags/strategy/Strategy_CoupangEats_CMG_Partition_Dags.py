"""
쿠팡이츠 CMG 마케팅 데이터 파티션 저장 및 OneDrive 백업 DAG

Schedule: 매일 22:00 (데이터 수집 후)
Dataset: coupangeats_cmg_*.csv
Output: C:/Users/민준/OneDrive - 주식회사 도리당/data/analytics/coupang_marketing/
        brand=도리당/store={매장명}/ym={YYYY-MM}/data.csv
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.strategy.SMP_CoupangEats_CMG_Partition import (
    load_coupangeats_cmg_partition
)

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    'Strategy_CoupangEats_CMG_Partition_Dags',
    default_args=default_args,
    description='쿠팡이츠 CMG 파티션 저장 및 OneDrive 백업',
    schedule_interval='0 22 * * *',  # 매일 22:00
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['strategy', 'coupang', 'partition'],
)

task_partition = PythonOperator(
    task_id='load_coupangeats_cmg_partition',
    python_callable=load_coupangeats_cmg_partition,
    dag=dag,
)

task_partition
