"""
배민 마케팅 DB DAG

baemin_marketing_*.csv 파일을 수집 → 전처리 → 파티션 CSV 저장하는
수동 실행 전용 파이프라인.

파티션 구조: ANALYTICS_DB/baemin_marketing/brand=X/store=Y/ym=Z/baemin_marketing_data.csv
동일 날짜 중복 데이터는 collected_at 최신 1건만 유지 (idempotent).
실행 이력: ANALYTICS_DB/baemin_marketing/log.parquet
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.db_baemin_marketing import (
    load_baemin_marketing_files,
    transform_baemin_marketing,
    save_partitioned_csv,
    write_baemin_log,
)
from modules.transform.utility.notifier import on_failure_callback

# ==================================================
# DAG 정의
# ==================================================
filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),   # db_baemin_marketing_dags
    schedule='0 16 * * 1',                         # 매주 월요일 오후 4시 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    },
    tags=['db', 'baemin', 'marketing'],
) as dag:

    load_task = PythonOperator(
        task_id='load_baemin_marketing',
        python_callable=load_baemin_marketing_files,
    )

    transform_task = PythonOperator(
        task_id='transform_baemin_marketing',
        python_callable=transform_baemin_marketing,
    )

    save_task = PythonOperator(
        task_id='save_partitioned_csv',
        python_callable=save_partitioned_csv,
    )

    log_task = PythonOperator(
        task_id='write_log',
        python_callable=write_baemin_log,
    )

    load_task >> transform_task >> save_task >> log_task
