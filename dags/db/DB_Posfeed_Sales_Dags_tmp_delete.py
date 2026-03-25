"""
Posfeed 백필 DAG (임시 — 사용 후 삭제)

DOWN_DIR 에 수동으로 쌓인 order-info-list_*.xlsx 파일을
일괄로 OneDrive 파티션에 적재한다.

- schedule=None: 수동 트리거만 허용
- 처리 완료 파일은 DOWN_DIR/업로드_temp/ 로 이동
"""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Posfeed_Sales import backfill_xlsx_to_onedrive

dag_id = Path(__file__).stem

with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['db', 'posfeed', 'backfill', 'onedrive'],
) as dag:

    backfill_task = PythonOperator(
        task_id='backfill_xlsx_to_onedrive',
        python_callable=backfill_xlsx_to_onedrive,
    )
