"""
투오더 VOC 전처리 DAG

변경사항:
- review_df_preprocess_df 태스크 내부에서 LLM secondary_category 분류 처리
"""

import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.transform.pipelines.strategy.SMP_crawling_toorder_voc_02_trans import (
    load_toorder_voc_df,
    load_toorder_voc_upload_temp_df,
    voc_df_store_summary_preprocess_df,
    voc_df_store_topic_summary_preprocess_df,
    review_df_preprocess_df,
    move_processed_voc_files,
    upload_store_summary_to_gsheet,
    upload_topic_summary_to_gsheet,
    upload_review_summary_to_gsheet,
)
from modules.transform.utility.io import SMP_TOORDER_VOC_TIME

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMP_TOORDER_VOC_TIME,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["02_transform", "toorder", "voc", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:

    # ── 01 DAG 완료 대기 ───────────────────────────────────────────
    wait_for_crawling = ExternalTaskSensor(
        task_id='wait_for_crawling',
        external_dag_id='Strategy_ToOrderVoc_01_Crawl_Dags',
        external_task_id='move_voc_files',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        timeout=3600,
        poke_interval=60,
    )

    # ── 파일 로드 ──────────────────────────────────────────────────
    load_voc_df_task = PythonOperator(
        task_id='load_toorder_voc_df',
        python_callable=load_toorder_voc_df,
    )

    load_voc_upload_temp_df_task = PythonOperator(
        task_id='load_toorder_voc_upload_temp_df',
        python_callable=load_toorder_voc_upload_temp_df,
    )

    # ── 집계 전처리 ────────────────────────────────────────────────
    store_summary_task = PythonOperator(
        task_id='voc_df_store_summary_preprocess_df',
        python_callable=voc_df_store_summary_preprocess_df,
        op_kwargs={
            'input_xcom_key': 'toorder_voc_upload_temp_path',
            'output_xcom_key': 'toorder_voc_store_summary_path',
        }
    )

    topic_summary_task = PythonOperator(
        task_id='voc_df_store_topic_summary_preprocess_df',
        python_callable=voc_df_store_topic_summary_preprocess_df,
        op_kwargs={
            'input_xcom_key': 'toorder_voc_upload_temp_path',
            'output_xcom_key': 'toorder_voc_store_topic_summary_path',
        }
    )

    # ── 리뷰 전처리 ────────────────────────────────────────────────
    review_summary_task = PythonOperator(
        task_id='review_df_preprocess_df',
        python_callable=review_df_preprocess_df,
        op_kwargs={
            'input_xcom_key': 'toorder_voc_path',
            'output_xcom_key': 'toorder_voc_review_summary_path',
        },
        # LLM 처리 시간 고려해 타임아웃 넉넉하게
        execution_timeout=pendulum.duration(hours=2),
    )

    # ── GSheet 업로드 ──────────────────────────────────────────────
    upload_store_task = PythonOperator(
        task_id='upload_store_summary_to_gsheet',
        python_callable=upload_store_summary_to_gsheet,
    )

    upload_topic_task = PythonOperator(
        task_id='upload_topic_summary_to_gsheet',
        python_callable=upload_topic_summary_to_gsheet,
    )

    upload_review_task = PythonOperator(
        task_id='upload_review_summary_to_gsheet',
        python_callable=upload_review_summary_to_gsheet,
    )

    move_processed_files_task = PythonOperator(
        task_id='move_processed_voc_files',
        python_callable=move_processed_voc_files,
    )

    # ── 의존성 ────────────────────────────────────────────────────
    # 01 완료 → 로드
    wait_for_crawling >> [load_voc_df_task, load_voc_upload_temp_df_task]

    # 리뷰는 DOWN_DIR 기준, 집계는 업로드_temp 기준
    load_voc_df_task >> review_summary_task
    load_voc_upload_temp_df_task >> [store_summary_task, topic_summary_task]

    # 각 전처리 → 각 업로드
    store_summary_task >> upload_store_task
    topic_summary_task >> upload_topic_task
    review_summary_task >> upload_review_task

    # 모든 업로드 완료 후 원본 파일 이동
    [upload_store_task, upload_topic_task, upload_review_task] >> move_processed_files_task