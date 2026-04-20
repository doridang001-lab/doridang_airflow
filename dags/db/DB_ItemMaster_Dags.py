"""
item_master 생성·증분 업데이트 DAG

최초 실행: 기존 unified_sales parquet 전체 스캔 → LLM 분류 → DB 저장
이후 실행: 신규 unique item만 증분 분류·저장
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_ItemMaster import (
    classify_and_save_with_checkpoints,
    extract_unique_items,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def task_extract(**context):
    df = extract_unique_items()
    context["ti"].xcom_push(key="item_count", value=len(df))
    df.to_json(orient="records", force_ascii=False)  # XCom 직렬화용 (로그)
    logger.info("unique item: %d건", len(df))
    # parquet 경로 대신 DataFrame을 모듈 수준 캐시로 넘기기 어려우므로
    # t2에서 재추출 방식 사용 (항목 수가 적어 부담 없음)
    return f"unique item 추출: {len(df)}건"


def task_classify_and_save(**context):
    df = extract_unique_items()
    result = classify_and_save_with_checkpoints(df, chunk_size=300)
    if result.get("csv_fallback"):
        return (
            f"item_master: DB 저장 실패(CSV 보존) — 전체 {result['total']}건 | "
            f"CSV 경로: {result['csv_fallback']}"
        )
    return (
        f"item_master: 신규 {result['inserted']}건 저장, "
        f"중복 {result['duplicated']}건 스킵, 전체 {result['total']}건"
    )


with DAG(
    dag_id=dag_id,
    schedule=None,  # 수동 트리거 (최초 1회 + 필요 시)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "item_master", "llm", "okpos"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_unique_items",
        python_callable=task_extract,
    )

    t2 = PythonOperator(
        task_id="classify_and_save",
        python_callable=task_classify_and_save,
    )

    t1 >> t2
