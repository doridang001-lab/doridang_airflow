"""
order_menu_structure DAG — unified_sales 신규 주문을 구조화해 PowerBI LEFT JOIN용 테이블에 증분 저장.

unified_sales LEFT JOIN order_menu_structure
  ON sale_date + store + platform + order_id
"""

import logging
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_ItemMaster import (
    classify_items,
    extract_new_orders,
    load_item_master_labels,
    reconstruct_orders,
    save_order_menu,
    save_item_master,
    update_item_master_labels,
)
from modules.transform.utility.mailer import send_email
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import ITEM_MASTER_CHECKPOINT_DIR
from modules.transform.utility.schedule import DB_ITEM_MASTER_TIME

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def on_failure_callback(context):
    ti = context.get("task_instance")
    body = f"DAG: {dag_id}\nTask: {ti.task_id}\n에러: {context.get('exception', '')}\n로그: {ti.log_url}"
    try:
        send_email(
            subject=f"[Airflow 실패] {dag_id}",
            html_content=f"Task: {ti.task_id}<br>오류: {context.get('exception', '')}",
            to_emails="tjrrjwu92@gmail.com",
        )
    except Exception as exc:
        logger.warning("실패 알림 발송 실패: %s", exc)
    send_telegram(body + "\n해결해라")


def task_extract_and_classify(**context):
    # Allow prompt/threshold tuning from Airflow Variables without code changes
    extra_prompt = Variable.get("ITEM_MASTER_LLM_SYSTEM_PROMPT_EXTRA", default_var="").strip()
    if extra_prompt:
        os.environ["ITEM_MASTER_LLM_SYSTEM_PROMPT_EXTRA"] = extra_prompt

    confidence_review = Variable.get("ITEM_MASTER_LLM_CONFIDENCE_REVIEW", default_var="").strip()
    if confidence_review:
        os.environ["ITEM_MASTER_LLM_CONFIDENCE_REVIEW"] = confidence_review

    try:
        from modules.transform.pipelines.db import DB_ItemMaster as _im
        logger.info("ITEM_MASTER LABELS_PATH=%s", getattr(_im, "LABELS_PATH", ""))
        logger.info("ITEM_MASTER FALLBACK_LABELS_PATH=%s", getattr(_im, "FALLBACK_LABELS_PATH", ""))
    except Exception as exc:
        logger.warning("ITEM_MASTER path 로깅 실패(무시): %s", exc)

    df_items = extract_new_orders()

    if df_items.empty:
        df_labels = load_item_master_labels()
        if not df_labels.empty:
            # labels 파일 있음 — item_master만 최신화
            try:
                save_item_master(df_labels)
                logger.info("신규 주문 없음 — item_master만 갱신")
            except Exception as exc:
                logger.warning("item_master 갱신 실패(무시): %s", exc)
        else:
            # labels 파일 없음 — 전체 parquet 재분류로 부트스트랩
            logger.info("labels 없음 — 전체 parquet 부트스트랩 시작")
            try:
                df_all = extract_new_orders(skip_dedup=True)
                if not df_all.empty:
                    df_classified = classify_items(df_all)
                    checkpoint_path = ITEM_MASTER_CHECKPOINT_DIR / "item_master_classified.csv"
                    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                    df_classified.to_csv(checkpoint_path, index=False, encoding="utf-8-sig")
                    df_labels = update_item_master_labels(df_classified)
                    save_item_master(df_labels)
                    context["ti"].xcom_push(key="order_count", value=int(df_all["_pk"].nunique()))
                    context["ti"].xcom_push(key="temp_path", value=str(checkpoint_path))
                    return f"부트스트랩 완료: {df_all['_pk'].nunique()}개 order / {len(df_all)}개 item"
            except Exception as exc:
                logger.warning("부트스트랩 실패(무시): %s", exc)
        context["ti"].xcom_push(key="order_count", value=0)
        context["ti"].xcom_push(key="temp_path", value="")
        return "신규 주문 없음 — 스킵"

    order_count = int(df_items["_pk"].nunique())
    df_classified = classify_items(df_items)
    llm_count = int(df_classified["llm_used"].sum())

    checkpoint_path = ITEM_MASTER_CHECKPOINT_DIR / "item_master_classified.csv"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not checkpoint_path.exists()
    df_classified.to_csv(checkpoint_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")

    # item_id 기반 labels + item_master 차원 테이블 갱신
    try:
        df_labels = update_item_master_labels(df_classified)
        save_item_master(df_labels)
    except Exception as exc:
        logger.warning("item_master 갱신 실패(무시하고 계속): %s", exc)

    context["ti"].xcom_push(key="order_count", value=order_count)
    context["ti"].xcom_push(key="temp_path", value=str(checkpoint_path))
    return f"분류 완료: {order_count}개 주문 / {len(df_items)}개 item / LLM {llm_count}건"


def task_reconstruct_and_save(**context):
    # Airflow Variable로 체크포인트 경로를 직접 지정하면 XCom 대신 그 파일을 사용
    override = Variable.get("ITEM_MASTER_CHECKPOINT_PATH", default_var="").strip()
    temp_path_str = override or context["ti"].xcom_pull(task_ids="extract_and_classify", key="temp_path")
    if not temp_path_str:
        return "데이터 없음 — 스킵"

    df_classified = pd.read_csv(temp_path_str, encoding="utf-8-sig")
    df_orders = reconstruct_orders(df_classified)
    result = save_order_menu(df_orders)

    return (
        f"order_menu_structure: 신규 {result['inserted']}건 저장, "
        f"중복 {result['duplicated']}건 스킵, 전체 {result['total']}건"
    )


with DAG(
    dag_id=dag_id,
    schedule=DB_ITEM_MASTER_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "item_master", "order_structure", "powerbi"],
    on_failure_callback=on_failure_callback,
) as dag:

    t1 = PythonOperator(
        task_id="extract_and_classify",
        python_callable=task_extract_and_classify,
        on_failure_callback=on_failure_callback,
    )

    t2 = PythonOperator(
        task_id="reconstruct_and_save",
        python_callable=task_reconstruct_and_save,
        on_failure_callback=on_failure_callback,
    )

    t1 >> t2
