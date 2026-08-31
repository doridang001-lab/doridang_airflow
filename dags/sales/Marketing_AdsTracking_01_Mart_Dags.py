"""
광고 프로젝트 성과 추적 마트 DAG.

Flow에 등록된 광고 프로젝트와 네이버/당근 광고 실적을 광고 ID로 연결해
mart/Marketing_Ads_Tracking 아래 일별/ID매핑/Flow 비교/Flow 업무 태그 CSV를 만든다.
"""

from datetime import timedelta
from pathlib import Path

import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.BSP_MarketingAds_Mart import (
    annotate_ads_daily_with_flow_tasks,
    build_ads_daily_mart,
    build_campaign_table,
    build_flow_ad_performance_mart,
    notify_missing_collection,
    parse_flow_campaigns,
)
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import BSP_MARKETING_ADS_MART_TIME

logger = logging.getLogger(__name__)


def task_parse_flow_campaigns(**context):
    payload = parse_flow_campaigns(**context)
    context["ti"].xcom_push(key="campaign_payload", value=payload)
    logger.info("Flow 광고 캠페인 파싱: %s건", len(payload.get("campaigns") or []))
    return payload


def task_build_ads_daily_mart(**context):
    payload = context["ti"].xcom_pull(task_ids="task_parse_flow_campaigns", key="campaign_payload")
    message = build_ads_daily_mart(payload or {}, **context)
    context["ti"].xcom_push(key="daily_message", value=message)
    return message


def task_build_campaign_table(**context):
    payload = context["ti"].xcom_pull(task_ids="task_parse_flow_campaigns", key="campaign_payload")
    message = build_campaign_table(payload or {}, **context)
    context["ti"].xcom_push(key="campaign_message", value=message)
    return message


def task_build_flow_ad_performance_mart(**context):
    message = build_flow_ad_performance_mart(**context)
    context["ti"].xcom_push(key="flow_compare_message", value=message)
    return message


def task_annotate_ads_daily_with_flow_tasks(**context):
    message = annotate_ads_daily_with_flow_tasks(**context)
    context["ti"].xcom_push(key="daily_flow_tasks_message", value=message)
    return message


def task_notify_missing_collection(**context):
    message = notify_missing_collection(**context)
    context["ti"].xcom_push(key="alert_message", value=message)
    return message


def task_write_log(**context):
    ti = context["ti"]
    daily_message = ti.xcom_pull(task_ids="task_build_ads_daily_mart", key="daily_message")
    campaign_message = ti.xcom_pull(task_ids="task_build_campaign_table", key="campaign_message")
    flow_compare_message = ti.xcom_pull(task_ids="task_build_flow_ad_performance_mart", key="flow_compare_message")
    daily_flow_tasks_message = ti.xcom_pull(task_ids="task_annotate_ads_daily_with_flow_tasks", key="daily_flow_tasks_message")
    alert_message = ti.xcom_pull(task_ids="task_notify_missing_collection", key="alert_message")
    message = (
        f"{daily_message or '일별 마트 없음'} / {campaign_message or '캠페인 테이블 없음'} / "
        f"{flow_compare_message or 'Flow 비교 마트 없음'} / "
        f"{daily_flow_tasks_message or 'Flow 업무 태그 없음'} / "
        f"{alert_message or '수집 누락 점검 없음'}"
    )
    logger.info("광고 성과 추적 마트 DAG 종료: %s", message)
    return message


with DAG(
    dag_id=Path(__file__).stem,
    description="Flow 광고 프로젝트 + 네이버/당근 광고 ID 매핑 마트 생성",
    schedule=BSP_MARKETING_ADS_MART_TIME,
    start_date=pendulum.datetime(2026, 8, 26, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["02_transform", "marketing", "ads", "flow", "mart", "daily"],
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
) as dag:
    t1 = PythonOperator(
        task_id="task_parse_flow_campaigns",
        python_callable=task_parse_flow_campaigns,
        show_return_value_in_logs=False,
    )
    t2 = PythonOperator(
        task_id="task_build_ads_daily_mart",
        python_callable=task_build_ads_daily_mart,
    )
    t3 = PythonOperator(
        task_id="task_build_campaign_table",
        python_callable=task_build_campaign_table,
    )
    t4 = PythonOperator(
        task_id="task_build_flow_ad_performance_mart",
        python_callable=task_build_flow_ad_performance_mart,
    )
    t5 = PythonOperator(
        task_id="task_annotate_ads_daily_with_flow_tasks",
        python_callable=task_annotate_ads_daily_with_flow_tasks,
    )
    t6 = PythonOperator(
        task_id="task_notify_missing_collection",
        python_callable=task_notify_missing_collection,
    )
    t7 = PythonOperator(
        task_id="task_write_log",
        python_callable=task_write_log,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
