"""송파삼전점 메뉴 계층 시험 주문서 CSV 생성 DAG."""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_MenuHierarchy_Test import (
    build_orders as pipeline_build_orders,
    resolve_yms as pipeline_resolve_yms,
)
from modules.transform.utility.notifier import on_failure_callback

logger = logging.getLogger(__name__)

MENU_HIERARCHY_TEST_SCHEDULE = "0 13,8 * * *"


def resolve_ym(**context) -> list[str]:
    yms = pipeline_resolve_yms(None)
    context["ti"].xcom_push(key="ym_list", value=yms)
    logger.info("메뉴계층 시험 대상 yms=%s store=송파삼전점", yms)
    return yms


def _target_yms(context) -> list[str]:
    return context["ti"].xcom_pull(task_ids="resolve_ym", key="ym_list") or pipeline_resolve_yms(None)


def build_orders(**context) -> str:
    return pipeline_build_orders(_target_yms(context))


default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id=Path(__file__).stem,
    description="송파삼전점 메뉴 계층 시험 주문서를 CSV로 생성",
    start_date=pendulum.datetime(2026, 7, 31, tz="Asia/Seoul"),
    schedule=MENU_HIERARCHY_TEST_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "menu-hierarchy", "test"],
) as dag:
    t_resolve_ym = PythonOperator(
        task_id="resolve_ym",
        python_callable=resolve_ym,
    )

    t_build_orders = PythonOperator(
        task_id="build_orders",
        python_callable=build_orders,
    )

    t_resolve_ym >> t_build_orders
    
# 화서, 부서옥길, 대화
