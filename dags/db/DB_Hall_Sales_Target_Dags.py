"""
홀 매장 주간 매출 실적 vs 목표 DAG

처리 흐름:
1. unified_sales parquet → 주간 집계 CSV (hall_sale_target.csv)
2. CSV + 마케팅 CSV → 주간보고 Excel (hall_weekly_report.xlsx)

실행: DB_UnifiedSales(10:30) 완료 후 10:55 실행 (매주 월요일)
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_HALL_SALES_TARGET_TIME
from modules.transform.pipelines.db.DB_Hall_Sales_Target import build_hall_sales_target
from modules.transform.pipelines.db.DB_Hall_Sales_Excel import (
    append_weekly_ai_log,
    build_weekly_report_excel,
)
from modules.transform.pipelines.db.DB_Hall_Daily_Excel import build_daily_tracking_excel
# 목표치 계산은 airflow 비의존 헬퍼로 분리 (로컬 Windows 스크립트와 공유)
from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets

# 매출/마케팅 목표치 (매월 업데이트는 DB_Hall_Sales_Target_config.py 에서)
MONTHLY_TARGETS, MARKETING_MONTHLY_TARGETS, DAILY_TRACKING_TARGET = build_targets()

dag_id = Path(__file__).stem

with DAG(
    dag_id=dag_id,
    schedule=DB_HALL_SALES_TARGET_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["db", "hall", "sales_target"],
) as dag:

    t_csv = PythonOperator(
        task_id="build_hall_sales_target",
        python_callable=build_hall_sales_target,
        op_kwargs={"monthly_targets": MONTHLY_TARGETS},
    )

    t_excel = PythonOperator(
        task_id="build_weekly_report_excel",
        python_callable=build_weekly_report_excel,
        op_kwargs={
            "monthly_targets":           MONTHLY_TARGETS,
            "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
        },
    )

    t_daily_excel = PythonOperator(
        task_id="build_daily_tracking_excel",
        python_callable=build_daily_tracking_excel,
        op_kwargs={
            "monthly_targets":           MONTHLY_TARGETS,
            "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
            "daily_target":              DAILY_TRACKING_TARGET,
        },
    )

    t_llm_log = PythonOperator(
        task_id="append_weekly_ai_log",
        python_callable=append_weekly_ai_log,
    )

    t_csv >> [t_excel, t_daily_excel]
    t_excel >> t_llm_log
