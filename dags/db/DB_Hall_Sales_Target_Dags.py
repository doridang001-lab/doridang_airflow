"""
홀 매장 주간 매출 실적 vs 목표 DAG

처리 흐름:
1. unified_sales parquet → 주간 집계 CSV (hall_sale_target.csv)
2. CSV + 마케팅 CSV → 주간보고 Excel (hall_weekly_report.xlsx)

실행: DB_UnifiedSales 갱신 완료 후 11:00 실행 (매주 월요일)
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
    check_marketing_input,
)
from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv
# 목표치 계산은 airflow 비의존 헬퍼로 분리 (로컬 Windows 스크립트와 공유)
from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
from modules.transform.utility.notifier import on_failure_callback


dag_id = Path(__file__).stem


def _build_hall_sales_target() -> str:
    # 목표 JSON은 사용자가 수시로 바꿀 수 있으므로 태스크 실행 시점에 읽는다.
    monthly_targets, _, _ = build_targets()
    return build_hall_sales_target(monthly_targets)


def _build_weekly_report_excel() -> str:
    # DAG 파싱 시점 값이 아니라 실행 시점의 hall_sale_target_input.json을 기준으로 한다.
    monthly_targets, marketing_monthly_targets, _ = build_targets()
    return build_weekly_report_excel(monthly_targets, marketing_monthly_targets)


def _build_daily_tracking_csv(ym: str = "") -> str:
    # ym이 비어 있으면 build_daily_tracking_csv 내부에서 당월로 처리된다.
    monthly_targets, marketing_monthly_targets, daily_tracking_target = build_targets()
    return build_daily_tracking_csv(
        monthly_targets,
        marketing_monthly_targets,
        daily_tracking_target,
        ym=ym,
    )


with DAG(
    dag_id=dag_id,
    schedule=DB_HALL_SALES_TARGET_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": on_failure_callback,
    },
    tags=["db", "hall", "sales_target"],
) as dag:

    t_csv = PythonOperator(
        task_id="build_hall_sales_target",
        python_callable=_build_hall_sales_target,
    )

    t_excel = PythonOperator(
        task_id="build_weekly_report_excel",
        python_callable=_build_weekly_report_excel,
    )

    t_daily_csv = PythonOperator(
        task_id="build_daily_tracking_csv",
        python_callable=_build_daily_tracking_csv,
        op_kwargs={
            "ym":                        "{{ dag_run.conf.get('ym') or '' }}",
        },
    )

    t_check_mkt = PythonOperator(
        task_id="check_marketing_input",
        python_callable=check_marketing_input,
    )

    t_llm_log = PythonOperator(
        task_id="append_weekly_ai_log",
        python_callable=append_weekly_ai_log,
    )

    t_csv >> [t_excel, t_daily_csv, t_check_mkt]
    t_excel >> t_llm_log
