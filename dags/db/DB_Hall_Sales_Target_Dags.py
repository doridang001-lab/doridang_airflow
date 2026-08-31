"""
홀 매장 주간 매출 실적 vs 목표 DAG

처리 흐름:
1. unified_sales parquet → 주간 집계 CSV (hall_sale_target.csv)
2. CSV + 마케팅 CSV → 주간보고 Excel (hall_weekly_report.xlsx)

실행: DB_UnifiedSales 갱신 완료 후 11:00 실행 (매주 월·화요일)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_HALL_SALES_TARGET_TIME
from modules.transform.pipelines.db.DB_Hall_Marketing_Sync import sync_naver_marketing
from modules.transform.pipelines.db.DB_Hall_Sales_Target import build_hall_sales_target
from modules.transform.pipelines.db.DB_Hall_Sales_Excel import (
    append_weekly_ai_log,
    build_weekly_report_excel,
)
from modules.transform.pipelines.db.DB_Hall_Daily_Excel import build_daily_tracking_excel
from modules.transform.pipelines.db.DB_Hall_Daily_Excel import build_daily_tracking_csv
from modules.transform.pipelines.db.DB_Bsp_Monthly_Kpi import sync_monthly_kpi
from modules.transform.pipelines.strategy.SMP_bsp_kpi_weekly import (
    build_kpi_weekly,
    notify_missing_alert,
    resolve_target_week_start,
)
# 목표치 계산은 airflow 비의존 헬퍼로 분리 (로컬 Windows 스크립트와 공유)
from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.paths import BSP_KPI_WEEKLY_PARQUET

# 매출/마케팅 목표치 (매월 업데이트는 DB_Hall_Sales_Target_config.py 에서)
MONTHLY_TARGETS, MARKETING_MONTHLY_TARGETS, DAILY_TRACKING_TARGET = build_targets()

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem


def task_build_bsp_kpi_weekly(**context) -> str:
    """브랜드전략기획팀 주간 KPI mart를 생성하고 parquet 경로를 XCom에 남긴다."""
    parquet_path = build_kpi_weekly(**context)
    context["ti"].xcom_push(key="parquet_path", value=parquet_path)
    return parquet_path


def task_notify_bsp_kpi_missing_input(**context) -> str:
    """BSP KPI 주간 실적 미입력 알림을 발송한다."""
    parquet_path = context["ti"].xcom_pull(
        task_ids="build_bsp_kpi_weekly",
        key="parquet_path",
    )
    if not parquet_path:
        parquet_path = str(BSP_KPI_WEEKLY_PARQUET)
        logger.info("BSP KPI XCom 경로 없음 -> 마트 상수 경로 사용 | path=%s", parquet_path)
    if not Path(parquet_path).is_file():
        raise AirflowException(f"BSP KPI 통합 마트 파일이 없습니다: {parquet_path}")

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    try:
        target_week_start = resolve_target_week_start(
            conf,
            now=context.get("data_interval_end"),
        )
    except ValueError as exc:
        raise AirflowException(str(exc)) from exc

    df = pd.read_parquet(parquet_path)
    result = notify_missing_alert(df, target_week_start=target_week_start)
    logger.info(
        "BSP KPI 미입력 점검 | week=%s checked=%s missing=%s telegram=%s emails=%s skipped=%s",
        result.target_week_start,
        result.checked_count,
        result.missing_count,
        result.sent_telegram,
        list(result.sent_emails),
        result.skipped_reason,
    )
    return (
        f"주차={result.target_week_start} 점검={result.checked_count} "
        f"미입력={result.missing_count}"
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
        python_callable=build_hall_sales_target,
        op_kwargs={"monthly_targets": MONTHLY_TARGETS},
    )

    t_sync_mkt = PythonOperator(
        task_id="sync_naver_marketing",
        python_callable=sync_naver_marketing,
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

    t_daily_csv = PythonOperator(
        task_id="build_daily_tracking_csv",
        python_callable=build_daily_tracking_csv,
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

    t_monthly_kpi = PythonOperator(
        task_id="sync_monthly_kpi",
        python_callable=sync_monthly_kpi,
    )

    t_bsp_weekly = PythonOperator(
        task_id="build_bsp_kpi_weekly",
        python_callable=task_build_bsp_kpi_weekly,
    )

    t_bsp_notify = PythonOperator(
        task_id="notify_bsp_kpi_missing_input",
        python_callable=task_notify_bsp_kpi_missing_input,
    )

    t_csv >> [t_excel, t_daily_excel, t_daily_csv]
    t_sync_mkt >> [t_excel, t_daily_excel, t_daily_csv]
    t_excel >> t_llm_log
    t_csv >> t_monthly_kpi >> t_bsp_weekly >> t_bsp_notify
