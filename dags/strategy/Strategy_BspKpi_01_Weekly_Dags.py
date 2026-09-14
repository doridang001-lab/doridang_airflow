"""브랜드전략기획팀 주간 KPI 목표·실적 통합 마트 생성 및 미입력 알림.

수동 실행 전용. 예약 실행은 DB_Hall_Sales_Target_Dags의 BSP KPI task가 담당한다.
conf 없는 수동 실행: 직전 완료 주차(실행일이 속한 주의 월요일 - 7일)를 점검
특정 주차 점검 예시: {"week_start": "2026-08-03"}
"""

import importlib
import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.paths import BSP_KPI_WEEKLY_PARQUET

logger = logging.getLogger(__name__)

DAG_ID = Path(__file__).stem

pipeline_module = importlib.import_module(
    "modules.transform.pipelines.strategy.SMP_bsp_kpi_weekly"
)
build_kpi_weekly = pipeline_module.build_kpi_weekly
notify_missing_alert = pipeline_module.notify_missing_alert
resolve_target_week_start = pipeline_module.resolve_target_week_start


def task_build_kpi_weekly(**context) -> str:
    parquet_path = build_kpi_weekly(**context)
    context["ti"].xcom_push(key="parquet_path", value=parquet_path)
    return parquet_path


def task_notify_missing_input(**context) -> str:
    # 알림 task만 단독 clear/재실행해도 동작하도록 XCom이 없으면 마트 상수 경로를 쓴다.
    parquet_path = context["ti"].xcom_pull(task_ids="build_kpi_weekly", key="parquet_path")
    if not parquet_path:
        parquet_path = str(BSP_KPI_WEEKLY_PARQUET)
        logger.info("XCom 경로 없음 → 마트 상수 경로 사용 | path=%s", parquet_path)
    if not Path(parquet_path).is_file():
        raise AirflowException(f"통합 마트 파일이 없습니다: {parquet_path}")

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    try:
        target_week_start = resolve_target_week_start(
            conf, now=context.get("data_interval_end")
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
    dag_id=DAG_ID,
    description="브랜드전략기획팀 주간 KPI 목표·실적 통합 마트 생성 및 미입력 알림",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 3, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": on_failure_callback,
    },
    tags=["strategy", "weekly", "kpi", "bsp_kpi", "dashboard", "powerbi"],
) as dag:
    build_task = PythonOperator(
        task_id="build_kpi_weekly",
        python_callable=task_build_kpi_weekly,
    )

    notify_task = PythonOperator(
        task_id="notify_missing_input",
        python_callable=task_notify_missing_input,
    )

    build_task >> notify_task
