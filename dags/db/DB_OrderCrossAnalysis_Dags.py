from __future__ import annotations

import logging
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from modules.transform.pipelines.db.DB_OrderCrossAnalysis import (
    backup_order_cross_to_onedrive,
    backfill_order_cross_analysis,
    run_lookback_order_cross_analysis,
    run_order_cross_analysis,
    validate_order_cross,
    StaleCrossInputError,
)
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_ORDER_CROSS_ANALYSIS_TIME

logger = logging.getLogger(__name__)

dag_id = "DB_OrderCrossAnalysis_Dags"
LOOKBACK_DAYS = 3


def _on_failure_callback(context):
    ti = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {ti.log_url}"
    )
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def resolve_date(**context) -> str:
    conf = _conf(context)
    sale_date = conf.get("sale_date")
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)
    if _truthy(conf.get("backfill")):
        return "전체 backfill 모드"
    if sale_date:
        return f"정정 날짜: {sale_date}"
    if LOOKBACK_DAYS is None:
        return "LOOKBACK_DAYS=None - 전체 재생성 모드"
    return f"Lookback {LOOKBACK_DAYS}일 모드"


def build_cross(**context) -> str:
    conf = _conf(context)
    # 운영 승인은 변수로 지속하고, 단일 승인 실행은 conf로 지정한다.
    enabled = _truthy(conf.get("publish")) or _truthy(
        Variable.get("order_cross_publish_enabled", default_var="false")
    )
    if not enabled:
        raise AirflowSkipException("교차분석 운영 반영 승인 대기: 로컬 검증 결과 확인 필요")
    if _truthy(conf.get("backfill")):
        result = backfill_order_cross_analysis(allow_publish=True)
        context["ti"].xcom_push(key="processed_dates", value=json.loads(result)["dates"])
        return result

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        result = run_order_cross_analysis(sale_date, overwrite=True, allow_publish=True)
        context["ti"].xcom_push(key="processed_dates", value=[sale_date])
        return result
    result = run_lookback_order_cross_analysis(days=LOOKBACK_DAYS, allow_publish=True)
    context["ti"].xcom_push(key="processed_dates", value=json.loads(result)["dates"])
    return result


def backup_onedrive(**context) -> str:
    conf = _conf(context)
    if not _truthy(conf.get("onedrive_backup")):
        return "onedrive_backup=false - 백업 스킵"
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return backup_order_cross_to_onedrive(sale_date)


def validate_cross(**context) -> str:
    dates = context["ti"].xcom_pull(task_ids="build_cross", key="processed_dates")
    if dates is None:
        raise ValueError("검증 대상 날짜 기록 누락")
    results = []
    for date in dates:
        try:
            results.append(validate_order_cross(date))
        except StaleCrossInputError as exc:
            raise AirflowSkipException(
                f"{date}: {exc} - 동시 갱신으로 인한 대기, 다음 실행에서 재검증"
            ) from exc
    return " | ".join(results) or "입력 변경 없음: 재계산 대상 0일"


with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule=DB_ORDER_CROSS_ANALYSIS_TIME,
    start_date=datetime(2026, 7, 8, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    tags=["db", "mart", "order_cross"],
) as dag:
    t1_resolve_date = PythonOperator(
        task_id="resolve_date",
        python_callable=resolve_date,
    )

    t2_build_cross = PythonOperator(
        task_id="build_cross",
        python_callable=build_cross,
    )

    t3_backup_onedrive = PythonOperator(
        task_id="backup_onedrive",
        python_callable=backup_onedrive,
    )

    t4_validate_cross = PythonOperator(
        task_id="validate_cross",
        python_callable=validate_cross,
    )

    t1_resolve_date >> t2_build_cross >> t3_backup_onedrive >> t4_validate_cross
