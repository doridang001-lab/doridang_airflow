from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import enqueue_heal_task, on_retry_callback, send_telegram
from modules.transform.utility.schedule import SMP_TOORDER_VOC_TIME
from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import (
    t1_prepare,
    t2_collect,
    t3_save,
    t4_validate,
)

# ── 수집 기간 설정 ─────────────────────────────────────────────────────
LOOKBACK_DAYS = 1                       # None/1                    → 어제 누적 스냅샷
#                  7                       → scheduled에서는 최신 하루만 사용
#                  "2026-01-01"            → 해당 날짜 ~ 어제
#                  "2026-01-01~2026-01-15" → 해당 범위 고정
# scheduled 실행은 최신 D-1 누적 스냅샷을 기존 파일이 있어도 재수집
# ───────────────────────────────────────────────────────────────────────

KST = pendulum.timezone("Asia/Seoul")


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logical_date = context.get("logical_date") or ti.execution_date
    execution_date = logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
    body = (
        f"DAG: {ti.dag_id}\nTask: {ti.task_id}\n"
        f"실행일시(KST): {execution_date}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    send_telegram(body)
    enqueue_heal_task(context)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder VOC analysis daily collection and parquet save",
    schedule=SMP_TOORDER_VOC_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["sales", "toorder", "voc", "parquet"],
    default_args=default_args,
) as dag:
    prepare = PythonOperator(
        task_id="t1_prepare",
        python_callable=t1_prepare,
        op_kwargs={"lookback_days": LOOKBACK_DAYS},
    )
    collect = PythonOperator(
        task_id="t2_collect",
        python_callable=t2_collect,
        retries=3,  # 브라우저 세션은 재시도 더 많이
    )
    save = PythonOperator(
        task_id="t3_save",
        python_callable=t3_save,
        retries=1,
    )
    validate = PythonOperator(
        task_id="t4_validate",
        python_callable=t4_validate,
        retries=1,
    )

    prepare >> collect >> save >> validate
