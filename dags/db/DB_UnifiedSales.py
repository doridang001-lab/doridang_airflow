"""
unified_sales 일별 생성 DAG (toorder + okpos)

처리 흐름 (일반):
1. 날짜 결정 (conf['sale_date'] 있으면 정정 모드, 없으면 Lookback 7일 모드)
2. t2: toorder → Lookback 7일 누락분 append (또는 정정 overwrite)
3. t3: okpos  → Lookback 7일 누락분 append (또는 정정 overwrite)  ← t2 완료 후 실행

정정 모드 (conf['sale_date'] 지정):
  특정 날짜 parquet을 소스 데이터로 완전 교체.

백필 모드 (conf['backfill'] = true):
  전체 소스 CSV를 일괄 처리.
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_UnifiedSales import (
    backfill as pipeline_backfill,
    backfill_okpos as pipeline_backfill_okpos,
    reclassify_hall_platform as pipeline_reclassify,
    run as pipeline_run,
    run_lookback_okpos as pipeline_lookback_okpos,
    run_lookback_toorder as pipeline_lookback_toorder,
    run_okpos as pipeline_run_okpos,
    resave_existing_unified_sales as pipeline_resave_existing,
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

# LOOKBACK_DAYS:
# - int  : 최근 N일 누락분 보충
# - None : 현재 저장된 unified_sales parquet 전체 재저장
LOOKBACK_DAYS: int | None = None


def resolve_date(**context) -> str:
    """conf['sale_date'] → XCom push (정정 모드). 없으면 None push."""
    dag_run   = context.get("dag_run")
    conf      = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = conf.get("sale_date") if isinstance(conf, dict) else None

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)

    if sale_date:
        logger.info("정정 모드 → %s", sale_date)
        return f"정정 날짜: {sale_date}"
    if LOOKBACK_DAYS is None:
        logger.info("LOOKBACK_DAYS=None → 기존 unified_sales 전체 재저장 모드")
        return "기존 unified_sales 전체 재저장 모드"
    logger.info("Lookback %d일 모드", LOOKBACK_DAYS)
    return f"Lookback {LOOKBACK_DAYS}일 모드"


def build_unified(**context) -> str:
    """toorder → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback 7일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_resave_existing()
    return pipeline_lookback_toorder(days=LOOKBACK_DAYS)


def build_okpos(**context) -> str:
    """okpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback 7일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_okpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_okpos(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return "LOOKBACK_DAYS=None: 기존 unified_sales 재저장 모드 (okpos 스킵)"
    # sale_date 없으면 저장된 raw CSV 전체 처리 (append-only, 중복 PK 자동 스킵)
    return pipeline_backfill_okpos()


def reclassify_platform(**context) -> str:
    """unified_sales의 포스/제휴사주문 platform을 테이블명 기반으로 재분류."""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if not sale_date:
        return "Lookback 모드: 재분류 스킵 (신규 데이터에 이미 적용됨)"
    return pipeline_reclassify(sale_date, overwrite=True)


with DAG(
    dag_id=dag_id,
    schedule="30 10 * * *",  # 매일 10:30
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "toorder", "okpos", "unified_sales"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_date",
        python_callable=resolve_date,
    )

    t2 = PythonOperator(
        task_id="build_unified",
        python_callable=build_unified,
    )

    t3 = PythonOperator(
        task_id="build_okpos",
        python_callable=build_okpos,
    )

    t4 = PythonOperator(
        task_id="reclassify_platform",
        python_callable=reclassify_platform,
    )

    # 순차 실행: 같은 날짜 parquet에 동시 write 방지
    t1 >> t2 >> t3 >> t4
