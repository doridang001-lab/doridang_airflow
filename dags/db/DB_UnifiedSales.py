"""
unified_sales 일별 생성 DAG (toorder + okpos)

처리 흐름:
1. 날짜 결정 (conf['sale_date'] 또는 어제)
2. toorder CSV → MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet 저장
3. okpos CSV  → MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet 저장(병합)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_UnifiedSales import run as pipeline_run
from modules.transform.pipelines.db.DB_UnifiedSales import run_okpos as pipeline_run_okpos

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def resolve_date(**context) -> str:
    """conf['sale_date'] 또는 KST 어제 날짜 결정 → XCom push"""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = conf.get("sale_date") if isinstance(conf, dict) else None

    if sale_date:
        logger.info("conf override → %s", sale_date)
    else:
        kst       = pendulum.timezone("Asia/Seoul")
        sale_date = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")
        logger.info("어제 날짜 사용 → %s", sale_date)

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)
    return f"날짜 결정: {sale_date}"


def build_unified(**context) -> str:
    """toorder CSV → unified_sales 저장"""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return pipeline_run(sale_date)


def build_okpos(**context) -> str:
    """OKPOS order+order_item → unified_sales 저장"""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return pipeline_run_okpos(sale_date)


with DAG(
    dag_id=dag_id,
    schedule=None,  # 수동 실행 전용 (자동 수집 중지)
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

    t1 >> [t2, t3]
