"""
배민 변경이력 수집 테스트 DAG. 현재는 shop_change만 단독 수집한다.

기본 실행 시에는 TEST_STORES에 적은 매장만 테스트한다.
필요하면 dag_run.conf["stores"]로 일시 override 할 수 있다.

conf 예시:
  {"stores": ["도리당 역삼점"]}  -> 특정 매장만 테스트
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.DB_Beamin_03_shop_change import (
    collect_shop_change as pipeline_collect_shop_change,
)

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

# Empty list means all stores, matching the pipeline default behavior.
TEST_STORES = ["도리당 송파삼전점"]


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")
    target = stores_override if stores_override else TEST_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=bool(target))
    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개: {stores}"


def collect_shop_change(**context) -> str:
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    return pipeline_collect_shop_change(account_list)


with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "depends_on_past": False,
        "email_on_failure": False,
    },
    tags=["db", "baemin", "crawl", "test"],
) as dag:

    t1 = PythonOperator(
        task_id="load_accounts",
        python_callable=load_accounts,
    )

    t2 = PythonOperator(
        task_id="collect_shop_change",
        python_callable=collect_shop_change,
        execution_timeout=timedelta(minutes=60),
    )

    t1 >> t2
