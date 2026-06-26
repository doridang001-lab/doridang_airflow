"""
fin_product_map 송파삼전점 표준화 DAG

매일 자동 실행하며 송파삼전점 대상 fin_product_map CSV/JSON을 갱신한다.
사람은 fin_product_map_review.csv만 검수하고, 전체 map은 파이프라인이 병합한다.
수동 테스트 실행은 DAG 실행 conf에 {"dry_run": true}를 명시한다.
"""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_FinProduct_Map import (
    llm_product_map,
    migrate_product_map,
    build_fin_product_map_train_json,
)
from modules.transform.utility.notifier import on_failure_callback, send_telegram
from modules.transform.utility.schedule import DB_FIN_PRODUCT_MAP_TIME

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

DRY_RUN_BY_DEFAULT = False
DRY_RUN_LLM_LIMIT = 20

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


def _dag_conf(context: dict[str, Any]) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf if isinstance(conf, dict) else {}


def _conf_bool(conf: dict[str, Any], key: str, default: bool) -> bool:
    value = conf.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "y", "yes"}
    return bool(value)


def _conf_int(conf: dict[str, Any], key: str, default: int | None) -> int | None:
    value = conf.get(key, default)
    if value in (None, ""):
        return None
    return int(value)


def run_migrate(**context) -> dict:
    conf = _dag_conf(context)
    dry_run = _conf_bool(conf, "dry_run", DRY_RUN_BY_DEFAULT)
    logger.info("fin_product_map migrate 시작: dry_run=%s", dry_run)
    return migrate_product_map(dry_run=dry_run)


def run_build_train_json(**context) -> dict:
    conf = _dag_conf(context)
    dry_run = _conf_bool(conf, "dry_run", DRY_RUN_BY_DEFAULT)
    logger.info("fin_product_map_train_json 빌드 시작: dry_run=%s", dry_run)
    return build_fin_product_map_train_json(dry_run=dry_run)


def run_llm(**context) -> dict:
    conf = _dag_conf(context)
    dry_run = _conf_bool(conf, "dry_run", DRY_RUN_BY_DEFAULT)
    default_limit = DRY_RUN_LLM_LIMIT if dry_run else None
    limit = _conf_int(conf, "limit", default_limit)
    logger.info("fin_product_map llm 시작: dry_run=%s limit=%s", dry_run, limit)
    result = llm_product_map(dry_run=dry_run, limit=limit)
    new_count = int(result.get("new_classified") or 0)
    if not dry_run and new_count > 0:
        send_telegram(
            "[상품 매핑] 신규 상품 분류 완료\n"
            f"신규 상품: {new_count}건\n"
            f"검수 대상: {result.get('review_rows', 0)}건\n"
            "fin_product_map_review.csv를 확인해주세요."
        )
    return result


with DAG(
    dag_id=dag_id,
    schedule=DB_FIN_PRODUCT_MAP_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "product", "llm", "manual", "songpa"],
) as dag:

    t1 = PythonOperator(
        task_id="migrate_product_map",
        python_callable=run_migrate,
    )

    t2 = PythonOperator(
        task_id="llm_product_map",
        python_callable=run_llm,
    )

    t1_5 = PythonOperator(
        task_id="build_fin_product_map_train_json",
        python_callable=run_build_train_json,
    )

    t1 >> t1_5 >> t2
