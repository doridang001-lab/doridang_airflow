"""송파삼전점 원재료·직원식대 근거형 mart."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.samjeon_material_agent.mart import (
    plan_build, read_plan, save_plan, publish_snapshot, stage_aggregate, stage_purchases, stage_sales,
    validate_snapshot,
)
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT
from modules.transform.utility.schedule import DB_SAMJEON_MATERIAL_MART_TIME


def _plan(**context):
    conf = context["dag_run"].conf or {}
    if set(conf) - {"sale_date", "date_from", "date_to"}:
        raise ValueError("지원 conf: sale_date 또는 date_from/date_to")
    plan = plan_build(conf.get("sale_date"), date_from=conf.get("date_from"),
                      date_to=conf.get("date_to"), run_id=context["dag_run"].run_id)
    return save_plan(plan)


def _run(step, **context):
    reference = context["ti"].xcom_pull(task_ids="resolve_inputs")
    if step == "publish" and str(Variable.get(
        "SAMJEON_MATERIAL_MART_PUBLISH_ENABLED", default_var="false"
    )).strip().lower() != "true":
        raise AirflowSkipException("OneDrive 게시 승인이 없어 스냅샷 게시를 건너뜁니다")
    plan = read_plan(reference)
    functions = {
        "sales": stage_sales, "purchases": stage_purchases,
        "aggregate": stage_aggregate, "validate": validate_snapshot,
        "publish": publish_snapshot,
    }
    return functions[step](plan)


with DAG(
    dag_id=Path(__file__).stem,
    schedule=DB_SAMJEON_MATERIAL_MART_TIME,
    start_date=pendulum.datetime(2026, 10, 12, tz="Asia/Seoul"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    default_args={"retries": 0, "depends_on_past": False},
    tags=["db", "mart", "samjeon", "material"],
) as dag:
    resolve_inputs = PythonOperator(
        task_id="resolve_inputs", python_callable=_plan,
        execution_timeout=timedelta(minutes=10),
    )
    build_sales = PythonOperator(
        task_id="build_sales", python_callable=_run,
        op_kwargs={"step": "sales"}, execution_timeout=timedelta(minutes=30),
    )
    build_purchases = PythonOperator(
        task_id="build_purchases", python_callable=_run,
        op_kwargs={"step": "purchases"}, execution_timeout=timedelta(minutes=30),
    )
    aggregate = PythonOperator(
        task_id="aggregate", python_callable=_run,
        op_kwargs={"step": "aggregate"}, execution_timeout=timedelta(minutes=20),
    )
    validate = PythonOperator(
        task_id="validate", python_callable=_run,
        op_kwargs={"step": "validate"}, execution_timeout=timedelta(minutes=20),
    )
    publish = PythonOperator(
        task_id="publish", python_callable=_run,
        op_kwargs={"step": "publish"}, execution_timeout=timedelta(minutes=20),
    )
    resolve_inputs >> [build_sales, build_purchases] >> aggregate >> validate >> publish
