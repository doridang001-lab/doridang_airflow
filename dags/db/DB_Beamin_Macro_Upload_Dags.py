"""Central Baemin macro upload DAG.

이 DAG은 중앙 PC에서만 unpause한다. 수집 PC는 _baemin_upload_inbox까지만
내보내고, analytics/baemin_macro 적재와 검증은 이 DAG이 담당한다.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import (
    ingest,
    notify_upload_result,
    precheck_manual,
    trigger_retry_if_needed,
    validate_ad_funnel,
    validate_orders,
    validate_toorder,
)
from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_UPLOAD_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["db", "baemin", "upload"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        execution_timeout=timedelta(minutes=60),
    )

    t_precheck = PythonOperator(
        task_id="precheck_manual_baemin_orders",
        python_callable=precheck_manual,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=15),
    )

    t_validate_orders = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_validate_ad_funnel = PythonOperator(
        task_id="validate_ad_funnel",
        python_callable=validate_ad_funnel,
        pool="selenium_pool",
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120),
    )

    t_validate_toorder = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )

    t_notify = PythonOperator(
        task_id="notify_upload_result",
        python_callable=notify_upload_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_trigger_retry = PythonOperator(
        task_id="trigger_retry_if_needed",
        python_callable=trigger_retry_if_needed,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ingest >> t_precheck >> [t_validate_orders, t_validate_ad_funnel, t_validate_toorder] >> t_trigger_retry >> t_notify
