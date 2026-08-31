"""Central Baemin macro validation DAG.

DB_Beamin_Macro_Upload_Dags(적재)가 handoff_path를 conf로 넘겨 트리거한다.
Selenium 검증이 길어져도 inbox 적재를 막지 않도록 분리되어 있다.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import (
    notify_upload_result,
    precheck_manual,
    trigger_retry_if_needed,
    validate_ad_funnel,
    validate_orders,
    validate_toorder,
)
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    cleanup_manual_baemin_files,
    load_manual_baemin_files,
)
from modules.transform.utility.notifier import on_failure_callback_no_telegram

dag_id = Path(__file__).stem

default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "upload", "validate"],
) as dag:
    t_ingest_manual = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_files,
        execution_timeout=timedelta(minutes=15),
    )

    t_precheck = PythonOperator(
        task_id="precheck_manual_baemin_orders",
        python_callable=precheck_manual,
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

    t_trigger_retry = PythonOperator(
        task_id="trigger_retry_if_needed",
        python_callable=trigger_retry_if_needed,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_notify = PythonOperator(
        task_id="notify_upload_result",
        python_callable=notify_upload_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_cleanup_manual = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_files,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        t_ingest_manual
        >> t_precheck
        >> [t_validate_orders, t_validate_ad_funnel, t_validate_toorder]
        >> t_trigger_retry
        >> t_notify
        >> t_cleanup_manual
    )
