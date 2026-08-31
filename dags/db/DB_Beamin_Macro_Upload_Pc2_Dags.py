"""하위(bottom) PC 도착분 감지와 중앙 upload DAG 트리거."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import has_upload_inbox_folders
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import BOTTOM_FOLDER_PATTERN
from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_PC2_TIME

dag_id = Path(__file__).stem
TARGET_UPLOAD_DAG_ID = "DB_Beamin_Macro_Upload_Dags"

default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_UPLOAD_PC2_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["db", "baemin", "upload", "pc2"],
) as dag:
    t_has_pending = ShortCircuitOperator(
        task_id="has_bottom_pending",
        python_callable=has_upload_inbox_folders,
        op_kwargs={"folder_pattern": BOTTOM_FOLDER_PATTERN},
    )

    t_trigger_upload = TriggerDagRunOperator(
        task_id="trigger_upload",
        trigger_dag_id=TARGET_UPLOAD_DAG_ID,
        trigger_run_id="pc2_bottom__{{ logical_date | ts_nodash }}",
        conf={
            "folder_pattern": BOTTOM_FOLDER_PATTERN,
            "skip_if_empty": True,
            "source": "pc2_bottom_sweep",
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    t_has_pending >> t_trigger_upload
