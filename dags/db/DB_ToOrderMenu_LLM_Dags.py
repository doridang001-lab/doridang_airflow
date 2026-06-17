"""ToOrder menu LLM enrichment DAG."""

import logging
from pathlib import Path

import pendulum
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.transform.pipelines.db.DB_ToOrderMenu_LLM import (
    get_target_dates,
    run_toorder_llm,
    save_llm_log_yesterday,
)
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_TOORDER_MENU_LLM_TIME

logger = logging.getLogger(__name__)


def _latest_toorder_menu_execution_date(dt, **context):
    """Return latest successful execution_date for DB_ToOrderMenu_Dags.save_menu_detail_parquet."""
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI

    session = settings.Session()
    try:
        row = (
            session.query(DagRun.execution_date)
            .join(
                TI,
                (TI.dag_id == DagRun.dag_id) & (TI.run_id == DagRun.run_id),
            )
            .filter(
                TI.dag_id == "DB_ToOrderMenu_Dags",
                TI.task_id == "save_menu_detail_parquet",
                TI.state == "success",
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        if row:
            latest_execution_date = row[0]
            logger.info("[sensor] Parent DAG success execution_date: %s", latest_execution_date)
            return latest_execution_date
    except Exception as exc:
        logger.warning("[sensor] TaskInstance lookup failed: %s", exc)
    finally:
        session.close()

    logger.info("[sensor] No success execution_date found, using logical_date: %s", dt)
    return dt


with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder menu LLM enrichment add column",
    schedule=DB_TOORDER_MENU_LLM_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["02_transform", "toorder", "llm", "daily"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
) as dag:

    wait_toorder_menu = ExternalTaskSensor(
        task_id="wait_toorder_menu",
        external_dag_id="DB_ToOrderMenu_Dags",
        external_task_id="save_menu_detail_parquet",
        execution_date_fn=_latest_toorder_menu_execution_date,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=120,
        timeout=14400,
        mode="reschedule",
        soft_fail=True,
    )

    def _get_target_dates(**context):
        conf = context["dag_run"].conf or {}
        dates = get_target_dates(conf, context=context)
        context["ti"].xcom_push(key="date_list", value=dates)
        logger.info("target dates: %s", dates)
        return dates

    get_target_dates_task = PythonOperator(
        task_id="get_target_dates",
        python_callable=_get_target_dates,
    )

    def _run_llm_enrichment(**context):
        conf = context["dag_run"].conf or {}
        summary = run_toorder_llm(conf, context=context)
        logger.info(summary)
        return summary

    run_llm_enrichment = PythonOperator(
        task_id="run_llm_enrichment",
        python_callable=_run_llm_enrichment,
    )

    def _save_llm_log(**context):
        conf = context["dag_run"].conf or {}
        result = save_llm_log_yesterday(conf, context=context)
        logger.info(result)
        return result

    save_llm_log = PythonOperator(
        task_id="save_llm_log",
        python_callable=_save_llm_log,
    )

    wait_toorder_menu >> get_target_dates_task >> run_llm_enrichment >> save_llm_log
