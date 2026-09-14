"""Flow store project history collection DAG."""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import SMP_FLOW_COLLECT_TIME

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

dag_file_stem = Path(__file__).stem
FLOW_VISIT_MART_DAG_ID = "Sales_FlowVisit_01_Mart_Dags"
pipeline_module_name = "SMP_flow_store_collect"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

extract_project_list = pipeline_module.extract_project_list
extract_post_list = pipeline_module.extract_post_list
detect_changed_posts = pipeline_module.detect_changed_posts
collect_post_details = pipeline_module.collect_post_details
save_flow_parquet = pipeline_module.save_flow_parquet


def _slot_time_kst(context):
    """실제 발화시각(data_interval_end) 기준 KST 시각.

    cron 스케줄에서 logical_date는 '직전 발화시각'이라 한 칸 밀린다.
    슬롯 판정은 실제로 실행되는 시각으로 해야 주석의 수집 시각과 일치한다.
    """
    fire_time = (
        context.get("data_interval_end")
        or context.get("logical_date")
        or context.get("execution_date")
    )
    return pendulum.instance(fire_time).in_timezone("Asia/Seoul")


def task_validate_collect_slot(**context):
    dag_run = context.get("dag_run")
    if getattr(dag_run, "run_type", "") != "scheduled":
        return "manual run"
    logical_time = _slot_time_kst(context)
    allowed = (
        (logical_time.hour == 8 and logical_time.minute == 7)
        or (logical_time.hour in {12, 14, 16, 19, 21} and logical_time.minute == 37)
    )
    if not allowed:
        raise AirflowSkipException(
            f"Flow 수집 허용 슬롯 아님: {logical_time.format('YYYY-MM-DD HH:mm')}"
        )
    return logical_time.to_datetime_string()


def task_extract_project_list(**context):
    projects = extract_project_list(**context)
    if not projects:
        raise AirflowSkipException("Flow 프로젝트 목록이 비어 있습니다.")
    context["ti"].xcom_push(key="projects", value=projects)
    logger.info("Flow 프로젝트 목록 XCom 저장 완료: %s건", len(projects))
    return projects


def task_extract_post_list(**context):
    projects = context["ti"].xcom_pull(task_ids="task_extract_project_list", key="projects")
    if not projects:
        raise AirflowSkipException("Flow 프로젝트 목록이 없습니다.")
    post_list = extract_post_list(projects=projects, **context)
    if not post_list:
        raise AirflowSkipException("Flow 게시글 목록이 비어 있습니다.")
    context["ti"].xcom_push(key="post_list", value=post_list)
    logger.info("Flow 게시글 목록 XCom 저장 완료: %s건", len(post_list))
    return post_list


def task_detect_changed_posts(**context):
    post_list = context["ti"].xcom_pull(task_ids="task_extract_post_list", key="post_list")
    if not post_list:
        raise AirflowSkipException("Flow 게시글 목록이 없습니다.")
    changed_posts = detect_changed_posts(post_list=post_list, **context)
    if not changed_posts:
        raise AirflowSkipException("Flow 신규/변경 게시글이 없습니다.")
    context["ti"].xcom_push(key="changed_posts", value=changed_posts)
    logger.info("Flow 신규/변경 게시글 XCom 저장 완료: %s건", len(changed_posts))
    return changed_posts


def task_collect_post_details(**context):
    changed_posts = context["ti"].xcom_pull(task_ids="task_detect_changed_posts", key="changed_posts")
    if not changed_posts:
        raise AirflowSkipException("Flow 신규/변경 게시글이 없습니다.")
    details = collect_post_details(changed_posts=changed_posts, **context)
    projects = context["ti"].xcom_pull(task_ids="task_extract_project_list", key="projects") or []
    if projects:
        details["projects"] = projects
    if not details.get("posts"):
        raise AirflowSkipException("Flow 상세 수집 결과가 비어 있습니다.")
    context["ti"].xcom_push(key="details", value=details)
    logger.info(
        "Flow 상세 XCom 저장 완료: posts=%s comments=%s failures=%s",
        len(details.get("posts") or []),
        len(details.get("comments") or []),
        len(details.get("failures") or []),
    )
    return details


def task_save_flow_parquet(**context):
    details = context["ti"].xcom_pull(task_ids="task_collect_post_details", key="details")
    if not details:
        raise AirflowSkipException("Flow 상세 수집 결과가 없습니다.")
    saved_message = save_flow_parquet(details=details, **context)
    context["ti"].xcom_push(key="saved_message", value=saved_message)
    return saved_message


def task_write_log(**context):
    ti = context["ti"]
    saved_message = ti.xcom_pull(task_ids="task_save_flow_parquet", key="saved_message")
    logger.info("Flow 가맹점 프로젝트 수집 DAG 종료: %s", saved_message or "저장 단계 생략")
    return saved_message or "Flow 수집 DAG 종료"


with DAG(
    dag_id=dag_file_stem,
    description="Flow 가맹점 프로젝트 방문 히스토리 수집 및 mart parquet 저장",
    schedule=SMP_FLOW_COLLECT_TIME,
    start_date=pendulum.datetime(2026, 8, 3, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "flow", "store_history", "half_hourly"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
) as dag:
    slot_guard = PythonOperator(
        task_id="task_validate_collect_slot",
        python_callable=task_validate_collect_slot,
    )

    t1 = PythonOperator(
        task_id="task_extract_project_list",
        python_callable=task_extract_project_list,
        show_return_value_in_logs=False,
    )
    t2 = PythonOperator(
        task_id="task_extract_post_list",
        python_callable=task_extract_post_list,
        show_return_value_in_logs=False,
    )
    t3 = PythonOperator(
        task_id="task_detect_changed_posts",
        python_callable=task_detect_changed_posts,
        show_return_value_in_logs=False,
    )
    t4 = PythonOperator(
        task_id="task_collect_post_details",
        python_callable=task_collect_post_details,
        show_return_value_in_logs=False,
    )
    t5 = PythonOperator(
        task_id="task_save_flow_parquet",
        python_callable=task_save_flow_parquet,
    )
    t6 = PythonOperator(
        task_id="task_write_log",
        python_callable=task_write_log,
    )

    t_trigger_mart = TriggerDagRunOperator(
        task_id="trigger_flow_visit_mart",
        trigger_dag_id=FLOW_VISIT_MART_DAG_ID,
        trigger_run_id="flow_collect__{{ logical_date | ts_nodash }}",
        conf={
            "source": dag_file_stem,
            "parent_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    slot_guard >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t_trigger_mart
