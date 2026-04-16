"""
DAG 모니터링 - 당일 DAG 실행 현황 점검 및 알림 DAG

처리 흐름:
    1. collect_dag_runs      : Airflow 메타DB에서 당일 dag_run + task_instance 수집
    2. apply_failure_rules   : FAIL/WARN/OK 3단계 판정 규칙 적용
    3. save_monitoring_results: OneDrive CSV 저장
    4. send_alert_email      : FAIL/WARN 존재 시 관리자 이메일 발송

스케줄: 매일 16:30 KST (SMP_DAG_MONITORING_TIME)
"""

import importlib
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import SMP_DAG_MONITORING_TIME

# ============================================================
# 파이프라인 모듈 동적 임포트
# ============================================================
_pipeline = importlib.import_module(
    "modules.transform.pipelines.strategy.dag_monitoring_pipeline"
)

collect_dag_runs = _pipeline.collect_dag_runs
apply_failure_rules = _pipeline.apply_failure_rules
save_monitoring_results = _pipeline.save_monitoring_results
send_monitoring_alert_email = _pipeline.send_monitoring_alert_email

# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=Path(__file__).stem,
    description="당일 실행 DAG 성공/실패 점검 및 이상 발생 시 관리자 알림",
    schedule=SMP_DAG_MONITORING_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["strategy", "monitoring", "daily", "alert"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    task_collect = PythonOperator(
        task_id="collect_dag_runs",
        python_callable=collect_dag_runs,
        execution_timeout=pendulum.duration(minutes=10),
    )

    task_rules = PythonOperator(
        task_id="apply_failure_rules",
        python_callable=apply_failure_rules,
        execution_timeout=pendulum.duration(minutes=5),
    )

    task_save = PythonOperator(
        task_id="save_monitoring_results",
        python_callable=save_monitoring_results,
        execution_timeout=pendulum.duration(minutes=5),
    )

    task_email = PythonOperator(
        task_id="send_alert_email",
        python_callable=send_monitoring_alert_email,
        execution_timeout=pendulum.duration(minutes=5),
    )

    task_collect >> task_rules >> [task_save, task_email]
