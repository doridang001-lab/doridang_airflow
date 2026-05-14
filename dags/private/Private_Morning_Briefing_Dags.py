"""
매일 08:50 KST 오전 브리핑 자동 발송 DAG

처리 흐름:
1. run_briefing  — Flow 로그인 → Flow AI 이슈 수집 → 캘린더 스크래핑
2. send_briefing — 브리핑 HTML 생성 → a17019@kakao.com 이메일 발송
"""

import os
import pendulum
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.schedule import SMP_MORNING_BRIEFING_TIME
from modules.transform.pipelines.private.private_morning_briefing import (
    run_briefing,
    send_briefing,
)

filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace(".py", ""),
    description="매일 08:50 KST 오전 브리핑 (Flow AI + 캘린더 + KPI)",
    schedule=SMP_MORNING_BRIEFING_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["briefing", "private", "flow", "morning"],
) as dag:

    t_run = PythonOperator(
        task_id="run_briefing",
        python_callable=run_briefing,
        execution_timeout=timedelta(minutes=10),
    )

    t_send = PythonOperator(
        task_id="send_briefing",
        python_callable=send_briefing,
        execution_timeout=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_DONE,  # run_briefing 실패해도 KPI라도 발송
    )

    t_run >> t_send
