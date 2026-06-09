"""
출근길 AI 브리핑 DAG

처리 흐름:
    1. collect_briefing_data : Google Calendar + 어제 FAIL/WARN DAG + git 상태 수집
    2. generate_briefing     : gpt-oss로 DAG 원인 분석 → 전체 우선순위 브리핑 생성
    3. send_briefing         : Telegram 전송

스케줄: 평일 오전 8:50 (SMP_MORNING_BRIEFING_TIME)

사전 준비:
    - Airflow Variables: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (notifier.py와 공유, 이미 세팅됨)
    - Google Calendar token: scripts/generate_calendar_token.py 실행 (최초 1회, 없으면 일정 섹션 생략)
"""

import importlib
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import SMP_MORNING_BRIEFING_TIME

_pipeline = importlib.import_module(
    "modules.transform.pipelines.strategy.morning_briefing_pipeline"
)

collect_briefing_data = _pipeline.collect_briefing_data
generate_briefing = _pipeline.generate_briefing
send_briefing = _pipeline.send_briefing

with DAG(
    dag_id=Path(__file__).stem,
    description="출근길 AI 브리핑 — Calendar·DAG 실패·git 상태 → gpt-oss 요약 → Telegram",
    schedule=SMP_MORNING_BRIEFING_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["strategy", "briefing", "llm"],
    default_args={
        "retries": 0,
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    task_collect = PythonOperator(
        task_id="collect_briefing_data",
        python_callable=collect_briefing_data,
        execution_timeout=pendulum.duration(minutes=3),
    )

    task_generate = PythonOperator(
        task_id="generate_briefing",
        python_callable=generate_briefing,
        execution_timeout=pendulum.duration(minutes=10),
    )

    task_send = PythonOperator(
        task_id="send_briefing",
        python_callable=send_briefing,
        execution_timeout=pendulum.duration(minutes=2),
    )

    task_collect >> task_generate >> task_send
