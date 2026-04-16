"""
땡겨요 정책 수집 DAG

처리 흐름:
1. Playwright 로그인 및 공지사항 목록 파싱
2. 각 공지 본문 수집 (재로그인 후 상세 페이지 접근)
3. 정책 행 생성 (정책유형 8종 분류, 실행 제안 생성)
4. CSV 누적 저장 (중복제거, 정렬)

실행 시각: 매일 10:15 (KST, 정책 DAG 5분 간격 분산)
데이터 스키마: policy_id, platform, collected_at, policy_date, title,
               category, content, content_summary, policy_type,
               recommended_action, source_url
"""

import logging
import pendulum
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from modules.transform.utility.schedule import SMP_POLICY_DDANGYO_TIME
from modules.transform.pipelines.strategy.SMP_ddangyo_policy_collect import (
    login_and_collect_notices,
    collect_notice_bodies,
    build_policy_rows,
    save_policy_csv,
    write_policy_log,
)

dag_id = Path(__file__).stem
logger = logging.getLogger(__name__)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ============================================================
# Task 래퍼 함수
# ============================================================

def task_login_and_collect(**context):
    """요기요 정책 수집 후 땡겨요 로그인 및 공지 목록 파싱"""
    notice_list = login_and_collect_notices(**context)
    if not notice_list:
        raise AirflowSkipException("수집된 공지가 없습니다.")
    return notice_list


def task_collect_notice_bodies(**context):
    """각 공지 본문 수집"""
    notices_with_content = collect_notice_bodies(**context)
    if not notices_with_content:
        raise AirflowSkipException("본문이 수집된 공지가 없습니다.")
    return notices_with_content


def task_build_policy_rows(**context):
    """정책 행 생성 (정책유형 8종 분류)"""
    policy_rows = build_policy_rows(**context)
    if not policy_rows:
        raise AirflowSkipException("생성된 정책 행이 없습니다.")
    return policy_rows


def task_save_policy_csv(**context):
    """CSV 누적 저장 (중복제거, 정렬)"""
    saved_path = save_policy_csv(**context)
    policy_rows = context["ti"].xcom_pull(task_ids="task_build_policy_rows", key="policy_rows") or []
    logger.info(f"땡겨요 정책 수집 완료 | 저장 경로: {saved_path} | 처리 건수: {len(policy_rows)}개")
    return saved_path


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_id,
    description="땡겨요 정책 변경 수집 (Playwright 기반)",
    schedule=SMP_POLICY_DDANGYO_TIME,
    start_date=pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "ddangyo", "policy", "daily"],
    default_args=default_args,
) as dag:

    t1 = PythonOperator(
        task_id="task_login_and_collect",
        python_callable=task_login_and_collect,
    )

    t2 = PythonOperator(
        task_id="task_collect_notice_bodies",
        python_callable=task_collect_notice_bodies,
    )

    t3 = PythonOperator(
        task_id="task_build_policy_rows",
        python_callable=task_build_policy_rows,
    )

    t4 = PythonOperator(
        task_id="task_save_policy_csv",
        python_callable=task_save_policy_csv,
    )

    t5 = PythonOperator(
        task_id="task_write_log",
        python_callable=write_policy_log,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4 >> t5
