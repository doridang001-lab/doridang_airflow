"""
정책 통합 최신화 DAG

📋 처리 흐름:
1. 8개 플랫폼 raw 정책 CSV 로드 → 플랫폼별 최신 policy_date 1건 필터링
2. 기존 통합 CSV와 비교하여 신규/변경 정책 감지
3. 통합 CSV 저장 (전체 컬럼)
4. 신규/변경 정책 있을 시 HTML 이메일 알림 (a17019@kakao.com)
5. Flow 하위업무 자동 게시 (https://flow.team/l/QdNBw)
6. 실행 로그 기록

⚙️ 실행 시각: 매일 10:45 KST (각 플랫폼 수집 완료 후)
📊 출력: analytics/policy/policy_consolidated_latest.csv
"""

import pendulum
import importlib
import sys
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from modules.transform.utility.schedule import SMP_POLICY_CONSOLIDATE_TIME
from modules.transform.utility.notifier import on_failure_callback

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

dag_file_stem = Path(__file__).stem
pipeline_module_name = "SMP_policy_consolidate"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

load_and_filter_latest = pipeline_module.load_and_filter_latest
detect_new_policies = pipeline_module.detect_new_policies
save_consolidated_csv = pipeline_module.save_consolidated_csv
send_policy_alert = pipeline_module.send_policy_alert
post_to_flow = pipeline_module.post_to_flow
write_consolidate_log = pipeline_module.write_consolidate_log


# ============================================================
# Task 1: 플랫폼별 최신 정책 로드 및 필터링
# ============================================================

def task_load_and_filter(**context):
    """8개 플랫폼 raw CSV 로드 → 플랫폼별 최신 1건 필터링"""
    result = load_and_filter_latest(**context)
    return result


# ============================================================
# Task 2: 신규/변경 정책 감지
# ============================================================

def task_detect_new_policies(**context):
    """기존 통합 CSV와 비교하여 신규/변경 정책 감지"""
    new_policies = detect_new_policies(**context)
    return new_policies


# ============================================================
# Task 3: 통합 CSV 저장
# ============================================================

def task_save_consolidated_csv(**context):
    """플랫폼별 최신 정책 통합 CSV 덮어쓰기 저장"""
    saved_path = save_consolidated_csv(**context)
    return saved_path


# ============================================================
# Task 4: 이메일 알림
# ============================================================

def task_send_alert(**context):
    """신규/변경 정책 HTML 이메일 알림 발송"""
    result = send_policy_alert(**context)
    return result


# ============================================================
# Task 5: Flow 하위업무 게시
# ============================================================

def task_post_to_flow(**context):
    """Flow 프로젝트에 오늘 날짜 하위업무 생성 + 정책 테이블 본문 삽입"""
    result = post_to_flow(**context)
    return result


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="플랫폼별 최신 정책 통합 CSV 생성 및 신규 공지 이메일 알림",
    schedule=SMP_POLICY_CONSOLIDATE_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["02_transform", "policy", "daily", "alert"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    },
) as dag:

    t1 = PythonOperator(
        task_id="task_load_and_filter",
        python_callable=task_load_and_filter,
    )

    t2 = PythonOperator(
        task_id="task_detect_new_policies",
        python_callable=task_detect_new_policies,
    )

    t3 = PythonOperator(
        task_id="task_save_consolidated_csv",
        python_callable=task_save_consolidated_csv,
    )

    t4 = PythonOperator(
        task_id="task_send_alert",
        python_callable=task_send_alert,
    )

    t5 = PythonOperator(
        task_id="task_post_to_flow",
        python_callable=task_post_to_flow,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t6 = PythonOperator(
        task_id="task_write_log",
        python_callable=write_consolidate_log,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 의존성
    t1 >> t2 >> [t3, t4] >> t5 >> t6
