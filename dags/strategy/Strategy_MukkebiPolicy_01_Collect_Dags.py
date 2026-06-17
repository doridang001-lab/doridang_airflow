"""
먹깨비 정책 수집 DAG

처리 흐름:
1. REST API로 이벤트 게시글 전 페이지 수집 (CSR - Next.js, 인증 불필요)
2. bd_idx 중복 판정 및 신규 목록 생성
3. 기존 CSV 병합 후 OneDrive 저장 (LLM 분석 없음)

실행 시각: 매일 10:25 (KST, 정책 DAG 5분 간격 분산)
데이터 스키마: policy_id, platform, collected_at, policy_date,
              title, category, content(null), content_summary(null),
              policy_type(null), recommended_action(null), source_url
"""

import pendulum
import importlib
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from modules.transform.utility.schedule import SMP_POLICY_MUKKEBI_TIME
from modules.transform.utility.notifier import on_failure_callback

# 모듈 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# DAG ID 및 파이프라인 모듈 설정
dag_file_stem = Path(__file__).stem
pipeline_module_name = "SMP_mukkebi_policy_collect"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

# 파이프라인 함수 import
extract_notice_list = pipeline_module.extract_notice_list
detect_new_notices = pipeline_module.detect_new_notices
save_policy_csv = pipeline_module.save_policy_csv
write_policy_log = pipeline_module.write_policy_log


# ============================================================
# Task 1: 공지 목록 수집
# ============================================================

def task_extract_notice_list(**context):
    """먹깨비 이벤트 게시판 전 페이지 수집 (REST API)"""

    print(f"\n{'='*60}")
    print("[먹깨비 정책 수집] 시작")
    print(f"{'='*60}")

    notice_list = extract_notice_list(**context)

    print(f"\n공지 목록 수집 완료: {len(notice_list)}건")

    context['ti'].xcom_push(key='notice_list', value=notice_list)
    return notice_list


# ============================================================
# Task 2: 신규 공지 판정
# ============================================================

def task_detect_new_notices(**context):
    """bd_idx 중복 판정 및 신규 목록 생성"""

    notice_list = context['ti'].xcom_pull(task_ids='task_extract_notice_list', key='notice_list')

    if not notice_list:
        print("파싱된 공지가 없습니다. 후속 작업을 스킵합니다.")
        raise AirflowSkipException("공지 목록이 비어있습니다.")

    new_notices = detect_new_notices(notice_list=notice_list, **context)

    print(f"\n신규 공지 판정 완료: {len(new_notices)}건")

    if not new_notices:
        print("신규 공지가 없습니다. 후속 작업을 스킵합니다.")
        raise AirflowSkipException("신규 공지가 없습니다.")

    context['ti'].xcom_push(key='new_notices', value=new_notices)
    return new_notices


# ============================================================
# Task 3: CSV 저장 (LLM 없이 직접 저장)
# ============================================================

def task_save_policy_csv(**context):
    """신규 공지 누적 저장 및 중복제거, 정렬"""

    new_notices = context['ti'].xcom_pull(task_ids='task_detect_new_notices', key='new_notices')

    if not new_notices:
        print("신규 공지가 없습니다. 작업을 스킵합니다.")
        raise AirflowSkipException("신규 공지가 없습니다.")

    saved_path = save_policy_csv(new_notices=new_notices, **context)

    print(f"\n{'='*60}")
    print(f"먹깨비 정책 수집 완료")
    print(f"   저장 경로: {saved_path}")
    print(f"   처리 건수: {len(new_notices)}건")
    print(f"{'='*60}\n")

    return saved_path


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="먹깨비 이벤트/정책 목록 수집 (LLM 없음)",
    schedule=SMP_POLICY_MUKKEBI_TIME,
    start_date=pendulum.datetime(2026, 4, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "mukkebi", "policy", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    },
) as dag:

    t1 = PythonOperator(
        task_id='task_extract_notice_list',
        python_callable=task_extract_notice_list,
    )

    t2 = PythonOperator(
        task_id='task_detect_new_notices',
        python_callable=task_detect_new_notices,
    )

    t3 = PythonOperator(
        task_id='task_save_policy_csv',
        python_callable=task_save_policy_csv,
    )

    t4 = PythonOperator(
        task_id='task_write_log',
        python_callable=write_policy_log,
        trigger_rule='all_done',
    )

    # Task 의존성 설정 (LLM 분석 태스크 없음)
    t1 >> t2 >> t3 >> t4
