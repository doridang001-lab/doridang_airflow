"""
배달의민족 정책 수집 DAG

📋 처리 흐름:
1. 서비스안내 1페이지 공지 목록 파싱
2. title+policy_date 중복 판정 및 신규 목록 생성
3. 신규 공지 본문 수집 및 LLM(gpt-oss-20b) 기반 정책행 생성
4. policy_type 표준 코드 매핑 (8종: 할인/광고/노출/수수료/정산/운영/기능변경/기타)
5. 정책행 누적 저장 및 중복제거(title+policy_date), 정렬(policy_date desc)

⚙️ 실행 시각: 매일 10:00 (KST, 정책 DAG 5분 간격 분산)
📊 데이터 스키마: policy_id, platform, collected_at, policy_date, title, content_summary, policy_type, recommended_action, source_url
"""

import pendulum
import importlib
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from modules.transform.utility.schedule import SMP_POLICY_BAEMIN_TIME
from modules.transform.utility.notifier import on_failure_callback

# 모듈 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# DAG ID 및 파이프라인 모듈 설정
dag_file_stem = Path(__file__).stem
pipeline_module_name = "SMP_baemin_policy_collect"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

# 파이프라인 함수 import
extract_notice_list = pipeline_module.extract_notice_list
detect_new_notices = pipeline_module.detect_new_notices
collect_policy_rows = pipeline_module.collect_policy_rows
normalize_policy_types = pipeline_module.normalize_policy_types
save_policy_csv = pipeline_module.save_policy_csv
write_policy_log = pipeline_module.write_policy_log


# ============================================================
# Task 1: 공지 목록 파싱
# ============================================================

def task_extract_notice_list(**context):
    """배민 CEO 공지사항 1페이지에서 공지 목록 파싱"""
    
    print(f"\n{'='*60}")
    print("[배민 정책 수집] 시작")
    print(f"{'='*60}")
    
    notice_list = extract_notice_list(**context)
    
    print(f"\n✅ 공지 목록 파싱 완료: {len(notice_list)}개")
    
    # XCom에 저장
    context['ti'].xcom_push(key='notice_list', value=notice_list)
    
    return notice_list


# ============================================================
# Task 2: 신규 공지 판정
# ============================================================

def task_detect_new_notices(**context):
    """title+policy_date 중복 판정 및 신규 목록 생성"""
    
    # 이전 task에서 공지 목록 가져오기
    notice_list = context['ti'].xcom_pull(task_ids='task_extract_notice_list', key='notice_list')
    
    if not notice_list:
        print("⚠️  파싱된 공지가 없습니다. 후속 작업을 스킵합니다.")
        raise AirflowSkipException("공지 목록이 비어있습니다.")
    
    new_notices = detect_new_notices(notice_list=notice_list, **context)
    
    print(f"\n✅ 신규 공지 판정 완료: {len(new_notices)}개")
    
    if not new_notices:
        print("⚠️  신규 공지가 없습니다. 후속 작업을 스킵합니다.")
        raise AirflowSkipException("신규 공지가 없습니다.")
    
    # XCom에 저장
    context['ti'].xcom_push(key='new_notices', value=new_notices)
    
    return new_notices


# ============================================================
# Task 3: 정책 본문 수집 및 LLM 분석
# ============================================================

def task_collect_policy_rows(**context):
    """신규 공지 본문 수집 및 LLM(gpt-oss-20b) 기반 정책행 생성"""
    
    # 이전 task에서 신규 공지 목록 가져오기
    new_notices = context['ti'].xcom_pull(task_ids='task_detect_new_notices', key='new_notices')
    
    if not new_notices:
        print("⚠️  신규 공지가 없습니다. 작업을 스킵합니다.")
        raise AirflowSkipException("신규 공지가 없습니다.")
    
    policy_rows = collect_policy_rows(new_notices=new_notices, **context)
    
    print(f"\n✅ 정책행 생성 완료: {len(policy_rows)}개")
    
    if not policy_rows:
        print("⚠️  생성된 정책행이 없습니다. 후속 작업을 스킵합니다.")
        raise AirflowSkipException("정책행이 생성되지 않았습니다.")
    
    # XCom에 저장
    context['ti'].xcom_push(key='policy_rows', value=policy_rows)
    
    return policy_rows


# ============================================================
# Task 4: policy_type 정규화
# ============================================================

def task_normalize_policy_types(**context):
    """policy_type 표준 코드 매핑 (8종)"""
    
    # 이전 task에서 정책행 가져오기
    policy_rows = context['ti'].xcom_pull(task_ids='task_collect_policy_rows', key='policy_rows')
    
    if not policy_rows:
        print("⚠️  정책행이 없습니다. 작업을 스킵합니다.")
        raise AirflowSkipException("정책행이 없습니다.")
    
    normalized_rows = normalize_policy_types(policy_rows=policy_rows, **context)
    
    print(f"\n✅ policy_type 정규화 완료: {len(normalized_rows)}개")
    
    # XCom에 저장
    context['ti'].xcom_push(key='normalized_rows', value=normalized_rows)
    
    return normalized_rows


# ============================================================
# Task 5: CSV 저장
# ============================================================

def task_save_policy_csv(**context):
    """정책행 누적 저장 및 중복제거, 정렬"""
    
    # 이전 task에서 정규화된 정책행 가져오기
    normalized_rows = context['ti'].xcom_pull(task_ids='task_normalize_policy_types', key='normalized_rows')
    
    if not normalized_rows:
        print("⚠️  정규화된 정책행이 없습니다. 작업을 스킵합니다.")
        raise AirflowSkipException("정규화된 정책행이 없습니다.")
    
    saved_path = save_policy_csv(normalized_rows=normalized_rows, **context)
    
    print(f"\n{'='*60}")
    print(f"✅ 배민 정책 수집 완료")
    print(f"   저장 경로: {saved_path}")
    print(f"   처리 건수: {len(normalized_rows)}개")
    print(f"{'='*60}\n")
    
    return saved_path


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="배달의민족 정책 변경 수집 및 LLM 기반 분석",
    schedule=SMP_POLICY_BAEMIN_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "baemin", "policy", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    },
) as dag:
    
    # Task 정의
    t1 = PythonOperator(
        task_id='task_extract_notice_list',
        python_callable=task_extract_notice_list,
    )
    
    t2 = PythonOperator(
        task_id='task_detect_new_notices',
        python_callable=task_detect_new_notices,
    )
    
    t3 = PythonOperator(
        task_id='task_collect_policy_rows',
        python_callable=task_collect_policy_rows,
    )
    
    t4 = PythonOperator(
        task_id='task_normalize_policy_types',
        python_callable=task_normalize_policy_types,
    )
    
    t5 = PythonOperator(
        task_id='task_save_policy_csv',
        python_callable=task_save_policy_csv,
    )

    t6 = PythonOperator(
        task_id='task_write_log',
        python_callable=write_policy_log,
        trigger_rule='all_done',
    )

    # Task 의존성 설정
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
