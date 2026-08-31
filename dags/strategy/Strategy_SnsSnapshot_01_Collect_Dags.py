"""
SNS 스냅샷 일일 수집 DAG

처리 흐름:
1. 인스타그램 프로필(doridang_official) 렌더 → 팔로워/팔로잉/게시물 수 수집
2. 카카오톡 채널(_UxiaxiG, 도리당) 렌더 → 친구 수 수집
3. 각각 OneDrive analytics/Instagram, analytics/Kakao/Friends 에 1행 누적 저장

두 페이지 모두 CSR이라 Playwright로 렌더한 뒤 홈페이지에서 직접 읽는다 (API 미사용).
수집은 직렬로 돈다 — headless Chrome을 동시에 2개 띄우지 않기 위해서다.
collect_kakao/save_snapshots에 ALL_DONE을 걸어 인스타 실패가 카카오 수집·저장을 막지 않게 한다.

실패 시 on_failure_callback이 텔레그램 + 이메일 알림을 보낸다.

실행 시각: 매일 04:00 (KST)
"""

import importlib
import sys
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from modules.transform.utility.schedule import SMP_SNS_SNAPSHOT_TIME
from modules.transform.utility.notifier import on_failure_callback

# 모듈 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# DAG ID 및 파이프라인 모듈 설정
dag_file_stem = Path(__file__).stem
pipeline_module_name = "SMP_sns_snapshot_collect"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

# 파이프라인 함수 import
collect_instagram = pipeline_module.collect_instagram
collect_kakao = pipeline_module.collect_kakao
save_snapshots = pipeline_module.save_snapshots


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="인스타그램/카카오톡채널 팔로워·친구수 일일 스냅샷 수집",
    schedule=SMP_SNS_SNAPSHOT_TIME,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "instagram", "kakao", "sns", "daily"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
) as dag:

    # --------------------------------------------------------
    # Task 1: 인스타그램 수집
    # --------------------------------------------------------

    t_instagram = PythonOperator(
        task_id="collect_instagram",
        python_callable=collect_instagram,
    )

    # --------------------------------------------------------
    # Task 2: 카카오톡 채널 수집
    # --------------------------------------------------------

    t_kakao = PythonOperator(
        task_id="collect_kakao",
        python_callable=collect_kakao,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --------------------------------------------------------
    # Task 3: 누적 CSV 저장
    # --------------------------------------------------------

    t_save = PythonOperator(
        task_id="save_snapshots",
        python_callable=save_snapshots,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --------------------------------------------------------
    # 의존성
    # --------------------------------------------------------

    t_instagram >> t_kakao >> t_save
