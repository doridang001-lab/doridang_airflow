"""
육계 시세 수집 DAG

처리 흐름:
1. poultry.or.kr / chicken.or.kr 에서 육계 '대' 시세 병렬 수집
2. 각 결과 이상치 검증 (전일 대비 20% 이상 변동 플래그)
3. 두 결과 병합 → JSON XCom 전달
4. CHICKEN_PRICE_CSV_PATH 누적 저장 (중복 제거)
5. 이동평균(20/60/120일) 포함 이메일 알림 발송

실행 시각: 매일 09:10 (KST)
"""

import importlib
import sys
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.transform.utility.schedule import SMP_CHICKEN_PRICE_TIME

# 모듈 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# DAG ID 및 파이프라인 모듈 설정
dag_file_stem = Path(__file__).stem
pipeline_module_name = "SMP_chicken_price_collect"
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

# 파이프라인 함수 import
extract_poultry_price = pipeline_module.extract_poultry_price
extract_chicken_price = pipeline_module.extract_chicken_price
validate_price = pipeline_module.validate_price
merge_results = pipeline_module.merge_results
save_results = pipeline_module.save_results
send_notification = pipeline_module.send_notification


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="poultry.or.kr / chicken.or.kr 육계 시세 일일 수집 및 알림",
    schedule=SMP_CHICKEN_PRICE_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "chicken", "price", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    },
) as dag:

    # --------------------------------------------------------
    # Task 1: 수집 (병렬)
    # --------------------------------------------------------

    t_extract_poultry = PythonOperator(
        task_id="extract_poultry_price",
        python_callable=extract_poultry_price,
    )

    t_extract_chicken = PythonOperator(
        task_id="extract_chicken_price",
        python_callable=extract_chicken_price,
    )

    # --------------------------------------------------------
    # Task 2: 이상치 검증 (각 수집 결과에 독립 적용)
    # --------------------------------------------------------

    t_validate_poultry = PythonOperator(
        task_id="validate_poultry_price",
        python_callable=validate_price,
        op_kwargs={"task_id": "extract_poultry_price"},
    )

    t_validate_chicken = PythonOperator(
        task_id="validate_chicken_price",
        python_callable=validate_price,
        op_kwargs={"task_id": "extract_chicken_price"},
    )

    # --------------------------------------------------------
    # Task 3: 결과 병합
    # --------------------------------------------------------

    t_merge = PythonOperator(
        task_id="merge_results",
        python_callable=merge_results,
    )

    # --------------------------------------------------------
    # Task 4: CSV 저장
    # --------------------------------------------------------

    t_save = PythonOperator(
        task_id="save_results",
        python_callable=save_results,
    )

    # --------------------------------------------------------
    # Task 5: 이메일 알림
    # --------------------------------------------------------

    t_notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    # --------------------------------------------------------
    # 의존성
    # --------------------------------------------------------

    t_extract_poultry >> t_validate_poultry
    t_extract_chicken >> t_validate_chicken
    [t_validate_poultry, t_validate_chicken] >> t_merge >> t_save >> t_notify
