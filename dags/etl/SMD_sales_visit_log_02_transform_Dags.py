# dags/etl/load_coupang_coupon.py (또는 원하는 파일명)
"""
쿠팡이츠 쿠폰 데이터 로드 DAG

🔄 두 가지 모드:
1. 정기 실행 (월/수 10:45): 원본 데이터 새로 로드
2. 재업로드 모드: "업로드_temp" + "원드라이브"의 이전 파일들 재사용
"""
import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor  # task 의존성 추가
from modules.transform.utility.paths import DOWN_DIR
from modules.transform.utility.io3 import SMD_VISIT_LOG
from airflow.models import DagRun
from airflow.utils.state import State

# 함수 import
from modules.transform.pipelines.SMD_sales_visit_log_02_transform import (
    load_flow_vist_log_df,
    load_flow_vist_log_master_df,
    join_master_flow_df,
    preprocess_visit_new_df,
    preprocess_llm_df,
    append_to_master,
    move_files
)

# ==================================================
# DAG 정의
# ==================================================
filename = os.path.basename(__file__)


def _latest_successful_execution_date(dt, **context):
    """외부 DAG의 최신 성공 실행일을 사용 (없으면 현재 dt 사용)."""
    runs = DagRun.find(dag_id='SMD_sales_visit_log_01_crawling_Dags', state=State.SUCCESS)
    if not runs:
        return dt
    return max(r.execution_date for r in runs)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMD_VISIT_LOG,  # 매주 화 오전 10시
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    wait_for_smd_01 = ExternalTaskSensor(
        task_id='wait_for_smd_01',
        external_dag_id='SMD_sales_visit_log_01_crawling_Dags',
        external_task_id='crawl_flow_visit',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=3600,  # 60분 대기
        poke_interval=60,  # 1분마다 확인
        check_existence=True,  # 태스크 존재 여부 확인
        soft_fail=False,  # 실패 시 즉시 실패
        execution_date_fn=_latest_successful_execution_date,
    )
    
    
    load_flow_visit_log = PythonOperator(
        task_id='load_flow_visit_log',
        python_callable=load_flow_vist_log_df,
    )
    
    load_flow_visit_log_master = PythonOperator(
        task_id='load_flow_visit_log_master',
        python_callable=load_flow_vist_log_master_df,
    )
    
    # load_flow_visit_log + load_flow_visit_log_master left 조인
    join_master_flow = PythonOperator(
        task_id='join_master_flow',
        python_callable=join_master_flow_df,
        op_kwargs={
            'left_task': {'task_id': 'load_flow_visit_log', 'xcom_key': 'flow_visit_log_path'},
            'right_task': {'task_id': 'load_flow_visit_log_master', 'xcom_key': 'visit_sales_log_master_path'},
            'on': ['post_date', 'author'],
            'how': 'left',
            'drop_right_keys': True,
            'output_xcom_key': 'joined_flow_visit_log_path'
        }
    )
    
    # new 방문일지 전처리
    preprocess_visit_new = PythonOperator(
        task_id='preprocess_visit_new',
        python_callable=preprocess_visit_new_df,
        op_kwargs={
            'input_xcom_key': 'joined_flow_visit_log_path', # 조인된 데이터 경로
            'output_xcom_key': 'processed_flow_visit_log_path', # 전처리된 데이터 경로
        }
    )
    
    # LLM 방문일지 자동 분석
    preprocess_llm = PythonOperator(
        task_id='preprocess_llm',
        python_callable=preprocess_llm_df,
        op_kwargs={
            'input_xcom_key': 'processed_flow_visit_log_path', # 전처리된 데이터
            'output_xcom_key': 'llm_processed_visit_log_path', # LLM 처리 완료 데이터
        }
    )
    
    # 마스터 파일에 중복 검사 후 append
    append_master = PythonOperator(
        task_id='append_to_master',
        python_callable=append_to_master,
    )
    
    # load_flow_visit_log 파일이동
    move_files_task = PythonOperator(
        task_id='move_files', # 파일 이동 태스크
        python_callable=move_files,
        op_kwargs={
            'patterns': ['flow_visit_*.csv'],
            'source_dir': '/opt/airflow/download/업로드_temp',
            'dest_dir': str(DOWN_DIR / 'flow_visit_archive'),
        }
    )


    wait_for_smd_01 >> [load_flow_visit_log, load_flow_visit_log_master] >> join_master_flow >> preprocess_visit_new >> preprocess_llm >> append_master >> move_files_task