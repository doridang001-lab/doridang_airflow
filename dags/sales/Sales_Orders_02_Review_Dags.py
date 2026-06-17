# dags/etl/SMD_02_sales_orders_csv_review.py (또는 원하는 파일명)
"""
쿠팡이츠 쿠폰 데이터 로드 DAG

🔄 두 가지 모드:
1. 정기 실행 (월/수 10:40): 원본 데이터 새로 로드
2. 재업로드 모드: "업로드_temp" + "원드라이브"의 이전 파일들 재사용
"""
import pendulum
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor  # ⭐ 추가
from airflow.models import DagRun
from airflow.utils.state import State
from airflow import settings
from modules.transform.utility.io import SMD_ORDERS_TIME


# ============================================================
# ExternalTaskSensor용 최신 성공 execution_date 검색
# ============================================================
def _latest_smd01_execution_date(dt, **context):
    """SMD_01의 save_to_csv 태스크가 성공한 최신 execution_date를 찾는다."""
    from airflow.models.taskinstance import TaskInstance as TI
    
    session = settings.Session()
    try:
        ti = session.query(TI).filter(
            TI.dag_id == 'Sales_Orders_01_Extract_Dags',
            TI.task_id == 'save_to_csv',
            TI.state == 'success'
        ).order_by(TI.execution_date.desc()).first()
        
        if ti:
            print(f"[sensor] SMD_01 save_to_csv 성공 찾음: {ti.execution_date}")
            return ti.execution_date
    except Exception as e:
        print(f"[sensor] TaskInstance 조회 실패: {e}")
    finally:
        session.close()
    
    print(f"[sensor] 성공한 save_to_csv 없음, 기본값 사용: {dt}")
    return dt

# 함수 import
from modules.transform.pipelines.sales.SMD_02_sales_orders_csv_review import (
    load_sales_orders_daily_csv,
    clear_sales_orders_daily_csv,
    fin_save_to_csv

)
from modules.transform.utility.notifier import on_failure_callback

# ==================================================
# DAG 정의
# ==================================================
filename = os.path.basename(__file__)

with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SMD_ORDERS_TIME,  # 매주 월,수 10:30 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 🔒 동시 실행 방지
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    "on_failure_callback": on_failure_callback,
    },
    tags=['02_sales', 'crawling', 'coupang'],
) as dag:
    
    # ⭐ SMD_01 완료 대기 (최신 성공 실행 자동 검색)
    wait_for_smd_01 = ExternalTaskSensor(
        task_id='wait_for_smd_01_completion',
        external_dag_id='Sales_Orders_01_Extract_Dags',
        external_task_id='save_to_csv',  # SMD_01의 마지막 task
        execution_date_fn=_latest_smd01_execution_date,  # ⭐ 최신 성공 실행 찾기
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',  # poke → reschedule (작업 슬롯 반환)
        timeout=3600,  # 1시간 타임아웃
        poke_interval=60,  # 1분마다 체크
        soft_fail=True,  # ⭐ timeout 시에도 계속 진행
    )
    
    # 데이터 로드
    load_sales_orders_daily_csv = PythonOperator(
        task_id='load_sales_orders_daily_csv',
        python_callable=load_sales_orders_daily_csv,
    )
    
    
    # 기존 데이터 삭제 <<
    clear_sales_orders_daily_csv = PythonOperator(
        task_id='clear_sales_orders_daily_csv',
        python_callable=clear_sales_orders_daily_csv,
    )
    
    # 최종 저장
    fin_save_to_csv = PythonOperator(
        task_id='fin_save_to_csv',
        python_callable=fin_save_to_csv,
        op_kwargs={
            'input_task_id': 'clear_sales_orders_daily_csv',
            'input_xcom_key': 'sales_orders_daily_processed_path',
            'output_filename': 'sales_daily_orders.csv',
            'output_subdir': '영업관리부_DB'
        }
    )
    
    
    wait_for_smd_01 >> load_sales_orders_daily_csv  >> clear_sales_orders_daily_csv >> fin_save_to_csv
