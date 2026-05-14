"""
📧 매출 이상 알림 전용 DAG (SMD_07)

SMD_06 후속으로 알림 이메일 발송

📋 처리 흐름:
1. SMD_06 완료 대기 (Google Sheets 업로드 완료)
2. CSV 파일에서 알림 대상 필터링 (위험/주의 2일 연속)
3. 담당자별 알림 이메일 발송

⚙️ 실행 조건:
- SMD_06 완료 후 자동 실행 (ExternalTaskSensor)
- 또는 수동 실행 가능

📊 알림 기준:
- 위험 (5~6점): 즉시 알림
- 주의 (3~4점): 즉시 알림
- 정상 (0~2점): 알림 없음

📁 입력 파일:
- sales_daily_orders_upload.csv (SMD_06이 생성)
"""

import pendulum
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from modules.transform.utility.io import SMD_ORDERS_TIME

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 함수 임포트
from modules.transform.pipelines.sales.SMD_07_store_ordesr_alert import (
    filter_alerts,
    send_alert_email,
    upload_llm_to_gsheet,
)

# 파일명
filename = os.path.basename(__file__)


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='📧 매출 이상 알림 전용 DAG (SMD_06 후속)',
    schedule=SMD_ORDERS_TIME,  # SMD_06 완료 후 자동 실행 또는 스케줄 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['alert', 'email', '알림'],
) as dag:
    
    # ============================================================
    # 1️⃣ SMD_06 완료 대기 (Google Sheets 업로드 완료)
    # ============================================================
    wait_for_smd_06 = ExternalTaskSensor(
        task_id='wait_for_smd_06',
        external_dag_id='Sales_Orders_06_Gsheet_Dags',
        external_task_id='upload_alerts_to_gsheet',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
        check_existence=True,
        soft_fail=False,
    )
    
    # ============================================================
    # 2️⃣ 알림 대상 필터링
    # ============================================================
    filter_alerts_task = PythonOperator(
        task_id='filter_alerts',
        python_callable=filter_alerts,
        op_kwargs={
            'output_xcom_key': 'alert_targets'
        }
    )
    
    # ============================================================
    # 3️⃣ 매출 이상 알림 이메일 발송
    # ============================================================
    send_alert_task = PythonOperator(
        task_id='send_alert_email',
        python_callable=send_alert_email,
    )

    # ============================================================
    # 4️⃣ LLM 컬럼 GSheet 업로드
    # ============================================================
    llm_gsheet_task = PythonOperator(
        task_id='upload_llm_to_gsheet',
        python_callable=upload_llm_to_gsheet,
    )

    # ============================================================
    # Task 의존성
    # ============================================================
    wait_for_smd_06 >> filter_alerts_task >> send_alert_task >> llm_gsheet_task
