"""
투오더 크롤링 DAG - 간단 버전
"""

import glob
import os
import sys
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract.croling_toorder import run_toorder_crawling
from modules.load.load_onedrive_csv import load_to_onedrive_csv

# 현재 파일명 (DAG ID로 사용)
filename = os.path.basename(__file__)

# ========== 설정 ==========
TOORDER_ACCOUNTS = pd.DataFrame([
    {"channel": "toorder", 
     "id": "doridang1", 
     "pw": "ehfl3122"},
])

from modules.transform.utility.paths import ONEDRIVE_DB

DOWNLOAD_DIR = Path.home() / "Downloads" / "airflow"
ONEDRIVE_SALES_CSV = ONEDRIVE_DB / "영업팀_DB" / "orders.csv"


# ============================================================
# Task 1: 크롤링
# ============================================================
def crawl_toorder(**context):
    """투오더 크롤링 실행"""
    # 어제 날짜
    target_date = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
    
    # 크롤링 실행
    result_df = run_toorder_crawling(
        account_df=TOORDER_ACCOUNTS,
        target_date=target_date,
    )
    
    # XCom에 날짜 저장
    context['task_instance'].xcom_push(key='target_date', value=target_date)
    print(f"크롤링 완료: {len(result_df)}개 파일")
    return f"크롤링 완료: {len(result_df)}개 파일"


# ============================================================
# Task 2: CSV 저장
# ============================================================

def save_to_onedrive(**context):
    """다운로드된 투오더 파일을 OneDrive CSV에 누적 저장"""
    ti = context['task_instance']
    target_date = ti.xcom_pull(task_ids='crawl_task', key='target_date')

    pattern = str(DOWNLOAD_DIR / f"toorder_*_{target_date}.*")
    file_list = glob.glob(pattern)

    if not file_list:
        print(f"❌ 파일 없음: {pattern}")
        return "파일 없음"

    result = load_to_onedrive_csv(
        df=file_list, # df 전달
        onedrive_csv_path=str(ONEDRIVE_SALES_CSV),
        target_date=target_date,
        platform="toorder",
        duplicate_subset=['주문번호', '주문일시'],  # 중복 제거 기준 컬럼
        delete_local_files=False,  # 필요 시 True로 설정하면 로컬 파일 삭제
    )

    return result.get("message", "저장 완료")


# ============================================================
# Task 3: 이메일 전송
# ============================================================
from airflow.providers.smtp.operators.smtp import EmailOperator

to_members = ['a17019@kakao.com']

# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 3 * * *",  # 매일 새벽 3시
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=['crawling', 'toorder']
) as dag:
    
    # Task 1: 크롤링
    crawl_task = PythonOperator(
        task_id='crawl_task',
        python_callable=crawl_toorder,
    )
    
    # Task 2: CSV 저장
    save_task = PythonOperator(
        task_id='save_task',
        python_callable=save_to_onedrive,
    )

    # Task 3: 이메일 알림
    email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='conn_smtp_gmail',  # Gmail SMTP 연결 설정
        to=to_members,  # 받는 사람 메일 주소
        subject='df print test 메일',  # 메일 제목
        # Jinja2 템플릿: fetch_task의 반환값을 메일 내용에 삽입
        html_content="""
        <html>
            <body>
                <p>조회된 데이터:</p>
                {{ task_instance.xcom_pull(task_ids='fetch_df', key='df_html') }}
                <p><br/>삭제 됬는지 확인하기.</p>
            </body>
        </html>
        """,
    )

    # Task 의존성
    crawl_task >> save_task >> email_task