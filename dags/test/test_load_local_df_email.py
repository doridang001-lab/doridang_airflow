from airflow import DAG
import os
import sys
import pendulum
from pathlib import Path
from airflow.operators.python import PythonOperator
import pandas as pd


# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 메일 받을 사람들
to_members = ['a17019@kakao.com']

# DB 데이터 로드 함수 및 이메일 전송 함수
from modules import read_local_file, send_email_graph

# 현재 파일명 (DAG ID로 사용)
filename = os.path.basename(__file__)

# PythonOperator에서 실행할 함수
# doridang 스키마의 baemin_sales 테이블에서 데이터 조회
def fetch_and_send_email(**context):
    # coupang_eats_2* 패턴으로 최신 파일 자동 선택, 처리 후 삭제
    df = read_local_file(
        # "E:\d_down\coupang_baemin_orders_daily_score_닭도리탕_전문_도리당_가락점_20251230.csv"
        file_name="coupang_baemin_orders_daily_score_*",
        select="latest",
        delete_after=True  # 작업 완료 후 파일 삭제
    ).head()

    # DataFrame을 HTML 테이블로 변환 (깔끔한 형식)
    df_html = df.to_html(index=False, border=0)
    
    # Microsoft Graph API를 사용하여 이메일 발송
    html_content = f"""
    <html>
        <body>
            <p>조회된 데이터:</p>
            {df_html}
            <p><br/>삭제 됬는지 확인하기.</p>
        </body>
    </html>
    """
    
    result = send_email_graph(
        conn_id='msgraph_outlook',
        to=to_members,
        subject='df print test 메일',
        html_content=html_content
    )
    
    return result

# DAG 정의
with DAG(
    dag_id=filename.replace('.py',''),  # DAG ID = test_load_local_df_email
    schedule="31 6 * * *",  # 매일 아침 6시 31분 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),  # 시작 날짜
    catchup=False,  # 과거 실행 스킵
    max_active_runs=1,  # 동시 실행 최대 1개 (중복 방지)
) as dag:
    # Task: 데이터 조회 및 이메일 발송
    fetch_and_send_task = PythonOperator(
        task_id="fetch_and_send_email",
        python_callable=fetch_and_send_email,  # 데이터 조회 및 이메일 발송 함수
    )