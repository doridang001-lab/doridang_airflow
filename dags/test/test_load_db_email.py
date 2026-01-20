from airflow import DAG
import os
import sys
import pendulum
from pathlib import Path
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 메일 받을 사람들
to_members = ['a17019@kakao.com']

# DB 데이터 로드 함수
from modules import db_load_data, read_local_file, send_email_graph

# 현재 파일명 (DAG ID로 사용)
filename = os.path.basename(__file__)

# PythonOperator에서 실행할 함수
# doridang 스키마의 baemin_sales 테이블에서 데이터 조회
def fetch_df(**context):
    df = db_load_data(
        schema="doridang",
        table="baemin_sales",
    )
    # 필터링 예시: airflow_db_load_data(columns=["order_id"], order_by=["-order_amount"])


    # DataFrame을 HTML 테이블로 변환 (깔끔한 형식)
    df_html = df.to_html(index=False, border=0)
    # XCom에 저장 (EmailOperator가 가져갈 데이터)
    context['task_instance'].xcom_push(key='df_html', value=df_html)
    return df_html

# DAG 정의
with DAG(
    dag_id=filename.replace('.py',''),  # DAG ID = test_load_db_email
    schedule="31 6 * * *",  # 매일 아침 6시 31분 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),  # 시작 날짜
    catchup=False,  # 과거 실행 스킵
    max_active_runs=1,  # 동시 실행 최대 1개 (중복 방지)
) as dag:
    # Task 1: DB에서 데이터 조회
    fetch_task = PythonOperator(
        task_id="fetch_df",
        python_callable=fetch_df,  # 위에서 정의한 fetch_df 함수 실행
    )

    # Task 2: 조회한 데이터를 메일로 발송
    send_email_task = EmailOperator(
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
                <p><br/>df 출력 테스트 메일입니다.</p>
            </body>
        </html>
        """,
    )

    # Task 의존성: fetch_task 완료 후 send_email_task 실행
    fetch_task >> send_email_task