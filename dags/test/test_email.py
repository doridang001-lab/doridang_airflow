import pendulum
from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
filename = os.path.basename(__file__)

to_members = ['doridang001@gmail.com', 'a17019@kakao.com']

with DAG(
    dag_id=filename.replace('.py',''),
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='doridang_conn_smtp_gmail',
        to=to_members,
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )