import pendulum
import sys
from pathlib import Path

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from modules import check_threshold

to_members = ['tjrrjwu92@gmail.com', 'a17019@kakao.com']

import os
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
filename = os.path.basename(__file__)

def fetch_and_check(ti, **_):
    # TODO: coupang_eates_sales_df() 함수가 삭제되어 임시 주석 처리
    # df = coupang_eates_sales_df()
    # 임시로 빈 DataFrame 사용
    import pandas as pd
    df = pd.DataFrame()
    
    # 예시 1: 주문금액 합계가 10만원 미만이면 알림
    is_alert, message = check_threshold(
        df=df,
        target_col='주문금액',
        threshold=100000,
        agg_func='sum',
        condition='lt'
    )
    
    # 예시 2: 주문건수가 5건 이하면 알림
    # is_alert, message = check_threshold(
    #     df=df,
    #     target_col='order_id',
    #     threshold=5,
    #     agg_func='count',
    #     condition='le',
    #     alert_message='주문건수 {value}건 (최소 {threshold}건 필요)'
    # )
    
    ti.xcom_push(key='alert_message', value=message)
    return is_alert

with DAG(
    dag_id=filename.replace('.py',''),
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    check_task = ShortCircuitOperator(
        task_id='check_sales',
        python_callable=fetch_and_check
    )
    
    alert_task = EmailOperator(
        task_id='send_alert',
        conn_id='conn_smtp_gmail',
        to=to_members,
        subject='매출 임계값 미달 알림',
        html_content="{{ ti.xcom_pull(task_ids='check_sales', key='alert_message') }}"
    )
    
    check_task >> alert_task