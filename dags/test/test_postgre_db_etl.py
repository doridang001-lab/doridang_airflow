from airflow import DAG
import os
import sys
import pendulum
from pathlib import Path
from airflow.operators.python import PythonOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 함수 정의
from modules import add_surrogate_key
from modules import postgre_db_save

filename = os.path.basename(__file__)

# 데이터 정의
def fetch_df(**_):
    # TODO: coupang_eates_sales_df() 함수가 삭제되어 임시 주석 처리
    # df = coupang_eates_sales_df()
    # 임시로 빈 DataFrame 사용
    import pandas as pd
    df = pd.DataFrame()
    df.rename(columns={"key" : "order_id",
                   "주문번호" : "order_number",
                    "일자" : "order_date",
                    "시간" : "order_time",
                    "주문금액" : "order_amount"
                   }, inplace=True)
    return df

# PostgreSQL DB 업로드
def db_upload_df(ti, **_):
    df = ti.xcom_pull(task_ids="fetch_df")
    return postgre_db_save(
        df=df,
        table="baemin_sales",
        schema="doridang",
        pk_col="order_id",
        if_exists="append"
    )

with DAG(
    dag_id=filename.replace('.py',''),
    schedule="31 6 * * *",  # 6시 31분 도는 데일리 작업
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_df",
        python_callable=fetch_df,
    )

    db_upload_task = PythonOperator(
        task_id="db_upload_df",
        python_callable=db_upload_df,
    )

    fetch_task >> db_upload_task
