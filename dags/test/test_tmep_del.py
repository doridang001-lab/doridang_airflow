"""
쿠팡이츠 크롤링 DAG - 테스트용

주의: 네트워크 호출은 태스크 실행 시점에만 발생하도록 구성합니다.
"""
from modules.extract.extract_gsheet import extract_gsheet
import pendulum
import pandas as pd
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract import run_coupang_crawling

# 파일명
filename = os.path.basename(__file__)

def crawl_coupang_eats():
    """쿠팡이츠 크롤링 실행 (컨텍스트 인자 없이)"""
    try:
        # ✅ 구글시트에서 계정 정보 로드 (실행 시점)
        account_df = extract_gsheet(
            file_name="판매자_계정",
            url="https://docs.google.com/spreadsheets/d/1P9QAcEJX0AIFGFbK_hS_vArCIVMNYaf-0_FVxHnE5hs/edit?usp=sharing",
        )
        account_df = account_df[account_df["channel"] == "coupang_eats"]

        
    
        target_date = pendulum.now("Asia/Seoul").format("YYYY-MM")

        # 크롤링 실행
        stats_df = run_coupang_crawling(account_df, target_date)

        if stats_df.empty:
            raise AirflowException("수집된 데이터가 없습니다")

        # PythonOperator는 반환값을 자동으로 XCom에 저장합니다.
        return stats_df.to_json()

    except Exception as e:
        raise AirflowException(f"크롤링 실패: {str(e)}")



# DAG 정의
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 3 * * *",  # 매일 새벽 3시
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['crawling', 'coupang'],
) as dag:
    coupang_crawl_task = PythonOperator(
        task_id='coupang_crawl_task',
        python_callable=crawl_coupang_eats,
    )

