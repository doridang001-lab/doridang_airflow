"""
배민 크롤링 DAG - 테스트용

============================================================================
사용법
============================================================================

# 크롤링 모드 변경: crawl_baemin() 함수 내 mode 파라미터 수정
# - mode="stats": 통계만 수집 (기본값)
# - mode="all": 모든 기능 (추후 확장용)

============================================================================
"""
from modules.extract.extract_gsheet import extract_gsheet
from modules.extract.croling_beamin import run_baemin_crawling, MODE_STATS
from modules.transform.key_generator import add_surrogate_key
from modules.load.load_gsheet import save_to_gsheet
# from modules.load.load_postgre_db import save_to_db  # DB 사용 시 주석 해제
import modules.common.config as config
import random
import time
import io
import pendulum
import pandas as pd
import os
import json
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)


# ========== 설정 ==========
CRAWL_MODE = "stats"  # "stats" 또는 "all"

GSHEET_URL = "https://docs.google.com/spreadsheets/d/1MEwMFpfIA06C3_PMGcZgIvncnTTcVnkGUN1VxUOE71Y/edit?usp=sharing"
ACCOUNT_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1P9QAcEJX0AIFGFbK_hS_vArCIVMNYaf-0_FVxHnE5hs/edit?usp=sharing"


# ========== 컬럼 순서 정의 ==========
COLUMN_ORDER = [
    'store_op_id',
    'platform',
    'account_id',
    'store_id',
    'store_name',
    'collected_at',
    '조리소요시간',
    '조리소요시간_순위구분',
    '조리소요시간_순위',
    '주문접수시간',
    '주문접수시간_순위구분',
    '주문접수시간_순위',
    '최근재주문율',
    '최근재주문율_순위구분',
    '최근재주문율_순위',
    '조리시간준수율',
    '조리시간준수율_순위구분',
    '조리시간준수율_순위',
    '주문접수율',
    '주문접수율_순위구분',
    '주문접수율_순위',
    '최근별점',
    '최근별점_순위구분',
    '최근별점_순위',
]
# ============================================================
# 태스크 0: 랜덤 대기 함수
# ============================================================

def random_wait():
    wait_time = random.randint(0, 1200) # 0~20분
    time.sleep(wait_time)
    print(f"랜덤 대기 시간: {wait_time}초")


# ============================================================
# 태스크 1: 크롤링
# ============================================================
def crawl_baemin():
    """배민 크롤링 실행"""
    
    # 1. 계정 정보 로드
    account_df = extract_gsheet(
        file_name="판매자_계정",
        url=ACCOUNT_GSHEET_URL,
    )
    account_df["channel"] = account_df["channel"].astype(str).str.replace(" ", "").str.lower()
    account_df = account_df[account_df["channel"] == "baemin"]
    account_df = account_df[account_df["store_ids"].astype(str).str.strip() != ""]
    
    if account_df.empty:
        return json.dumps({"status": "no_data", "reason": "no_accounts"})
    
    # 2. 크롤링 실행 (mode 적용)
    stats_df = run_baemin_crawling(account_df, mode=CRAWL_MODE)
    
    if stats_df.empty:
        return json.dumps({"status": "no_data", "reason": "crawl_empty"})
    
    print(f"크롤링 완료: {len(stats_df)}건 (mode: {CRAWL_MODE})")
    return stats_df.to_json()


# ============================================================
# 태스크 2: 전처리 + 업로드
# ============================================================
def transform_and_upload(ti, **_):
    """전처리 + 업로드"""
    
    # ==========================================================
    # 1. XCom에서 데이터 가져오기
    # ==========================================================
    raw_json = ti.xcom_pull(task_ids='crawl_task')
    
    if not raw_json:
        print("데이터 없음")
        return {"success": False, "reason": "no_data"}
    
    # 크롤링 실패 체크
    try:
        check = json.loads(raw_json)
        if isinstance(check, dict) and check.get("status") == "no_data":
            print(f"크롤링 결과 없음: {check.get('reason')}")
            return {"success": False, "reason": check.get("reason")}
    except:
        pass
    
    df = pd.read_json(io.StringIO(raw_json))
    print(f"원본 데이터: {len(df)}건")
    
    # ==========================================================
    # 2. 전처리
    # ==========================================================
    
    # collected_at 문자열 변환
    df['collected_at'] = df['collected_at'].astype(str)
    
    # store_name 매핑
    store_map = {store['store_id']: store['store_name'] for store in config.STORES}
    df['store_name'] = df['store_id'].map(store_map).fillna('알 수 없음')
    
    # surrogate key 생성
    df = add_surrogate_key(df, ['account_id', 'store_id', 'collected_at'])
    df = df.rename(columns={'key': 'store_op_id'})
    
    # 컬럼 순서 정렬
    existing_columns = [col for col in COLUMN_ORDER if col in df.columns]
    df = df[existing_columns]
    
    print(f"전처리 완료: {len(df)}건")
    print(f"컬럼: {list(df.columns)}")
    
    # ==========================================================
    # 3. 업로드
    # ==========================================================
    results = {}
    
    # Google Sheets 업로드
    results['gsheet'] = save_to_gsheet(
        df=df,
        sheet_name="시트1",
        mode="append_unique",
        primary_key="store_op_id",
        url=GSHEET_URL,
    )
    print(f"Google Sheets: {results['gsheet']}")
    
    # PostgreSQL 업로드 (사용 시 주석 해제)
    # results['db'] = save_to_db(
    #     df=df,
    #     table_name="baemin_store_stats",
    #     if_exists="append",
    # )
    # print(f"PostgreSQL: {results['db']}")
    
    return results


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['crawling', 'baemin'],
) as dag:
    wait_randomly_task = PythonOperator(
        task_id='wait_randomly_task',
        python_callable=random_wait,
        dag=dag
    )
    
    crawl_task = PythonOperator(
        task_id='crawl_task',
        python_callable=crawl_baemin,
    )
    
    transform_upload_task = PythonOperator(
        task_id='transform_upload_task',
        python_callable=transform_and_upload,
    )

wait_randomly_task >> crawl_task >> transform_upload_task