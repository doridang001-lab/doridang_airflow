"""
쿠팡이츠 크롤링 DAG - 테스트용

============================================================================
사용법
============================================================================

# 크롤링 모드 변경: CRAWL_MODE 변수 수정
# - mode="stats": 통계만 수집
# - mode="download": 파일만 다운로드 (target_date 필수)
# - mode="all": 둘 다 (target_date 필수)

============================================================================
"""
from modules.extract.extract_gsheet import extract_gsheet
from modules.extract.croling_coupang import run_coupang_crawling, MODE_STATS, MODE_DOWNLOAD, MODE_ALL
from modules.transform.key_generator import add_surrogate_key
from modules.load.load_gsheet import save_to_gsheet
# from modules.load.load_postgre_db import save_to_db  # DB 사용 시 주석 해제
import modules.common.config as config

import io
import pendulum
import pandas as pd
import os
import json
import glob
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)


# ========== 설정 ==========
CRAWL_MODE = "all"  # "stats", "download", "all"

GSHEET_STATS_URL = "https://docs.google.com/spreadsheets/d/1xnnCmS_7ZCDJZVpmv70AgLHcpSJnaXt3g1gGaaFJUM0/edit?usp=sharing"
GSHEET_SALES_URL = "https://docs.google.com/spreadsheets/d/1xnnCmS_7ZCDJZVpmv70AgLHcpSJnaXt3g1gGaaFJUM0/edit?usp=sharing"  # 매출 데이터용
ACCOUNT_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1P9QAcEJX0AIFGFbK_hS_vArCIVMNYaf-0_FVxHnE5hs/edit?usp=sharing"

DOWNLOAD_DIR = Path("/opt/airflow/download")


# ========== 컬럼 순서 정의 (통계) ==========
STATS_COLUMN_ORDER = [
    'store_op_id',
    'platform',
    'account_id',
    'store_id',
    'store_name',
    'collected_at',
    'avg_order_amount',
    'total_order_count',
]

# ========== 컬럼 순서 정의 (매출) ==========
SALES_COLUMN_ORDER = [
    'sales_id',
    'platform',
    'account_id',
    'store_id',
    'store_name',
    # ... 매출 파일 컬럼에 맞게 수정
]


# ============================================================
# 태스크 1: 크롤링
# ============================================================
def crawl_coupang():
    """쿠팡이츠 크롤링 실행"""
    
    # 1. 계정 정보 로드
    account_df = extract_gsheet(
        file_name="판매자_계정",
        url=ACCOUNT_GSHEET_URL,
    )
    account_df["channel"] = account_df["channel"].astype(str).str.replace(" ", "").str.lower()
    account_df = account_df[account_df["channel"] == "coupangeats"]
    account_df = account_df[account_df["store_ids"].astype(str).str.strip() != ""]
    
    if account_df.empty:
        return json.dumps({"status": "no_data", "reason": "no_accounts"})
    
    # 2. target_date 설정
    target_date = None
    if CRAWL_MODE in [MODE_DOWNLOAD, MODE_ALL, "download", "all"]:
        target_date = pendulum.now("Asia/Seoul").format("YYYY-MM")
    
    # 3. 크롤링 실행
    stats_df = run_coupang_crawling(
        account_df=account_df,
        mode=CRAWL_MODE,
        target_date=target_date,
    )
    
    # 4. 결과 반환 (stats + target_date 정보)
    result = {
        "target_date": target_date,
        "stats": stats_df.to_dict(orient='records') if not stats_df.empty else [],
    }
    
    print(f"크롤링 완료: 통계 {len(stats_df)}건 (mode: {CRAWL_MODE})")
    return json.dumps(result)


# ============================================================
# 태스크 2: 통계 전처리 + 업로드
# ============================================================
def transform_stats_upload(ti, **_):
    """통계 데이터 전처리 + 업로드"""
    
    # 1. XCom에서 데이터 가져오기
    raw_json = ti.xcom_pull(task_ids='crawl_task')
    
    if not raw_json:
        print("데이터 없음")
        return {"success": False, "reason": "no_data"}
    
    data = json.loads(raw_json)
    
    # 상태 체크
    if data.get("status") == "no_data":
        print(f"크롤링 결과 없음: {data.get('reason')}")
        return {"success": False, "reason": data.get("reason")}
    
    stats_list = data.get("stats", [])
    if not stats_list:
        print("통계 데이터 없음")
        return {"success": False, "reason": "no_stats"}
    
    df = pd.DataFrame(stats_list)
    print(f"원본 통계 데이터: {len(df)}건")
    
    # 2. 전처리
    df['collected_at'] = df['collected_at'].astype(str)
    
    store_map = {store['store_id']: store['store_name'] for store in config.STORES}
    df['store_name'] = df['store_id'].map(store_map).fillna('알 수 없음')
    
    df = add_surrogate_key(df, ['account_id', 'store_id', 'collected_at'])
    df = df.rename(columns={'key': 'store_op_id'})
    
    existing_columns = [col for col in STATS_COLUMN_ORDER if col in df.columns]
    df = df[existing_columns]
    
    print(f"전처리 완료: {len(df)}건")
    
    # 3. 업로드
    results = {}
    
    # Google Sheets 업로드
    results['gsheet'] = save_to_gsheet(
        df=df,
        sheet_name="통계",
        mode="append_unique",
        primary_key="store_op_id",
        url=GSHEET_STATS_URL,
    )
    print(f"Google Sheets (통계): {results['gsheet']}")
    
    # PostgreSQL 업로드 (사용 시 주석 해제)
    # results['db'] = save_to_db(
    #     df=df,
    #     table_name="coupang_store_stats",
    #     if_exists="append",
    # )
    
    return results


# ============================================================
# 태스크 3: 다운로드 파일 전처리 + 업로드
# ============================================================
def transform_sales_upload(ti, **_):
    """다운로드된 매출 파일 전처리 + 업로드"""
    
    # 1. XCom에서 target_date 가져오기
    raw_json = ti.xcom_pull(task_ids='crawl_task')
    
    if not raw_json:
        print("크롤링 결과 없음")
        return {"success": False, "reason": "no_crawl_data"}
    
    data = json.loads(raw_json)
    target_date = data.get("target_date")
    
    if not target_date:
        print("다운로드 모드가 아님 (target_date 없음)")
        return {"success": False, "reason": "no_download_mode"}
    
    # 2. 다운로드된 파일 로드
    pattern = str(DOWNLOAD_DIR / f"coupang_*_{target_date}.xlsx")
    file_list = glob.glob(pattern)
    
    if not file_list:
        print(f"다운로드된 파일 없음: {pattern}")
        return {"success": False, "reason": "no_files"}
    
    print(f"발견된 파일: {len(file_list)}개")
    
    all_dfs = []
    for file_path in file_list:
        try:
            # 파일명에서 정보 추출: coupang_{account_id}_{store_id}_{target_date}.xlsx
            file_name = Path(file_path).stem
            parts = file_name.split("_")
            account_id = parts[1] if len(parts) > 1 else "unknown"
            store_id = parts[2] if len(parts) > 2 else "unknown"
            
            df = pd.read_excel(file_path)
            df['account_id'] = account_id
            df['store_id'] = store_id
            df['platform'] = 'coupangeats'
            df['file_date'] = target_date
            
            all_dfs.append(df)
            print(f"  로드 완료: {Path(file_path).name} ({len(df)}행)")
            
        except Exception as e:
            print(f"  로드 실패: {file_path} - {e}")
    
    if not all_dfs:
        print("로드된 데이터 없음")
        return {"success": False, "reason": "load_failed"}
    
    # 3. 합치기
    down_df = pd.concat(all_dfs, ignore_index=True)
    print(f"전체 매출 데이터: {len(down_df)}건")
    
    # 4. 전처리 (필요에 따라 수정)
    store_map = {store['store_id']: store['store_name'] for store in config.STORES}
    down_df['store_name'] = down_df['store_id'].map(store_map).fillna('알 수 없음')
    
    # surrogate key 생성 (예: 주문번호 + store_id)
    # down_df = add_surrogate_key(down_df, ['store_id', '주문번호'])
    # down_df = down_df.rename(columns={'key': 'sales_id'})
    
    # 컬럼 순서 정렬 (매출 파일 구조에 맞게 수정)
    # existing_columns = [col for col in SALES_COLUMN_ORDER if col in down_df.columns]
    # down_df = down_df[existing_columns]
    
    print(f"매출 전처리 완료: {len(down_df)}건")
    print(f"컬럼: {list(down_df.columns)}")
    
    # 5. 업로드
    results = {}
    
    # Google Sheets 업로드
    results['gsheet'] = save_to_gsheet(
        df=down_df,
        sheet_name="매출",
        mode="append_unique",
        primary_key="sales_id",  # 적절한 키로 수정
        url=GSHEET_SALES_URL,
    )
    print(f"Google Sheets (매출): {results['gsheet']}")
    
    # PostgreSQL 업로드 (사용 시 주석 해제)
    # results['db'] = save_to_db(
    #     df=down_df,
    #     table_name="coupang_sales",
    #     if_exists="append",
    # )
    
    return results

# ============================================================
# 정규화 처리시
# ============================================================

# def transform_sales_upload(ti, **_):
#     # 1. 데이터 로드 (XCom 또는 파일 읽기)
#     # 예: 파일에서 읽어온 경우
#     # down_df = pd.read_excel(file_path) 
    
#     # 2. 가져온 모듈의 함수 호출
#     # 이미 pd.read_excel 등으로 생성된 DataFrame 객체를 인자로 전달
#     down_df = normalize_coupang_sales(down_df) 
    
#     # 3. 이후 로직 (DB 저장 등)
#     print(down_df.info()) # 정규화 결과 확인


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['crawling', 'coupang'],
) as dag:
    
    # 태스크 1: 크롤링
    crawl_task = PythonOperator(
        task_id='crawl_task',
        python_callable=crawl_coupang,
    )
    
    # 태스크 2: 통계 업로드
    transform_stats_task = PythonOperator(
        task_id='transform_stats_task',
        python_callable=transform_stats_upload,
    )
    
    # 태스크 3: 다운로드 업로드
    transform_sales_task = PythonOperator(
        task_id='transform_sales_task',
        python_callable=transform_sales_upload,
    )

# 크롤링 후 통계/매출 병렬 처리
crawl_task >> [transform_stats_task, transform_sales_task]
