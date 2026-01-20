
"""
쿠팡이츠 쿠폰 데이터 로드 및 전처리 함수
"""
import pandas as pd
import numpy as np
from pathlib import Path

from modules.load.load_df_glob import load_data
from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB, COLLECT_DB


# ============================================================
# 경로 설정
# ============================================================
PATH_COUPON = COLLECT_DB / "전략기획팀_수집" / "coupangeats_coupon_*.csv"
PATH_BACKUP = "/opt/airflow/download/업로드_temp"


# ============================================================
# 범용 재업로드 로더 (핵심 함수)
# ============================================================
def load_reupload_generic(
    file_pattern: str,
    xcom_key: str,
    search_paths: list,
    fallback_func: callable,
    dedup_key: list = None,
    **context
):
    """
    재사용 가능한 스마트 로더
    
    Args:
        file_pattern: 파일 패턴 (예: 'coupangeats_coupon_*.csv')
        xcom_key: XCom 저장 키
        search_paths: 검색할 경로 리스트
        fallback_func: 파일 없을 때 호출할 함수
        dedup_key: 중복 제거 키 (원본 컬럼 기준)
    """
    all_files = []
    
    # 모든 경로에서 파일 찾기
    for path_str in search_paths:
        search_path = Path(path_str)
        if search_path.exists():
            found_files = list(search_path.glob(file_pattern))
            if found_files:
                print(f"[{Path(path_str).name}] {len(found_files)}개 파일 발견")
                all_files.extend(found_files)
    
    if all_files:
        print(f"[✅ 재사용] 총 {len(all_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거 (파일명 기준, 최신 우선)
        unique_files = {}
        for f in all_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        print(f"[중복 제거] {len(file_paths)}개 파일 사용")
        
        # load_data 호출
        return load_data(
            file_path=file_paths,
            xcom_key=xcom_key,
            use_glob=False,
            dedup_key=dedup_key,
            add_source_info=False,
            **context
        )
    else:
        print(f"[🔄 새로 로드] 모든 경로에서 파일 없음 → fallback 함수 호출")
        return fallback_func(**context)


# ============================================================
# 쿠팡이츠 쿠폰 데이터 로더
# ============================================================
def load_reupload_coupang_coupon(**context):
    """쿠팡이츠 쿠폰 데이터 스마트 로더"""
    return load_reupload_generic(
        file_pattern='coupangeats_coupon_*.csv',
        xcom_key='coupang_coupon_path',
        search_paths=[
            PATH_BACKUP,
            str(COLLECT_DB / "전략기획팀_수집")
        ],
        fallback_func=load_coupang_coupon_df,
        dedup_key=['store_id', '날짜'],
        **context
    )


def load_coupang_coupon_df(**context):
    """쿠팡이츠 쿠폰 원본 로드 (fallback용)"""
    return load_data(
        file_path=PATH_COUPON,
        xcom_key='coupang_coupon_path',
        use_glob=True,
        dedup_key=['store_id', '날짜'],
        add_source_info=False,
        **context
    )


# ============================================================
# 전처리 함수 (필요시 추가)
# ============================================================
def preprocess_coupang_coupon_df(**context):
    """쿠팡이츠 쿠폰 데이터 전처리"""
    ti = context['task_instance']
    parquet_path = ti.xcom_pull(task_ids='load_coupang_coupon', key='coupang_coupon_path')
    
    if not parquet_path:
        ti.xcom_push(key='processed_coupang_coupon_path', value=None)
        return "0건 (입력 데이터 없음)"
    
    df = pd.read_parquet(parquet_path)
    
    # ============================================================
    # 전처리 로직 추가
    # ============================================================
    # 예시:
    # df['날짜'] = pd.to_datetime(df['날짜'])
    # df['stores_name'] = "도리당 " + df['매장명']
    # 필요한 컬럼만 선택
    # col = ['날짜', 'stores_name', 'store_id', '쿠폰수', ...]
    # df = df[col]
    
    output_path = TEMP_DIR / f"processed_coupang_coupon_{context['ds_nodash']}.parquet"
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key='processed_coupang_coupon_path', value=str(output_path))
    return f"전처리: {len(df):,}행"