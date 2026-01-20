r"""
로컬 DB CSV 저장 모듈 (충돌 방지용)
from modules.load.load_local_db import local_db_save

사용 예시:
---------

# 1. 기본 사용 - orders 테이블 (order_id 기준)
result = local_db_save(
    df=orders_df,
    file_path=r"C:\Local_DB\영업관리부_DB\orders.csv",
    pk_col="order_id",
    timestamp_col="created_at"
)
print(f"신규: {result['inserted']}건, 중복: {result['duplicated']}건")

# 2. Replace 모드 (기존 파일 덮어쓰기)
result = local_db_save(
    df=df,
    file_path=r"C:\Local_DB\영업관리부_DB\orders.csv",
    pk_col="order_id",
    timestamp_col="created_at",
    if_exists="replace"
)
"""

from datetime import datetime
from typing import Literal, Set
from pathlib import Path

import pandas as pd


MODE_APPEND = "append"
MODE_REPLACE = "replace"


def _create_save_result(inserted: int, duplicated: int, total: int) -> dict:
    return {
        "inserted": inserted,
        "duplicated": duplicated,
        "total": total,
    }


def _add_timestamp_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df_with_timestamp = df.copy()
    
    if column_name not in df_with_timestamp.columns:
        df_with_timestamp[column_name] = datetime.now()
    
    return df_with_timestamp


def _fetch_existing_data(file_path: str) -> pd.DataFrame:
    """기존 CSV 파일에서 데이터 읽기"""
    try:
        if Path(file_path).exists():
            existing_df = pd.read_csv(file_path, encoding='utf-8-sig')
            print(f"[로컬DB] 기존 데이터 {len(existing_df)}건 발견")
            return existing_df
        else:
            print(f"[로컬DB] 기존 파일 없음. 새로 생성됩니다.")
            return pd.DataFrame()
    except Exception as e:
        print(f"[로컬DB ERROR] 파일 읽기 실패: {e}")
        return pd.DataFrame()


def _get_existing_primary_keys(df: pd.DataFrame, primary_key_column: str) -> Set:
    """기존 데이터의 primary key 집합 추출"""
    if df.empty or primary_key_column not in df.columns:
        return set()
    return set(df[primary_key_column].dropna().unique())


def _deduplicate_new_data(
    new_df: pd.DataFrame,
    existing_keys: Set,
    primary_key_column: str,
    timestamp_column: str
) -> tuple[pd.DataFrame, int, int]:
    """신규 데이터에서 중복 제거 및 통계 계산"""
    if new_df.empty:
        return new_df, 0, 0

    # Primary key 없는 행 필터링
    new_df = new_df[new_df[primary_key_column].notna()].copy()
    
    # 중복 여부 판단
    new_df['_is_duplicate'] = new_df[primary_key_column].isin(existing_keys)
    
    duplicated_count = new_df['_is_duplicate'].sum()
    inserted_count = len(new_df) - duplicated_count
    
    # 중복 제거
    deduplicated_df = new_df[~new_df['_is_duplicate']].copy()
    deduplicated_df = deduplicated_df.drop(columns=['_is_duplicate'])
    
    # timestamp 최신순 정렬
    if timestamp_column in deduplicated_df.columns:
        deduplicated_df = deduplicated_df.sort_values(timestamp_column, ascending=False)
    
    return deduplicated_df, inserted_count, duplicated_count


def _save_to_csv(df: pd.DataFrame, file_path: str, mode: str = MODE_APPEND):
    """CSV 파일 저장 (안전한 쓰기)"""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    if mode == MODE_REPLACE or not file_path.exists():
        # Replace 모드 또는 파일 없음 → 새로 저장
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"[로컬DB] ✅ 파일 저장 완료: {file_path} ({len(df)}건)")
    else:
        # Append 모드
        df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"[로컬DB] ✅ 데이터 추가 완료: {file_path} (+{len(df)}건)")


def local_db_save(
    df: pd.DataFrame,
    file_path: str,
    pk_col: str,
    timestamp_col: str,
    if_exists: Literal["append", "replace"] = MODE_APPEND,
    add_timestamp: bool = True
) -> dict:
    """
    로컬 DB에 CSV 저장 (충돌 없는 로컬 저장)
    
    Args:
        df: 저장할 DataFrame
        file_path: 로컬 DB CSV 파일 경로 (예: C:\\Local_DB\\영업관리부_DB\\orders.csv)
        pk_col: Primary Key 컬럼명
        timestamp_col: Timestamp 컬럼명
        if_exists: "append" (중복 제거 후 추가) 또는 "replace" (전체 덮어쓰기)
        add_timestamp: True이면 timestamp 컬럼 자동 추가
    
    Returns:
        dict: {"inserted": 신규 건수, "duplicated": 중복 건수, "total": 전체 건수}
    """
    
    # 입력 검증
    if df.empty:
        print("[로컬DB] ⚠️ 빈 DataFrame. 저장하지 않습니다.")
        return _create_save_result(0, 0, 0)
    
    if pk_col not in df.columns:
        raise ValueError(f"[로컬DB ERROR] Primary key 컬럼 '{pk_col}'이 DataFrame에 없습니다.")
    
    # Timestamp 추가
    if add_timestamp:
        df = _add_timestamp_column(df, timestamp_col)
    
    total_count = len(df)
    
    # Replace 모드
    if if_exists == MODE_REPLACE:
        _save_to_csv(df, file_path, mode=MODE_REPLACE)
        return _create_save_result(total_count, 0, total_count)
    
    # Append 모드 (중복 제거)
    existing_df = _fetch_existing_data(file_path)
    existing_keys = _get_existing_primary_keys(existing_df, pk_col)
    
    deduplicated_df, inserted, duplicated = _deduplicate_new_data(
        df, existing_keys, pk_col, timestamp_col
    )
    
    if inserted > 0:
        _save_to_csv(deduplicated_df, file_path, mode=MODE_APPEND)
    else:
        print(f"[로컬DB] ℹ️ 신규 데이터 없음. 저장하지 않습니다.")
    
    print(f"[로컬DB] 📊 신규: {inserted}건 | 중복: {duplicated}건 | 전체: {total_count}건")
    
    return _create_save_result(inserted, duplicated, total_count)
