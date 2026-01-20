r"""
OneDrive CSV 저장 모듈
from modules.load.load_onedrive import onedrive_csv_save

사용 예시:
---------

# 1. 기본 사용 - orders 테이블 (order_id 기준)
result = onedrive_csv_save(
    df=orders_df,
    file_path=r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB\영업관리\매출\orders.csv",
    pk_col="order_id",
    timestamp_col="created_at"
)
print(f"신규: {result['inserted']}건, 중복: {result['duplicated']}건")

# 2. store_name 기준 - 매장별 데이터
result = onedrive_csv_save(
    df=store_df,
    file_path=r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB\영업관리\매장\stores.csv",
    pk_col="store_name",
    timestamp_col="order_daily"
)

# 3. 복합 키 사용 - store_name + order_date 조합
df_with_composite_key = df.copy()
df_with_composite_key['composite_key'] = df['store_name'] + '_' + df['order_date']
result = onedrive_csv_save(
    df=df_with_composite_key,
    file_path=r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB\영업관리\일별매출\daily_sales.csv",
    pk_col="composite_key",
    timestamp_col="updated_at"
)

# 4. timestamp 추가 안함
result = onedrive_csv_save(
    df=df,
    file_path=r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB\영업관리\매출\orders.csv",
    pk_col="order_id",
    timestamp_col="created_at",
    add_timestamp=False
)

# 5. Replace 모드 (기존 파일 덮어쓰기)
result = onedrive_csv_save(
    df=df,
    file_path=r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB\영업관리\매출\orders.csv",
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
            print(f"[DEBUG] 기존 데이터 {len(existing_df)}건 발견")
            return existing_df
        else:
            print(f"[INFO] 기존 파일 없음. 새로 생성됩니다.")
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] 파일 읽기 실패: {e}")
        return pd.DataFrame()


def _get_existing_primary_keys(df: pd.DataFrame, primary_key_column: str) -> Set:
    """기존 데이터의 primary key 집합 추출"""
    if df.empty or primary_key_column not in df.columns:
        return set()
    return set(df[primary_key_column].tolist())


def _filter_new_rows(df: pd.DataFrame, existing_keys: Set, primary_key_column: str) -> pd.DataFrame:
    """신규 데이터만 필터링"""
    is_new_row = ~df[primary_key_column].isin(existing_keys)
    return df[is_new_row].copy()


def _ensure_directory_exists(file_path: str):
    """디렉토리가 없으면 생성"""
    directory = Path(file_path).parent
    directory.mkdir(parents=True, exist_ok=True)


def _execute_replace(
    df: pd.DataFrame,
    file_path: str,
    add_timestamp: bool,
    timestamp_column: str,
) -> dict:
    """Replace 모드: 기존 파일 덮어쓰기"""
    df_to_save = df.copy()
    
    if add_timestamp:
        df_to_save = _add_timestamp_column(df_to_save, timestamp_column)
    
    _ensure_directory_exists(file_path)
    df_to_save.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    total_count = len(df_to_save)
    print(f"[OK] {file_path} 덮어쓰기: {total_count}건")
    
    return _create_save_result(inserted=total_count, duplicated=0, total=total_count)


def _execute_append_unique(
    df: pd.DataFrame,
    file_path: str,
    primary_key_column: str,
    add_timestamp: bool,
    timestamp_column: str,
) -> dict:
    """Append 모드: 중복 제외하고 추가"""
    total_count = len(df)
    
    # 기존 데이터 읽기
    existing_df = _fetch_existing_data(file_path)
    existing_keys = _get_existing_primary_keys(existing_df, primary_key_column)
    
    # 신규 데이터만 필터링
    new_rows_df = _filter_new_rows(df, existing_keys, primary_key_column)
    duplicate_count = total_count - len(new_rows_df)
    
    if len(new_rows_df) == 0:
        print(f"[WARN] {file_path}: 신규 없음 (중복 {duplicate_count}건)")
        return _create_save_result(inserted=0, duplicated=duplicate_count, total=total_count)
    
    # timestamp 추가
    if add_timestamp:
        new_rows_df = _add_timestamp_column(new_rows_df, timestamp_column)
    
    # 기존 데이터와 합치기
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_rows_df], ignore_index=True)
    else:
        combined_df = new_rows_df
    
    # CSV로 저장
    _ensure_directory_exists(file_path)
    print(f"[DEBUG] 저장 직전 combined_df: {len(combined_df)}행, 컬럼: {list(combined_df.columns)[:5]}")
    
    # 파일 저장 (명시적으로 flush)
    try:
        # 기존 파일 삭제 (권한 문제 방지)
        import os
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"[DEBUG] 기존 파일 삭제 완료: {file_path}")
        
        # 새로 저장 - 파일 핸들을 명시적으로 관리
        with open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
            combined_df.to_csv(f, index=False)
            f.flush()  # 버퍼를 디스크에 강제 쓰기
            os.fsync(f.fileno())  # OS 버퍼도 디스크에 강제 쓰기
        
        print(f"[DEBUG] to_csv() 완료 및 fsync(): {file_path}")
        
        # 파일 시스템 동기화 강제
        import time
        time.sleep(2)
        
        # 저장 후 검증 - 새로운 프로세스처럼 파일 열기
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"[DEBUG] 파일 크기: {file_size} bytes")
            
            # 실제 저장된 내용 확인
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
                print(f"[DEBUG] 파일 실제 행 수 (readlines): {len(lines)}행")
                if len(lines) > 0:
                    print(f"[DEBUG] 첫 줄: {lines[0][:80]}")
                if len(lines) > 1:
                    print(f"[DEBUG] 둘째 줄: {lines[1][:80]}")
        else:
            print(f"[ERROR] 파일이 존재하지 않음: {file_path}")
            
    except Exception as e:
        print(f"[ERROR] 파일 저장 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    print(f"[OK] {file_path}: 신규 {len(new_rows_df)}건, 중복 {duplicate_count}건")
    
    return _create_save_result(
        inserted=len(new_rows_df),
        duplicated=duplicate_count,
        total=total_count,
    )


def onedrive_csv_save(
    df: pd.DataFrame,
    file_path: str,
    pk_col: str,
    timestamp_col: str = None,
    add_timestamp: bool = False,
    if_exists: Literal["append", "replace"] = MODE_APPEND,
) -> dict:
    """
    중복 제외 후 OneDrive CSV에 저장
    
    Args:
        df: 저장할 DataFrame
        file_path: CSV 파일 경로 (예: "C:\\Users\\민준\\OneDrive - 주식회사 도리당\\Doridang_DB\\영업관리\\매출\\orders.csv")
        pk_col: Primary key 컬럼명 (필수) - 중복 체크 기준 (예: "order_id", "store_name")
        timestamp_col: timestamp 컬럼명 (선택) - add_timestamp=True일 때 사용
        add_timestamp: timestamp 컬럼 추가 여부 (기본값: False)
        if_exists: "append" 또는 "replace" (기본값: "append")
    
    Returns:
        dict: {"inserted": 신규 건수, "duplicated": 중복 건수, "total": 전체 건수}
    
    Examples:
        >>> # 주문 데이터 저장 (order_id 기준)
        >>> result = onedrive_csv_save(
        ...     df=orders_df,
        ...     file_path=r"C:\...\orders.csv",
        ...     pk_col="order_id",
        ...     timestamp_col="created_at",
        ...     add_timestamp=True
        ... )
        
        >>> # 매장 데이터 저장 (store_name 기준)
        >>> result = onedrive_csv_save(
        ...     df=store_df,
        ...     file_path=r"C:\...\stores.csv",
        ...     pk_col="store_name"
        ... )
        
        >>> # 복합 키 사용 (store_name + order_date 조합)
        >>> df['composite_key'] = df['store_name'] + '_' + df['order_date']
        >>> result = onedrive_csv_save(
        ...     df=df,
        ...     file_path=r"C:\...\daily_sales.csv",
        ...     pk_col="composite_key"
        ... )
    """
    
    if df.empty:
        print(f"[WARN] 저장할 데이터 없음")
        return _create_save_result(inserted=0, duplicated=0, total=0)
    
    # pk_col 존재 확인
    if pk_col not in df.columns:
        raise ValueError(f"Primary key 컬럼 '{pk_col}'이 DataFrame에 존재하지 않습니다.")
    
    # timestamp 설정
    if add_timestamp and timestamp_col is None:
        timestamp_col = "created_at"  # 기본값
    
    try:
        if if_exists == MODE_REPLACE:
            return _execute_replace(
                df=df,
                file_path=file_path,
                add_timestamp=add_timestamp,
                timestamp_column=timestamp_col,
            )
        
        return _execute_append_unique(
            df=df,
            file_path=file_path,
            primary_key_column=pk_col,
            add_timestamp=add_timestamp,
            timestamp_column=timestamp_col,
        )
        
    except Exception as e:
        print(f"[ERROR] CSV 저장 실패: {e}")
        raise