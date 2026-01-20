"""
DB 저장 모듈
"""
from datetime import datetime
from typing import Literal, Set

import pandas as pd
from sqlalchemy import create_engine

from modules.common.config import get_db_url


MODE_APPEND = "append"
MODE_REPLACE = "replace"

DEFAULT_SCHEMA = "public"
DEFAULT_PRIMARY_KEY_COLUMN = "order_id"
DEFAULT_TIMESTAMP_COLUMN = "created_at"


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


def _fetch_existing_primary_keys(engine, table: str, schema: str, primary_key_column: str) -> Set:
    try:
        existing_df = pd.read_sql_table(
            table,
            con=engine,
            schema=schema,
            columns=[primary_key_column],
        )
        existing_keys = set(existing_df[primary_key_column].tolist())
        print(f"[DEBUG] 기존 데이터 {len(existing_keys)}건 발견")
        return existing_keys
        
    except Exception as e:
        print(f"[INFO] 기존 데이터 없음: {e}")
        return set()


def _filter_new_rows(df: pd.DataFrame, existing_keys: Set, primary_key_column: str) -> pd.DataFrame:
    is_new_row = ~df[primary_key_column].isin(existing_keys)
    return df[is_new_row].copy()


def _execute_replace(
    engine,
    df: pd.DataFrame,
    table: str,
    schema: str,
    add_timestamp: bool,
    timestamp_column: str,
) -> dict:
    df_to_save = df.copy()
    
    if add_timestamp:
        df_to_save = _add_timestamp_column(df_to_save, timestamp_column)
    
    df_to_save.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists=MODE_REPLACE,
        index=False,
    )
    
    total_count = len(df_to_save)
    print(f"[OK] {schema}.{table} 덮어쓰기: {total_count}건")
    
    return _create_save_result(inserted=total_count, duplicated=0, total=total_count)


def _execute_append_unique(
    engine,
    df: pd.DataFrame,
    table: str,
    schema: str,
    primary_key_column: str,
    add_timestamp: bool,
    timestamp_column: str,
) -> dict:
    total_count = len(df)
    existing_keys = _fetch_existing_primary_keys(engine, table, schema, primary_key_column)
    
    new_rows_df = _filter_new_rows(df, existing_keys, primary_key_column)
    duplicate_count = total_count - len(new_rows_df)
    
    if len(new_rows_df) == 0:
        print(f"[WARN] {schema}.{table}: 신규 없음 (중복 {duplicate_count}건)")
        return _create_save_result(inserted=0, duplicated=duplicate_count, total=total_count)
    
    if add_timestamp:
        new_rows_df = _add_timestamp_column(new_rows_df, timestamp_column)
    
    new_rows_df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists=MODE_APPEND,
        index=False,
    )
    
    print(f"[OK] {schema}.{table}: 신규 {len(new_rows_df)}건, 중복 {duplicate_count}건")
    
    return _create_save_result(
        inserted=len(new_rows_df),
        duplicated=duplicate_count,
        total=total_count,
    )


def postgre_db_save(
    df: pd.DataFrame,
    table: str,
    schema: str = DEFAULT_SCHEMA,
    pk_col: str = DEFAULT_PRIMARY_KEY_COLUMN,
    add_timestamp: bool = True,
    timestamp_col: str = DEFAULT_TIMESTAMP_COLUMN,
    if_exists: Literal["append", "replace"] = MODE_APPEND,
    db_config: dict = None,
) -> dict:
    """중복 제외 후 DB에 저장"""
    
    if df.empty:
        print(f"[WARN] 저장할 데이터 없음")
        return _create_save_result(inserted=0, duplicated=0, total=0)
    
    engine = create_engine(get_db_url())
    
    try:
        if if_exists == MODE_REPLACE:
            return _execute_replace(
                engine=engine,
                df=df,
                table=table,
                schema=schema,
                add_timestamp=add_timestamp,
                timestamp_column=timestamp_col,
            )
        
        return _execute_append_unique(
            engine=engine,
            df=df,
            table=table,
            schema=schema,
            primary_key_column=pk_col,
            add_timestamp=add_timestamp,
            timestamp_column=timestamp_col,
        )
        
    except Exception as e:
        print(f"[ERROR] DB 저장 실패: {e}")
        raise