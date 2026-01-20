"""
DB 테이블 범용 조회 함수
"""
import pandas as pd
from sqlalchemy import create_engine
from typing import List, Dict, Union

from modules.common.config import get_db_url, nb_get_db_url


# ============================================================================
# 상수
# ============================================================================

ALL_COLUMNS = "*"
DEFAULT_SCHEMA = "public"
DESC_PREFIX = "-"


# ============================================================================
# 쿼리 빌드 헬퍼
# ============================================================================

def _build_column_clause(columns: List[str] | None) -> str:
    if columns is None:
        return ALL_COLUMNS
    return ", ".join(columns)


def _build_where_clause(where: Dict | None) -> tuple[str, Dict]:
    """WHERE 절과 파라미터 dict 반환"""
    if not where:
        return "", {}
    
    conditions = []
    params = {}
    
    for idx, (column, value) in enumerate(where.items()):
        param_name = f"param_{idx}"
        conditions.append(f"{column} = :{param_name}")
        params[param_name] = value
    
    clause = " WHERE " + " AND ".join(conditions)
    return clause, params


def _normalize_order_by(order_by: Union[str, List[str], None]) -> List[str]:
    if order_by is None:
        return []
    if isinstance(order_by, str):
        return [order_by]
    return order_by


def _build_order_clause(order_by: Union[str, List[str], None]) -> str:
    order_columns = _normalize_order_by(order_by)
    
    if not order_columns:
        return ""
    
    order_parts = []
    for col in order_columns:
        is_descending = col.startswith(DESC_PREFIX)
        if is_descending:
            order_parts.append(f"{col[1:]} DESC")
        else:
            order_parts.append(f"{col} ASC")
    
    return " ORDER BY " + ", ".join(order_parts)


def _build_select_query(
    table: str,
    schema: str,
    columns: List[str] | None,
    where: Dict | None,
    order_by: Union[str, List[str], None],
) -> tuple[str, Dict]:
    """SELECT 쿼리 문자열과 파라미터 반환"""
    column_clause = _build_column_clause(columns)
    where_clause, params = _build_where_clause(where)
    order_clause = _build_order_clause(order_by)
    
    query = f"SELECT {column_clause} FROM {schema}.{table}{where_clause}{order_clause}"
    
    return query, params


# ============================================================================
# 내부 실행 함수
# ============================================================================

def _execute_read_query(
    db_url: str,
    table: str,
    schema: str,
    columns: List[str] | None,
    where: Dict | None,
    order_by: Union[str, List[str], None],
) -> pd.DataFrame:
    """쿼리 실행 공통 로직"""
    engine = create_engine(db_url)
    
    query, params = _build_select_query(table, schema, columns, where, order_by)
    
    df = pd.read_sql(query, engine, params=params if params else None)
    engine.dispose()
    
    return df


# ============================================================================
# 공개 함수 (함수명 변경 금지)
# ============================================================================

def db_read_table(
    table: str,
    schema: str = DEFAULT_SCHEMA,
    columns: List[str] | None = None,
    where: Dict | None = None,
    order_by: Union[str, List[str], None] = None,
) -> pd.DataFrame:
    """Airflow/운영 환경용 DB 테이블 조회"""
    return _execute_read_query(
        db_url=get_db_url(),
        table=table,
        schema=schema,
        columns=columns,
        where=where,
        order_by=order_by,
    )


def nb_read_table(
    table: str,
    schema: str = DEFAULT_SCHEMA,
    columns: List[str] | None = None,
    where: Dict | None = None,
    order_by: Union[str, List[str], None] = None,
) -> pd.DataFrame:
    """Jupyter Notebook용 DB 테이블 조회"""
    return _execute_read_query(
        db_url=nb_get_db_url(),
        table=table,
        schema=schema,
        columns=columns,
        where=where,
        order_by=order_by,
    )


read_table = db_read_table