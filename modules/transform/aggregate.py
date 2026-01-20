import pandas as pd
from typing import Dict, List, Union

AggFunc = Union[str, List[str]]


def agg_df(
    df: pd.DataFrame,
    group_cols: List[str],
    agg_map: Dict[str, AggFunc],
    *,
    reset_index: bool = True,
) -> pd.DataFrame:
    """
    DataFrame 그룹 집계 함수

    Args:
        df: 원본 DataFrame
        group_cols: 그룹 기준 컬럼 리스트
        agg_map: 집계 규칙
            예)
            {
                "매출": "sum",
                "주문수": "sum",
                "객단가": "mean",
                "재고": ["min", "max"]
            }
        reset_index: index 초기화 여부

    Returns:
        집계된 DataFrame
    """

    # ===== 컬럼 존재 여부 체크 =====
    missing_cols = set(group_cols) - set(df.columns)
    missing_aggs = set(agg_map.keys()) - set(df.columns)

    if missing_cols:
        raise ValueError(f"그룹 컬럼이 존재하지 않습니다: {missing_cols}")
    if missing_aggs:
        raise ValueError(f"집계 컬럼이 존재하지 않습니다: {missing_aggs}")

    # ===== groupby aggregation =====
    result = df.groupby(group_cols).agg(agg_map)

    # ===== 컬럼 평탄화 (멀티 인덱스 제거) =====
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = [
            "_".join(col).rstrip("_")
            for col in result.columns.values
        ]

    if reset_index:
        result = result.reset_index()

    return result
