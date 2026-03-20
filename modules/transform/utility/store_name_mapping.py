"""
매장명 매핑 유틸리티
- 수집 데이터(배민 등)의 이전 매장명 → 현재 매장명(gsheet 기준) 변환
- 신규 변경 발생 시 STORE_NAME_MAP에 항목 추가만 하면 전 파이프라인에 반영됨

사용법:
    from modules.transform.utility.store_name_mapping import normalize_store_names

    df["store_names"] = normalize_store_names(df["store_names"])
"""

# ============================================================
# 매장명 매핑 딕셔너리  (이전 이름 → 현재 이름)
# 지점명 변경 시 아래에 한 줄만 추가하면 됩니다.
# ============================================================
STORE_NAME_MAP: dict[str, str] = {
    # ---- 2025-2026 변경 이력 ----
    "도리당 일산백석점":       "도리당 백석점",
    "도리당 구로디지털단지점": "도리당 구로디지털점",
    "도리당 충주봉방점":       "도리당 충주역점",
    # ---- 신규 변경은 여기에 추가 ----
}


def normalize_store_names(series):
    """
    pandas Series의 매장명을 STORE_NAME_MAP에 따라 일괄 변환합니다.

    Args:
        series (pd.Series): 매장명 컬럼 (예: store_names, stores_name, 매장명 등)

    Returns:
        pd.Series: 매핑이 적용된 매장명 Series
    """
    if STORE_NAME_MAP:
        return series.replace(STORE_NAME_MAP)
    return series
