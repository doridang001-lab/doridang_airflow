"""
매장명 매핑 유틸리티  (Single Source of Truth)
- 수집 데이터(배민/토더/CS 등)의 이전 매장명 → 현재 매장명(gsheet 기준) 변환
- 직원테이블 · CS · 토더 등 모든 데이터 소스에서 이 모듈을 임포트하여 사용
- 신규 변경 발생 시 STORE_NAME_MAP에 항목 추가만 하면 전 파이프라인에 반영됨

☑️ 자동 확장:
  "도리당 파주운정점" → "도리당 운정점" 한 줄만 등록하면
  "파주운정점", "운정점", "도리당 운정점" 등 모든 변형이 자동 처리됨

사용법:
    from modules.transform.utility.store_name_mapping import (
        normalize_store_names,
        normalize_for_join,
    )

    # 1) 이전 이름 → 현재 이름 일괄 변환 (접두어 유무 무관)
    df["store_names"] = normalize_store_names(df["store_names"])

    # 2) 양쪽 테이블에 조인키 생성 (접두어·공백 차이 자동 흡수)
    df["_key"] = normalize_for_join(df["매장명"])
"""

import re

# ============================================================
# 매장명 매핑 딕셔너리  (이전 이름 → 현재 이름)
# "도리당 " 접두어 포함 형태로 등록하세요.
# 접두어 없는 변형("파주운정점" 등)은 _build_expanded_map()이 자동 생성합니다.
# ============================================================
STORE_NAME_MAP: dict[str, str] = {
    # ---- 2025-2026 변경 이력 ----
    "도리당 일산백석점":       "도리당 백석점",
    "도리당 구로디지털단지점": "도리당 구로디지털점",
    "도리당 충주봉방점":       "도리당 충주역점",
    "도리당 파주운정점":       "도리당 운정점",
    # ---- 신규 변경은 여기에 추가 ----
}

_PREFIX_RE = re.compile(r"^도리당\s*")


def _build_expanded_map(base_map: dict[str, str]) -> dict[str, str]:
    """
    STORE_NAME_MAP에서 "도리당 " 접두어 없는 변형을 자동 추가.

    등록: "도리당 파주운정점" → "도리당 운정점"
    자동 생성:
      "파주운정점" → "도리당 운정점"   (토더 원본처럼 접두어 없이 들어올 때)
    """
    expanded = dict(base_map)
    for old, new in base_map.items():
        old_stripped = _PREFIX_RE.sub("", old).strip()
        # 접두어 없는 이전 이름도 같은 현재 이름으로 매핑
        if old_stripped and old_stripped != old and old_stripped not in expanded:
            expanded[old_stripped] = new
    return expanded


# 모듈 로드 시 1회 확장 (런타임 비용 없음)
_EXPANDED_MAP: dict[str, str] = _build_expanded_map(STORE_NAME_MAP)


def normalize_store_names(series):
    """
    pandas Series의 매장명을 정규화합니다.
    "도리당 파주운정점" / "파주운정점" 어느 쪽이든 → "도리당 운정점" 으로 통일.
    """
    if _EXPANDED_MAP:
        return series.replace(_EXPANDED_MAP)
    return series


def _strip_store_name(name: str) -> str:
    """단일 매장명에서 '도리당' 접두어와 공백을 제거하여 조인키를 만든다."""
    s = str(name).strip()
    s = _PREFIX_RE.sub("", s)
    s = re.sub(r"\s+", "", s)
    return s


def normalize_for_join(series):
    """
    매장명 Series에서 매핑 적용 후 '도리당' 접두어·공백을 제거한 조인키를 반환.
    양쪽 테이블에 동일하게 적용하면 접두어/이름 차이를 모두 흡수합니다.

    예시 (모두 같은 키 "운정점" 으로 수렴):
      "도리당 파주운정점" → "운정점"
      "파주운정점"        → "운정점"
      "도리당 운정점"     → "운정점"
      "운정점"            → "운정점"
    """
    normalized = normalize_store_names(series)
    return normalized.map(_strip_store_name)
