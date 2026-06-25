"""
store_normalize.py — 매장명 정규화 Single Source of Truth

사용법:
    from modules.transform.utility.store_normalize import normalize, normalize_for_join

    df['매장명'] = normalize(df['매장명'])          # Series 정규화
    df['_key']  = normalize_for_join(df['매장명'])  # JOIN 키 생성
"""
import re
import pandas as pd

# ============================================================
# 명시적 매핑 (구버전 → 현재 이름, "도리당 " 접두어 포함)
# 접두어 없는 변형은 _build_expanded_map()이 자동 생성
# 신규 이름 변경은 여기에만 추가하면 전 파이프라인에 자동 반영
# ============================================================
STORE_NAME_MAP: dict[str, str] = {
    "도리당 일산백석점":       "도리당 백석점",
    "도리당 구로디지털단지점": "도리당 구로디지털점",
    "도리당 충주봉방점":       "도리당 충주역점",
    "도리당 파주운정점":       "도리당 운정점",
    "도리당 인천간석점":       "도리당 인천간석중앙점",
    "도리당 간석중앙점":       "도리당 인천간석중앙점",
    "나홀로 간석중앙점":       "나홀로 인천간석중앙점",
    "도리당 서울역삼점":       "도리당 역삼점",
    "해운대 중동점":           "도리당 해운대중동점",
    "도리당 성정점":           "도리당 천안성정점",
    "나홀로 대신점":           "나홀로 부산대신점",
    "대신점":                  "부산대신점",
    "해운대중동점":            "도리당 해운대중동점",
    "도리당 중동점":           "도리당 해운대중동점",
    # ---- 신규 변경은 여기에만 추가 ----
}

_PREFIX_RE        = re.compile(r"^도리당\s*")
_SPACE_RE         = re.compile(r"\s+")
_BRAND_PREFIX_RE  = re.compile(r"^(도리당|나홀로)\s+")


def _build_expanded_map(base: dict) -> dict:
    """접두어 없는 변형을 자동 추가 ("파주운정점" → "도리당 운정점" 등)"""
    expanded = dict(base)
    for old, new in base.items():
        stripped = _PREFIX_RE.sub("", old).strip()
        if stripped and stripped not in expanded:
            expanded[stripped] = new
    return expanded


_MAP = _build_expanded_map(STORE_NAME_MAP)


def normalize(series: pd.Series) -> pd.Series:
    """매장명 Series를 현재 공식명으로 치환. 접두어 유무 무관."""
    return series.replace(_MAP)


def normalize_for_join(series: pd.Series) -> pd.Series:
    """
    JOIN 키 생성: 정규화 → '도리당' 접두어·공백 제거
    양쪽 테이블에 동일하게 적용하면 이름 차이를 완전히 흡수.

    예: "도리당 구로디지털단지점" / "구로디지털점" → 둘 다 "구로디지털점"
    예: "도리당 파주운정점" / "운정점" → 둘 다 "운정점"
    """
    return normalize(series).map(
        lambda s: _SPACE_RE.sub("", _PREFIX_RE.sub("", str(s)).strip())
    )


def strip_brand(series: pd.Series) -> pd.Series:
    """매장명에서 브랜드 접두어('도리당 ', '나홀로 ') 제거 → 지점명만 반환.

    예: "도리당 역삼점"     → "역삼점"
        "나홀로 간석중앙점" → "간석중앙점"
    """
    return series.map(lambda s: _BRAND_PREFIX_RE.sub("", str(s)).strip())


def lookup_store_key(brand: str, store: str) -> str:
    """입력 브랜드/지점명을 매장명 매핑 테이블 기반으로 정규화한다.

    매핑 존재 시 브랜드 접두어를 제거한 지점 키를 반환하고,
    매핑이 없으면 입력 지점명을 그대로 반환한다.
    """
    normalized_store = f"{brand} {store}".strip()
    if not normalized_store:
        return ""

    normalized = _MAP.get(normalized_store, normalized_store)
    prefix = f"{brand} ".strip()
    if brand and normalized.startswith(prefix):
        return normalized[len(prefix):].strip()
    return store.strip()


# 하위 호환 alias
normalize_store_names = normalize
