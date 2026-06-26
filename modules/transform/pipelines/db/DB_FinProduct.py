"""
통합 상품 마스터 파이프라인.

처리 흐름:
1) OKPOS + EASYPOS 상품조회.xlsx 로드 및 정규화
2) fin_product_grp.csv와 비교 → 신규/변경 상품 감지
3) LLM(Ollama)으로 수동분류 + is_main_candidate 분류
4) CSV에 신규 행 append (llm_check=Y)
5) 이메일 알림 발송
"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_GRP_TRAIN_JSON_PATH,
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    MART_DB,
)
from modules.transform.utility.mailer import send_email

logger = logging.getLogger(__name__)


def _safe_replace(src: Path, dst: Path) -> None:
    """os.replace with fallback for FUSE/OneDrive mounts where rename may fail."""
    try:
        os.replace(src, dst)
    except (PermissionError, OSError):
        dst.write_bytes(src.read_bytes())

OKPOS_PRODUCT_XLSX = ANALYTICS_DB / "okpos_product" / "상품조회.xlsx"
EASYPOS_PRODUCT_XLSX = ANALYTICS_DB / "easypos_product" / "상품조회.xlsx"
SOURCE_CODE = "okpos"
EASYPOS_SOURCE_CODE = "easypos"
ALERT_EMAIL = "a17019@kakao.com"

# LLM 설정 - gpt-oss:20b 우선, 실패 시 qwen2.5:7b 폴백
_GPT_MODEL_CANDIDATES = ["gpt-oss:20b", "gpt-oss:latest", "gpt-oss", "qwen2.5:7b", "qwen2.5:latest"]
_OLLAMA_HOST = "http://host.docker.internal:11434"

# OKPOS xlsx 컬럼 인덱스 → 표준 컬럼명 매핑
_XLSX_COL_IDX = {
    "대메뉴": 0,
    "중메뉴": 1,
    "상품코드": 3,
    "상품명": 4,
    "판매단가": 9,
}

# OKPOS 대메뉴 필터 - 도리당/나홀로 포함 행만 처리
_OKPOS_DAEGROUP_KEYWORDS = ["도리당", "나홀로"]

# EASYPOS xlsx 컬럼명 → 표준 컬럼명 매핑
_EASYPOS_COL_MAP = {
    "대메뉴": "대분류명",
    "중메뉴": "소분류명",
    "상품코드": "상품코드",
    "상품명": "상품명",
    "판매단가": "판매가",
}

_CLASSIFY_LABELS = ["메인", "1인", "사이드", "토핑", "옵션", "주류", "음료", "이벤트", "기타"]
_TRAIN_EXAMPLES_PER_LABEL = 30
_DIRECT_RULE_CONFIDENCE = 90
_DIRECT_EXAMPLE_CONFIDENCE = 85
_TRAIN_RULES = {
    "version": 1,
    "allowed_labels": _CLASSIFY_LABELS,
    "decision_order": ["주류", "음료", "이벤트", "기타", "옵션", "토핑", "사이드", "1인", "메인"],
    "hard_rules": [
        "배달비, 할인, 직원호출, 쿠폰, 리뷰 안내처럼 실제 판매 음식이 아닌 항목은 기타",
        "맵기, 맛, 단계, 선택, 변경처럼 조리 옵션을 고르는 항목은 옵션",
        "사리, 떡, 버섯, 분모자, 당면, 치즈, 우삼겹, 대창, 묵은지, 파김치, 미나리 추가는 토핑",
        "소주, 맥주, 막걸리, 하이볼, 참이슬, 처음처럼, 카스, 테라, 켈리는 주류",
        "콜라, 사이다, 환타, 제로, 생수, 음료는 음료",
        "리뷰, 이벤트, 서비스, 증정 목적의 항목은 이벤트",
        "닭도리탕, 곱도리탕, 닭한마리, 전골, 찜닭처럼 주문 중심이 되는 본품은 메인",
        "메인이 아니면 is_main_candidate는 반드시 N",
    ],
    "label_rules": {
        "메인": {
            "description": "주문의 중심이 되는 대표 음식 메뉴",
            "keywords": ["닭도리탕", "곱도리탕", "닭한마리", "전골", "찜닭", "세트", "정식"],
        },
        "1인": {
            "description": "1인 전용 메인 메뉴 또는 혼밥형 본품",
            "keywords": ["1인", "혼밥", "나홀로"],
        },
        "사이드": {
            "description": "단독 사이드 음식",
            "keywords": ["공기밥", "볶음밥", "주먹밥", "계란찜", "튀김", "전", "감자전"],
        },
        "토핑": {
            "description": "메인 메뉴에 추가되는 재료",
            "keywords": ["추가", "사리", "떡", "버섯", "분모자", "당면", "치즈", "우삼겹", "대창", "묵은지", "파김치", "미나리", "누룽지"],
        },
        "옵션": {
            "description": "맛, 맵기, 단계, 조리 방식, 선택값",
            "keywords": ["맵기", "맛", "단계", "선택", "변경", "순한맛", "보통맛", "매운맛"],
        },
        "주류": {
            "description": "술",
            "keywords": ["소주", "맥주", "막걸리", "하이볼", "참이슬", "처음처럼", "카스", "테라", "켈리", "새로"],
        },
        "음료": {
            "description": "비주류 음료",
            "keywords": ["콜라", "사이다", "환타", "제로", "생수", "음료", "토닉워터"],
        },
        "이벤트": {
            "description": "리뷰, 증정, 프로모션성 메뉴",
            "keywords": ["리뷰", "이벤트", "서비스", "증정"],
        },
        "기타": {
            "description": "배달비, 할인, 호출, 시스템성 항목",
            "keywords": ["배달비", "할인", "직원호출", "쿠폰", "봉투"],
        },
    },
}
_CHANGE_KEYS = ["대메뉴", "중메뉴", "상품명"]

_XCOM_OKPOS = "okpos_df_json"
_XCOM_CHANGES = "changes_json"
_XCOM_CLASSIFIED = "classified_json"

_REVIEW_APPROVE_COL = "approve"  # Y/N/blank


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _load_alias_map() -> dict[str, str]:
    """fin_product_alias.csv 로드 → {alias: canonical}."""
    if not FIN_PRODUCT_ALIAS_CSV_PATH.exists():
        return {}
    try:
        df = pd.read_csv(FIN_PRODUCT_ALIAS_CSV_PATH, dtype=str).fillna("")
    except Exception:
        return {}
    # columns: alias, canonical (Korean headers also supported)
    col_alias = "alias" if "alias" in df.columns else ("별칭" if "별칭" in df.columns else None)
    col_canon = "canonical" if "canonical" in df.columns else ("표준명" if "표준명" in df.columns else None)
    if not col_alias or not col_canon:
        return {}
    df[col_alias] = df[col_alias].fillna("").astype(str).str.strip()
    df[col_canon] = df[col_canon].fillna("").astype(str).str.strip()
    df = df[(df[col_alias] != "") & (df[col_canon] != "")]
    if df.empty:
        return {}
    # last wins
    return dict(zip(df[col_alias].tolist(), df[col_canon].tolist()))


def _normalize_updated_at(ts: str) -> str:
    """'YYYY-MM-DD H:MM' 형식을 문자열 정렬 가능한 datetime 문자열로 정규화."""
    m = re.match(r"^(\d{4}-\d{2}-\d{2}) (\d):(\d{2})$", ts)
    if m:
        return f"{m.group(1)} 0{m.group(2)}:{m.group(3)}:00"
    return ts


_STRIP_TOKENS_RE = re.compile(
    r"(?:-?\s*(?:점심|런치|저녁|디너|야식|평일|주말|특가|할인|프로모션|행사|세트|포장|배달))\s*$"
)
_BRACKETS_RE = re.compile(r"^\s*[\[\(（【]\s*(.*?)\s*[\]\)）】]\s*$")
_LEADING_OPTION_TOKENS_RE = re.compile(r"^\s*(?:순살|뼈있는|뼈|순살_)\s*[_\-\s]+\s*")


def _name_key(s: str) -> str:
    """별칭 매칭용 키(공백/구분자 제거, 소문자)."""
    s = "" if s is None else str(s)
    s = s.strip().lower()
    # treat underscore/hyphen as whitespace, then drop whitespace
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.replace(" ", "")


def _normalize_product_name(raw: str, alias_map: dict[str, str]) -> tuple[str, str]:
    """상품명 정리 + 표준명 산정.

    Returns:
      - cleaned_name: 옵션/토큰 제거 후 정리된 상품명
      - canonical_name: alias 매핑까지 적용된 표준명 (없으면 cleaned_name)
    """
    s = "" if raw is None else str(raw).strip()
    if not s or s.lower() == "nan":
        return "", ""

    # Outer brackets like "[...]" or "(...)"
    m = _BRACKETS_RE.match(s)
    if m:
        s = (m.group(1) or "").strip()

    # underscore/hyphen normalization
    s = s.replace("_", " ").strip()

    # Common trailing tokens (e.g. "-점심")
    s = _STRIP_TOKENS_RE.sub("", s).strip()

    # Leading option tokens (e.g. "순살 누룽지 ...", "순살_누룽지 ...")
    s = _LEADING_OPTION_TOKENS_RE.sub("", s).strip()

    # Normalize whitespace
    s = re.sub(r"\s+", " ", s).strip()

    cleaned = s

    # Alias mapping (exact match after normalization)
    if cleaned in alias_map:
        return cleaned, alias_map[cleaned]
    # Alias mapping (key match: ignore spaces/underscore/hyphen)
    key = _name_key(cleaned)
    if key:
        for a, c in alias_map.items():
            if _name_key(a) == key:
                return cleaned, c
    return cleaned, cleaned


def _load_review_file() -> pd.DataFrame:
    """fin_product_review.csv 로드. 없으면 빈 DF."""
    if not FIN_PRODUCT_REVIEW_CSV_PATH.exists():
        return pd.DataFrame(columns=["상품코드", _REVIEW_APPROVE_COL, "note", "checked_at"])
    try:
        df = pd.read_csv(FIN_PRODUCT_REVIEW_CSV_PATH, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=["상품코드", _REVIEW_APPROVE_COL, "note", "checked_at"])
    if "상품코드" not in df.columns:
        df["상품코드"] = ""
    if _REVIEW_APPROVE_COL not in df.columns:
        df[_REVIEW_APPROVE_COL] = ""
    if "note" not in df.columns:
        df["note"] = ""
    if "checked_at" not in df.columns:
        df["checked_at"] = ""
    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df[_REVIEW_APPROVE_COL] = (
        df[_REVIEW_APPROVE_COL].fillna("").astype(str).str.strip().str.upper().replace({"": ""})
    )
    return df



def _pending_latest(df_master: pd.DataFrame) -> pd.DataFrame:
    """fin_product_grp.csv에서 llm_check=Y인 미확정 상품을 코드별 최신 1행으로 추출."""
    if df_master.empty:
        return pd.DataFrame()
    if "상품코드" not in df_master.columns:
        return pd.DataFrame()
    if "llm_check" not in df_master.columns:
        return pd.DataFrame()
    df = df_master.copy()
    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    if "updated_at" not in df.columns:
        df["updated_at"] = ""
    pending = df[df["llm_check"] == "Y"].copy()
    if pending.empty:
        return pd.DataFrame()
    pending["_updated_at_ts"] = pd.to_datetime(pending["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
    pending = (
        pending.sort_values(["상품코드", "_updated_at_ts"], na_position="last")
        .groupby("상품코드", as_index=False)
        .last()
    )
    pending = pending.drop(columns=["_updated_at_ts"], errors="ignore")
    pending = pending[pending["상품코드"].fillna("").astype(str).str.strip() != ""].copy()
    return pending


def _read_xlsx() -> pd.DataFrame:
    """OKPOS 상품조회.xlsx를 읽어 표준 컬럼으로 반환.

    - 가능한 경우 원본의 '구분'(홀/배달 등)을 보존한다.
    - '구분' 컬럼이 없거나 비어있으면 기본값은 '홀'로 둔다.
    - 도리당/나홀로 대메뉴는 기본 포함.
    - 미매칭(필터 미포함) 행이더라도 구분이 '배달'로 식별되면 포함한다.
    """
    df_raw = pd.read_excel(OKPOS_PRODUCT_XLSX, engine="openpyxl", header=0, dtype=str)
    df_raw = df_raw.fillna("")
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    result = pd.DataFrame()
    for name, idx in _XLSX_COL_IDX.items():
        result[name] = df_raw.iloc[:, idx]

    # '구분' 컬럼은 엑셀마다 존재 여부가 달라 방어적으로 처리
    if "구분" in df_raw.columns:
        result["구분"] = df_raw["구분"]
    elif "분류" in df_raw.columns:
        result["구분"] = df_raw["분류"]
    else:
        result["구분"] = ""

    result["source"] = SOURCE_CODE
    result["상품코드"] = result["상품코드"].str.strip()
    result["판매단가"] = pd.to_numeric(result["판매단가"], errors="coerce").fillna(0).astype(int)
    result["구분"] = result["구분"].fillna("").astype(str).str.strip()
    result["구분"] = result["구분"].replace({"": "홀"})

    result = result[result["상품코드"] != ""]

    pat = "|".join(_OKPOS_DAEGROUP_KEYWORDS)
    mask_brand = result["대메뉴"].str.contains(pat, na=False)
    mask_delivery = result["구분"].astype(str).str.contains("배달", na=False)
    result = result[mask_brand | mask_delivery]
    return result.reset_index(drop=True)


def _read_easypos_xlsx() -> pd.DataFrame:
    """EASYPOS 상품조회.xlsx를 읽어 표준 컬럼으로 반환."""
    df_raw = pd.read_excel(EASYPOS_PRODUCT_XLSX, engine="openpyxl", header=0, dtype=str)
    df_raw = df_raw.fillna("")
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    result = pd.DataFrame()
    for std_col, src_col in _EASYPOS_COL_MAP.items():
        if src_col not in df_raw.columns:
            raise KeyError(f"EASYPOS xlsx에 '{src_col}' 컬럼 없음. 실제 컬럼: {list(df_raw.columns)}")
        result[std_col] = df_raw[src_col]

    result["source"] = EASYPOS_SOURCE_CODE
    result["상품코드"] = result["상품코드"].str.strip()
    # 상품코드 정규화: "000001" → "1" (마스터 CSV의 수동 입력 포맷과 일치)
    result["상품코드"] = result["상품코드"].apply(
        lambda x: str(int(x)) if x.isdigit() else x
    )
    # 판매가에 쉼표(,) 포함 가능 → 제거 후 변환
    result["판매단가"] = pd.to_numeric(
        result["판매단가"].str.replace(",", "", regex=False), errors="coerce"
    ).fillna(0).astype(int)

    result = result[result["상품코드"] != ""]
    return result.reset_index(drop=True)


def _mark_is_latest(df: pd.DataFrame) -> pd.DataFrame:
    """source+상품코드별 updated_at 최대 행에 is_latest=Y, 나머지 N 마킹 (저장 직전 호출)."""
    if df.empty or "상품코드" not in df.columns:
        return df
    df = df.copy()
    if "source" not in df.columns:
        df["source"] = ""
    _grp_keys = ["source", "상품코드"]
    df["is_latest"] = "N"
    if "updated_at" in df.columns:
        # ISO 형식(YYYY-MM-DD 또는 YYYY-MM-DD HH:MM:SS)은 문자열 사전순 = 시간순
        # → datetime 파싱 없이 문자열로 비교 (혼합 날짜/datetime 형식 파싱 오류 방지)
        ts_str = df["updated_at"].fillna("").astype(str).str.strip()
        latest_idx = (
            df.assign(_ts=ts_str, _pos=range(len(df)))
            .sort_values(["_ts", "_pos"])
            .groupby(_grp_keys)
            .tail(1)
            .index
        )
        df.loc[latest_idx, "is_latest"] = "Y"
    else:
        df.loc[df.groupby(_grp_keys).tail(1).index, "is_latest"] = "Y"
    return df


def _read_master() -> pd.DataFrame:
    """fin_product_grp.csv 로드. 없으면 빈 DataFrame."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        return pd.DataFrame(columns=["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명",
                                     "판매단가", "수동분류", "is_main_candidate", "llm_check",
                                     "exclude_check", "updated_at", "is_latest"])
    df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")

    # Backward compatibility: 신규 컬럼이 뒤늦게 추가될 수 있음
    if "exclude_check" not in df.columns:
        df["exclude_check"] = "N"
    if "updated_at" not in df.columns:
        df["updated_at"] = ""
    if "구분" not in df.columns:
        df["구분"] = ""
    if "is_latest" not in df.columns:
        df["is_latest"] = "N"
    # 표준_메뉴명 컬럼은 더 이상 사용하지 않음(과거 파일 호환을 위해 존재할 수는 있으나 파이프라인에서는 제거)
    if "표준_메뉴명" in df.columns:
        df = df.drop(columns=["표준_메뉴명"], errors="ignore")
    if "중복_수동분류" not in df.columns:
        df["중복_수동분류"] = "N"

    # Normalize flags
    df["exclude_check"] = df["exclude_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    if "is_main_candidate" in df.columns:
        df["is_main_candidate"] = (
            df["is_main_candidate"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
        )
    if "llm_check" in df.columns:
        df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})

    if "updated_at" in df.columns:
        df["updated_at"] = df["updated_at"].apply(_normalize_updated_at)

    return df


def _example_row(row: pd.Series) -> dict:
    return {
        "source": str(row.get("source", "")).strip(),
        "구분": str(row.get("구분", "")).strip(),
        "대메뉴": str(row.get("대메뉴", "")).strip(),
        "중메뉴": str(row.get("중메뉴", "")).strip(),
        "상품명": str(row.get("상품명", "")).strip(),
        "판매단가": str(row.get("판매단가", "")).strip(),
        "수동분류": str(row.get("수동분류", "")).strip(),
        "is_main_candidate": str(row.get("is_main_candidate", "")).strip().upper() or "N",
    }


def _build_train_payload(master: pd.DataFrame) -> dict:
    df = master.copy()
    for col in ["source", "수동분류", "llm_check", "exclude_check", "is_latest", "상품명", "updated_at"]:
        if col not in df.columns:
            df[col] = ""

    df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
    df["수동분류"] = df["수동분류"].fillna("").astype(str).str.strip()
    df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    df["exclude_check"] = df["exclude_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    df["is_latest"] = df["is_latest"].fillna("").astype(str).str.strip().str.upper()
    df["_ts"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(pd.Timestamp.min)

    confirmed = df[
        (df["source"] != "posfeed")
        & (df["llm_check"] != "Y")
        & (df["exclude_check"] != "Y")
        & (df["수동분류"].isin(_CLASSIFY_LABELS))
        & (df["상품명"].fillna("").astype(str).str.strip() != "")
    ].copy()
    latest = confirmed[confirmed["is_latest"] == "Y"].copy()
    if not latest.empty:
        confirmed = latest

    label_rules = json.loads(json.dumps(_TRAIN_RULES["label_rules"], ensure_ascii=False))
    counts = confirmed["수동분류"].value_counts().to_dict() if not confirmed.empty else {}

    for label in _CLASSIFY_LABELS:
        label_df = confirmed[confirmed["수동분류"] == label].copy()
        if label_df.empty:
            label_rules[label]["examples"] = []
            continue
        label_df["_name_key"] = label_df["상품명"].fillna("").astype(str).str.strip()
        label_df = (
            label_df.sort_values(["_ts", "_name_key"], ascending=[False, True])
            .drop_duplicates(subset=["_name_key"], keep="first")
            .head(_TRAIN_EXAMPLES_PER_LABEL)
        )
        label_rules[label]["examples"] = [_example_row(row) for _, row in label_df.iterrows()]

    return {
        "version": _TRAIN_RULES["version"],
        "source": str(FIN_PRODUCT_CSV_PATH),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "allowed_labels": _CLASSIFY_LABELS,
        "decision_order": _TRAIN_RULES["decision_order"],
        "hard_rules": _TRAIN_RULES["hard_rules"],
        "label_counts": {label: int(counts.get(label, 0)) for label in _CLASSIFY_LABELS},
        "label_rules": label_rules,
    }


def _load_train_payload(master: pd.DataFrame) -> dict:
    if FIN_PRODUCT_GRP_TRAIN_JSON_PATH.exists():
        try:
            data = json.loads(FIN_PRODUCT_GRP_TRAIN_JSON_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("allowed_labels") and data.get("label_rules"):
                return data
        except Exception as exc:
            logger.warning("fin_product_grp_train.json 로드 실패, 즉시 생성값 사용: %s", exc)
    return _build_train_payload(master)


def _normalize_match_text(value: str) -> str:
    text = re.sub(r"[\[\](){}]", "", str(value))
    return re.sub(r"[^가-힣a-zA-Z0-9一-鿿]", "", text).lower()


def _tokenize_match_text(value: str) -> set[str]:
    normalized = _normalize_match_text(value)
    raw_tokens = re.split(r"[^가-힣a-zA-Z0-9一-鿿]+", str(value).lower())
    tokens = {token for token in raw_tokens if len(token) >= 2}
    tokens.update(
        keyword
        for keyword in [
            "닭도리탕", "곱도리탕", "닭한마리", "묵은지", "우삼겹", "대창", "파김치",
            "미나리", "누룽지", "추가", "토핑", "사리", "콜라", "사이다", "소주", "맥주",
            "배달비", "할인", "리뷰",
        ]
        if keyword in normalized
    )
    return tokens


def _item_match_text(item: dict) -> str:
    return " ".join(
        str(item.get(col, "")).strip()
        for col in ("대메뉴", "중메뉴", "상품명")
    )


def _rule_based_classify(item: dict, train_payload: dict) -> str:
    text = _item_match_text(item)
    main_keywords = train_payload.get("label_rules", {}).get("메인", {}).get("keywords", [])
    topping_markers = ["추가", "토핑", "사리"]
    if any(keyword in text for keyword in main_keywords) and not any(marker in text for marker in topping_markers):
        return "메인"

    label_rules = train_payload.get("label_rules", {})
    for label in train_payload.get("decision_order", _CLASSIFY_LABELS):
        keywords = label_rules.get(label, {}).get("keywords", [])
        if any(str(keyword) and str(keyword) in text for keyword in keywords):
            return label
    return "기타"


def _match_train_json(item: dict, train_payload: dict) -> dict:
    text = _item_match_text(item)
    norm_text = _normalize_match_text(text)
    label_rules = train_payload.get("label_rules", {})

    main_keywords = label_rules.get("메인", {}).get("keywords", [])
    topping_markers = ["추가", "토핑", "사리"]
    if any(_normalize_match_text(keyword) in norm_text for keyword in main_keywords) and not any(marker in norm_text for marker in topping_markers):
        return {
            "label": "메인",
            "confidence": _DIRECT_RULE_CONFIDENCE,
            "method": "json_rule",
            "reason": "메인 키워드 일치 및 추가/토핑 표현 없음",
            "evidence": [],
        }

    for label in train_payload.get("decision_order", _CLASSIFY_LABELS):
        keywords = label_rules.get(label, {}).get("keywords", [])
        hit_keywords = [
            keyword for keyword in keywords
            if keyword and _normalize_match_text(keyword) in norm_text
        ]
        if hit_keywords:
            return {
                "label": label,
                "confidence": _DIRECT_RULE_CONFIDENCE,
                "method": "json_rule",
                "reason": "키워드 일치: " + ", ".join(hit_keywords[:5]),
                "evidence": [],
            }

    item_tokens = _tokenize_match_text(text)
    candidates = []
    for label, rule in label_rules.items():
        if label not in _CLASSIFY_LABELS:
            continue
        for example in rule.get("examples", []):
            ex_text = " ".join(str(example.get(col, "")) for col in ("대메뉴", "중메뉴", "상품명"))
            ex_tokens = _tokenize_match_text(ex_text)
            if not ex_tokens:
                continue
            overlap = item_tokens & ex_tokens
            if not overlap:
                continue
            score = int(100 * len(overlap) / max(len(item_tokens), len(ex_tokens), 1))
            if _normalize_match_text(example.get("상품명", "")) == _normalize_match_text(item.get("상품명", "")):
                score = 100
            candidates.append({
                "label": label,
                "score": score,
                "overlap": sorted(overlap),
                "example": example,
            })

    candidates.sort(key=lambda c: c["score"], reverse=True)
    if candidates and candidates[0]["score"] >= _DIRECT_EXAMPLE_CONFIDENCE:
        top = candidates[0]
        return {
            "label": top["label"],
            "confidence": top["score"],
            "method": "json_example",
            "reason": "유사 확정 예시 일치",
            "evidence": candidates[:5],
        }

    return {
        "label": "",
        "confidence": candidates[0]["score"] if candidates else 0,
        "method": "needs_llm",
        "reason": "확정 규칙/예시만으로 판정 불충분",
        "evidence": candidates[:5],
    }


def _format_llm_evidence_prompt(item: dict, train_payload: dict, match: dict) -> str:
    lines = [
        "아래 JSON 검색 결과의 후보와 근거만 참고해 분류하세요.",
        "전체 룰북을 추측하지 말고 허용 분류 중 하나만 선택하세요.",
        "허용 수동분류: " + ", ".join(train_payload.get("allowed_labels", _CLASSIFY_LABELS)),
        "메인이 아니면 is_main_candidate는 N입니다.",
        "",
        "대상:",
        f'- 대메뉴="{item.get("대메뉴", "")}", 중메뉴="{item.get("중메뉴", "")}", 상품명="{item.get("상품명", "")}", 판매단가="{item.get("판매단가", "")}"',
        "",
        f'JSON 검색 후보: {match.get("label", "") or "없음"}',
        f'검색 신뢰도: {match.get("confidence", 0)}',
        f'검색 사유: {match.get("reason", "")}',
        "",
        "유사 확정 예시:",
    ]
    for evidence in match.get("evidence", [])[:5]:
        ex = evidence.get("example", {})
        lines.append(
            f'- score={evidence.get("score", 0)}, 분류="{evidence.get("label", "")}", '
            f'상품명="{ex.get("상품명", "")}", 중메뉴="{ex.get("중메뉴", "")}"'
        )
    if not match.get("evidence"):
        lines.append("- 없음")
    lines.append("")
    lines.append('JSON만 응답: {"수동분류": "...", "is_main_candidate": "Y|N"}')
    return "\n".join(lines)


def _normalize_classify_result(label: str) -> dict:
    if label not in _CLASSIFY_LABELS:
        label = "기타"
    return {"수동분류": label, "is_main_candidate": "Y" if label == "메인" else "N"}


def _classify_with_llm_evidence(item: dict, train_payload: dict, match: dict) -> dict:
    import json as _json
    client, model = _get_gpt_client()
    prompt = _format_llm_evidence_prompt(item, train_payload, match)
    response = client.chat(
        model=model,
        messages=[
            {"role": "system", "content": "당신은 F&B 상품 분류 보조자입니다. 반드시 JSON만 응답하세요."},
            {"role": "user", "content": prompt},
        ],
        stream=False,
    )
    raw = response.get("message", {}).get("content", "")
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0].strip()
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0].strip()
    result = _json.loads(raw.strip())
    return _normalize_classify_result(str(result.get("수동분류", "")).strip())


def _get_gpt_client():
    """gpt-oss:20b 우선으로 Ollama 클라이언트와 모델명 반환."""
    import ollama
    client = ollama.Client(host=_OLLAMA_HOST)
    model_names = [m["model"] for m in client.list().get("models", [])]
    for candidate in _GPT_MODEL_CANDIDATES:
        if any(candidate in m for m in model_names):
            logger.info("LLM 모델 선택: %s", candidate)
            return client, candidate
    raise RuntimeError(f"사용 가능한 LLM 모델 없음. 설치 목록: {model_names}")


def _classify_one(item: dict, train_payload: dict) -> dict:
    """train JSON 직접 매칭을 우선 적용하고, 애매할 때만 LLM을 보조로 사용."""
    match = _match_train_json(item, train_payload)
    if match["label"] and match["confidence"] >= _DIRECT_EXAMPLE_CONFIDENCE:
        result = _normalize_classify_result(match["label"])
        result.update({
            "llm_error": False,
            "classified_by": match["method"],
            "classification_reason": match["reason"],
        })
        return result

    try:
        result = _classify_with_llm_evidence(item, train_payload, match)
        result.update({
            "llm_error": False,
            "classified_by": "llm_evidence",
            "classification_reason": match["reason"],
        })
        return result
    except Exception as e:
        logger.warning("LLM 보조 분류 실패 (%s): %s", item.get("상품명"), e)
        label = _rule_based_classify(item, train_payload)
        result = _normalize_classify_result(label)
        result.update({
            "llm_error": True,
            "classified_by": "json_fallback",
            "classification_reason": "LLM 실패 후 JSON 룰 fallback",
        })
        return result


# ---------------------------------------------------------------------------
# task functions
# ---------------------------------------------------------------------------

def _read_xlsx() -> pd.DataFrame:
    """Reloaded OKPOS xlsx reader with clearer workbook corruption errors."""
    try:
        df_raw = pd.read_excel(OKPOS_PRODUCT_XLSX, engine="openpyxl", header=0, dtype=str)
    except Exception as exc:
        raise ValueError(
            f"OKPOS product xlsx load failed: {OKPOS_PRODUCT_XLSX} | "
            f"actual Excel file is missing, not a real workbook, or corrupted: {exc}"
        ) from exc

    df_raw = df_raw.fillna("")
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    dae_col = next(name for name, idx in _XLSX_COL_IDX.items() if idx == 0)
    jung_col = next(name for name, idx in _XLSX_COL_IDX.items() if idx == 1)
    code_col = next(name for name, idx in _XLSX_COL_IDX.items() if idx == 3)
    name_col = next(name for name, idx in _XLSX_COL_IDX.items() if idx == 4)
    price_col = next(name for name, idx in _XLSX_COL_IDX.items() if idx == 9)

    if df_raw.shape[1] <= max(_XLSX_COL_IDX.values()):
        raise ValueError(
            f"OKPOS product xlsx has fewer columns than expected: "
            f"expected_index<={max(_XLSX_COL_IDX.values())}, actual_columns={df_raw.shape[1]}"
        )

    result = pd.DataFrame()
    for name, idx in _XLSX_COL_IDX.items():
        result[name] = df_raw.iloc[:, idx]

    if "援щ텇" in df_raw.columns:
        result["援щ텇"] = df_raw["援щ텇"]
    elif "遺꾨쪟" in df_raw.columns:
        result["援щ텇"] = df_raw["遺꾨쪟"]
    else:
        result["援щ텇"] = ""

    result["source"] = SOURCE_CODE
    result[code_col] = result[code_col].astype(str).str.strip()
    result[price_col] = pd.to_numeric(result[price_col], errors="coerce").fillna(0).astype(int)
    result["援щ텇"] = result["援щ텇"].fillna("").astype(str).str.strip()
    result["援щ텇"] = result["援щ텇"].replace({"": "?"})

    result = result[result[code_col] != ""]

    pat = "|".join(_OKPOS_DAEGROUP_KEYWORDS)
    mask_brand = result[dae_col].astype(str).str.contains(pat, na=False)
    mask_delivery = result["援щ텇"].astype(str).str.contains("諛곕떖", na=False)
    result = result[mask_brand | mask_delivery]
    return result.reset_index(drop=True)


def load_okpos_product_xlsx(**context) -> str:
    """OKPOS + EASYPOS 상품조회.xlsx 로드 후 정규화된 DataFrame을 XCom에 저장."""
    if not OKPOS_PRODUCT_XLSX.exists():
        raise FileNotFoundError(f"OKPOS 상품조회.xlsx 없음: {OKPOS_PRODUCT_XLSX}")

    alias_map = _load_alias_map()
    df_okpos = _read_xlsx()
    if "상품명" in df_okpos.columns:
        norm = df_okpos["상품명"].apply(lambda x: _normalize_product_name(x, alias_map))
        # 표준_메뉴명 컬럼은 제거(상품명에 표준명을 반영)
        df_okpos["상품명"] = norm.apply(lambda t: (t[1] or t[0]))
    logger.info("OKPOS 상품 로드: %d건 (도리당/나홀로 필터 적용)", len(df_okpos))

    if EASYPOS_PRODUCT_XLSX.exists():
        df_easypos = _read_easypos_xlsx()
        if "구분" not in df_easypos.columns:
            df_easypos["구분"] = "홀"
        if "상품명" in df_easypos.columns:
            norm = df_easypos["상품명"].apply(lambda x: _normalize_product_name(x, alias_map))
            df_easypos["상품명"] = norm.apply(lambda t: (t[1] or t[0]))
        logger.info("EASYPOS 상품 로드: %d건", len(df_easypos))
        df = pd.concat([df_okpos, df_easypos], ignore_index=True)
    else:
        logger.warning("EASYPOS 상품조회.xlsx 없음 - OKPOS만 처리: %s", EASYPOS_PRODUCT_XLSX)
        df = df_okpos

    allowed_gubun = context.get("allowed_gubun")
    if allowed_gubun:
        if isinstance(allowed_gubun, str):
            allowed = {allowed_gubun.strip()}
        else:
            allowed = {str(x).strip() for x in allowed_gubun if str(x).strip()}
        if allowed:
            if "구분" not in df.columns:
                df["구분"] = "홀"
            df["구분"] = df["구분"].fillna("").astype(str).str.strip().replace({"": "홀"})
            before = len(df)
            df = df[df["구분"].isin(allowed)].reset_index(drop=True)
            logger.info("구분 필터 적용: %s | %d -> %d", sorted(allowed), before, len(df))

    context["ti"].xcom_push(key=_XCOM_OKPOS, value=df.to_json(orient="records", force_ascii=False))
    logger.info("전체 상품 로드 완료: %d건", len(df))
    return f"상품 로드 {len(df)}건 (OKPOS {len(df_okpos)}건 + EASYPOS {len(df) - len(df_okpos)}건)"


def detect_product_changes(**context) -> str:
    """신규/변경 상품 감지 후 XCom에 저장."""
    okpos_json = context["ti"].xcom_pull(task_ids="load_okpos_product_xlsx", key=_XCOM_OKPOS)
    df_new = pd.DataFrame(json.loads(okpos_json))

    df_master = _read_master()
    changes = []

    # source+상품코드 복합 키로 비교 (okpos/001과 easypos/001을 별도로 처리)
    master_src = df_master["source"].fillna("").astype(str).str.strip()
    master_code = df_master["상품코드"].fillna("").astype(str).str.strip()
    existing_keys = set(zip(master_src, master_code))

    for _, row in df_new.iterrows():
        code = str(row.get("상품코드", "")).strip()
        src = str(row.get("source", "")).strip()
        key = (src, code)

        if key not in existing_keys:
            d = row.to_dict()
            d["_change_type"] = "신규"
            changes.append(d)
        else:
            matching = df_master[(master_src == src) & (master_code == code)]
            if "is_latest" in matching.columns:
                latest_rows = matching[matching["is_latest"].str.upper() == "Y"]
                prev = latest_rows.iloc[-1] if not latest_rows.empty else matching.iloc[-1]
            else:
                prev = matching.iloc[-1]
            if any(str(row.get(k, "")).strip() != str(prev.get(k, "")).strip() for k in _CHANGE_KEYS):
                d = row.to_dict()
                d["_change_type"] = "변경"
                changes.append(d)

    context["ti"].xcom_push(key=_XCOM_CHANGES, value=json.dumps(changes, ensure_ascii=False))
    logger.info("변경 감지: %d건 (신규+변경)", len(changes))
    return f"변경 감지 {len(changes)}건"


def build_fin_product_grp_train_json(**context) -> str:
    """확정된 fin_product_grp.csv 분류를 LLM 학습용 JSON으로 저장."""
    df_master = _read_master()
    payload = _build_train_payload(df_master)
    FIN_PRODUCT_GRP_TRAIN_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_GRP_TRAIN_JSON_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _safe_replace(tmp, FIN_PRODUCT_GRP_TRAIN_JSON_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    counts = payload.get("label_counts", {})
    count_msg = ", ".join(f"{label}={counts.get(label, 0)}" for label in _CLASSIFY_LABELS)
    logger.info("fin_product_grp_train.json 저장: %s | %s", FIN_PRODUCT_GRP_TRAIN_JSON_PATH, count_msg)
    return f"train_json 저장: {count_msg}"


def classify_with_llm(enable_llm: bool = True, **context) -> str:
    """신규/변경 항목을 LLM으로 분류.

    이미 llm_check=N 확정 분류가 있는 상품코드는 LLM 재분류 없이 기존 분류를 재사용.
    확정 분류 없는 신규/변경만 LLM 분류 후 _reused_confirmed 플래그로 구분.
    """
    changes_json = context["ti"].xcom_pull(task_ids="detect_product_changes", key=_XCOM_CHANGES)
    changes = json.loads(changes_json)

    if not changes:
        context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value="[]")
        return "변경 없음 - 분류 스킵"

    df_master = _read_master()

    # 확정 분류 맵 빌드: llm_check=N 행 중 updated_at 최신값의 수동분류 (source+상품코드 복합 키)
    confirmed_map: dict[tuple, str] = {}
    if not df_master.empty and "llm_check" in df_master.columns and "상품코드" in df_master.columns:
        confirmed = df_master[
            df_master["llm_check"].fillna("").str.upper() != "Y"
        ].copy()
        confirmed["상품코드"] = confirmed["상품코드"].fillna("").astype(str).str.strip()
        confirmed["source"] = confirmed["source"].fillna("").astype(str).str.strip() if "source" in confirmed.columns else ""
        if not confirmed.empty and "수동분류" in confirmed.columns:
            confirmed["_ts"] = pd.to_datetime(confirmed.get("updated_at", pd.Series(dtype=str)), errors="coerce")
            grp = (
                confirmed[confirmed["상품코드"] != ""]
                .sort_values("_ts")
                .groupby(["source", "상품코드"])
                .last()["수동분류"]
            )
            confirmed_map = {k: v for k, v in grp.items()}

    def _confirmed_key(item: dict) -> tuple:
        return (str(item.get("source", "")).strip(), str(item.get("상품코드", "")).strip())

    if not enable_llm:
        classified = []
        for item in changes:
            item = dict(item)
            key = _confirmed_key(item)
            if key in confirmed_map:
                prev = confirmed_map[key]
                item.update({"수동분류": prev, "is_main_candidate": "Y" if prev == "메인" else "N",
                             "llm_error": False, "_reused_confirmed": True})
            else:
                item.update({"수동분류": "기타", "is_main_candidate": "N", "llm_error": True})
            classified.append(item)
        context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value=json.dumps(classified, ensure_ascii=False))
        return f"LLM OFF - 분류 스킵: {len(classified)}건"

    train_payload = _load_train_payload(df_master)
    reused_cnt, llm_cnt = 0, 0

    classified = []
    for item in changes:
        item = dict(item)
        key = _confirmed_key(item)
        if key in confirmed_map:
            # 이미 확정 분류 있음 → LLM 재분류 금지
            prev = confirmed_map[key]
            item.update({
                "수동분류": prev,
                "is_main_candidate": "Y" if prev == "메인" else "N",
                "llm_error": False,
                "_reused_confirmed": True,
            })
            logger.info("[%s] %s → 확정분류 재사용: %s", item.get("_change_type"), item.get("상품명"), prev)
            reused_cnt += 1
        else:
            result = _classify_one(item, train_payload)
            item.update(result)
            logger.info("[%s] %s → LLM: %s", item.get("_change_type"), item.get("상품명"), result["수동분류"])
            llm_cnt += 1
        classified.append(item)

    context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value=json.dumps(classified, ensure_ascii=False))
    return f"분류 완료: {len(classified)}건 (확정재사용 {reused_cnt}, LLM신규 {llm_cnt})"


def update_product_master(**context) -> str:
    """분류된 신규/변경 항목을 fin_product_grp.csv에 append."""
    classified_json = context["ti"].xcom_pull(task_ids="classify_with_llm", key=_XCOM_CLASSIFIED)
    classified = json.loads(classified_json)

    if not classified:
        return "변경 없음 - 업데이트 스킵"

    df_master = _read_master()
    exclude_map = {}
    if not df_master.empty and "상품코드" in df_master.columns and "exclude_check" in df_master.columns:
        exclude_map = (
            df_master.assign(상품코드=df_master["상품코드"].fillna("").astype(str).str.strip())
            .groupby("상품코드")["exclude_check"]
            .last()
            .to_dict()
        )

    new_rows = []
    for item in classified:
        code = str(item.get("상품코드", "")).strip()
        exclude_check = exclude_map.get(code, "N") if item.get("_change_type") != "신규" else "N"
        # 확정 분류 재사용 → 검토 불필요(N), LLM 새 분류(신규/미확정변경) → 검토 필요(Y)
        reused = item.get("_reused_confirmed", False)
        llm_check = "N" if reused else "Y"
        new_rows.append({
            "source": item.get("source", SOURCE_CODE),
            "구분": item.get("구분", "홀"),
            "대메뉴": item.get("대메뉴", ""),
            "중메뉴": item.get("중메뉴", ""),
            "상품코드": item.get("상품코드", ""),
            "상품명": item.get("상품명", ""),
            "판매단가": item.get("판매단가", 0),
            "수동분류": item.get("수동분류", "기타"),
            "is_main_candidate": item.get("is_main_candidate", "N"),
            "llm_check": llm_check,
            "exclude_check": exclude_check,
            _REVIEW_APPROVE_COL: "",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "중복_수동분류": "N",
        })

    df_append = pd.DataFrame(new_rows)
    df_out = pd.concat([df_master, df_append], ignore_index=True)
    df_out = _mark_is_latest(df_out)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df_out.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("마스터 업데이트 완료: +%d행 (총 %d행)", len(df_append), len(df_out))
    return f"마스터 +{len(df_append)}행 추가"


def send_alert_email(**context) -> str:
    """신규/변경 상품 이메일 알림 발송."""
    classified_json = context["ti"].xcom_pull(task_ids="classify_with_llm", key=_XCOM_CLASSIFIED)
    classified = json.loads(classified_json)

    if not classified:
        return "변경 없음 - 이메일 스킵"

    # 확정분류 재사용 항목은 이메일 불필요 (이미 확정됨, 리뷰 대상 아님)
    review_needed = [i for i in classified if not i.get("_reused_confirmed", False)]
    if not review_needed:
        logger.info("확정분류 재사용 %d건 - 이메일 스킵", len(classified))
        return f"확정분류 재사용 {len(classified)}건 - 이메일 스킵"

    # 이메일에는 리뷰 필요 항목만 표시
    classified = review_needed
    has_llm_error = any(item.get("llm_error") for item in classified)
    신규_cnt = sum(1 for i in classified if i.get("_change_type") == "신규")
    변경_cnt = sum(1 for i in classified if i.get("_change_type") == "변경")

    rows_html = ""
    for item in classified:
        reused = item.get("_reused_confirmed", False)
        if reused:
            tag_color = "#78909C"  # 회색 — 기존 확정 분류 재사용
            tag = f"{item.get('_change_type', '')}(재사용)"
        elif item.get("_change_type") == "신규":
            tag_color = "#2196F3"  # 파랑 — 신규
            tag = "신규"
        else:
            tag_color = "#FF9800"  # 주황 — 변경+LLM 새 분류
            tag = "변경"
        error_mark = " ⚠️" if item.get("llm_error") else ""
        review_mark = "" if reused else " 🔍"  # 검토 필요 표시
        rows_html += (
            f"<tr>"
            f'<td style="padding:6px 10px;"><span style="background:{tag_color};color:#fff;'
            f'border-radius:3px;padding:2px 6px;font-size:12px;">{tag}</span></td>'
            f"<td style='padding:6px 10px;'>{item.get('대메뉴','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('중메뉴','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('상품코드','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('상품명','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('판매단가',0):,}원</td>"
            f"<td style='padding:6px 10px;'>{item.get('수동분류','')}{error_mark}{review_mark}</td>"
            f"<td style='padding:6px 10px;text-align:center;'>{item.get('is_main_candidate','N')}</td>"
            f"</tr>"
        )

    warning_html = ""
    if has_llm_error:
        warning_html = (
            '<p style="color:#e65100;background:#fff3e0;padding:10px;border-radius:4px;">'
            "⚠️ 일부 항목은 Ollama 연결 실패로 기타/N으로 분류됐습니다. 직접 확인 필요.</p>"
        )

    df_master = _read_master()
    pending = _pending_latest(df_master)
    exported = 0

    html = f"""
<html><body style="font-family:sans-serif;color:#333;">
<h2 style="color:#1565C0;">📦 상품 마스터 업데이트 감지</h2>
<p>신규 <b>{신규_cnt}건</b> · 변경 <b>{변경_cnt}건</b> 이 감지되어 LLM 분류 후 <code>fin_product_grp.csv</code>에 추가됐습니다 (신규는 <code>llm_check=Y</code>).</p>
{warning_html}
<table border="0" cellspacing="0" cellpadding="0"
  style="border-collapse:collapse;width:100%;margin-top:16px;font-size:14px;">
  <thead>
    <tr style="background:#1565C0;color:#fff;">
      <th style="padding:8px 10px;">구분</th>
      <th style="padding:8px 10px;">대메뉴</th>
      <th style="padding:8px 10px;">중메뉴</th>
      <th style="padding:8px 10px;">상품코드</th>
      <th style="padding:8px 10px;">상품명</th>
      <th style="padding:8px 10px;">판매단가</th>
      <th style="padding:8px 10px;">LLM 분류</th>
      <th style="padding:8px 10px;">메인후보</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
<p style="margin-top:20px;font-size:13px;color:#777;">
  ✅ <code>fin_product_grp.csv</code>에서 <code>llm_check=Y</code>인 행을 찾아 <code>approve</code> 컬럼에 <b>Y</b>를 입력 후 저장하면 다음 DAG 실행 시 자동으로 <code>llm_check=N</code> 확정행이 append됩니다.<br>
  ❌ 분류가 틀리면 같은 행의 <code>수동분류</code>/<code>is_main_candidate</code>도 함께 수정하세요.<br>
  📄 마스터 파일: <code>{FIN_PRODUCT_CSV_PATH}</code>
</p>
</body></html>
"""

    subject = f"[상품 마스터] 신규/변경 {len(classified)}건 감지 - 검토 필요"
    file_link = (
        f'<p style="margin-top:16px;">'
        f'<a href="{FIN_PRODUCT_CSV_PATH.as_uri()}"'
        f' style="display:inline-block;padding:8px 16px;background:#1565C0;color:#fff;border-radius:4px;text-decoration:none;font-size:13px;">'
        f'📂 CSV 파일 열기</a>'
        f'<span style="color:#999;font-size:11px;margin-left:10px;">{FIN_PRODUCT_CSV_PATH}</span>'
        f'</p>'
    )
    html = html.replace("</body></html>", f"{file_link}</body></html>")

    send_email(subject=subject, html_content=html, to_emails=ALERT_EMAIL)
    logger.info("이메일 발송 완료: %s (%d건)", ALERT_EMAIL, len(classified))
    return f"이메일 발송 완료 ({len(classified)}건)"


def apply_review_approvals(**context) -> str:
    """fin_product_grp.csv에서 approve=Y, llm_check=Y인 상품을 llm_check=N 확정행으로 append."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        return "fin_product_grp.csv 없음 - 스킵"

    master = _read_master()
    if master.empty:
        return "fin_product_grp.csv 비어있음 - 스킵"

    for col in ("llm_check", _REVIEW_APPROVE_COL, "is_latest", "상품코드"):
        if col not in master.columns:
            return f"컬럼 부족({col}) - 스킵"

    master["상품코드"] = master["상품코드"].fillna("").astype(str).str.strip()
    master["llm_check"] = master["llm_check"].fillna("").astype(str).str.strip().str.upper()
    master[_REVIEW_APPROVE_COL] = master[_REVIEW_APPROVE_COL].fillna("").astype(str).str.strip().str.upper()
    master["is_latest"] = master["is_latest"].fillna("").astype(str).str.strip().str.upper()

    candidates = master[
        (master["is_latest"] == "Y") &
        (master["llm_check"] == "Y") &
        (master[_REVIEW_APPROVE_COL] == "Y")
    ]
    if candidates.empty:
        return "approve=Y 없음 - 스킵"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, row in candidates.iterrows():
        new_row = row.to_dict()
        new_row["llm_check"] = "N"
        new_row[_REVIEW_APPROVE_COL] = ""
        new_row["updated_at"] = now
        rows.append(new_row)

    # 기존 행의 approve 클리어 (재처리 방지)
    approved_codes = set(candidates["상품코드"])
    master_copy = master.copy()
    mask = master_copy["상품코드"].isin(approved_codes)
    master_copy.loc[mask, _REVIEW_APPROVE_COL] = ""

    df_append = pd.DataFrame(rows)
    out = pd.concat([master_copy, df_append], ignore_index=True)
    out = _mark_is_latest(out)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        out.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("approve 확정: %d건 (llm_check=N append)", len(df_append))
    return f"approve 확정 {len(df_append)}건 (llm_check=N append)"


def finalize_unionpos_pending(enable_llm: bool = True, **context) -> str:
    """fin_product_grp.csv에 누적된 unionpos 신규(=llm_check=Y) 상품을 LLM으로 분류하고 확정행(llm_check=N)으로 append."""
    if not enable_llm:
        return "LLM OFF - unionpos pending 확정 스킵"
    df_master = _read_master()
    if df_master.empty:
        return "fin_product_grp.csv 비어있음 - 스킵"

    if "source" not in df_master.columns or "llm_check" not in df_master.columns:
        return "fin_product_grp.csv 컬럼 부족(source/llm_check) - 스킵"

    df = df_master.copy()
    df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
    df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    if "updated_at" not in df.columns:
        df["updated_at"] = ""

    pending = df[(df["source"] == "unionpos") & (df["llm_check"] == "Y")].copy()
    if pending.empty:
        return "unionpos pending 없음 - 스킵"

    pending["_updated_at_ts"] = pd.to_datetime(pending["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
    pending = (
        pending.sort_values(["상품코드", "_updated_at_ts"], na_position="last")
        .groupby("상품코드", as_index=False)
        .last()
    )
    pending = pending.drop(columns=["_updated_at_ts"], errors="ignore")

    train_payload = _load_train_payload(df_master)

    rows = []
    for _, row in pending.iterrows():
        item = row.to_dict()
        result = _classify_one(item, train_payload)

        out = row.to_dict()
        out["수동분류"] = result.get("수동분류", out.get("수동분류", "기타"))
        out["is_main_candidate"] = result.get("is_main_candidate", out.get("is_main_candidate", "N"))
        out["llm_check"] = "N"
        out["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(out)

    if not rows:
        return "unionpos pending 분류 대상 없음 - 스킵"

    df_append = pd.DataFrame(rows)
    df_out = pd.concat([df_master, df_append], ignore_index=True)
    df_out = _mark_is_latest(df_out)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df_out.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("unionpos pending 확정: %d건", len(df_append))
    return f"unionpos pending 확정 {len(df_append)}건 (llm_check=N append)"


def build_fin_product_mart(**context) -> str:
    """fin_product_grp.csv에 중복 플래그와 launch_date를 갱신.

    - 컬럼: source, 상품코드, 상품명, is_main_candidate, 수동분류, 중복_수동분류, launch_date
    - 동일 source+상품코드에 수동분류가 2가지 이상이면 이메일 알림 발송.
    """
    master = _read_master()
    if master.empty:
        logger.warning("fin_product_grp.csv 비어있음 - 마트 생성 스킵")
        return "스킵: 원천 데이터 없음"

    # is_latest=Y이고 수동분류가 있는 행만 사용
    df = master.copy()
    if "is_latest" in df.columns:
        df = df[df["is_latest"].astype(str).str.upper() == "Y"]
    if "수동분류" in df.columns:
        df = df[df["수동분류"].notna() & (df["수동분류"].astype(str).str.strip() != "")]

    if df.empty:
        logger.warning("is_latest=Y + 수동분류 있는 행 없음")
        return "스킵: 해당 행 없음"

    df["source"] = df["source"].astype(str).str.strip()
    df["상품코드"] = df["상품코드"].astype(str).str.strip()
    df["수동분류"] = df["수동분류"].astype(str).str.strip()

    # 중복 감지: is_latest 필터 없이 llm_check=N 확정 전체 이력 기준
    # (같은 source+상품코드에 두 개의 확정 분류가 이력에 존재하는 케이스 감지)
    confirmed_all = master.copy()
    confirmed_all["source"] = confirmed_all["source"].astype(str).str.strip()
    confirmed_all["상품코드"] = confirmed_all["상품코드"].astype(str).str.strip()
    confirmed_all["수동분류"] = confirmed_all["수동분류"].astype(str).str.strip()
    if "llm_check" in confirmed_all.columns:
        confirmed_all = confirmed_all[
            confirmed_all["llm_check"].fillna("").astype(str).str.upper() == "N"
        ]
    confirmed_all = confirmed_all[confirmed_all["수동분류"] != ""]

    conflict_df = (
        confirmed_all.groupby(["source", "상품코드"])["수동분류"]
        .nunique()
        .reset_index(name="분류_종류수")
    )
    conflicts = conflict_df[conflict_df["분류_종류수"] > 1]

    if not conflicts.empty:
        detail_rows = []
        for _, row in conflicts.iterrows():
            labels = (
                confirmed_all[
                    (confirmed_all["source"] == row["source"]) & (confirmed_all["상품코드"] == row["상품코드"])
                ]["수동분류"]
                .unique()
                .tolist()
            )
            detail_rows.append(
                f"  {row['source']} {row['상품코드']} → {', '.join(labels)}"
            )
        conflict_body = (
            "아래 상품코드에 수동분류가 2가지 이상 지정되어 있습니다.\n"
            "fin_product_grp.csv에서 직접 수정 후 DAG를 재실행해주세요.\n\n"
            + "\n".join(detail_rows)
        )
        logger.warning("수동분류 충돌 %d건:\n%s", len(conflicts), conflict_body)
        try:
            from modules.transform.utility.mailer import send_email, text_to_html
            send_email(
                subject=f"[fin_product] 수동분류 충돌 {len(conflicts)}건 수정 필요",
                html_content=text_to_html(conflict_body),
                to_emails=[ALERT_EMAIL],
            )
        except Exception as e:
            logger.error("충돌 알림 발송 실패: %s", e)

    conflict_keys = set(
        zip(conflicts["source"].astype(str), conflicts["상품코드"].astype(str))
    ) if not conflicts.empty else set()

    # 마트 생성: source+상품코드 기준 중복 제거 (수동분류 충돌 시 첫 번째 값 유지)
    df["중복_수동분류"] = df.apply(
        lambda r: "Y" if (str(r["source"]), str(r["상품코드"])) in conflict_keys else "N", axis=1
    )
    df["상품명"] = df["상품명"].astype(str).str.strip() if "상품명" in df.columns else ""
    df["is_main_candidate"] = (
        df["is_main_candidate"].astype(str).str.strip().str.upper()
        if "is_main_candidate" in df.columns
        else "N"
    )

    if "updated_at" in master.columns:
        _master_for_launch = master.copy()
        _master_for_launch["source"] = _master_for_launch["source"].astype(str).str.strip()
        _master_for_launch["상품코드"] = _master_for_launch["상품코드"].astype(str).str.strip()
        _master_for_launch["_ts"] = _master_for_launch["updated_at"].apply(_normalize_updated_at)
        master_launch_dates = (
            _master_for_launch[_master_for_launch["_ts"] != ""]
            .groupby(["source", "상품코드"])["_ts"]
            .min()
            .reset_index(name="master_launch_date")
        )
    else:
        master_launch_dates = pd.DataFrame(columns=["source", "상품코드", "master_launch_date"])

    first_sale_dates = _build_first_sale_dates()
    launch_dates = master_launch_dates.merge(
        first_sale_dates,
        on=["source", "상품코드"],
        how="outer",
    )
    launch_dates["launch_date"] = launch_dates["first_sale_date"].fillna(
        launch_dates["master_launch_date"]
    )
    launch_dates = launch_dates[["source", "상품코드", "launch_date"]]

    mart = (
        df[["source", "상품코드", "상품명", "is_main_candidate", "수동분류", "중복_수동분류"]]
        .drop_duplicates(subset=["source", "상품코드"], keep="first")
        .merge(launch_dates, on=["source", "상품코드"], how="left")
        .sort_values(["source", "상품코드"])
        .reset_index(drop=True)
    )

    updated_master = master.copy()
    updated_master["_key"] = list(
        zip(updated_master["source"].astype(str).str.strip(), updated_master["상품코드"].astype(str).str.strip())
    )
    updated_master["중복_수동분류"] = updated_master["_key"].apply(
        lambda k: "Y" if k in conflict_keys else "N"
    )
    launch_date_map = mart.set_index(["source", "상품코드"])["launch_date"].fillna("").to_dict()
    previous_launch_date = (
        updated_master["launch_date"].fillna("").astype(str)
        if "launch_date" in updated_master.columns
        else pd.Series([""] * len(updated_master), index=updated_master.index)
    )
    updated_master["launch_date"] = [
        launch_date_map.get(k, previous)
        for k, previous in zip(updated_master["_key"], previous_launch_date)
    ]
    updated_master = updated_master.drop(columns=["_key"])

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        updated_master.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    conflict_msg = f", 충돌 {len(conflicts)}건 알림" if not conflicts.empty else ""
    logger.info("fin_product_grp.csv launch_date write-back: %d행%s", len(mart), conflict_msg)
    return f"grp 업데이트: {len(mart)}행{conflict_msg}"


def _build_first_sale_dates() -> pd.DataFrame:
    """unified_sales에서 source+상품코드별 최초 판매일을 계산."""
    parquet_dir = MART_DB / "unified_sales_grp"
    files = sorted(parquet_dir.glob("unified_sales_*.parquet")) if parquet_dir.exists() else []
    if not files:
        logger.warning("unified_sales parquet 없음: %s", parquet_dir)
        return pd.DataFrame(columns=["source", "상품코드", "first_sale_date"])

    first_by_key: dict[tuple[str, str], pd.Timestamp] = {}
    for file_path in files:
        try:
            frame = pd.read_parquet(file_path, columns=["source", "item_id", "sale_date"])
        except Exception as exc:
            logger.warning("최초판매일 산정 parquet 스킵: %s (%s)", file_path, exc)
            continue

        frame["source"] = frame["source"].astype(str).str.strip()
        frame["상품코드"] = frame["item_id"].astype(str).str.strip()
        frame["sale_date"] = pd.to_datetime(frame["sale_date"], errors="coerce")
        frame = frame[(frame["source"] != "") & (frame["상품코드"] != "")].dropna(subset=["sale_date"])
        if frame.empty:
            continue

        daily_first = frame.groupby(["source", "상품코드"])["sale_date"].min()
        for key, sale_date in daily_first.items():
            current = first_by_key.get(key)
            if current is None or sale_date < current:
                first_by_key[key] = sale_date

    if not first_by_key:
        return pd.DataFrame(columns=["source", "상품코드", "first_sale_date"])

    first_sale_dates = pd.DataFrame(
        [
            {"source": source, "상품코드": item_id, "first_sale_date": sale_date.strftime("%Y-%m-%d")}
            for (source, item_id), sale_date in first_by_key.items()
        ]
    )
    return first_sale_dates


def build_launch_tracking(**context) -> str:
    """신규 메인 메뉴의 출시 후 30일 매출 성과 집계."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        logger.warning("fin_product_grp.csv 없음 - 스킵")
        return "스킵: grp 없음"

    mart = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    if "is_latest" in mart.columns:
        mart = mart[mart["is_latest"].astype(str).str.upper() == "Y"]
    mart = mart.drop_duplicates(subset=["source", "상품코드"], keep="first")
    is_main = (
        mart["is_main_candidate"].astype(str).str.strip().str.upper()
        if "is_main_candidate" in mart.columns
        else pd.Series(["N"] * len(mart), index=mart.index)
    )
    launch_date = (
        mart["launch_date"].astype(str).str.strip()
        if "launch_date" in mart.columns
        else pd.Series([""] * len(mart), index=mart.index)
    )
    mart = mart[(is_main == "Y") & (launch_date != "")]
    if mart.empty:
        return "추적 대상 없음 (is_main_candidate=Y + launch_date 있는 메뉴 없음)"

    mart["launch_date"] = pd.to_datetime(mart["launch_date"], errors="coerce")
    mart = mart.dropna(subset=["launch_date"])
    if mart.empty:
        return "추적 대상 없음 (유효한 launch_date 없음)"

    mart["source"] = mart["source"].astype(str).str.strip()
    mart["상품코드"] = mart["상품코드"].astype(str).str.strip()
    mart["상품명"] = mart["상품명"].astype(str).str.strip() if "상품명" in mart.columns else ""
    mart["수동분류"] = mart["수동분류"].astype(str).str.strip() if "수동분류" in mart.columns else ""
    mart = mart.drop_duplicates(subset=["source", "상품코드", "상품명", "수동분류", "launch_date"])
    mart["_launch_end"] = mart["launch_date"] + pd.Timedelta(days=30)
    target_keys = set(zip(mart["source"], mart["상품코드"]))

    parquet_dir = MART_DB / "unified_sales_grp"
    files = sorted(parquet_dir.glob("unified_sales_*.parquet")) if parquet_dir.exists() else []
    if not files:
        logger.warning("unified_sales parquet 없음: %s", parquet_dir)
        return "스킵: unified_sales 없음"

    result = mart[["source", "상품코드", "상품명", "수동분류", "launch_date"]].copy()
    result["orders_30d"] = 0
    result["revenue_30d"] = 0
    result = result.set_index(["source", "상품코드", "상품명", "수동분류", "launch_date"])

    read_columns = ["source", "item_id", "sale_date", "qty", "total_price"]
    for file_path in files:
        try:
            sales = pd.read_parquet(file_path, columns=read_columns)
        except Exception as exc:
            logger.warning("launch_tracking parquet 스킵: %s (%s)", file_path, exc)
            continue

        sales["source"] = sales["source"].astype(str).str.strip()
        sales["item_id"] = sales["item_id"].astype(str).str.strip()
        sales_keys = pd.MultiIndex.from_arrays([sales["source"], sales["item_id"]])
        sales = sales[sales_keys.isin(target_keys)]
        if sales.empty:
            continue

        sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce")
        sales = sales.dropna(subset=["sale_date"])
        if sales.empty:
            continue

        sales["qty"] = pd.to_numeric(sales["qty"], errors="coerce").fillna(0)
        sales["total_price"] = pd.to_numeric(sales["total_price"], errors="coerce").fillna(0)
        merged = mart.merge(
            sales,
            left_on=["source", "상품코드"],
            right_on=["source", "item_id"],
            how="inner",
        )
        merged = merged[
            (merged["sale_date"] >= merged["launch_date"])
            & (merged["sale_date"] <= merged["_launch_end"])
        ]
        if merged.empty:
            continue

        grouped = (
            merged.groupby(["source", "상품코드", "상품명", "수동분류", "launch_date"])
            .agg(orders_30d=("qty", "sum"), revenue_30d=("total_price", "sum"))
        )
        result.loc[grouped.index, ["orders_30d", "revenue_30d"]] += grouped[["orders_30d", "revenue_30d"]]

    result = result.reset_index()
    result["launch_date"] = result["launch_date"].dt.strftime("%Y-%m-%d")
    result["orders_30d"] = result["orders_30d"].astype(int)
    result["revenue_30d"] = result["revenue_30d"].astype(int)

    out_path = FIN_PRODUCT_CSV_PATH.parent / "fin_product_launch_tracking.csv"
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("launch_tracking 저장: %d건", len(result))
    return f"launch_tracking {len(result)}건 저장"
