"""
통합 상품 마스터 파이프라인.

처리 흐름:
1) OKPOS + EASYPOS 상품조회.xlsx 로드 및 정규화
2) fin_product_grp_input.csv와 비교 → 신규/변경 상품 감지
3) LLM(Ollama)으로 검수 후보 분류 + is_main_candidate 분류
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
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    FIN_PRODUCT_MART_CSV_PATH,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    RAW_OKPOS_SALES,
    existing_fin_product_csv_path,
)
from modules.transform.utility.qwen_client import query_qwen_json
from modules.transform.utility.mailer import send_email
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.pipelines.db.DB_FinProduct_Rules import (
    build_rules_from_manual,
    classify_by_rules,
    load_rules,
    reconcile,
    rules_to_prompt_block,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    SOURCE_ITEM_BASE,
    allocate_manual_item_ids,
    canonical_source,
    ensure_allocator_columns,
    item_block_size,
    normalize_item_key,
    validate_fin_product_codes,
)

logger = logging.getLogger(__name__)

_MAP_APPROVED = {"1", "1.0", "y", "yes", "true", "approved", "승인", "완료", "검수완료"}
_MAP_DUP_LABEL_COL = "중복_수동분류"
_MAP_ENRICHMENT_COLS = ["source", "item_id", _MAP_DUP_LABEL_COL, "표준상품명", "수동분류_edit", "대표메뉴", "검수유무"]


def _safe_replace(src: Path, dst: Path) -> None:
    """os.replace with fallback for FUSE/OneDrive mounts where rename may fail."""
    try:
        os.replace(src, dst)
    except (PermissionError, OSError):
        dst.write_bytes(src.read_bytes())


def _load_representative_menu_lookup() -> dict[tuple[str, str], str]:
    if not FIN_PRODUCT_MAP_CSV_PATH.exists():
        return {}
    try:
        df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("fin_product_map.csv 대표메뉴 로드 실패: %s", e)
        return {}

    for col in ("source", "item_id", "대표메뉴"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["source"] = df["source"].map(canonical_source)
    df = df[(df["source"] != "") & (df["item_id"] != "") & (df["대표메뉴"] != "")]
    if df.empty:
        return {}
    deduped = df.drop_duplicates(subset=["source", "item_id"], keep="last")
    return {
        (row["source"], row["item_id"]): row["대표메뉴"]
        for row in deduped[["source", "item_id", "대표메뉴"]].to_dict("records")
    }


def _load_map_enrichment_df() -> pd.DataFrame:
    """fin_product_map_review_input.csv에서 검수 완료된 분석용 표준명/분류만 로드."""
    empty = pd.DataFrame(columns=_MAP_ENRICHMENT_COLS)
    if not FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists():
        return empty

    try:
        df = pd.read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("fin_product_map_review_input.csv 로드 실패, 표준명 합침 생략: %s", e)
        return empty

    required_cols = (
        "source",
        "item_id",
        "표준_메뉴명_edit",
        "수동분류_edit",
        _MAP_DUP_LABEL_COL,
        "검수유무",
    )
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    df["source"] = df["source"].astype(str).str.strip()
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["표준상품명"] = df["표준_메뉴명_edit"].astype(str).str.strip()
    df["수동분류_edit"] = df["수동분류_edit"].astype(str).str.strip()
    df[_MAP_DUP_LABEL_COL] = df[_MAP_DUP_LABEL_COL].astype(str).str.strip().str.upper().replace({"": "N"})
    df["검수유무"] = df["검수유무"].astype(str).str.strip()
    approved = df["검수유무"].str.lower().isin(_MAP_APPROVED)
    df = df[approved & (df["source"] != "") & (df["item_id"] != "") & (df["표준상품명"] != "")].copy()
    if df.empty:
        return empty

    representative_menu = _load_representative_menu_lookup()
    df["대표메뉴"] = [
        representative_menu.get((source, item_id), "")
        for source, item_id in zip(df["source"], df["item_id"])
    ]
    for col in ["표준상품명", "수동분류_edit", "대표메뉴", "검수유무"]:
        df[col] = df[col].replace("", pd.NA)
    return df.drop_duplicates(subset=["source", "item_id"], keep="last")[_MAP_ENRICHMENT_COLS]

OKPOS_PRODUCT_XLSX = ANALYTICS_DB / "okpos_product" / "상품조회.xlsx"
EASYPOS_PRODUCT_XLSX = ANALYTICS_DB / "easypos_product" / "상품조회.xlsx"
SOURCE_CODE = "okpos"
EASYPOS_SOURCE_CODE = "easypos"
ALERT_EMAIL = MAIL_CMJ_PM

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
_BRAND_STORE_DAEGROUP_RE = re.compile(r"^\s*(도리당|나홀로)\s+(.+점)\s*$")

# EASYPOS xlsx 컬럼명 → 표준 컬럼명 매핑
_EASYPOS_COL_MAP = {
    "대메뉴": "대분류명",
    "중메뉴": "소분류명",
    "상품코드": "상품코드",
    "상품명": "상품명",
    "판매단가": "판매가",
}

_CLASSIFY_LABELS = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
_MAP_FEW_SHOT_SOURCES = {"okpos", "posfeed"}
_FEW_SHOT_PER_LABEL = 8
_CHANGE_KEYS = ["대메뉴", "중메뉴", "상품명"]

_XCOM_OKPOS = "okpos_df_json"
_XCOM_CHANGES = "changes_json"
_XCOM_CLASSIFIED = "classified_json"

_REVIEW_APPROVE_COL = "approve"  # Y/N/blank


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _split_brand_store_daemenu(value: object) -> tuple[str, str, str]:
    text = str(value or "").strip()
    if not text:
        return "", "", ""
    match = _BRAND_STORE_DAEGROUP_RE.match(text)
    if match:
        brand = match.group(1)
        return brand, brand, match.group(2).strip()
    return text, "", ""


def _normalize_daemenu(value: object) -> str:
    return _split_brand_store_daemenu(value)[0]


def _normalize_daemenu_series(series: pd.Series) -> pd.Series:
    return series.fillna("").map(_normalize_daemenu)


def _fill_brand_store_from_daemenu(df: pd.DataFrame) -> pd.DataFrame:
    if "대메뉴" not in df.columns:
        return df
    split = df["대메뉴"].fillna("").map(_split_brand_store_daemenu)
    df["대메뉴"] = split.map(lambda x: x[0])
    if "brand" not in df.columns:
        df["brand"] = ""
    if "store" not in df.columns:
        df["store"] = ""
    brand = split.map(lambda x: x[1])
    store = split.map(lambda x: x[2])
    brand_blank = df["brand"].fillna("").astype(str).str.strip().eq("")
    store_blank = df["store"].fillna("").astype(str).str.strip().eq("")
    df.loc[brand_blank & brand.ne(""), "brand"] = brand[brand_blank & brand.ne("")]
    df.loc[store_blank & store.ne(""), "store"] = store[store_blank & store.ne("")]
    return df


def _okpos_store_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("store="):
            return part.split("=", 1)[1].strip()
    return ""


def _normalize_okpos_product_code(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return text
    return str(int(numeric))


def _load_okpos_product_code_store_map() -> dict[str, set[str]]:
    """OKPOS 매출 원천에서 원본 상품코드별 실제 판매 매장 목록을 만든다."""
    brand_root = RAW_OKPOS_SALES / "brand=도리당"
    result: dict[str, set[str]] = {}
    if not brand_root.exists():
        logger.warning("OKPOS 원천 매출 경로 없음: %s", brand_root)
        return result

    code_candidates = ("상품코드", "item_id", "상품번호", "상품ID")
    for path in sorted(brand_root.glob("store=*/ym=*/okpos_order_item.csv")):
        store = _okpos_store_from_path(path)
        if not store:
            continue
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception as e:
            logger.warning("OKPOS 상품코드-매장 매핑 로드 실패: %s | %s", path, e)
            continue

        code_col = next((col for col in code_candidates if col in df.columns), "")
        if not code_col:
            logger.warning("OKPOS order_item 상품코드 컬럼 없음: %s | columns=%s", path, list(df.columns))
            continue

        codes = df[code_col].map(_normalize_okpos_product_code)
        for code in codes[codes != ""].drop_duplicates():
            result.setdefault(code, set()).add(store)
    return result


def _expand_okpos_rows_by_sales_store(df: pd.DataFrame) -> pd.DataFrame:
    """상품조회에서 매장이 빈 OKPOS 행을 실제 매출 원천의 매장별 행으로 확장한다."""
    if df.empty or "source" not in df.columns or "상품코드" not in df.columns:
        return df

    result = df.copy()
    for col in ("brand", "store", "상품코드"):
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()

    source_s = result["source"].fillna("").astype(str).str.strip().str.lower()
    blank_store = source_s.eq(SOURCE_CODE) & result["store"].eq("")
    if not blank_store.any():
        return result

    code_to_stores = _load_okpos_product_code_store_map()
    expanded_rows: list[pd.Series] = []
    existing_keys = {
        (
            SOURCE_CODE,
            str(row.get("brand", "")).strip(),
            str(row.get("store", "")).strip(),
            _normalize_okpos_product_code(row.get("상품코드", "")),
        )
        for _, row in result[~blank_store].iterrows()
        if str(row.get("store", "")).strip()
    }
    dropped = 0
    for _, row in result[blank_store].iterrows():
        code = _normalize_okpos_product_code(row.get("상품코드", ""))
        stores = sorted(code_to_stores.get(code, set()))
        if not stores:
            dropped += 1
            continue
        for store in stores:
            new_row = row.copy()
            if not str(new_row.get("brand", "")).strip():
                new_row["brand"] = "도리당"
            new_row["store"] = store
            key = (SOURCE_CODE, str(new_row.get("brand", "")).strip(), store, code)
            if key in existing_keys:
                continue
            existing_keys.add(key)
            expanded_rows.append(new_row)

    kept = result[~blank_store].copy()
    if expanded_rows:
        kept = pd.concat([kept, pd.DataFrame(expanded_rows, columns=result.columns)], ignore_index=True)
    if dropped:
        logger.warning("OKPOS 상품조회 매장 미확인 행 제외: %d건", dropped)
    logger.info(
        "OKPOS 상품조회 빈 매장 행 확장: blank=%d expanded=%d dropped=%d",
        int(blank_store.sum()),
        len(expanded_rows),
        dropped,
    )
    return kept.reset_index(drop=True)


def _normalize_launch_date(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return text
    return ts.strftime("%Y-%m-%d")

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
    """fin_product_grp_input.csv에서 llm_check=Y인 미확정 상품을 코드별 최신 1행으로 추출."""
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
    result = _fill_brand_store_from_daemenu(result)
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
    result["brand"] = "도리당"
    result["store"] = "송파점"
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
    valid_code = df["상품코드"].fillna("").astype(str).str.strip() != ""
    work = df[valid_code]
    if work.empty:
        return df
    if "updated_at" in df.columns:
        # ISO 형식(YYYY-MM-DD 또는 YYYY-MM-DD HH:MM:SS)은 문자열 사전순 = 시간순
        # → datetime 파싱 없이 문자열로 비교 (혼합 날짜/datetime 형식 파싱 오류 방지)
        ts_str = work["updated_at"].fillna("").astype(str).str.strip()
        latest_idx = (
            work.assign(_ts=ts_str, _pos=range(len(work)))
            .sort_values(["_ts", "_pos"])
            .groupby(_grp_keys)
            .tail(1)
            .index
        )
        df.loc[latest_idx, "is_latest"] = "Y"
    else:
        df.loc[work.groupby(_grp_keys).tail(1).index, "is_latest"] = "Y"
    return df


def _read_master() -> pd.DataFrame:
    """fin_product_grp_input.csv 로드. 없으면 기존 fin_product_grp.csv를 읽는다."""
    source_path = existing_fin_product_csv_path()
    if not source_path.exists():
        return ensure_allocator_columns(pd.DataFrame(columns=["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명",
                                     "판매단가", "is_main_candidate", "llm_check",
                                     "exclude_check", "updated_at", "is_latest"]))
    df = pd.read_csv(source_path, dtype=str).fillna("")
    df = ensure_allocator_columns(df)

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
    if "중복_수동분류" in df.columns:
        df = df.drop(columns=["중복_수동분류"], errors="ignore")

    # Normalize flags
    df["exclude_check"] = df["exclude_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    if "is_main_candidate" in df.columns:
        df["is_main_candidate"] = (
            df["is_main_candidate"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
        )
    if "llm_check" in df.columns:
        df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})

    return df


def _apply_scoped_product_codes(df: pd.DataFrame) -> pd.DataFrame:
    """상품조회 원본 코드를 매장 스코프 숫자 상품코드로 변환한다."""
    if df.empty:
        return ensure_allocator_columns(df)
    result = ensure_allocator_columns(df.copy())
    for col in ("source", "brand", "store", "상품코드", "상품명", "판매단가"):
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()
    result["상품코드"] = allocate_manual_item_ids(
        result[["source", "brand", "store", "상품코드", "상품명", "판매단가"]]
        .rename(columns={"상품코드": "item_id", "상품명": "item_name", "판매단가": "unit_price"}),
        persist=False,
    )
    result["item_key"] = result["상품명"].map(normalize_item_key)
    for idx in result.index:
        source = canonical_source(result.at[idx, "source"])
        base = SOURCE_ITEM_BASE.get(source)
        if base is None:
            continue
        try:
            offset = int(str(result.at[idx, "상품코드"]).strip()) - base
        except ValueError:
            continue
        if offset < 0:
            continue
        block_size = item_block_size(source)
        result.at[idx, "store_seq"] = str(offset // block_size)
        result.at[idx, "item_seq"] = str(offset % block_size)
    return result


def _fill_allocator_identity_from_code(item: dict) -> dict:
    out = dict(item)
    source = canonical_source(out.get("source", SOURCE_CODE))
    base = SOURCE_ITEM_BASE.get(source)
    code = str(out.get("상품코드", "")).strip()
    if base is None or not code:
        return out
    try:
        offset = int(code) - base
    except ValueError:
        return out
    if offset < 0:
        return out
    block_size = item_block_size(source)
    out["store_seq"] = str(offset // block_size)
    out["item_seq"] = str(offset % block_size)
    item_key = str(out.get("item_key", "")).strip()
    if not item_key:
        out["item_key"] = normalize_item_key(out.get("상품명", out.get("item_name", "")))
    return out


def _review_few_shot_rows() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("fin_product_map_review_input.csv few-shot 로드 실패: %s", exc)
        return pd.DataFrame()

    required = ("source", "item_name", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴", "검수유무")
    for col in required:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    approved = df["검수유무"].str.lower().isin(_MAP_APPROVED)
    source_ok = df["source"].map(canonical_source).isin(_MAP_FEW_SHOT_SOURCES)
    label_ok = df["수동분류_edit"].isin(_CLASSIFY_LABELS)
    name_ok = df["item_name"].ne("")
    out = df[approved & source_ok & label_ok & name_ok].copy()
    if out.empty:
        return out

    out["_source_rank"] = out["source"].map(canonical_source).map({"okpos": 0, "posfeed": 1}).fillna(9)
    out["_name_len"] = out["item_name"].str.len()
    return (
        out.sort_values(["수동분류_edit", "_source_rank", "_name_len", "item_name"])
        .drop_duplicates(subset=["수동분류_edit", "item_name"], keep="first")
        .reset_index(drop=True)
    )


def _master_few_shot_rows(master: pd.DataFrame) -> pd.DataFrame:
    if master.empty or "수동분류" not in master.columns:
        return pd.DataFrame()
    confirmed = master[master.get("llm_check", pd.Series(["N"] * len(master), index=master.index)) != "Y"].copy()
    if "exclude_check" in confirmed.columns:
        confirmed = confirmed[confirmed["exclude_check"].fillna("").astype(str).str.strip().str.upper() != "Y"]
    if confirmed.empty:
        return pd.DataFrame()
    for col in ("상품명", "중메뉴", "대메뉴", "수동분류"):
        if col not in confirmed.columns:
            confirmed[col] = ""
        confirmed[col] = confirmed[col].fillna("").astype(str).str.strip()
    return confirmed[confirmed["수동분류"].isin(_CLASSIFY_LABELS) & confirmed["상품명"].ne("")]


def _available_rules(master: pd.DataFrame | None = None) -> list[dict]:
    rules = load_rules()
    if rules or master is None or master.empty:
        return rules
    source = master.copy()
    for col in ("상품명", "수동분류", "llm_check", "classified_by", "검수사유"):
        if col not in source.columns:
            source[col] = ""
        source[col] = source[col].fillna("").astype(str).str.strip()
    source = source[
        (source["상품명"] != "")
        & source["수동분류"].isin(_CLASSIFY_LABELS)
        & (source["llm_check"].str.upper() != "Y")
        & (~source["classified_by"].str.lower().isin({"rule", "llm", "rule+llm"}))
        & (~source["classified_by"].str.startswith("rule/llm_conflict"))
        & (~source["검수사유"].str.contains("충돌|미입력|확인|불일치", regex=True, na=False))
    ]
    return build_rules_from_manual(source)


def _build_few_shot(master: pd.DataFrame, rules: list[dict] | None = None) -> str:
    """검수 완료된 map review와 확정 master 기준으로 LLM 분류 예시를 만든다."""
    lines: list[str] = []
    rule_block = rules_to_prompt_block(rules or [])
    if rule_block != "(자동 키워드 규칙 없음)":
        lines.extend(["자동 키워드 규칙:", rule_block, "검수 완료 예시:"])
    review_rows = _review_few_shot_rows()
    if not review_rows.empty:
        for label in _CLASSIFY_LABELS:
            samples = review_rows[review_rows["수동분류_edit"] == label].head(_FEW_SHOT_PER_LABEL)
            for _, row in samples.iterrows():
                standard = row.get("표준_메뉴명_edit", "")
                main = row.get("대표메뉴", "")
                detail = []
                if standard:
                    detail.append(f'표준명="{standard}"')
                if main:
                    detail.append(f'대표메뉴="{main}"')
                detail_text = ", " + ", ".join(detail) if detail else ""
                lines.append(f'  상품명="{row["item_name"]}"{detail_text} → 수동분류="{label}"')

    master_rows = _master_few_shot_rows(master)
    if not master_rows.empty:
        existing_names = {line.split('상품명="', 1)[1].split('"', 1)[0] for line in lines if '상품명="' in line}
        for label in _CLASSIFY_LABELS:
            current_count = sum(f'수동분류="{label}"' in line for line in lines)
            if current_count >= _FEW_SHOT_PER_LABEL:
                continue
            samples = master_rows[master_rows["수동분류"] == label].head(_FEW_SHOT_PER_LABEL - current_count)
            for _, row in samples.iterrows():
                name = str(row.get("상품명", "")).strip()
                if not name or name in existing_names:
                    continue
                existing_names.add(name)
                lines.append(
                    f'  상품명="{name}", 중메뉴="{row.get("중메뉴", "")}", '
                    f'대메뉴="{row.get("대메뉴", "")}" → 수동분류="{label}"'
                )

    return "\n".join(lines) if lines else "  (예시 없음)"


def _rule_based_classification(item: dict) -> dict | None:
    rules = item.pop("_rules", None)
    rule_result = classify_by_rules(item, rules or load_rules())
    if not rule_result:
        return None
    label = rule_result.get("수동분류", "기타")
    return {
        **rule_result,
        "수동분류": label,
        "is_main_candidate": "Y" if label == "메인" else "N",
        "llm_error": False,
    }


def _format_won(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([str(value or "").replace(",", "").strip()]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return str(value or "").strip()
    return f"{int(numeric):,}"


def _classify_one(item: dict, few_shot: str, rules: list[dict] | None = None) -> dict:
    """gpt-oss:20b로 단일 항목 분류. 실패 시 폴백값 반환."""
    rule_item = dict(item)
    rule_item["_rules"] = rules or []
    rule_result = _rule_based_classification(rule_item)

    try:
        system_prompt = (
            "당신은 F&B 상품 분류 전문가입니다. 반드시 JSON만 응답하세요.\n"
            f"수동분류 카테고리: {', '.join(_CLASSIFY_LABELS)}\n"
            "아래 자동 키워드 규칙과 검수 완료 예시를 최우선으로 참고하되, 상품 문맥과 다르면 더 적절한 분류를 선택하세요.\n"
            "is_main_candidate는 수동분류가 메인일 때만 Y, 그 외에는 N으로 응답하세요.\n"
            "확정된 기존 분류 기준 (학습 데이터):\n"
            f"{few_shot}\n\n"
            '응답 형식: {"수동분류": "메인", "is_main_candidate": "Y"}'
        )
        prompt = (
            f'대메뉴: {item["대메뉴"]}, 중메뉴: {item["중메뉴"]}, '
            f'상품명: {item["상품명"]}, 판매단가: {item["판매단가"]}'
        )
        result = query_qwen_json(prompt, system_prompt=system_prompt)
        if result.get("parse_error"):
            raise ValueError(result.get("parse_error"))

        분류 = result.get("수동분류", "기타")
        if 분류 not in _CLASSIFY_LABELS:
            분류 = "기타"
        is_main = result.get("is_main_candidate", "N")
        if is_main not in ("Y", "N"):
            is_main = "Y" if 분류 == "메인" else "N"
        if 분류 != "메인":
            is_main = "N"

        llm_result = {"수동분류": 분류, "is_main_candidate": is_main, "llm_error": False}

    except Exception as e:
        logger.warning("LLM 분류 실패 (%s): %s", item.get("상품명"), e)
        if rule_result is not None:
            resolved = reconcile(rule_result, {})
            label = resolved.get("수동분류", "기타")
            return {
                "수동분류": label,
                "is_main_candidate": "Y" if label == "메인" else "N",
                "llm_error": True,
                "classified_by": resolved.get("classified_by", "rule"),
                "검수사유": resolved.get("검수사유", ""),
            }
        return {"수동분류": "기타", "is_main_candidate": "N", "llm_error": True, "classified_by": "llm"}

    resolved = reconcile(rule_result, llm_result)
    label = resolved.get("수동분류", llm_result["수동분류"])
    is_main = resolved.get("is_main_candidate") or ("Y" if label == "메인" else "N")
    if label != "메인":
        is_main = "N"
    return {
        "수동분류": label,
        "is_main_candidate": is_main,
        "llm_error": llm_result.get("llm_error", False),
        "classified_by": resolved.get("classified_by", "llm"),
        "검수사유": resolved.get("검수사유", ""),
    }


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

    if df_raw.shape[1] <= max(_XLSX_COL_IDX.values()):
        raise ValueError(
            f"OKPOS product xlsx has fewer columns than expected: "
            f"expected_index<={max(_XLSX_COL_IDX.values())}, actual_columns={df_raw.shape[1]}"
        )

    result = pd.DataFrame()
    for name, idx in _XLSX_COL_IDX.items():
        result[name] = df_raw.iloc[:, idx]

    if "구분" in df_raw.columns:
        result["구분"] = df_raw["구분"]
    elif "분류" in df_raw.columns:
        result["구분"] = df_raw["분류"]
    else:
        result["구분"] = ""

    result["source"] = SOURCE_CODE
    result["상품코드"] = result["상품코드"].astype(str).str.strip()
    result = _fill_brand_store_from_daemenu(result)
    result["판매단가"] = pd.to_numeric(result["판매단가"], errors="coerce").fillna(0).astype(int)
    result["구분"] = result["구분"].fillna("").astype(str).str.strip()
    result["구분"] = result["구분"].replace({"": "홀"})

    result = result[result["상품코드"] != ""]

    pat = "|".join(_OKPOS_DAEGROUP_KEYWORDS)
    mask_brand = result["대메뉴"].astype(str).str.contains(pat, na=False)
    mask_delivery = result["구분"].astype(str).str.contains("배달", na=False)
    result = result[mask_brand | mask_delivery]
    return result.reset_index(drop=True)


def load_okpos_product_xlsx(**context) -> str:
    """OKPOS + EASYPOS 상품조회.xlsx 로드 후 정규화된 DataFrame을 XCom에 저장."""
    if not OKPOS_PRODUCT_XLSX.exists():
        raise FileNotFoundError(f"OKPOS 상품조회.xlsx 없음: {OKPOS_PRODUCT_XLSX}")

    alias_map = _load_alias_map()
    df_okpos = _read_xlsx()
    df_okpos = _expand_okpos_rows_by_sales_store(df_okpos)
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

    df = _apply_scoped_product_codes(df)

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
            prev = matching.iloc[-1]
            if any(str(row.get(k, "")).strip() != str(prev.get(k, "")).strip() for k in _CHANGE_KEYS):
                d = row.to_dict()
                d["_change_type"] = "변경"
                changes.append(d)

    context["ti"].xcom_push(key=_XCOM_CHANGES, value=json.dumps(changes, ensure_ascii=False))
    logger.info("변경 감지: %d건 (신규+변경)", len(changes))
    return f"변경 감지 {len(changes)}건"


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

    rules = _available_rules(df_master)
    few_shot = _build_few_shot(df_master, rules=rules)
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
            result = _classify_one(item, few_shot, rules=rules)
            item.update(result)
            if result.get("검수사유"):
                logger.info(
                    "[%s] %s → %s: %s | %s",
                    item.get("_change_type"),
                    item.get("상품명"),
                    result.get("classified_by", "llm"),
                    result["수동분류"],
                    result["검수사유"],
                )
            else:
                logger.info(
                    "[%s] %s → %s: %s",
                    item.get("_change_type"),
                    item.get("상품명"),
                    result.get("classified_by", "llm"),
                    result["수동분류"],
                )
            llm_cnt += 1
        classified.append(item)

    context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value=json.dumps(classified, ensure_ascii=False))
    return f"분류 완료: {len(classified)}건 (확정재사용 {reused_cnt}, LLM신규 {llm_cnt})"


def update_product_master(**context) -> str:
    """분류된 신규/변경 항목을 fin_product_grp_input.csv에 append."""
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
        item = _fill_allocator_identity_from_code(item)
        code = str(item.get("상품코드", "")).strip()
        if not code:
            logger.warning("상품코드 빈 신규/변경 항목 저장 스킵: %s", item.get("상품명", ""))
            continue
        exclude_check = exclude_map.get(code, "N") if item.get("_change_type") != "신규" else "N"
        # 확정 분류 재사용 → 검토 불필요(N), LLM 새 분류(신규/미확정변경) → 검토 필요(Y)
        reused = item.get("_reused_confirmed", False)
        llm_check = "N" if reused else "Y"
        new_rows.append({
            "source": item.get("source", SOURCE_CODE),
            "구분": item.get("구분", "홀"),
            "대메뉴": item.get("대메뉴", ""),
            "중메뉴": item.get("중메뉴", ""),
            "상품코드": code,
            "상품명": item.get("상품명", ""),
            "판매단가": item.get("판매단가", 0),
            "수동분류": item.get("수동분류", "기타"),
            "is_main_candidate": item.get("is_main_candidate", "N"),
            "llm_check": llm_check,
            "exclude_check": exclude_check,
            _REVIEW_APPROVE_COL: "",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "brand": item.get("brand", ""),
            "store": item.get("store", ""),
            "launch_date": _normalize_launch_date(item.get("launch_date", "")),
            "store_seq": item.get("store_seq", ""),
            "item_seq": item.get("item_seq", ""),
            "item_key": item.get("item_key", ""),
        })

    df_append = pd.DataFrame(new_rows)
    df_out = pd.concat([df_master, df_append], ignore_index=True)
    df_out = ensure_allocator_columns(df_out)
    df_out = _mark_is_latest(df_out)
    validate_fin_product_codes(df_out, allow_legacy_blank=True)

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
            f"<td style='padding:6px 10px;'>{_format_won(item.get('판매단가',0))}원</td>"
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
<p>신규 <b>{신규_cnt}건</b> · 변경 <b>{변경_cnt}건</b> 이 감지되어 LLM 분류 후 <code>fin_product_grp_input.csv</code>에 추가됐습니다 (신규는 <code>llm_check=Y</code>).</p>
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
  ✅ <code>fin_product_grp_input.csv</code>에서 <code>llm_check=Y</code>인 행을 찾아 <code>approve</code> 컬럼에 <b>Y</b>를 입력 후 저장하면 다음 DAG 실행 시 자동으로 <code>llm_check=N</code> 확정행이 append됩니다.<br>
  ❌ 대표메뉴 후보가 틀리면 같은 행의 <code>is_main_candidate</code>를 수정하세요.<br>
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
    """fin_product_grp_input.csv에서 approve=Y, llm_check=Y인 상품을 llm_check=N 확정행으로 append."""
    if not existing_fin_product_csv_path().exists():
        return "fin_product_grp_input.csv 없음 - 스킵"

    master = _read_master()
    if master.empty:
        return "fin_product_grp_input.csv 비어있음 - 스킵"

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
    out = ensure_allocator_columns(out)
    out = _mark_is_latest(out)
    validate_fin_product_codes(out, allow_legacy_blank=True)

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


def classify_pending_master(
    limit: int | None = 1000,
    since: str | None = None,
    sources: list[str] | None = None,
    enable_llm: bool = True,
    dry_run: bool = False,
) -> str:
    """수동분류가 비어 있는 최신 상품 마스터 행을 source 무관하게 LLM 초안 분류한다."""
    df = _read_master()
    if df.empty:
        return "fin_product_grp_input.csv 비어있음 - 스킵"

    for col in (
        "수동분류",
        "is_latest",
        "상품명",
        "updated_at",
        "source",
        "대메뉴",
        "중메뉴",
        "판매단가",
        "is_main_candidate",
        "llm_check",
    ):
        if col not in df.columns:
            df[col] = ""

    lab = df["수동분류"].fillna("").astype(str).str.strip()
    latest = df["is_latest"].fillna("").astype(str).str.strip().str.upper() == "Y"
    name_ok = df["상품명"].fillna("").astype(str).str.strip() != ""
    mask = (lab == "") & latest & name_ok

    if since:
        mask &= df["updated_at"].fillna("").astype(str).str[:10] >= since
    if sources:
        source_set = {str(source).strip().lower() for source in sources}
        mask &= df["source"].fillna("").astype(str).str.strip().str.lower().isin(source_set)

    targets = df.loc[mask].copy()
    total = len(targets)
    if total == 0:
        return f"pending 분류: 대상 0건, 분류 0건, 실패 0건 (limit={limit}, since={since})"

    if not enable_llm:
        return f"LLM OFF - pending 분류 스킵: 대상 {total}건 (limit={limit}, since={since})"

    targets["_updated_at_ts"] = pd.to_datetime(targets["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
    targets = targets.sort_values("_updated_at_ts", ascending=False)
    if limit is not None:
        targets = targets.head(limit)
    targets = targets.drop(columns=["_updated_at_ts"], errors="ignore")

    rules = _available_rules(df)
    few_shot = _build_few_shot(df, rules=rules)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    done = 0
    fail = 0

    for idx, row in targets.iterrows():
        item = row.to_dict()
        try:
            result = _classify_one(item, few_shot, rules=rules)
            label = result.get("수동분류", "기타")
            if label not in _CLASSIFY_LABELS:
                label = "기타"
            is_main = result.get("is_main_candidate", "N")
            if is_main not in ("Y", "N"):
                is_main = "Y" if label == "메인" else "N"
            if label != "메인":
                is_main = "N"
            df.at[idx, "수동분류"] = label
            df.at[idx, "is_main_candidate"] = is_main
            df.at[idx, "llm_check"] = "Y"
            df.at[idx, "updated_at"] = now
            if result.get("llm_error"):
                fail += 1
            else:
                done += 1
            if result.get("검수사유"):
                logger.info(
                    "pending 분류: %s → %s (%s) | %s",
                    item.get("상품명"),
                    label,
                    result.get("classified_by", "llm"),
                    result["검수사유"],
                )
        except Exception as exc:
            fail += 1
            logger.warning("pending 분류 실패 (%s): %s", item.get("상품명"), exc)

    processed = done + fail
    if dry_run:
        return f"pending 분류 dry-run: 대상 {total}건, 분류 {done}건, 실패 {fail}건 (limit={limit}, since={since})"

    df = ensure_allocator_columns(df)
    df = _mark_is_latest(df)
    validate_fin_product_codes(df, allow_legacy_blank=True)

    label_counts = (
        df.loc[targets.index, "수동분류"]
        .fillna("")
        .astype(str)
        .str.strip()
        .value_counts()
        .to_dict()
    )
    logger.info("pending 분류 라벨 분포(%d건 처리): %s", processed, label_counts)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    backup = FIN_PRODUCT_CSV_PATH.with_name(
        f"fin_product_grp_input.backup_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )
    backup.write_bytes(FIN_PRODUCT_CSV_PATH.read_bytes())

    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    return f"pending 분류: 대상 {total}건, 분류 {done}건, 실패 {fail}건 (limit={limit}, since={since})"


def finalize_unionpos_pending(enable_llm: bool = True, **context) -> str:
    """fin_product_grp_input.csv에 누적된 unionpos 신규(=llm_check=Y) 상품을 LLM으로 분류하고 확정행(llm_check=N)으로 append."""
    if not enable_llm:
        return "LLM OFF - unionpos pending 확정 스킵"
    df_master = _read_master()
    if df_master.empty:
        return "fin_product_grp_input.csv 비어있음 - 스킵"

    if "source" not in df_master.columns or "llm_check" not in df_master.columns:
        return "fin_product_grp_input.csv 컬럼 부족(source/llm_check) - 스킵"

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

    rules = _available_rules(df_master)
    few_shot = _build_few_shot(df_master, rules=rules)

    rows = []
    for _, row in pending.iterrows():
        item = row.to_dict()
        result = _classify_one(item, few_shot, rules=rules)

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
    df_out = ensure_allocator_columns(df_out)
    df_out = _mark_is_latest(df_out)
    validate_fin_product_codes(df_out, allow_legacy_blank=True)

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
    """fin_product_grp_input.csv → fin_product_mart.csv 생성.

    - 컬럼: source, 상품코드 기준 상품 목록
    - 분석용 표준명/분류는 fin_product_map_review_input.csv에서 합친다.
    """
    master = _read_master()
    if master.empty:
        logger.warning("fin_product_grp_input.csv 비어있음 - 마트 생성 스킵")
        return "스킵: 원천 데이터 없음"

    # is_latest=Y인 행만 사용
    df = master.copy()
    if "is_latest" in df.columns:
        df = df[df["is_latest"].astype(str).str.upper() == "Y"]

    if df.empty:
        logger.warning("is_latest=Y 행 없음")
        return "스킵: 해당 행 없음"

    df["source"] = df["source"].astype(str).str.strip()
    df["상품코드"] = df["상품코드"].astype(str).str.strip()

    # 마트 생성: source+상품코드 기준 중복 제거.
    # 분석용 분류는 검수 완료된 fin_product_map_review_input.csv의 수동분류_edit만 사용한다.
    key_cols = ["source", "상품코드"]
    extra_src_cols = ["상품명", "exclude_check"]
    for col in extra_src_cols:
        if col not in df.columns:
            df[col] = ""

    mart = (
        df[key_cols + extra_src_cols]
        .drop_duplicates(subset=["source", "상품코드"], keep="first")
        .sort_values(["source", "상품코드"])
        .reset_index(drop=True)
    )
    try:
        enrichment = _load_map_enrichment_df()
        mart = mart.merge(
            enrichment.rename(columns={"item_id": "상품코드"}),
            on=["source", "상품코드"],
            how="left",
        )
        for col in [_MAP_DUP_LABEL_COL, "표준상품명", "수동분류_edit", "대표메뉴", "검수유무"]:
            if col not in mart.columns:
                mart[col] = pd.NA
        matched = int((mart["표준상품명"].astype(str).str.strip() != "").sum())
        logger.info("mart review 표준명/분류 합침: %d/%d행 매칭", matched, len(mart))
    except Exception as e:
        logger.warning("표준명 합침 실패, mart 기본 컬럼만 생성: %s", e)
        for col in [_MAP_DUP_LABEL_COL, "표준상품명", "수동분류_edit", "대표메뉴", "검수유무"]:
            mart[col] = pd.NA
    mart = mart[
        key_cols + [_MAP_DUP_LABEL_COL, "상품명", "표준상품명", "수동분류_edit", "대표메뉴", "검수유무", "exclude_check"]
    ]

    FIN_PRODUCT_MART_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MART_CSV_PATH.with_suffix(".tmp")
    try:
        mart.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MART_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("fin_product_mart.csv 저장: %d행", len(mart))
    return f"마트 생성: {len(mart)}행"
