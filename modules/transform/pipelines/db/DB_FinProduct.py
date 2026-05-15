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
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
)
from modules.transform.utility.mailer import send_email

logger = logging.getLogger(__name__)

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

_CLASSIFY_LABELS = ["메인", "1인", "사이드", "기타"]
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


def _write_review_file(pending_latest: pd.DataFrame) -> int:
    """llm_check=Y(미확정) 상품 목록을 fin_product_review.csv로 내보낸다.

    - 사용자는 review CSV에서 approve=Y로 체크만 하면 됨.
    - 기존 파일이 있으면 approve/note/checked_at는 유지(상품코드 기준).
    """
    if pending_latest.empty:
        return 0

    # single-file operation: approve=Y is read from fin_product_grp.csv (no fin_product_review.csv)
    master = _read_master()
    if master.empty:
        return "fin_product_grp.csv ë¹„ì–´ìžˆìŒ - ìŠ¤í‚µ"
    if "ìƒí’ˆì½”ë“œ" not in master.columns:
        return "fin_product_grp.csv ì»¬ëŸ¼ ë¶€ì¡±(ìƒí’ˆì½”ë“œ) - ìŠ¤í‚µ"

    master["ìƒí’ˆì½”ë“œ"] = master["ìƒí’ˆì½”ë“œ"].fillna("").astype(str).str.strip()
    if "updated_at" not in master.columns:
        master["updated_at"] = ""
    if "llm_check" not in master.columns:
        master["llm_check"] = "N"
    if _REVIEW_APPROVE_COL not in master.columns:
        return f"{_REVIEW_APPROVE_COL} ì»¬ëŸ¼ ì—†ìŒ - ìŠ¤í‚µ"
    master["llm_check"] = master["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    master[_REVIEW_APPROVE_COL] = master[_REVIEW_APPROVE_COL].fillna("").astype(str).str.strip().str.upper().replace({"": ""})

    latest = (
        master.copy()
        .assign(_ts=pd.to_datetime(master["updated_at"], errors="coerce").fillna(pd.Timestamp.min))
        .sort_values(["ìƒí’ˆì½”ë“œ", "_ts"], na_position="last")
        .groupby("ìƒí’ˆì½”ë“œ", as_index=False)
        .last()
        .drop(columns=["_ts"], errors="ignore")
    ).set_index("ìƒí’ˆì½”ë“œ")

    approve_codes = latest[(latest[_REVIEW_APPROVE_COL] == "Y") & (latest["llm_check"] == "Y")].index.astype(str).tolist()
    if not approve_codes:
        return "approve=Y ì—†ìŒ - ìŠ¤í‚µ"

    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for code in approve_codes:
        code = str(code).strip()
        if not code or code not in latest.index:
            continue
        base = latest.loc[code].to_dict()
        base["llm_check"] = "N"
        base["updated_at"] = now
        base[_REVIEW_APPROVE_COL] = ""
        rows.append(base)

    if not rows:
        return "approve ëŒ€ìƒ ì—†ìŒ(ì´ë¯¸ í™•ì •/ë¯¸ì¡´ìž¬) - ìŠ¤í‚µ"

    df_append = pd.DataFrame(rows)
    out = pd.concat([master, df_append], ignore_index=True)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        out.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    return f"approve í™•ì • {len(df_append)}ê±´ (llm_check=N append)"

    review = _load_review_file()
    review_map = {}
    if not review.empty:
        review_map = (
            review.assign(상품코드=review["상품코드"].fillna("").astype(str).str.strip())
            .groupby("상품코드")
            .last()
            .to_dict(orient="index")
        )

    out = pending_latest.copy()
    out["상품코드"] = out["상품코드"].fillna("").astype(str).str.strip()
    out[_REVIEW_APPROVE_COL] = out["상품코드"].map(lambda c: (review_map.get(c, {}) or {}).get(_REVIEW_APPROVE_COL, ""))
    out["note"] = out["상품코드"].map(lambda c: (review_map.get(c, {}) or {}).get("note", ""))
    out["checked_at"] = out["상품코드"].map(lambda c: (review_map.get(c, {}) or {}).get("checked_at", ""))

    cols = [
        "source",
        "대메뉴",
        "중메뉴",
        "상품코드",
        "상품명",
        "판매단가",
        "수동분류",
        "is_main_candidate",
        "exclude_check",
        "llm_check",
        "updated_at",
        _REVIEW_APPROVE_COL,
        "note",
        "checked_at",
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols].copy()

    FIN_PRODUCT_REVIEW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_REVIEW_CSV_PATH.with_suffix(".tmp")
    try:
        out.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, FIN_PRODUCT_REVIEW_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    return len(out)


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


def _read_master() -> pd.DataFrame:
    """fin_product_grp.csv 로드. 없으면 빈 DataFrame."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        return pd.DataFrame(columns=["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명",
                                     "판매단가", "수동분류", "is_main_candidate", "llm_check", "exclude_check", "updated_at"])
    df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")

    # Backward compatibility: 신규 컬럼이 뒤늦게 추가될 수 있음
    if "exclude_check" not in df.columns:
        df["exclude_check"] = "N"
    if "updated_at" not in df.columns:
        df["updated_at"] = ""
    if "구분" not in df.columns:
        df["구분"] = ""
    # 표준_메뉴명 컬럼은 더 이상 사용하지 않음(과거 파일 호환을 위해 존재할 수는 있으나 파이프라인에서는 제거)
    if "표준_메뉴명" in df.columns:
        df = df.drop(columns=["표준_메뉴명"], errors="ignore")

    # Normalize flags
    df["exclude_check"] = df["exclude_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    if "is_main_candidate" in df.columns:
        df["is_main_candidate"] = (
            df["is_main_candidate"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
        )
    if "llm_check" in df.columns:
        df["llm_check"] = df["llm_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})

    return df


def _build_few_shot(master: pd.DataFrame) -> str:
    """확정된 분류(llm_check=N) 기준 카테고리별 예시 3개씩 추출."""
    confirmed = master[master.get("llm_check", pd.Series(["N"] * len(master))) != "Y"] if "llm_check" in master.columns else master
    if "exclude_check" in confirmed.columns:
        confirmed = confirmed[confirmed["exclude_check"].fillna("").astype(str).str.strip().str.upper() != "Y"]
    lines = []
    for label in _CLASSIFY_LABELS:
        samples = confirmed[confirmed["수동분류"] == label].head(3)
        for _, row in samples.iterrows():
            lines.append(f'  상품명="{row["상품명"]}", 중메뉴="{row["중메뉴"]}", 대메뉴="{row["대메뉴"]}" → 수동분류="{label}"')
    return "\n".join(lines) if lines else "  (예시 없음)"


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


def _classify_one(item: dict, few_shot: str) -> dict:
    """gpt-oss:20b로 단일 항목 분류. 실패 시 폴백값 반환."""
    try:
        import json as _json
        client, model = _get_gpt_client()

        system_prompt = (
            "당신은 F&B 상품 분류 전문가입니다. 반드시 JSON만 응답하세요.\n"
            f"수동분류 카테고리: {', '.join(_CLASSIFY_LABELS)}\n"
            "확정된 기존 분류 예시 (학습 데이터):\n"
            f"{few_shot}\n\n"
            '응답 형식: {"수동분류": "메인", "is_main_candidate": "Y"}'
        )
        prompt = (
            f'대메뉴: {item["대메뉴"]}, 중메뉴: {item["중메뉴"]}, '
            f'상품명: {item["상품명"]}, 판매단가: {item["판매단가"]}'
        )
        response = client.chat(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            stream=False,
        )
        raw = response.get("message", {}).get("content", "")

        # JSON 블록 추출
        if "```json" in raw:
            raw = raw.split("```json")[1].split("```")[0].strip()
        elif "```" in raw:
            raw = raw.split("```")[1].split("```")[0].strip()
        result = _json.loads(raw.strip())

        분류 = result.get("수동분류", "기타")
        if 분류 not in _CLASSIFY_LABELS:
            분류 = "기타"
        is_main = result.get("is_main_candidate", "N")
        if is_main not in ("Y", "N"):
            is_main = "Y" if 분류 == "메인" else "N"

        return {"수동분류": 분류, "is_main_candidate": is_main, "llm_error": False}

    except Exception as e:
        logger.warning("LLM 분류 실패 (%s): %s", item.get("상품명"), e)
        return {"수동분류": "기타", "is_main_candidate": "N", "llm_error": True}


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

    existing_codes = set(df_master["상품코드"].tolist())

    for _, row in df_new.iterrows():
        code = row["상품코드"]
        if code not in existing_codes:
            d = row.to_dict()
            d["_change_type"] = "신규"
            changes.append(d)
        else:
            prev = df_master[df_master["상품코드"] == code].iloc[-1]
            keys = list(_CHANGE_KEYS)
            if any(str(row.get(k, "")).strip() != str(prev.get(k, "")).strip() for k in keys):
                d = row.to_dict()
                d["_change_type"] = "변경"
                changes.append(d)

    context["ti"].xcom_push(key=_XCOM_CHANGES, value=json.dumps(changes, ensure_ascii=False))
    logger.info("변경 감지: %d건 (신규+변경)", len(changes))
    return f"변경 감지 {len(changes)}건"


def classify_with_llm(enable_llm: bool = True, **context) -> str:
    """신규/변경 항목을 LLM으로 분류."""
    changes_json = context["ti"].xcom_pull(task_ids="detect_product_changes", key=_XCOM_CHANGES)
    changes = json.loads(changes_json)

    if not changes:
        context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value="[]")
        return "변경 없음 - 분류 스킵"

    if not enable_llm:
        classified = []
        for item in changes:
            item = dict(item)
            item.update({"수동분류": "기타", "is_main_candidate": "N", "llm_error": True})
            classified.append(item)
        context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value=json.dumps(classified, ensure_ascii=False))
        return f"LLM OFF - 분류 스킵(기타/N): {len(classified)}건"

    df_master = _read_master()
    few_shot = _build_few_shot(df_master)

    classified = []
    for item in changes:
        result = _classify_one(item, few_shot)
        item.update(result)
        classified.append(item)
        logger.info("[%s] %s → %s", item["_change_type"], item["상품명"], result["수동분류"])

    context["ti"].xcom_push(key=_XCOM_CLASSIFIED, value=json.dumps(classified, ensure_ascii=False))
    return f"LLM 분류 완료: {len(classified)}건"


def update_product_master(**context) -> str:
    """분류된 신규/변경 항목을 fin_product_grp.csv에 append."""
    classified_json = context["ti"].xcom_pull(task_ids="classify_with_llm", key=_XCOM_CLASSIFIED)
    classified = json.loads(classified_json)

    if not classified:
        return "ë³€ê²½ ì—†ìŒ - ì´ë©”ì¼ ìŠ¤í‚µ"
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
        # 신규 상품만 llm_check=Y (사용자 검토 필요), 변경은 N (자동 반영)
        llm_check = "Y" if item.get("_change_type") == "신규" else "N"
        code = str(item.get("상품코드", "")).strip()
        exclude_check = exclude_map.get(code, "N") if item.get("_change_type") != "신규" else "N"
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
        })

    df_append = pd.DataFrame(new_rows)
    df_out = pd.concat([df_master, df_append], ignore_index=True)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df_out.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, FIN_PRODUCT_CSV_PATH)
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
        # 미확정(pending) 목록만 갱신할 수 있으니, 마스터 기준으로 review 파일 업데이트
        df_master = _read_master()
        pending = _pending_latest(df_master)
        exported = 0
        return f"변경 없음 - 이메일 스킵 (review {exported}행)"

    has_llm_error = any(item.get("llm_error") for item in classified)
    신규_cnt = sum(1 for i in classified if i.get("_change_type") == "신규")
    변경_cnt = sum(1 for i in classified if i.get("_change_type") == "변경")

    rows_html = ""
    for item in classified:
        tag_color = "#2196F3" if item.get("_change_type") == "신규" else "#FF9800"
        tag = item.get("_change_type", "")
        error_mark = " ⚠️" if item.get("llm_error") else ""
        rows_html += (
            f"<tr>"
            f'<td style="padding:6px 10px;"><span style="background:{tag_color};color:#fff;'
            f'border-radius:3px;padding:2px 6px;font-size:12px;">{tag}</span></td>'
            f"<td style='padding:6px 10px;'>{item.get('대메뉴','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('중메뉴','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('상품코드','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('상품명','')}</td>"
            f"<td style='padding:6px 10px;'>{item.get('판매단가',0):,}원</td>"
            f"<td style='padding:6px 10px;'>{item.get('수동분류','')}{error_mark}</td>"
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
  ✅ 신규 상품은 <code>fin_product_review.csv</code>에서 <code>approve=Y</code>로 체크만 하면 다음 실행 때 자동으로 <code>llm_check=N</code> 확정행이 append됩니다.<br>
  ❌ 분류가 틀리면 <code>fin_product_grp.csv</code>의 <code>수동분류</code>/<code>is_main_candidate</code>를 수정하거나, review의 <code>note</code>에 남긴 뒤 재확인하세요.<br>
  📄 review 파일 위치: <code>{FIN_PRODUCT_REVIEW_CSV_PATH}</code> (현재 pending {exported}행)
</p>
</body></html>
"""

    subject = f"[상품 마스터] 신규/변경 {len(classified)}건 감지 - 검토 필요"
    # single-file operation: do not reference fin_product_review.csv in outgoing emails
    html = html.replace("fin_product_review.csv", "fin_product_grp.csv")
    html = html.replace("review íŒŒì¼ ìœ„ì¹˜", "grp íŒŒì¼ ìœ„ì¹˜")
    html = html.replace(str(FIN_PRODUCT_REVIEW_CSV_PATH), str(FIN_PRODUCT_CSV_PATH))
    html = html.replace("review", "grp")
    html = html.replace(f" (현재 pending {exported}행)", "")
    html = html.replace(f" (í˜„ìž¬ pending {exported}í–‰)", "")

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
    """fin_product_review.csv에서 approve=Y로 체크된 상품을 llm_check=N 확정행으로 append."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        return "fin_product_grp.csv 없음 - 스킵"

    review = _load_review_file()
    if review.empty:
        return "review 없음 - 스킵"

    approve = (
        review.assign(상품코드=review["상품코드"].fillna("").astype(str).str.strip())
        .query(f"{_REVIEW_APPROVE_COL} == 'Y'")
        .copy()
    )
    if approve.empty:
        return "approve=Y 없음 - 스킵"

    master = _read_master()
    if master.empty:
        return "fin_product_grp.csv 비어있음 - 스킵"
    if "상품코드" not in master.columns:
        return "fin_product_grp.csv 컬럼 부족(상품코드) - 스킵"

    master["상품코드"] = master["상품코드"].fillna("").astype(str).str.strip()
    if "updated_at" not in master.columns:
        master["updated_at"] = ""
    if "llm_check" not in master.columns:
        master["llm_check"] = "N"

    # 코드별 최신 1행을 기준으로 확정행을 만든다 (이미 llm_check=N이면 스킵)
    latest = (
        master.copy()
        .assign(_ts=pd.to_datetime(master["updated_at"], errors="coerce").fillna(pd.Timestamp.min))
        .sort_values(["상품코드", "_ts"], na_position="last")
        .groupby("상품코드", as_index=False)
        .last()
        .drop(columns=["_ts"], errors="ignore")
    )
    latest = latest.set_index("상품코드")

    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for code in approve["상품코드"].tolist():
        code = str(code).strip()
        if not code or code not in latest.index:
            continue
        base = latest.loc[code].to_dict()
        if str(base.get("llm_check", "")).strip().upper() != "Y":
            continue
        base["llm_check"] = "N"
        base["updated_at"] = now
        rows.append(base)

    if not rows:
        return "approve 대상 없음(이미 확정/미존재) - 스킵"

    df_append = pd.DataFrame(rows)
    out = pd.concat([master, df_append], ignore_index=True)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        out.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    # review의 checked_at 자동 기록(approve=Y인 행만)
    review = review.copy()
    review["상품코드"] = review["상품코드"].fillna("").astype(str).str.strip()
    mask = review["상품코드"].isin([str(r.get("상품코드", "")).strip() for r in rows])
    if "checked_at" not in review.columns:
        review["checked_at"] = ""
    review.loc[mask, "checked_at"] = now

    FIN_PRODUCT_REVIEW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp2 = FIN_PRODUCT_REVIEW_CSV_PATH.with_suffix(".tmp")
    try:
        review.to_csv(tmp2, index=False, encoding="utf-8-sig")
        os.replace(tmp2, FIN_PRODUCT_REVIEW_CSV_PATH)
    finally:
        try:
            tmp2.unlink(missing_ok=True)
        except Exception:
            pass

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

    few_shot = _build_few_shot(df_master)

    rows = []
    for _, row in pending.iterrows():
        item = row.to_dict()
        result = _classify_one(item, few_shot)

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

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df_out.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("unionpos pending 확정: %d건", len(df_append))
    return f"unionpos pending 확정 {len(df_append)}건 (llm_check=N append)"
