"""ToOrder menu LLM enrichment pipeline."""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pendulum

from modules.transform.utility.paths import ANALYTICS_DB, MART_DB
from modules.transform.utility.qwen_client import (
    get_ollama_client_with_candidates,
    query_qwen_json,
)

logger = logging.getLogger(__name__)

BACKFILL_DAYS = 7
TOORDER_MENU_DB = ANALYTICS_DB / "toorder_menu"
TOORDER_MENU_LLM_DB = MART_DB / "option_llm"
LLM_LOG_PATH = TOORDER_MENU_LLM_DB / "llm_log.md"
LLM_TOTAL_JSON_PATH = TOORDER_MENU_LLM_DB / "toorder_menu_llm_total.json"
LLM_STORE_JSON_PATH = TOORDER_MENU_LLM_DB / "toorder_menu_store.json"

COL_DATE = "수집일자"
COL_CATEGORY = "카테고리"
COL_MENU = "메뉴명"
COL_STORE = "매장명"
COL_QTY = "판매량"
COL_SALES = "매출액"
COL_YM = "ym"

LLM_COLS_STORE = ["매장_AI현황", "매장_AI특징", "매장_AI제안"]
LLM_COLS_TOTAL = ["전체_AI현황", "전체_AI특징", "전체_AI제안"]
LLM_COLS = LLM_COLS_STORE + LLM_COLS_TOTAL
DEPRECATED_LLM_COLS = []
ASCII_ALPHA_RE = re.compile(r"[A-Za-z]")
PLACEHOLDER_TEXTS = {"", "...", "…", "-", "nan", "none", "null"}
TOTAL_AMT_COL = "total_amt"


def _normalize_date(value: Any) -> str:
    return pendulum.parse(str(value).strip(), strict=False).in_timezone("Asia/Seoul").format("YYYY-MM-DD")


def _date_range(start_date: str, end_date: str) -> List[str]:
    start = pendulum.parse(start_date)
    end = pendulum.parse(end_date)
    if end < start:
        start, end = end, start
    days = (end.date() - start.date()).days
    return [start.add(days=i).format("YYYY-MM-DD") for i in range(days + 1)]


def _default_target_date(context: Optional[Dict[str, Any]] = None) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    data_interval_end = (context or {}).get("data_interval_end")
    if data_interval_end is not None:
        return pendulum.instance(data_interval_end).in_timezone(kst).subtract(days=1).format("YYYY-MM-DD")
    return pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")


def get_target_dates(conf: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return target dates from DAG conf or the scheduled KST data interval."""
    conf = conf or {}
    default_date = _default_target_date(context)
    if conf.get("sale_date"):
        return [_normalize_date(conf["sale_date"])]
    if conf.get("date_range"):
        raw = str(conf["date_range"]).strip()
        if "~" not in raw:
            raise ValueError("date_range must be 'YYYY-MM-DD~YYYY-MM-DD'")
        start_raw, end_raw = [part.strip() for part in raw.split("~", 1)]
        return _date_range(_normalize_date(start_raw), _normalize_date(end_raw))
    if conf.get("backfill"):
        end_dt = pendulum.parse(default_date)
        start_date = end_dt.subtract(days=BACKFILL_DAYS - 1).format("YYYY-MM-DD")
        return _date_range(start_date, default_date)
    return [default_date]


def _date_key(date_str: str) -> str:
    return date_str.replace("-", "")[2:]


def _parquet_path(date_str: str) -> Path:
    return TOORDER_MENU_DB / f"toorder_menu_detail_{_date_key(date_str)}.parquet"


def _llm_parquet_path(date_str: str) -> Path:
    return TOORDER_MENU_LLM_DB / f"toorder_menu_detail_{_date_key(date_str)}.parquet"


def _load_parquet(date_str: str) -> Optional[pd.DataFrame]:
    path = _parquet_path(date_str)
    if not path.exists():
        logger.warning("parquet 없음: %s", path)
        return None
    df = pd.read_parquet(path)
    missing = [col for col in [COL_DATE, COL_CATEGORY, COL_MENU, COL_STORE, COL_QTY, COL_SALES] if col not in df.columns]
    if missing:
        raise ValueError(f"{path.name} 필수 컬럼 없음: {missing}")
    return df


def _load_seed_parquet(date_str: str, original_df: pd.DataFrame) -> pd.DataFrame:
    path = _llm_parquet_path(date_str)
    if not path.exists():
        return original_df
    try:
        seed_df = pd.read_parquet(path)
        missing = [col for col in [COL_DATE, COL_CATEGORY, COL_MENU, COL_STORE, COL_QTY, COL_SALES] if col not in seed_df.columns]
        if missing:
            logger.warning("기존 LLM parquet 필수 컬럼 없음, 원본으로 재생성: %s / %s", path, missing)
            return original_df
        logger.info("기존 LLM parquet 기준으로 기존 AI값 재사용: %s", path)
        return seed_df
    except Exception as exc:
        logger.warning("기존 LLM parquet 로드 실패, 원본으로 재생성: %s / %s", path, exc)
        return original_df


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)


def _prepare_base(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base[COL_QTY] = _numeric(base[COL_QTY])
    base[COL_SALES] = _numeric(base[COL_SALES])
    for col in [COL_DATE, COL_CATEGORY, COL_MENU, COL_STORE]:
        base[col] = base[col].fillna("").astype(str).str.strip()
    return base


def _add_ym(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    parsed = pd.to_datetime(base[COL_DATE], errors="coerce")
    base[COL_YM] = parsed.dt.strftime("%Y-%m").fillna(base[COL_DATE].astype(str).str.slice(0, 7))
    return base


def _load_all_menu_parquets() -> pd.DataFrame:
    paths = sorted(TOORDER_MENU_DB.glob("toorder_menu_detail_*.parquet"))
    if not paths:
        raise FileNotFoundError(f"toorder menu parquet 없음: {TOORDER_MENU_DB}")

    frames = []
    required_cols = [COL_DATE, COL_CATEGORY, COL_MENU, COL_STORE, COL_QTY, COL_SALES]
    for path in paths:
        df = pd.read_parquet(path)
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"{path.name} 필수 컬럼 없음: {missing}")
        frames.append(df[required_cols])
    return pd.concat(frames, ignore_index=True)


def _build_store_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Build store-menu aggregate and comparison metrics."""
    base = _prepare_base(df)
    store = (
        base.groupby([COL_DATE, COL_STORE, COL_MENU, COL_CATEGORY], as_index=False)
        .agg(매장_판매량=(COL_QTY, "sum"), 매장_매출액=(COL_SALES, "sum"))
    )
    menu_avg = (
        store.groupby([COL_DATE, COL_MENU], as_index=False)
        .agg(
            전체매장_평균판매량=("매장_판매량", "mean"),
            전체매장_평균매출액=("매장_매출액", "mean"),
            전체_매장수=(COL_STORE, "nunique"),
        )
    )
    store = store.merge(menu_avg, on=[COL_DATE, COL_MENU], how="left")
    store["매장_판매량순위"] = store.groupby([COL_DATE, COL_MENU])["매장_판매량"].rank(
        ascending=False, method="min"
    ).astype(int)
    store["매장_매출순위"] = store.groupby([COL_DATE, COL_MENU])["매장_매출액"].rank(
        ascending=False, method="min"
    ).astype(int)
    return store


def _build_store_summary_agg(store_df: pd.DataFrame) -> pd.DataFrame:
    """Build one LLM input row per store to avoid menu-row level LLM calls."""
    summary = (
        store_df.groupby([COL_DATE, COL_STORE], as_index=False)
        .agg(
            매장_총판매량=("매장_판매량", "sum"),
            매장_총매출액=("매장_매출액", "sum"),
            판매메뉴수=(COL_MENU, "nunique"),
        )
    )
    top_qty = store_df.sort_values(["매장_판매량", "매장_매출액"], ascending=False).drop_duplicates([COL_DATE, COL_STORE])
    top_sales = store_df.sort_values(["매장_매출액", "매장_판매량"], ascending=False).drop_duplicates([COL_DATE, COL_STORE])
    summary = summary.merge(
        top_qty[[COL_DATE, COL_STORE, COL_MENU, "매장_판매량"]].rename(
            columns={COL_MENU: "최다판매메뉴", "매장_판매량": "최다판매메뉴_판매량"}
        ),
        on=[COL_DATE, COL_STORE],
        how="left",
    )
    summary = summary.merge(
        top_sales[[COL_DATE, COL_STORE, COL_MENU, "매장_매출액"]].rename(
            columns={COL_MENU: "최고매출메뉴", "매장_매출액": "최고매출메뉴_매출액"}
        ),
        on=[COL_DATE, COL_STORE],
        how="left",
    )
    summary["매장_판매량순위"] = summary.groupby(COL_DATE)["매장_총판매량"].rank(
        ascending=False, method="min"
    ).astype(int)
    summary["매장_매출순위"] = summary.groupby(COL_DATE)["매장_총매출액"].rank(
        ascending=False, method="min"
    ).astype(int)
    summary["전체_매장수"] = summary.groupby(COL_DATE)[COL_STORE].transform("nunique")
    return summary


def _build_total_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Build all-store menu aggregate and category metrics."""
    base = _prepare_base(df)
    total = (
        base.groupby([COL_DATE, COL_MENU, COL_CATEGORY], as_index=False)
        .agg(전체_판매량=(COL_QTY, "sum"), 전체_매출액=(COL_SALES, "sum"))
    )
    total["전체_메뉴_판매량순위"] = total.groupby(COL_DATE)["전체_판매량"].rank(
        ascending=False, method="min"
    ).astype(int)
    total["전체_메뉴_매출순위"] = total.groupby(COL_DATE)["전체_매출액"].rank(
        ascending=False, method="min"
    ).astype(int)
    cat_totals = total.groupby([COL_DATE, COL_CATEGORY])[["전체_판매량", "전체_매출액"]].transform("sum")
    total["카테고리_판매량순위"] = total.groupby([COL_DATE, COL_CATEGORY])["전체_판매량"].rank(
        ascending=False, method="min"
    ).astype(int)
    total["카테고리_매출순위"] = total.groupby([COL_DATE, COL_CATEGORY])["전체_매출액"].rank(
        ascending=False, method="min"
    ).astype(int)
    total["카테고리_판매량비중"] = (total["전체_판매량"] / cat_totals["전체_판매량"].replace(0, pd.NA) * 100).fillna(0).round(1)
    total["카테고리_매출비중"] = (total["전체_매출액"] / cat_totals["전체_매출액"].replace(0, pd.NA) * 100).fillna(0).round(1)
    return total


def _build_total_summary_agg(total_df: pd.DataFrame) -> pd.DataFrame:
    """Build one LLM input row per date for all-store summary columns."""
    summary = (
        total_df.groupby(COL_DATE, as_index=False)
        .agg(
            전체_총판매량=("전체_판매량", "sum"),
            전체_총매출액=("전체_매출액", "sum"),
            전체_메뉴수=(COL_MENU, "nunique"),
        )
    )
    top_qty = total_df.sort_values(["전체_판매량", "전체_매출액"], ascending=False).drop_duplicates(COL_DATE)
    top_sales = total_df.sort_values(["전체_매출액", "전체_판매량"], ascending=False).drop_duplicates(COL_DATE)
    summary = summary.merge(
        top_qty[[COL_DATE, COL_MENU, COL_CATEGORY, "전체_판매량"]].rename(
            columns={COL_MENU: "최다판매메뉴", COL_CATEGORY: "최다판매카테고리", "전체_판매량": "최다판매메뉴_판매량"}
        ),
        on=COL_DATE,
        how="left",
    )
    summary = summary.merge(
        top_sales[[COL_DATE, COL_MENU, COL_CATEGORY, "전체_매출액"]].rename(
            columns={COL_MENU: "최고매출메뉴", COL_CATEGORY: "최고매출카테고리", "전체_매출액": "최고매출메뉴_매출액"}
        ),
        on=COL_DATE,
        how="left",
    )
    return summary


def _build_llm_calc_aggs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build menu-option aggregate rows used by dashboard LLM comments."""
    base = _add_ym(_prepare_base(df))
    store = (
        base.groupby([COL_YM, COL_CATEGORY, COL_STORE], as_index=False)
        .agg(**{TOTAL_AMT_COL: (COL_SALES, "sum")})
        .sort_values([COL_YM, COL_CATEGORY, COL_STORE])
        .reset_index(drop=True)
    )
    total = (
        base.groupby([COL_YM, COL_CATEGORY], as_index=False)
        .agg(**{TOTAL_AMT_COL: (COL_SALES, "sum")})
        .sort_values([COL_YM, COL_CATEGORY])
        .reset_index(drop=True)
    )
    store[TOTAL_AMT_COL] = store[TOTAL_AMT_COL].round(0).astype("int64")
    total[TOTAL_AMT_COL] = total[TOTAL_AMT_COL].round(0).astype("int64")
    return store, total


def _save_calc_jsons(store_df: pd.DataFrame, total_df: pd.DataFrame) -> None:
    TOORDER_MENU_LLM_DB.mkdir(parents=True, exist_ok=True)
    store_df.to_json(LLM_STORE_JSON_PATH, orient="records", force_ascii=False, indent=2)
    total_df.to_json(LLM_TOTAL_JSON_PATH, orient="records", force_ascii=False, indent=2)
    logger.info("계산용 LLM JSON 저장: %s", LLM_STORE_JSON_PATH)
    logger.info("계산용 LLM JSON 저장: %s", LLM_TOTAL_JSON_PATH)


def _build_option_store_prompt(row: pd.Series) -> str:
    return f"""너는 메뉴 옵션 대시보드를 보기 전, 운영자가 흐름을 빠르게 파악하도록 돕는 AI 분석가다.

아래 데이터만 근거로 해당 매장의 옵션 카테고리 매출 의미와 확인 포인트를 짧게 정리하라.
대시보드 첫 화면에서 바로 읽히는 자연스러운 조언형 문장으로 작성하라.

[입력 데이터]
- 기준월: {row[COL_YM]}
- 매장명: {row[COL_STORE]}
- 카테고리: {row[COL_CATEGORY]}
- 월간 매장 카테고리 매출액: {int(row[TOTAL_AMT_COL])}원

[출력 형식]
반드시 아래 JSON 객체만 반환하라.
{{"매장_AI현황": "", "매장_AI특징": "", "매장_AI제안": ""}}

[작성 방향]
- 매장_AI현황: 해당 월 이 매장에서 카테고리가 만든 매출 규모를 말한다.
- 매장_AI특징: 카테고리에서 읽히는 수요 포인트를 말한다.
- 매장_AI제안: 사용자가 대시보드에서 바로 볼 비교 관점을 제안한다.

[규칙]
- 각 값은 한국어 1문장, 45자 이내로 작성한다.
- 숫자는 자연스럽게 포함한다.
- 딱딱한 보고서체보다 부드러운 조언형으로 쓴다.
- 입력에 없는 상승, 하락, 시간대, 고객층은 단정하지 않는다.
- 마크다운, 설명, 빈값, "...", "-"는 금지한다.
- 영어, 중국어, 일본어는 쓰지 않는다."""


def _build_option_total_prompt(row: pd.Series) -> str:
    return f"""너는 메뉴 옵션 대시보드를 보기 전, 운영자가 전체 매장 흐름을 빠르게 파악하도록 돕는 AI 분석가다.

아래 데이터만 근거로 해당 옵션 카테고리의 전체 매출 의미와 확인 포인트를 짧게 정리하라.
대시보드 첫 화면에서 바로 읽히는 자연스러운 조언형 문장으로 작성하라.

[입력 데이터]
- 기준월: {row[COL_YM]}
- 카테고리: {row[COL_CATEGORY]}
- 월간 전체 카테고리 매출액: {int(row[TOTAL_AMT_COL])}원

[출력 형식]
반드시 아래 JSON 객체만 반환하라.
{{"전체_AI현황": "", "전체_AI특징": "", "전체_AI제안": ""}}

[작성 방향]
- 전체_AI현황: 해당 월 전체 매장에서 이 카테고리가 만든 매출 규모를 말한다.
- 전체_AI특징: 카테고리에서 읽히는 옵션 성격을 말한다.
- 전체_AI제안: 사용자가 대시보드에서 우선 확인할 비교 관점을 제안한다.

[규칙]
- 각 값은 한국어 1문장, 45자 이내로 작성한다.
- 숫자는 자연스럽게 포함한다.
- 딱딱한 보고서체보다 부드러운 조언형으로 쓴다.
- 특정 매장 기준으로 해석하지 않는다.
- 입력에 없는 상승, 하락, 시간대, 고객층은 단정하지 않는다.
- 마크다운, 설명, 빈값, "...", "-"는 금지한다.
- 영어, 중국어, 일본어는 쓰지 않는다."""


def _build_store_prompt(row: pd.Series) -> str:
    return f"""프랜차이즈 메뉴 판매 데이터를 요약하는 데이터 분석가입니다.

아래 데이터는 특정 수집일자, 특정 매장의 전체 메뉴 판매 성과입니다.

[데이터]
- 수집일자: {row[COL_DATE]}
- 매장명: {row[COL_STORE]}
- 총 판매량: {int(row['매장_총판매량'])}건
- 총 매출액: {int(row['매장_총매출액'])}원
- 판매 메뉴 수: {int(row['판매메뉴수'])}개
- 판매량 순위: {int(row['매장_판매량순위'])}위 / {int(row['전체_매장수'])}개 매장
- 매출 순위: {int(row['매장_매출순위'])}위 / {int(row['전체_매장수'])}개 매장
- 최다 판매 메뉴: {row['최다판매메뉴']} ({int(row['최다판매메뉴_판매량'])}건)
- 최고 매출 메뉴: {row['최고매출메뉴']} ({int(row['최고매출메뉴_매출액'])}원)

아래 JSON 형식으로만 답하세요. 값은 한국어 30자 안팎의 짧은 문장으로 작성하세요.
{{"매장_AI현황": "", "매장_AI특징": "", "매장_AI제안": ""}}

작성 규칙:
- 각 항목은 반드시 1문장으로 작성
- 각 문장은 40자 이내로 짧게 작성
- 반드시 한국어만 사용하고 중국어, 영어 설명 금지
- 숫자를 포함해 구체적으로 작성
- 과장된 표현 금지
- 근거 없는 요일, 시간대, 옵션 내용 금지
- 옵션 데이터는 매장별 구분이 없으므로 특정 매장의 옵션 특징처럼 쓰지 말 것"""


def _build_total_prompt(row: pd.Series) -> str:
    return f"""프랜차이즈 메뉴 판매 데이터를 요약하는 데이터 분석가입니다.

아래 데이터는 특정 수집일자 기준, 전체 매장의 메뉴 판매 요약입니다.

[데이터]
- 수집일자: {row[COL_DATE]}
- 전체 총 판매량: {int(row['전체_총판매량'])}건
- 전체 총 매출액: {int(row['전체_총매출액'])}원
- 전체 메뉴 수: {int(row['전체_메뉴수'])}개
- 최다 판매 메뉴: {row['최다판매메뉴']} ({row['최다판매카테고리']}, {int(row['최다판매메뉴_판매량'])}건)
- 최고 매출 메뉴: {row['최고매출메뉴']} ({row['최고매출카테고리']}, {int(row['최고매출메뉴_매출액'])}원)

아래 JSON 형식으로만 답하세요. 값은 한국어 30자 안팎의 짧은 문장으로 작성하세요.
{{"전체_AI현황": "", "전체_AI특징": "", "전체_AI제안": ""}}

작성 규칙:
- 각 항목은 반드시 1문장으로 작성
- 각 문장은 40자 이내로 짧게 작성
- 반드시 한국어만 사용하고 중국어, 영어 설명 금지
- 전체 매장 합산 기준으로 작성
- 숫자를 포함해 구체적으로 작성
- 과장된 표현 금지
- 특정 매장 기준으로 해석하지 말 것
- 옵션 데이터가 없으므로 옵션 관련 내용은 작성하지 말 것"""


def _query_json(prompt: str, client=None, model_list: Optional[List[str]] = None) -> dict:
    return query_qwen_json(prompt, client=client, model_candidates=model_list)


def _has_disallowed_language(text: str) -> bool:
    return ASCII_ALPHA_RE.search(text) is not None or any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _has_text(value: Any) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if text.lower() in PLACEHOLDER_TEXTS:
        return False
    return not _has_disallowed_language(text)


def _validate_llm_result(result: dict, required_cols: List[str]) -> dict:
    if not isinstance(result, dict):
        raise ValueError("LLM 응답이 dict가 아님")
    if "raw_response" in result or "parse_error" in result:
        raise ValueError(result.get("parse_error") or "LLM JSON 파싱 실패")

    cleaned = {}
    for col in required_cols:
        value = str(result.get(col, "")).strip()
        if not _has_text(value):
            raise ValueError(f"{col} 값이 비었거나 허용되지 않은 언어 포함")
        cleaned[col] = value
    return cleaned


def _fmt_int(value: Any) -> str:
    return f"{int(value):,}"


def _safe_menu_name(value: Any) -> str:
    text = str(value).strip()
    if not text or _has_disallowed_language(text):
        return "상위 메뉴"
    return text[:12] if len(text) > 12 else text


def _fallback_store_result(row: pd.Series) -> dict:
    qty = _fmt_int(row["매장_총판매량"])
    sales_rank = int(row["매장_매출순위"])
    qty_rank = int(row["매장_판매량순위"])
    store_count = int(row["전체_매장수"])
    top_menu = _safe_menu_name(row["최다판매메뉴"])
    return {
        "매장_AI현황": f"총 {qty}건 판매되었습니다.",
        "매장_AI특징": f"판매 {qty_rank}위, 매출 {sales_rank}위입니다.",
        "매장_AI제안": f"{top_menu} 중심 점검이 필요합니다." if top_menu else f"{store_count}개 매장 대비 점검이 필요합니다.",
    }


def _fallback_total_result(row: pd.Series) -> dict:
    qty = _fmt_int(row["전체_총판매량"])
    menu_count = _fmt_int(row["전체_메뉴수"])
    top_menu = _safe_menu_name(row["최다판매메뉴"])
    return {
        "전체_AI현황": f"전체 {qty}건 판매되었습니다.",
        "전체_AI특징": f"{menu_count}개 메뉴가 판매되었습니다.",
        "전체_AI제안": f"{top_menu} 중심 운영을 점검하세요.",
    }


def _fallback_option_store_result(row: pd.Series) -> dict:
    amt = _fmt_int(row[TOTAL_AMT_COL])
    category = str(row[COL_CATEGORY]).strip()[:12] or "해당 카테고리"
    return {
        "매장_AI현황": f"월 매장 매출은 {amt}원입니다.",
        "매장_AI특징": f"{category} 흐름을 볼 구간입니다.",
        "매장_AI제안": "같은 카테고리 매장과 비교하세요.",
    }


def _fallback_option_total_result(row: pd.Series) -> dict:
    amt = _fmt_int(row[TOTAL_AMT_COL])
    category = str(row[COL_CATEGORY]).strip()[:12] or "해당 카테고리"
    return {
        "전체_AI현황": f"월 전체 매출은 {amt}원입니다.",
        "전체_AI특징": f"{category} 흐름을 볼 구간입니다.",
        "전체_AI제안": "매장별 편차를 먼저 확인하세요.",
    }


def _apply_result(df: pd.DataFrame, idx: Any, cols: List[str], result: dict) -> None:
    for col in cols:
        df.at[idx, col] = result[col]


def _run_option_store_llm(store_df: pd.DataFrame, client, model_list: List[str]) -> pd.DataFrame:
    for col in LLM_COLS_STORE:
        if col not in store_df.columns:
            store_df[col] = ""

    total = len(store_df)
    for pos, (idx, row) in enumerate(store_df.iterrows(), start=1):
        logger.info("매장 옵션 LLM 진행 %s/%s: %s / %s", pos, total, row.get(COL_STORE), row.get(COL_CATEGORY))
        try:
            result = _query_json(_build_option_store_prompt(row), client, model_list)
            _apply_result(store_df, idx, LLM_COLS_STORE, _validate_llm_result(result, LLM_COLS_STORE))
        except Exception as exc:
            logger.warning("매장 옵션 LLM 실패, 기본 문장 사용 [%s/%s]: %s", row.get(COL_STORE), row.get(COL_CATEGORY), exc)
            _apply_result(store_df, idx, LLM_COLS_STORE, _fallback_option_store_result(row))
    return store_df


def _run_option_total_llm(total_df: pd.DataFrame, client, model_list: List[str]) -> pd.DataFrame:
    for col in LLM_COLS_TOTAL:
        if col not in total_df.columns:
            total_df[col] = ""

    total = len(total_df)
    for pos, (idx, row) in enumerate(total_df.iterrows(), start=1):
        logger.info("전체 옵션 LLM 진행 %s/%s: %s", pos, total, row.get(COL_CATEGORY))
        try:
            result = _query_json(_build_option_total_prompt(row), client, model_list)
            _apply_result(total_df, idx, LLM_COLS_TOTAL, _validate_llm_result(result, LLM_COLS_TOTAL))
        except Exception as exc:
            logger.warning("전체 옵션 LLM 실패, 기본 문장 사용 [%s]: %s", row.get(COL_CATEGORY), exc)
            _apply_result(total_df, idx, LLM_COLS_TOTAL, _fallback_option_total_result(row))
    return total_df


def _run_store_llm(store_summary_df: pd.DataFrame, client, model_list: List[str]) -> pd.DataFrame:
    for col in LLM_COLS_STORE:
        if col not in store_summary_df.columns:
            store_summary_df[col] = ""

    total = len(store_summary_df)
    for pos, (idx, row) in enumerate(store_summary_df.iterrows(), start=1):
        logger.info("매장 LLM 진행 %s/%s: %s", pos, total, row.get(COL_STORE))
        if all(_has_text(row.get(col, "")) for col in LLM_COLS_STORE):
            logger.info("매장 LLM 기존값 사용 %s/%s: %s", pos, total, row.get(COL_STORE))
            continue
        try:
            result = _query_json(_build_store_prompt(row), client, model_list)
            _apply_result(store_summary_df, idx, LLM_COLS_STORE, _validate_llm_result(result, LLM_COLS_STORE))
        except Exception as exc:
            logger.warning("매장 LLM 실패, 기본 문장 사용 [%s]: %s", row.get(COL_STORE), exc)
            _apply_result(store_summary_df, idx, LLM_COLS_STORE, _fallback_store_result(row))
    return store_summary_df


def _run_total_llm(total_summary_df: pd.DataFrame, client, model_list: List[str]) -> pd.DataFrame:
    for col in LLM_COLS_TOTAL:
        if col not in total_summary_df.columns:
            total_summary_df[col] = ""

    total = len(total_summary_df)
    for pos, (idx, row) in enumerate(total_summary_df.iterrows(), start=1):
        logger.info("전체 LLM 진행 %s/%s: %s", pos, total, row.get(COL_DATE))
        if all(_has_text(row.get(col, "")) for col in LLM_COLS_TOTAL):
            logger.info("전체 LLM 기존값 사용 %s/%s: %s", pos, total, row.get(COL_DATE))
            continue
        try:
            result = _query_json(_build_total_prompt(row), client, model_list)
            _apply_result(total_summary_df, idx, LLM_COLS_TOTAL, _validate_llm_result(result, LLM_COLS_TOTAL))
        except Exception as exc:
            logger.warning("전체 LLM 실패, 기본 문장 사용 [%s]: %s", row.get(COL_DATE), exc)
            _apply_result(total_summary_df, idx, LLM_COLS_TOTAL, _fallback_total_result(row))
    return total_summary_df


def _fill_missing_with_fallback(store_summary_df: pd.DataFrame, total_summary_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    for idx, row in store_summary_df.iterrows():
        if not all(_has_text(row.get(col, "")) for col in LLM_COLS_STORE):
            _apply_result(store_summary_df, idx, LLM_COLS_STORE, _fallback_store_result(row))

    for idx, row in total_summary_df.iterrows():
        if not all(_has_text(row.get(col, "")) for col in LLM_COLS_TOTAL):
            _apply_result(total_summary_df, idx, LLM_COLS_TOTAL, _fallback_total_result(row))

    return store_summary_df, total_summary_df


def _seed_existing_llm(df: pd.DataFrame, store_summary_df: pd.DataFrame, total_summary_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    store_existing = [col for col in LLM_COLS_STORE if col in df.columns]
    if store_existing:
        keys = [COL_DATE, COL_STORE]
        existing = df[keys + store_existing].drop_duplicates(subset=keys)
        store_summary_df = store_summary_df.merge(existing, on=keys, how="left")

    total_existing = [col for col in LLM_COLS_TOTAL if col in df.columns]
    if total_existing:
        keys = [COL_DATE]
        existing = df[keys + total_existing].drop_duplicates(subset=keys)
        total_summary_df = total_summary_df.merge(existing, on=keys, how="left")

    return store_summary_df, total_summary_df


def _save_enriched_parquet(df: pd.DataFrame, store_summary_df: pd.DataFrame, total_summary_df: pd.DataFrame, date_str: str) -> None:
    """Merge LLM columns into a mart parquet while preserving the source parquet."""
    path = _llm_parquet_path(date_str)
    drop_cols = [col for col in LLM_COLS + DEPRECATED_LLM_COLS if col in df.columns]
    base = _add_ym(df.drop(columns=drop_cols).copy())

    store_keys = [COL_YM, COL_CATEGORY, COL_STORE]
    enriched = base.merge(store_summary_df[store_keys + LLM_COLS_STORE], on=store_keys, how="left")

    total_keys = [COL_YM, COL_CATEGORY]
    enriched = enriched.merge(total_summary_df[total_keys + LLM_COLS_TOTAL], on=total_keys, how="left")
    enriched = enriched.drop(columns=[COL_YM])

    TOORDER_MENU_LLM_DB.mkdir(parents=True, exist_ok=True)
    enriched.to_parquet(path, index=False)
    logger.info("enriched parquet 저장: %s", path)


def _to_markdown(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        headers = list(df.columns)
        rows = [headers] + df.astype(str).values.tolist()
        widths = [max(len(str(row[i])) for row in rows) for i in range(len(headers))]
        lines = [
            "| " + " | ".join(str(headers[i]).ljust(widths[i]) for i in range(len(headers))) + " |",
            "| " + " | ".join("-" * widths[i] for i in range(len(headers))) + " |",
        ]
        for row in rows[1:]:
            lines.append("| " + " | ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))) + " |")
        return "\n".join(lines)


def _save_llm_log(store_df: pd.DataFrame, total_df: pd.DataFrame, date_str: str) -> None:
    today_str = datetime.now().strftime("%Y-%m-%d")
    header = f"\n## {date_str} (처리일: {today_str})\n"

    store_cols = [COL_STORE, "매장_총판매량", "매장_총매출액", "판매메뉴수"] + LLM_COLS_STORE
    total_cols = ["전체_총판매량", "전체_총매출액", "전체_메뉴수"] + LLM_COLS_TOTAL
    store_md = _to_markdown(store_df[store_cols])
    total_md = _to_markdown(total_df[total_cols])
    content = f"{header}\n### 매장별\n{store_md}\n\n### 전체\n{total_md}\n"

    TOORDER_MENU_LLM_DB.mkdir(parents=True, exist_ok=True)
    with open(LLM_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(content)
    logger.info("llm_log.md append 완료: %s", date_str)


def _build_seeded_aggs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    store_df = _build_store_agg(df)
    store_summary_df = _build_store_summary_agg(store_df)
    total_df = _build_total_agg(df)
    total_summary_df = _build_total_summary_agg(total_df)
    return _seed_existing_llm(df, store_summary_df, total_summary_df)


def run_toorder_llm(conf: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> str:
    """Enrich target ToOrder menu parquet files with LLM summary columns."""
    target_dates = get_target_dates(conf, context=context)
    client, model_list = get_ollama_client_with_candidates()
    calc_source_df = _load_all_menu_parquets()
    store_df, total_df = _build_llm_calc_aggs(calc_source_df)
    _save_calc_jsons(store_df, total_df)
    store_df = _run_option_store_llm(store_df, client, model_list)
    total_df = _run_option_total_llm(total_df, client, model_list)

    results = []
    for date_str in target_dates:
        df = _load_parquet(date_str)
        if df is None:
            results.append(f"{date_str}: parquet 없음 -> skip")
            continue

        _save_enriched_parquet(df, store_df, total_df, date_str)
        results.append(f"{date_str}: store {len(store_df)}행, total {len(total_df)}행 처리")

    return "\n".join(results)


def save_llm_log_yesterday(conf: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> str:
    """Append target dates' enriched LLM data to llm_log.md."""
    results = []
    for target_date in get_target_dates(conf, context=context):
        output_path = _llm_parquet_path(target_date)
        if output_path.exists():
            df = pd.read_parquet(output_path)
        else:
            df = _load_parquet(target_date)
        if df is None:
            results.append(f"llm_log skip: {target_date} parquet 없음")
            continue

        store_df, total_df = _build_seeded_aggs(df)
        store_df, total_df = _fill_missing_with_fallback(store_df, total_df)
        _save_llm_log(store_df, total_df, target_date)
        results.append(f"llm_log.md 저장 완료: {target_date}")
    return "\n".join(results)
