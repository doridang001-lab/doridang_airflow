"""
매장별 매출 달성 분석 파이프라인

daily_actuals.csv + target.csv → left-join → 지표 계산 → LLM 진단 → sales_analysis.csv
- A/B 구간: 규칙 기반 진단
- C/D 구간: Ollama gpt-oss:20b 진단 (실패 시 규칙 fallback)
"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path

from modules.transform.utility.mailer import send_email, text_to_html
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM

import numpy as np
import ollama
import pandas as pd

from modules.transform.utility.paths import (
    STORE_SALES_ANALYSIS_CSV,
    STORE_SALES_DAILY_ACTUALS_CSV,
    STORE_SALES_TARGET_CSV,
    STORE_SALES_TARGET_DIR,
)

logger = logging.getLogger(__name__)

OLLAMA_HOST = "http://host.docker.internal:11434"
MODEL = "gpt-oss:20b"
FALLBACK_MODEL = "qwen2.5:7b"
LOCAL_FALLBACK_WRITE_DIR = Path.cwd() / "tmp" / "store_sales_target"

JOIN_KEY_COLS = ["기준월", "매장명", "브랜드"]
FINAL_OUTPUT_COLUMNS = [
    "매출일자",
    "매장명",
    "요일구분",
    "목표매출",
    "전체매출목표",
    "실제매출",
    "매출차이",
    "달성률",
    "달성구간",
    "목표테이블단가",
    "실제테이블단가",
    "단가차이",
    "테이블수",
    "테이블주문수",
    "포장매출",
    "포장비율",
    "진단유형",
    "기준월",
    "브랜드",
    "홀_월목표매출",
    "홀_테이블_월목표매출",
    "홀_포장_월목표매출",
    "배달_월목표매출",
    "전체_월목표매출",
    "평일_일목표매출",
    "주말_일목표매출",
]
FINAL_OUTPUT_SOURCE_COLUMNS = {
    "매출일자": "매출일자",
    "매장명": "매장명",
    "요일구분": "요일구분",
    "목표매출": "목표매출",
    "전체매출목표": "전체매출목표",
    "실제매출": "실제매출",
    "매출차이": "매출차이",
    "달성률": "달성률",
    "달성구간": "달성구간",
    "목표테이블단가": "목표테이블단가",
    "실제테이블단가": "실제테이블단가",
    "단가차이": "단가차이",
    "테이블수": "테이블수",
    "테이블주문수": "테이블주문수",
    "포장매출": "포장매출",
    "포장비율": "포장비율",
    "진단유형": "진단유형",
    "기준월": "기준월",
    "브랜드": "브랜드",
    "홀_월목표매출": "홀_월목표매출",
    "홀_테이블_월목표매출": "홀_테이블_월목표매출",
    "홀_포장_월목표매출": "홀_포장_월목표매출",
    "배달_월목표매출": "배달_월목표매출",
    "전체_월목표매출": "전체_월목표매출",
    "평일_일목표매출": "평일_일목표매출",
    "주말_일목표매출": "주말_일목표매출",
}

_MONTH_MAP = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def _pick_existing_analysis_csv():
    """
    Prefer the canonical `sales_analysis.csv`, but fall back to the newest
    `sales_analysis_*.csv` when the canonical file isn't usable (e.g. Windows
    bind-mount file locks/ACL causing PermissionError).
    """
    try:
        if STORE_SALES_ANALYSIS_CSV.exists():
            return STORE_SALES_ANALYSIS_CSV
        candidates = []
        candidates.extend(STORE_SALES_TARGET_DIR.glob("sales_analysis_*.csv"))
        if not candidates:
            return STORE_SALES_ANALYSIS_CSV
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    except Exception:
        return STORE_SALES_ANALYSIS_CSV


def _write_sales_analysis_csv(df: "pd.DataFrame", **context):
    """
    Write combined analysis safely.

    1) Write to a temp file in the same directory
    2) Try to atomically replace `sales_analysis.csv`
    3) On PermissionError, keep a versioned `sales_analysis_YYYYmmdd_HHMMSS.csv`
       and continue without failing the task.
    """
    STORE_SALES_TARGET_DIR.mkdir(parents=True, exist_ok=True)

    logical = context.get("logical_date") or context.get("execution_date")
    if logical is None:
        run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    else:
        try:
            run_tag = logical.strftime("%Y%m%d_%H%M%S")
        except Exception:
            run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    tmp_path = STORE_SALES_TARGET_DIR / f".sales_analysis.{run_tag}.{os.getpid()}.tmp.csv"
    versioned = STORE_SALES_TARGET_DIR / f"sales_analysis_{run_tag}.csv"
    try:
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    except PermissionError as e:
        try:
            df.to_csv(versioned, index=False, encoding="utf-8-sig")
            logger.warning(
                "PermissionError creating temp file in %s; wrote %s instead (%s)",
                STORE_SALES_TARGET_DIR,
                versioned,
                e,
            )
            return versioned
        except PermissionError:
            LOCAL_FALLBACK_WRITE_DIR.mkdir(parents=True, exist_ok=True)
            local_versioned = LOCAL_FALLBACK_WRITE_DIR / f"sales_analysis_{run_tag}.csv"
            df.to_csv(local_versioned, index=False, encoding="utf-8-sig")
            logger.warning(
                "PermissionError writing %s; wrote %s instead",
                STORE_SALES_TARGET_DIR,
                local_versioned,
            )
            return local_versioned

    try:
        os.replace(tmp_path, STORE_SALES_ANALYSIS_CSV)
        return STORE_SALES_ANALYSIS_CSV
    except PermissionError as e:
        try:
            os.replace(tmp_path, versioned)
        except Exception:
            try:
                # If replace fails (weird mount behavior), fall back to a fresh write.
                df.to_csv(versioned, index=False, encoding="utf-8-sig")
            except PermissionError:
                LOCAL_FALLBACK_WRITE_DIR.mkdir(parents=True, exist_ok=True)
                versioned = LOCAL_FALLBACK_WRITE_DIR / f"sales_analysis_{run_tag}.csv"
                df.to_csv(versioned, index=False, encoding="utf-8-sig")
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
        logger.warning(
            "PermissionError updating %s; wrote %s instead (%s)",
            STORE_SALES_ANALYSIS_CSV,
            versioned,
            e,
        )
        return versioned
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _parse_target_ym(val: str) -> str:
    """target.csv 기준월 → YYYY-MM 정규화 (01-May / 26-Apr / 2026-05 등 다양한 포맷 처리)"""
    val = str(val).strip()
    current_year = datetime.now().year
    # "2026-05" 형식
    try:
        dt = datetime.strptime(val, "%Y-%m")
        return f"{dt.year}-{dt.month:02d}"
    except ValueError:
        pass
    # "26-Apr" 형식 (YY-MMM) — 현재 연도 ±5 범위일 때만 연도로 해석
    try:
        dt = datetime.strptime(val, "%y-%b")
        if abs(dt.year - current_year) <= 5:
            return f"{dt.year}-{dt.month:02d}"
    except ValueError:
        pass
    # "01-May" 형식 (DD-MMM) — 연도는 현재 연도로 고정
    try:
        dt = datetime.strptime(val, "%d-%b")
        return f"{current_year}-{dt.month:02d}"
    except ValueError:
        pass
    return val


def _parse_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """콤마 포함 숫자 문자열 → float 변환"""
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _normalize_text_value(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalize_key_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].map(_normalize_text_value)
    return df


def _drop_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.loc[:, ~df.columns.duplicated(keep="first")]


def _prepare_existing_analysis(existing: pd.DataFrame, target: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if existing.empty:
        return existing, False

    existing = existing.copy()
    original_columns = list(existing.columns)
    existing.columns = existing.columns.map(lambda c: c.strip() if isinstance(c, str) else c)
    stripped_columns = list(existing.columns)
    existing = _drop_duplicate_columns(existing)
    deduped_columns = list(existing.columns)
    drop_cols: list[str] = []

    migrated_legacy = (
        original_columns != stripped_columns
        or stripped_columns != deduped_columns
        or bool(drop_cols)
    )
    if "테이블주문수" not in existing.columns and "테이블수" in existing.columns:
        store_table_count = (
            target[["매장명", "테이블수"]]
            .dropna(subset=["매장명"])
            .drop_duplicates(subset=["매장명"], keep="last")
            .set_index("매장명")["테이블수"]
            .to_dict()
        )
        existing["테이블주문수"] = existing["테이블수"]
        existing["테이블수"] = existing["매장명"].map(store_table_count).fillna(existing["테이블수"])
        migrated_legacy = True

    if "홀_월목표매출" not in existing.columns:
        hall_month_map = (
            target[["target_ym", "매장명", "브랜드", "홀_월목표매출"]]
            .dropna(subset=["target_ym", "매장명", "브랜드"])
            .drop_duplicates(subset=["target_ym", "매장명", "브랜드"], keep="last")
            .set_index(["target_ym", "매장명", "브랜드"])["홀_월목표매출"]
            .to_dict()
        )
        if "매출일자" in existing.columns:
            existing["__target_ym"] = pd.to_datetime(existing["매출일자"], errors="coerce").dt.strftime("%Y-%m")
            brand_series = existing["브랜드"] if "브랜드" in existing.columns else pd.Series([None] * len(existing))
            existing["홀_월목표매출"] = [
                hall_month_map.get((ym, store, brand))
                for ym, store, brand in zip(existing["__target_ym"], existing.get("매장명"), brand_series)
            ]
            existing = existing.drop(columns=["__target_ym"])
        else:
            existing["홀_월목표매출"] = None

    existing = _normalize_key_columns(existing, ["매장명"])
    finalized = _finalize_output(existing)
    migrated_legacy = migrated_legacy or list(finalized.columns) != original_columns
    return finalized, migrated_legacy


def _finalize_output(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)
    df = df.copy()
    df.columns = df.columns.map(lambda c: c.strip() if isinstance(c, str) else c)
    df = _drop_duplicate_columns(df)
    if "전체매출목표" not in df.columns and "전체_월목표매출" in df.columns:
        df["전체매출목표"] = df["전체_월목표매출"]

    out = pd.DataFrame(index=df.index)
    for display_col in FINAL_OUTPUT_COLUMNS:
        source_col = FINAL_OUTPUT_SOURCE_COLUMNS[display_col]
        out[display_col] = df[source_col] if source_col in df.columns else None

    if {"매출일자", "매장명"}.issubset(out.columns):
        score = (
            out["달성구간"].fillna("").ne("목표없음").astype(int) * 10
            + out["달성률"].notna().astype(int) * 5
            + out["진단유형"].fillna("").ne("목표없음").astype(int)
        )
        out["__row_score"] = score
        out = (
            out.sort_values(["매출일자", "매장명", "__row_score"], ascending=[True, True, False])
            .drop_duplicates(subset=["매출일자", "매장명"], keep="first")
            .drop(columns="__row_score")
            .reset_index(drop=True)
        )

    return out


def _log_excluded_rows(todo: pd.DataFrame, target_store_month_keys: set[tuple[str, str]], target_keys: set[tuple[str, str, str]]) -> None:
    if todo.empty:
        return

    store_month_key = list(zip(todo["target_ym"], todo["매장명"]))
    full_key = list(zip(todo["target_ym"], todo["매장명"], todo["브랜드"]))
    same_store_month = todo[[key in target_store_month_keys for key in store_month_key]].copy()
    mismatched_brand = same_store_month[[key not in target_keys for key in zip(same_store_month["target_ym"], same_store_month["매장명"], same_store_month["브랜드"])]]
    if not mismatched_brand.empty:
        samples = mismatched_brand[["매출일자", "target_ym", "매장명", "브랜드"]].drop_duplicates().head(10)
        logger.warning(
            "Excluded %d rows due to target key mismatch after store/month match: %s",
            len(mismatched_brand),
            samples.to_dict("records"),
        )

    unmatched = todo[[key not in target_keys for key in full_key]].copy()
    unmatched = unmatched[[key not in target_store_month_keys for key in zip(unmatched["target_ym"], unmatched["매장명"])]]
    if not unmatched.empty:
        samples = unmatched[["매출일자", "target_ym", "매장명", "브랜드"]].drop_duplicates().head(10)
        logger.info(
            "Excluded %d rows without target key definition: %s",
            len(unmatched),
            samples.to_dict("records"),
        )


def _find_missing_monthly_targets(merged: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["기준월", "매장명", "브랜드", "평일_일목표매출", "주말_일목표매출"]
    if any(col not in merged.columns for col in required_cols):
        return pd.DataFrame(columns=["기준월", "매장명", "브랜드"])

    missing_mask = merged["평일_일목표매출"].isna() | merged["주말_일목표매출"].isna()
    if not missing_mask.any():
        return pd.DataFrame(columns=["기준월", "매장명", "브랜드"])

    return (
        merged.loc[missing_mask, ["기준월", "매장명", "브랜드"]]
        .dropna(subset=["기준월", "매장명"])
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _backfill_existing_output_columns(existing: pd.DataFrame, daily: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    if existing.empty or daily.empty or target.empty:
        return existing

    daily_exact = daily.copy()
    daily_exact["target_ym"] = daily_exact["기준월"].apply(_parse_target_ym)
    daily_exact = daily_exact.merge(
        target[
            [
                "target_ym",
                "매장명",
                "브랜드",
                "평일_일목표매출",
                "주말_일목표매출",
                "홀_테이블_월목표매출",
                "홀_포장_월목표매출",
                "배달_월목표매출",
                "전체_월목표매출",
                "홀_월목표매출",
            ]
        ],
        on=["target_ym", "매장명", "브랜드"],
        how="left",
    )
    daily_exact = daily_exact[daily_exact["평일_일목표매출"].notna() | daily_exact["주말_일목표매출"].notna()].copy()
    if daily_exact.empty:
        return existing

    daily_exact["목표매출"] = np.where(
        daily_exact["요일구분"] == "평일",
        daily_exact["평일_일목표매출"],
        daily_exact["주말_일목표매출"],
    )
    daily_exact["전체매출목표"] = daily_exact["전체_월목표매출"]
    daily_exact["실제매출"] = daily_exact["총매출"]

    backfill = (
        daily_exact[
            [
                "매출일자",
                "매장명",
                "기준월",
                "브랜드",
                "목표매출",
                "전체매출목표",
                "실제매출",
                "홀_월목표매출",
                "홀_테이블_월목표매출",
                "홀_포장_월목표매출",
                "배달_월목표매출",
                "전체_월목표매출",
                "평일_일목표매출",
                "주말_일목표매출",
            ]
        ]
        .drop_duplicates(subset=["매출일자", "매장명"], keep="first")
    )

    merged = existing.merge(backfill, on=["매출일자", "매장명"], how="left", suffixes=("", "__backfill"))
    for col in [
        "기준월",
        "브랜드",
        "목표매출",
        "전체매출목표",
        "실제매출",
        "홀_월목표매출",
        "홀_테이블_월목표매출",
        "홀_포장_월목표매출",
        "배달_월목표매출",
        "전체_월목표매출",
        "평일_일목표매출",
        "주말_일목표매출",
    ]:
        backfill_col = f"{col}__backfill"
        if col not in merged.columns:
            merged[col] = merged[backfill_col]
        else:
            merged[col] = merged[col].where(merged[col].notna(), merged[backfill_col])
        merged = merged.drop(columns=[backfill_col])
    return merged


def _grade(rate: float) -> str:
    if rate >= 100:
        return "A"
    if rate >= 97:
        return "B"
    if rate >= 93:
        return "C"
    return "D"


def _rule_based_diagnosis(row: dict) -> str:
    """LLM fallback — 규칙 기반 진단"""
    grade = row.get("달성구간", "D")
    if grade == "A":
        return "목표초과"
    if grade == "B":
        return "목표근접"

    # C/D 2차 원인 분해
    목표단가 = float(row.get("목표테이블단가", 0) or 0)
    실제단가 = float(row.get("실제테이블단가", 0) or 0)
    # '테이블수'는 target.csv의 매장 고정 테이블 수(예: 9),
    # '테이블주문수'는 daily_actuals의 당일 홀_테이블 주문 수(= 방문/회전 지표)
    테이블주문수 = float(row.get("테이블주문수", row.get("테이블수", 0)) or 0)
    포장비율 = float(row.get("포장비율", 0) or 0)

    단가_ratio = (실제단가 / 목표단가) if 목표단가 > 0 else 1.0

    if 단가_ratio < 0.9 and 테이블주문수 < 20:
        return "유입과 구매단가 복합 문제"
    if 단가_ratio < 0.9:
        return "객단가/메뉴구성 문제"
    if 포장비율 < 8:
        return "포장유입 부족"
    return "테이블수 부족"


def _diagnose_llm(row: dict) -> str:
    """C/D 구간 행에 대해 LLM 진단 (실패 시 규칙 fallback)"""
    try:
        목표매출 = int(row.get("목표매출", 0) or 0)
        실제매출 = int(row.get("실제매출", 0) or 0)
        매출차이 = int(row.get("매출차이", 0) or 0)
        테이블주문수 = int(row.get("테이블주문수", row.get("테이블수", 0)) or 0)
        목표단가 = int(row.get("목표테이블단가", 0) or 0)
        실제단가 = int(row.get("실제테이블단가", 0) or 0)
        포장매출 = int(row.get("포장매출", 0) or 0)
        포장비율 = float(row.get("포장비율", 0) or 0)
        달성률 = float(row.get("달성률", 0) or 0)

        prompt = (
            f"매장: {row.get('매장명', '')} | 날짜: {row.get('매출일자', '')} ({row.get('요일구분', '')})\n"
            f"달성률: {달성률:.1f}% ({row.get('달성구간', '')}구간)\n"
            f"목표: {목표매출:,}원 / 실제: {실제매출:,}원 / 차이: {매출차이:,}원\n"
            f"테이블주문수: {테이블주문수} / 목표단가: {목표단가:,}원 / 실제단가: {실제단가:,}원\n"
            f"포장매출: {포장매출:,}원 (포장비율: {포장비율:.1f}%)\n\n"
            "매출 부진의 주요 원인을 10자 이내 한 줄로 진단하세요.\n"
            "예시: '테이블수 부족' / '객단가 하락' / '포장유입 부족' / '유입+단가 복합'\n"
            "진단:"
        )
        client = ollama.Client(host=OLLAMA_HOST)
        for model in [MODEL, FALLBACK_MODEL]:
            try:
                resp = client.chat(
                    model=model,
                    options={"temperature": 0},
                    messages=[
                        {"role": "system", "content": "당신은 F&B 매장 매출 성과를 진단하는 전문가입니다."},
                        {"role": "user", "content": prompt},
                    ],
                )
                text = (resp.message.content or "").strip().split("\n")[0]
                # 불필요한 prefix 제거 (예: "진단: 테이블수 부족")
                text = re.sub(r"^진단\s*[:：]\s*", "", text).strip()
                return text[:30] if text else _rule_based_diagnosis(row)
            except Exception as e:
                logger.warning("LLM 실패 (%s): %s", model, e)
    except Exception as e:
        logger.warning("진단 준비 실패: %s", e)

    return _rule_based_diagnosis(row)


def run_analysis(**context) -> str:
    """daily_actuals + target → 분석 테이블 생성 (증분 append)"""
    if not STORE_SALES_DAILY_ACTUALS_CSV.exists():
        logger.warning("daily_actuals.csv 없음 — daily_actuals task 먼저 실행 필요")
        return "SKIP: daily_actuals.csv 없음"

    if not STORE_SALES_TARGET_CSV.exists():
        logger.warning("target.csv 없음: %s", STORE_SALES_TARGET_CSV)
        return "SKIP: target.csv 없음"

    # target 로드 및 숫자 파싱 (legacy 스키마 마이그레이션에도 사용)
    target = pd.read_csv(STORE_SALES_TARGET_CSV, encoding="utf-8-sig", dtype=str)
    target.columns = target.columns.map(lambda c: c.strip() if isinstance(c, str) else c)
    target = _normalize_key_columns(target, JOIN_KEY_COLS)
    target = _parse_numeric_cols(
        target,
        ["홀_월목표매출", "홀_테이블_월목표매출", "홀_포장_월목표매출",
         "배달_월목표매출", "전체_월목표매출",
         "평일_일목표매출", "주말_일목표매출", "목표_테이블단가", "테이블수"],
    )
    target["target_ym"] = target["기준월"].apply(_parse_target_ym)

    # 기존 분석 파일에서 이미 처리된 날짜 파악
    existing_path = _pick_existing_analysis_csv()
    if existing_path.exists():
        existing_raw = pd.read_csv(existing_path, encoding="utf-8-sig", dtype=str)
        existing, migrated_legacy = _prepare_existing_analysis(existing_raw, target)
        done_dates: set[str] = set(existing["매출일자"].dropna().astype(str))
    else:
        existing = pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)
        migrated_legacy = False
        done_dates = set()

    # 일별 실적 로드
    daily = pd.read_csv(STORE_SALES_DAILY_ACTUALS_CSV, encoding="utf-8-sig", dtype=str)
    daily.columns = daily.columns.map(lambda c: c.strip() if isinstance(c, str) else c)
    daily = _normalize_key_columns(daily, JOIN_KEY_COLS)
    daily = _parse_numeric_cols(
        daily, ["홀매출", "홀_테이블_매출", "홀_포장_매출", "배달매출", "총매출", "테이블수", "테이블단가"]
    )
    # daily_actuals의 '테이블수'는 당일 홀_테이블 주문 수(= 방문/회전 지표)로 사용하고,
    # report에서는 target.csv의 '테이블수'(매장 고정 테이블 수)를 보여주기 위해 컬럼명을 분리한다.
    if "테이블수" in daily.columns and "테이블주문수" not in daily.columns:
        daily = daily.rename(columns={"테이블수": "테이블주문수"})

    existing = _backfill_existing_output_columns(existing, daily, target)

    # 미처리 날짜만 선택
    todo = daily[~daily["매출일자"].astype(str).isin(done_dates)].copy()
    if todo.empty:
        if migrated_legacy:
            _write_sales_analysis_csv(existing, **context)
            return "SKIP: 모든 날짜 이미 분석 완료 (legacy 스키마 마이그레이션 적용)"
        return "SKIP: 모든 날짜 이미 분석 완료"

    todo["target_ym"] = todo["기준월"].apply(_parse_target_ym)

    target_keys = set(
        target[["target_ym", "매장명", "브랜드"]].dropna().itertuples(index=False, name=None)
    )
    target_store_month_keys = set(
        target[["target_ym", "매장명"]].dropna().itertuples(index=False, name=None)
    )
    _log_excluded_rows(todo, target_store_month_keys, target_keys)
    todo = todo[
        [key in target_keys for key in zip(todo["target_ym"], todo["매장명"], todo["브랜드"])]
    ].copy()
    if todo.empty:
        logger.info("target 키와 매칭되는 미처리 실적 없음")
        if migrated_legacy:
            _write_sales_analysis_csv(existing, **context)
            return "SKIP: target 키 매칭 없음 (legacy 스키마 마이그레이션 적용)"
        return "SKIP: target 키 매칭 없음"

    # left join
    merged = todo.merge(
        target,
        on=["target_ym", "매장명", "브랜드"],
        how="left",
    ).drop(columns=["target_ym"])

    missing_targets = _find_missing_monthly_targets(merged)
    if not missing_targets.empty:
        logger.warning(
            "Unexpected target join misses after exact-key filtering: %s",
            missing_targets.to_dict("records"),
        )

    # 요일별 목표매출 선택
    merged["목표매출"] = np.where(
        merged["요일구분"] == "평일",
        merged["평일_일목표매출"],
        merged["주말_일목표매출"],
    )
    # 홀_월목표매출: target.csv의 월 고정 목표값 (보고서 참고용)
    # (join 결과에 이미 존재하는 컬럼을 그대로 노출)

    # 지표 계산
    merged["실제매출"] = merged["총매출"]
    merged["매출차이"] = merged["실제매출"] - merged["목표매출"]
    merged["달성률"] = np.where(
        merged["목표매출"] > 0,
        (merged["실제매출"] / merged["목표매출"] * 100).round(1),
        np.nan,
    )
    merged["달성구간"] = merged["달성률"].apply(
        lambda x: _grade(x) if pd.notna(x) else "목표없음"
    )
    merged["목표테이블단가"] = merged["목표_테이블단가"]
    merged["실제테이블단가"] = merged["테이블단가"]
    merged["단가차이"] = merged["실제테이블단가"] - merged["목표테이블단가"]
    # 포장매출: daily_actuals의 홀_포장_매출 참조 (분석 출력용 컬럼명은 포장매출로 통일)
    merged["포장매출"] = merged["홀_포장_매출"] if "홀_포장_매출" in merged.columns else 0
    merged["포장비율"] = np.where(
        merged["실제매출"] > 0,
        (merged["포장매출"] / merged["실제매출"] * 100).round(1),
        0,
    )

    # 진단유형
    diagnoses = []
    for _, row in merged.iterrows():
        grade = row.get("달성구간", "목표없음")
        if grade == "A":
            diagnoses.append("목표초과")
        elif grade == "B":
            diagnoses.append("목표근접")
        elif grade in ("C", "D"):
            diagnoses.append(_diagnose_llm(row.to_dict()))
        else:
            diagnoses.append("목표없음")
    merged["진단유형"] = diagnoses

    # 최종 컬럼 정리
    result = _finalize_output(merged)

    combined = _finalize_output(pd.concat([existing, result], ignore_index=True))
    written_path = _write_sales_analysis_csv(combined, **context)

    llm_count = result[result["달성구간"].isin(["C", "D"])].shape[0]
    logger.info(
        "sales_analysis.csv 저장: 전체 %d행 / 신규 %d행 (LLM %d건) -> %s",
        len(combined),
        len(result),
        llm_count,
        written_path,
    )

    if not missing_targets.empty:
        _alert_no_target(missing_targets, **context)

    return f"OK: {len(result)}행 분석 완료 (LLM {llm_count}건)"


def _alert_no_target(rows: "pd.DataFrame", **context) -> None:
    """월 목표 미설정 매장/브랜드 이메일 알림"""
    base_cols = ["기준월", "매장명"]
    if "브랜드" in rows.columns:
        base_cols.append("브랜드")
    items = rows[base_cols].drop_duplicates().to_dict("records")
    lines = "\n".join(
        f"  - {r['매장명']} / {r['기준월']}" + (f" / {r['브랜드']}" if r.get("브랜드") else "")
        for r in items
    )
    body = text_to_html(
        f"[도리당] 월 목표 미설정 매장 알림\n\n"
        f"아래 매장/기준월의 목표 데이터가 target.csv에 없습니다.\n"
        f"일별 실적이 아니라 기준월 단위로 target.csv를 확인해주세요.\n\n"
        f"{lines}\n\n"
        f"파일 경로: analytics/store_sales_target/target.csv"
    )
    try:
        send_email(
            subject="[도리당] 월 목표 미설정 매장 알림",
            html_content=body,
            to_emails=MAIL_CMJ_PM,
            **context,
        )
        logger.info("월 목표 미설정 알림 이메일 발송: %d건", len(items))
    except Exception as e:
        logger.warning("이메일 발송 실패 (Airflow 외부 실행 시 정상): %s", e)
