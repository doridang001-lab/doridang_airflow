"""
매장별 일별 실적 집계 파이프라인

unified_sales parquet → daily_actuals.csv (append, 중복 날짜 skip)

최초 실행 시 전체 parquet 처리 (backfill), 이후엔 신규 날짜만 append
"""

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from modules.transform.utility.paths import (
    MART_DB,
    STORE_SALES_DAILY_ACTUALS_CSV,
    STORE_SALES_TARGET_DIR,
)

logger = logging.getLogger(__name__)

UNIFIED_SALES_DIR = MART_DB / "unified_sales_grp"

_MONTH_MAP = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def _weekday_label(date_str: str) -> str:
    dt = datetime.strptime(str(date_str), "%Y-%m-%d")
    return "주말" if dt.weekday() >= 5 else "평일"


def _aggregate_day(df: pd.DataFrame) -> pd.DataFrame:
    """unified_sales DataFrame → 일별 실적 집계"""
    table_mask = df["order_type"] == "홀_테이블"
    # 포장: 홀_포장(신규) 또는 홀_카운터(구형) 모두 처리
    counter_mask = df["order_type"].isin(["홀_포장", "홀_카운터"])
    hall_mask = table_mask | counter_mask
    delivery_mask = ~df["order_type"].str.contains("홀", na=False)

    keys = ["sale_date", "ym", "store", "brand"]

    agg = df.groupby(keys, as_index=False)["total_price"].sum().rename(
        columns={"total_price": "총매출"}
    )

    for mask, col in [
        (hall_mask, "홀매출"),
        (table_mask, "홀_테이블_매출"),   # target.csv 컬럼명 기준 통일
        (counter_mask, "홀_포장_매출"),   # target.csv 컬럼명 기준 통일
        (delivery_mask, "배달매출"),
    ]:
        sub = (
            df[mask]
            .groupby(keys, as_index=False)["total_price"]
            .sum()
            .rename(columns={"total_price": col})
        )
        agg = agg.merge(sub, on=keys, how="left")

    # 테이블수: 홀_테이블 주문 고유 order_id 수
    tbl = (
        df[table_mask]
        .groupby(keys, as_index=False)["order_id"]
        .nunique()
        .rename(columns={"order_id": "테이블수"})
    )
    agg = agg.merge(tbl, on=keys, how="left")
    agg = agg.fillna(0)

    # 테이블단가 (ZeroDivision 방지)
    agg["테이블단가"] = np.where(
        agg["테이블수"] > 0, (agg["총매출"] / agg["테이블수"]).round(0), 0
    ).astype(int)

    agg["요일구분"] = agg["sale_date"].apply(_weekday_label)
    agg["수집일시"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    agg = agg.rename(columns={"sale_date": "매출일자", "ym": "기준월", "store": "매장명", "brand": "브랜드"})

    return agg[[
        "매출일자", "기준월", "매장명", "브랜드", "요일구분",
        "홀매출", "홀_테이블_매출", "홀_포장_매출", "배달매출", "총매출",
        "테이블수", "테이블단가", "수집일시",
    ]]


def _load_parquets(paths: list[Path]) -> pd.DataFrame:
    dfs = []
    for p in paths:
        try:
            dfs.append(pd.read_parquet(p))
        except Exception as e:
            logger.warning("parquet 읽기 실패 %s: %s", p.name, e)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def run_daily_actuals(**context) -> str:
    """어제 날짜 실적 집계 → daily_actuals.csv append"""
    exec_date: str = context["ds"]  # YYYY-MM-DD (어제 날짜)

    STORE_SALES_TARGET_DIR.mkdir(parents=True, exist_ok=True)

    # 기존 수집 날짜 파악
    if STORE_SALES_DAILY_ACTUALS_CSV.exists():
        existing = pd.read_csv(STORE_SALES_DAILY_ACTUALS_CSV, encoding="utf-8-sig", dtype=str)
        existing_dates: set[str] = set(existing["매출일자"].dropna().astype(str))
    else:
        existing = pd.DataFrame()
        existing_dates = set()

    # 처리 대상 parquet 결정
    if not existing_dates:
        # 최초 실행: 전체 backfill
        target_paths = sorted(UNIFIED_SALES_DIR.glob("unified_sales_*.parquet"))
        logger.info("최초 실행 — 전체 parquet %d개 처리", len(target_paths))
    else:
        date_suffix = exec_date.replace("-", "")[2:]  # "2026-04-21" → "260421"
        p = UNIFIED_SALES_DIR / f"unified_sales_{date_suffix}.parquet"
        target_paths = [p] if p.exists() else []
        if not target_paths:
            logger.warning("parquet 없음: %s", p)
            return f"SKIP: {exec_date} parquet 없음"

    raw = _load_parquets(target_paths)
    if raw.empty:
        return "SKIP: 데이터 없음"

    daily = _aggregate_day(raw)

    # 이미 수집된 날짜 제외
    if existing_dates:
        daily = daily[~daily["매출일자"].astype(str).isin(existing_dates)]

    if daily.empty:
        return f"SKIP: {exec_date} 이미 수집됨"

    combined = pd.concat([existing, daily], ignore_index=True) if not existing.empty else daily

    # 숫자 컬럼 정수형 저장
    for col in ["홀매출", "홀_테이블_매출", "홀_포장_매출", "배달매출", "총매출", "테이블수", "테이블단가"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").fillna(0).astype(int)

    combined.to_csv(STORE_SALES_DAILY_ACTUALS_CSV, index=False, encoding="utf-8-sig")
    logger.info("daily_actuals.csv 저장: 전체 %d행 / 신규 %d행", len(combined), len(daily))
    return f"OK: {len(daily)}행 추가"
