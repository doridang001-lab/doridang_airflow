"""
홀 매장 일단위 트래킹 CSV 생성 파이프라인

입력:
  - MART_DB/unified_sales_grp/unified_sales_*.parquet  (판매 실적, 자동)
  - MART_DB/hall_sales_target/hall_marketing_target.csv (마케팅, 기준일자 join)
출력:
  - MART_DB/hall_sales_target/hall_daily_report.csv  (고정명, 월별 누적)
  - MART_DB/hall_sales_target/hall_daily_report_YYMMDD.csv (날짜 스냅샷)
"""

import calendar
import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from modules.transform.pipelines.db.DB_Hall_Sales_Target import classify_hall_time_slots
from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

STORE_NAME = "송파삼전점"
SOURCE = "okpos"
PLATFORM = "홀"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
MKT_CSV = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"
OUTPUT_CSV = MART_DB / "hall_sales_target" / "hall_daily_report.csv"

PRED_METRICS = [
    "total_amt",
    "tot_order_cnt",
    "점심_매출",
    "점심_주문건수",
    "저녁_매출",
    "저녁_주문건수",
    "플레이스_유입",
    "홍보물_배포",
    "쿠폰_회수건수",
    "인스타_노출",
    "당근_노출",
    "네이버_오더",
]

MARKETING_METRICS = [
    "플레이스_유입",
    "홍보물_배포",
    "쿠폰_회수건수",
    "인스타_노출",
    "당근_노출",
    "네이버_오더",
]

OUTPUT_COLUMNS = [
    "sale_date",
    "ym",
    "구분",
    "total_amt",
    "월매출_목표",
    "누적매출",
    "tot_order_cnt",
    "일영수건수_목표",
    "누적주문수",
    "일영수건수(일평균)",
    "객단가",
    "객단가_목표",
    "누적_객단가",
    "점심_매출",
    "점심_일매출목표",
    "누적_점심_매출",
    "점심_주문건수",
    "누적_점심_주문건수",
    "점심_객단가",
    "누적_점심_객단가",
    "저녁_매출",
    "저녁_일매출목표",
    "누적_저녁_매출",
    "저녁_주문건수",
    "누적_저녁_주문건수",
    "저녁_객단가",
    "누적_저녁_객단가",
    "플레이스_유입",
    "플레이스_유입_목표",
    "플레이스_유입_누적",
    "플레이스_유입_일평균",
    "플레이스_유입_일평균_목표",
    "홍보물_배포",
    "홍보물_배포_목표",
    "홍보물_배포_누적",
    "쿠폰_회수건수",
    "쿠폰_회수율_목표",
    "쿠폰_회수건수_누적",
    "쿠폰_회수율",
    "인스타_노출",
    "인스타_노출_목표",
    "인스타_노출_누적",
    "당근_노출",
    "당근_노출_목표",
    "당근_노출_누적",
    "네이버_오더",
    "네이버_오더_일평균_목표",
    "네이버_오더_누적",
    "네이버_오더_일평균",
    "점심_영수건수_목표",
    "점심_테이블단가_목표",
    "점심_일매출_일평균_목표",
    "저녁_영수건수_목표",
    "저녁_테이블단가_목표",
    "저녁_일매출_일평균_목표",
]


def _ensure_ym_column(df: pd.DataFrame) -> pd.DataFrame:
    if "ym" in df.columns:
        return df
    parsed = pd.to_datetime(df["sale_date"], errors="coerce")
    df["ym"] = parsed.dt.strftime("%Y_%m")
    return df


def _merge_with_existing_report(df: pd.DataFrame) -> pd.DataFrame:
    if not OUTPUT_CSV.exists():
        return df

    try:
        prev_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
    except Exception as exc:
        logger.warning("기존 hall_daily_report.csv 읽기 실패: %s", exc)
        return df

    if prev_df.empty:
        return df

    prev_df = prev_df.reindex(columns=prev_df.columns.to_list())
    prev_df = _ensure_ym_column(prev_df)
    curr_df = _ensure_ym_column(df.copy())

    # 월+일자 기준으로 최신 결과로 교체(재실행/당월 업데이트 반영)
    combined = pd.concat([prev_df, curr_df], ignore_index=True)
    combined["sale_date"] = pd.to_datetime(combined["sale_date"], errors="coerce")
    combined["ym_sort"] = pd.to_datetime(combined["ym"].str.replace("_", "-"), errors="coerce")
    combined = (
        combined.sort_values(["ym_sort", "sale_date"], kind="mergesort")
        .drop_duplicates(subset=["ym", "sale_date"], keep="last")
    )
    combined["sale_date"] = combined["sale_date"].dt.date

    for col in OUTPUT_COLUMNS:
        if col not in combined.columns:
            combined[col] = 0

    combined = combined[OUTPUT_COLUMNS]
    return combined


def _to_int_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], dtype=int)
    cleaned = series.fillna("").astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce").fillna(0).astype(int)


def _load_sales(ym: str) -> pd.DataFrame:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 없음: {UNIFIED_ROOT}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0).astype(int)
    df["order_cnt"] = pd.to_numeric(df["order_cnt"], errors="coerce").fillna(0).astype(int)
    df["store"] = df["store"].astype(str).str.strip()
    source = df["source"] if "source" in df.columns else pd.Series("", index=df.index)
    df["source"] = source.fillna("").astype(str).str.strip()
    df["platform"] = df["platform"].fillna("").astype(str).str.strip()

    mask = (
        (df["store"] == STORE_NAME) &
        (df["source"] == SOURCE) &
        (df["platform"] == PLATFORM) &
        (df["sale_date"].dt.strftime("%Y-%m") == ym)
    )
    df = df[mask].copy()
    if df.empty:
        return pd.DataFrame(columns=[
            "sale_date",
            "total_amt",
            "tot_order_cnt",
            "점심_매출",
            "점심_주문건수",
            "저녁_매출",
            "저녁_주문건수",
        ])

    df = classify_hall_time_slots(df)

    base = (
        df.groupby("sale_date", as_index=False, observed=True)
        .agg(total_amt=("total_price", "sum"), tot_order_cnt=("order_cnt", "sum"))
    )
    lunch = (
        df[df["time_slot"] == "점심"]
        .groupby("sale_date", as_index=False, observed=True)
        .agg(점심_매출=("total_price", "sum"), 점심_주문건수=("order_cnt", "sum"))
    )
    dinner = (
        df[df["time_slot"] == "저녁"]
        .groupby("sale_date", as_index=False, observed=True)
        .agg(저녁_매출=("total_price", "sum"), 저녁_주문건수=("order_cnt", "sum"))
    )

    out = (
        base.merge(lunch, on="sale_date", how="left")
        .merge(dinner, on="sale_date", how="left")
    )
    for col in ["total_amt", "tot_order_cnt", "점심_매출", "점심_주문건수", "저녁_매출", "저녁_주문건수"]:
        out[col] = out[col].fillna(0).astype(int)

    out["sale_date"] = out["sale_date"].dt.date
    return out


def _load_marketing(ym: str) -> pd.DataFrame:
    month_start = pd.to_datetime(f"{ym}-01")
    year, month = int(ym[:4]), int(ym[5:7])
    month_days = calendar.monthrange(year, month)[1]
    month_dates = pd.date_range(month_start, periods=month_days, freq="D")

    base = pd.DataFrame({"sale_date": month_dates})

    if not MKT_CSV.exists():
        logger.warning("마케팅 파일 없음: %s", MKT_CSV)
        for col in MARKETING_METRICS:
            base[col] = 0
        return base

    df = pd.read_csv(MKT_CSV, dtype=str)
    df = df.rename(columns={"인스타_노출_traget": "인스타_노출"})
    df["입력날짜"] = pd.to_datetime(df["입력날짜"], errors="coerce")
    df["기준일자"] = pd.to_datetime(df["기준일자"], errors="coerce")
    df["sale_date"] = df["기준일자"].fillna(df["입력날짜"])
    for col in MARKETING_METRICS:
        if col in df.columns:
            df[col] = _to_int_series(df[col])
        else:
            df[col] = 0
    df = df[["sale_date"] + MARKETING_METRICS].copy()
    df = df[df["sale_date"].dt.strftime("%Y-%m") == ym]
    df = (
        base
        .merge(df.sort_values("sale_date").drop_duplicates("sale_date"), on="sale_date", how="left")
        .fillna(0)
        .sort_values("sale_date")
    )
    for col in MARKETING_METRICS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["sale_date"] = df["sale_date"].dt.date
    return df


def _metric_target_value(metric: str, daily_target: dict) -> int:
    return {
        "total_amt": daily_target.get("sale", 0),
        "tot_order_cnt": daily_target.get("orders", 0),
        "점심_매출": daily_target.get("lunch_sale", 0),
        "점심_주문건수": daily_target.get("lunch_orders", 0),
        "저녁_매출": daily_target.get("dinner_sale", 0),
        "저녁_주문건수": daily_target.get("dinner_orders", 0),
        "플레이스_유입": daily_target.get("place", 0),
        "홍보물_배포": daily_target.get("홍보물_배포", 300),
        "쿠폰_회수건수": daily_target.get("coupon", 0),
        "인스타_노출": daily_target.get("insta", 0),
        "당근_노출": daily_target.get("karrot", 0),
        "네이버_오더": daily_target.get("naver", 0),
    }.get(metric, 0)


def _fill_predictions(
    actual_df: pd.DataFrame,
    mkt_df: pd.DataFrame,
    all_dates: list[date],
    daily_target: dict,
) -> pd.DataFrame:
    actual_dates = sorted(actual_df["sale_date"]) if not actual_df.empty else []
    train_df = actual_df.merge(mkt_df, on="sale_date", how="left") if not actual_df.empty else pd.DataFrame()
    for col in MARKETING_METRICS:
        if not train_df.empty and col in train_df.columns:
            train_df[col] = train_df[col].fillna(0)

    actual_lookup = {}
    if actual_dates:
        merged = train_df.set_index("sale_date")
        for d in actual_dates:
            row = merged.loc[d]
            if isinstance(row, pd.Series):
                actual_lookup[d] = row
            else:
                actual_lookup[d] = row.iloc[0]

    train_days = [d.day for d in actual_dates]
    X_train = np.array([[d] for d in train_days])

    rows = []
    for d in all_dates:
        is_actual = d in actual_lookup
        row = {"sale_date": d, "구분": "실제값" if is_actual else "예측값"}
        for metric in PRED_METRICS:
            if is_actual:
                row[metric] = int(actual_lookup[d][metric])
                continue

            if len(train_days) < 3:
                row[metric] = _metric_target_value(metric, daily_target)
                continue

            y_train = np.array([int(actual_lookup[dt][metric]) for dt in actual_dates]).reshape(-1, 1).astype(float)
            model = LinearRegression()
            model.fit(X_train, y_train)
            pred_val = model.predict([[d.day]])[0][0]
            row[metric] = max(0, int(round(pred_val)))

        rows.append(row)

    return pd.DataFrame(rows)


def _daily_target_for_ym(daily_target: dict, ym: str) -> dict:
    """월별 일목표 원장이 있으면 해당 월 목표를 사용하고, 없으면 기존 dict를 그대로 사용한다."""
    by_ym = daily_target.get("_by_ym")
    if isinstance(by_ym, dict) and ym in by_ym:
        return by_ym[ym]
    return daily_target


def _add_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("sale_date").copy()
    df["경과일수"] = np.arange(1, len(df) + 1, dtype=int)

    df["누적매출"] = df["total_amt"].cumsum()
    df["누적주문수"] = df["tot_order_cnt"].cumsum()
    df["객단가"] = np.where(df["tot_order_cnt"] > 0, df["total_amt"] / df["tot_order_cnt"], 0)
    df["누적_객단가"] = np.where(df["누적주문수"] > 0, df["누적매출"] / df["누적주문수"], 0)
    df["일영수건수(일평균)"] = np.where(df["경과일수"] > 0, df["누적주문수"] / df["경과일수"], 0)

    df["점심_객단가"] = np.where(df["점심_주문건수"] > 0, df["점심_매출"] / df["점심_주문건수"], 0)
    df["누적_점심_매출"] = df["점심_매출"].cumsum()
    df["누적_점심_주문건수"] = df["점심_주문건수"].cumsum()
    df["누적_점심_객단가"] = np.where(
        df["누적_점심_주문건수"] > 0,
        df["누적_점심_매출"] / df["누적_점심_주문건수"],
        0,
    )

    df["저녁_객단가"] = np.where(df["저녁_주문건수"] > 0, df["저녁_매출"] / df["저녁_주문건수"], 0)
    df["누적_저녁_매출"] = df["저녁_매출"].cumsum()
    df["누적_저녁_주문건수"] = df["저녁_주문건수"].cumsum()
    df["누적_저녁_객단가"] = np.where(
        df["누적_저녁_주문건수"] > 0,
        df["누적_저녁_매출"] / df["누적_저녁_주문건수"],
        0,
    )

    df["플레이스_유입_누적"] = df["플레이스_유입"].cumsum()
    df["플레이스_유입_일평균"] = np.where(df["경과일수"] > 0, df["플레이스_유입_누적"] / df["경과일수"], 0)
    df["홍보물_배포_누적"] = df["홍보물_배포"].cumsum()
    df["쿠폰_회수건수_누적"] = df["쿠폰_회수건수"].cumsum()
    df["쿠폰_회수율"] = np.where(
        df["홍보물_배포_누적"] > 0,
        np.round(df["쿠폰_회수건수_누적"] / df["홍보물_배포_누적"] * 100, 1),
        0,
    )
    df["인스타_노출_누적"] = df["인스타_노출"].cumsum()
    df["당근_노출_누적"] = df["당근_노출"].cumsum()
    df["네이버_오더_누적"] = df["네이버_오더"].cumsum()
    df["네이버_오더_일평균"] = np.where(
        df["경과일수"] > 0,
        np.round(df["네이버_오더_누적"] / df["경과일수"], 1),
        0,
    )
    return df


def _add_targets(
    df: pd.DataFrame,
    monthly_targets: dict,
    marketing_monthly_targets: dict,
    daily_target: dict,
) -> pd.DataFrame:
    ym = df["sale_date"].dt.strftime("%Y-%m").iloc[0]
    mt = monthly_targets.get(ym, {})
    if isinstance(ym, str):
        y, m = map(int, ym.split("-"))
    else:
        y, m = df["sale_date"].dt.year.iloc[0], df["sale_date"].dt.month.iloc[0]
        y, m = int(y), int(m)
    month_days = calendar.monthrange(y, m)[1]
    mkt = marketing_monthly_targets.get(ym, {})

    if not mt:
        logger.warning("월 매출 목표 미정의: %s", ym)
    if not mkt:
        logger.warning("월 마케팅 목표 미정의: %s", ym)

    df["월매출_목표"] = mt.get("sale", 0)
    df["객단가_목표"] = mt.get("aov", 0)
    df["일영수건수_목표"] = mt.get("orders", 0)
    df["점심_일매출목표"] = mt.get("lunch_sale", 0)
    df["점심_영수건수_목표"] = mt.get("lunch_orders", 0)
    df["점심_테이블단가_목표"] = mt.get("lunch_aov", 0)
    df["점심_일매출_일평균_목표"] = (
        round(mt.get("lunch_sale", 0) / month_days, 0) if month_days else 0
    )
    df["저녁_일매출목표"] = mt.get("dinner_sale", 0)
    df["저녁_영수건수_목표"] = mt.get("dinner_orders", 0)
    df["저녁_테이블단가_목표"] = mt.get("dinner_aov", 0)
    df["저녁_일매출_일평균_목표"] = (
        round(mt.get("dinner_sale", 0) / month_days, 0) if month_days else 0
    )
    df["플레이스_유입_목표"] = mkt.get("플레이스_유입", 0)
    df["플레이스_유입_일평균_목표"] = daily_target.get("place", 0)
    df["홍보물_배포_목표"] = mkt.get("홍보물_배포", 0)
    df["쿠폰_회수율_목표"] = 0.7
    df["인스타_노출_목표"] = mkt.get("인스타_노출", 0)
    df["당근_노출_목표"] = mkt.get("당근_노출", 0)
    df["네이버_오더_일평균_목표"] = daily_target.get("naver", 0)
    return df

def build_daily_tracking_csv(
    monthly_targets: dict,
    marketing_monthly_targets: dict,
    daily_target: dict,
    ym: str | None = None,
) -> str:
    today = date.today()
    if not ym:
        ym = today.strftime("%Y-%m")
    daily_target = _daily_target_for_ym(daily_target, ym)
    year, month = int(ym[:4]), int(ym[5:7])
    days_in_month = calendar.monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    actual_df = _load_sales(ym)
    mkt_df = _load_marketing(ym)
    df = _fill_predictions(actual_df, mkt_df, all_dates, daily_target)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = _add_cumulative(df)
    df = _add_targets(df, monthly_targets, marketing_monthly_targets, daily_target)
    df["ym"] = ym.replace("-", "_")

    df = _merge_with_existing_report(df)

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = 0

    df = df[OUTPUT_COLUMNS]
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    snap = OUTPUT_CSV.parent / f"hall_daily_report_{today.strftime('%y%m%d')}.csv"
    df.to_csv(snap, index=False, encoding="utf-8-sig")

    logger.info("hall_daily_report.csv 저장 완료: %s", OUTPUT_CSV)
    return f"hall_daily_report.csv 저장 완료: {OUTPUT_CSV}"
