"""
홀 매장 주간 매출 실적 vs 목표 파이프라인

입력: MART_DB/unified_sales_grp/unified_sales_*.parquet
      (store=송파삼전점, platform=홀)
출력: MART_DB/hall_sales_target/hall_sale_target.csv

시간대 분류:
  점심: 06:00 <= order_time < 16:00
  저녁: 16:00 <= order_time < 24:00
  order_time이 00:00:00인 취소/조정 행은 같은 날짜의 원 주문번호 시간대로 재배정

집계 기준:
  (기준월, week_start) 2키 groupby → 크로스-먼스 주는 월별로 2행 분리
  기준월 = sale_date의 연월("YYYY-MM") → 5월 행 합산 시 정확한 월별 실적 보장
"""

import calendar
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

STORE_NAME   = "송파삼전점"
PLATFORM     = "홀"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
OUTPUT_PATH  = MART_DB / "hall_sales_target" / "hall_sale_target.csv"


def _week_label(ym: str, week_start: pd.Timestamp) -> str:
    """(기준월 'YYYY-MM', week_start 월요일) → '5월1주차' 형식.
    1주차 = 해당 월의 1일을 포함하는 주 (크로스-먼스 주도 1주차로 처리).
    """
    year, month = int(ym[:4]), int(ym[5:])
    first_day = pd.Timestamp(year, month, 1)
    first_week_monday = first_day - pd.Timedelta(days=first_day.dayofweek)
    week_num = (week_start - first_week_monday).days // 7 + 1
    return f"{month}월{week_num}주차"


def classify_hall_time_slots(df: pd.DataFrame) -> pd.DataFrame:
    """점심/저녁 시간대를 부여한다.

    OKPOS 취소/조정 행은 order_time이 00:00:00으로 들어올 수 있다. 이 경우 order_id의
    POS/주문번호를 원 주문과 매칭해 같은 시간대로 넣고, 매칭 실패 시 해당 일자의 매출 비중이
    큰 시간대로 배정해 점심+저녁 합계가 전체 매출과 일치하도록 한다.
    """
    df = df.copy()
    raw_hour = (
        df["order_time"].astype(str).str.strip()
        .str.extract(r"^(\d{2}):", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )
    raw_hour = raw_hour.where(raw_hour != 0, other=pd.NA)
    lunch_mask = ((raw_hour >= 6) & (raw_hour < 16)).fillna(False)
    dinner_mask = ((raw_hour >= 16) & (raw_hour < 24)).fillna(False)
    df["time_slot"] = pd.NA
    df.loc[lunch_mask, "time_slot"] = "점심"
    df.loc[dinner_mask, "time_slot"] = "저녁"

    if "order_id" not in df.columns or df.empty:
        df["time_slot"] = df["time_slot"].fillna("저녁")
        return df

    order_parts = (
        df["order_id"].fillna("").astype(str).str.strip()
        .str.extract(r"_(\d+)[-_](\d+)_\d{2}:\d{2}:\d{2}$")
    )
    df["_slot_pos"] = order_parts[0]
    df["_slot_seq"] = order_parts[1]

    match_cols = ["sale_date", "_slot_pos", "_slot_seq"]
    known = df[df["time_slot"].notna() & df["_slot_pos"].notna() & df["_slot_seq"].notna()]
    if not known.empty:
        lookup = known.drop_duplicates(match_cols).set_index(match_cols)["time_slot"]
        missing = df["time_slot"].isna() & df["_slot_pos"].notna() & df["_slot_seq"].notna()
        if missing.any():
            keys = pd.MultiIndex.from_frame(df.loc[missing, match_cols])
            df.loc[missing, "time_slot"] = lookup.reindex(keys).to_numpy()

    remaining = df["time_slot"].isna()
    if remaining.any():
        valid = df[df["time_slot"].notna()].copy()
        if not valid.empty:
            valid["_abs_sale"] = valid["total_price"].abs()
            day_default = (
                valid.groupby(["sale_date", "time_slot"], observed=True)["_abs_sale"].sum()
                .reset_index()
                .sort_values(["sale_date", "_abs_sale"])
                .drop_duplicates("sale_date", keep="last")
                .set_index("sale_date")["time_slot"]
            )
            df.loc[remaining, "time_slot"] = df.loc[remaining, "sale_date"].map(day_default)

    df["time_slot"] = df["time_slot"].fillna("저녁")
    return df.drop(columns=["_slot_pos", "_slot_seq"], errors="ignore")


def _load_hall_df() -> pd.DataFrame:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 없음: {UNIFIED_ROOT}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    df["sale_date"]   = pd.to_datetime(df["sale_date"],   errors="coerce")
    df["total_price"] = pd.to_numeric(df["total_price"],  errors="coerce").fillna(0).astype(int)
    df["order_cnt"]   = pd.to_numeric(df["order_cnt"],    errors="coerce").fillna(0).astype(int)
    df["store"]       = df["store"].astype(str).str.strip()
    df["platform"]    = df["platform"].fillna("").astype(str).str.strip()

    mask = (df["store"] == STORE_NAME) & (df["platform"] == PLATFORM)
    df = df[mask].copy()

    if df.empty:
        logger.warning("홀 데이터 없음 — store='%s', platform='%s'", STORE_NAME, PLATFORM)
        return df

    # 기준월: sale_date 기준 월("YYYY-MM") — groupby 키로 사용
    df["기준월"] = df["sale_date"].dt.strftime("%Y-%m")

    return classify_hall_time_slots(df)


def _agg_group(sub: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """(기준월, week_start) 2키로 집계. 객단가 컬럼명은 {prefix}테이블_객단가."""
    if sub.empty:
        return pd.DataFrame(columns=[
            "기준월", "week_start",
            f"{prefix}매출",
            f"{prefix}영수건수",
            f"{prefix}테이블_객단가",
        ])
    g = sub.groupby(["기준월", "week_start"], as_index=False).agg(
        **{
            f"{prefix}매출":     ("total_price", "sum"),
            f"{prefix}영수건수": ("order_cnt",   "sum"),
        }
    )
    g[f"{prefix}테이블_객단가"] = np.where(
        g[f"{prefix}영수건수"] > 0,
        (g[f"{prefix}매출"] / g[f"{prefix}영수건수"]).round(0).astype(int),
        0,
    )
    return g


def _weekly_agg(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["week_start"] = (
        df["sale_date"] - pd.to_timedelta(df["sale_date"].dt.dayofweek, unit="D")
    )

    total  = _agg_group(df,                            "")
    lunch  = _agg_group(df[df["time_slot"] == "점심"], "점심_")
    dinner = _agg_group(df[df["time_slot"] == "저녁"], "저녁_")

    merged = total.merge(lunch,  on=["기준월", "week_start"], how="left")
    merged = merged.merge(dinner, on=["기준월", "week_start"], how="left")

    non_key = [c for c in merged.columns if c not in ("기준월", "week_start")]
    for col in non_key:
        merged[col] = merged[col].fillna(0).astype(int)

    merged["입력날짜"] = merged["week_start"] + timedelta(days=7)
    merged["week_end"] = merged["week_start"] + timedelta(days=6)

    today = pd.Timestamp(date.today())
    completed = merged[merged["week_end"] < today].copy()
    return completed.sort_values(["입력날짜", "기준월"], ignore_index=True)


def _calc_weekly_targets(week_start: date, ym: str, monthly_targets: dict) -> dict:
    """ym에 해당하는 날 수(week 내)만 카운트해서 target 반환."""
    cnt = sum(
        1 for i in range(7)
        if (week_start + timedelta(days=i)).strftime("%Y-%m") == ym
    )

    result: dict[str, int] = {
        "매출_target": 0, "테이블_객단가_target": 0, "영수건수_target": 0,
        "점심_매출_target": 0, "점심_테이블_객단가_target": 0, "점심_영수건수_target": 0,
        "저녁_매출_target": 0, "저녁_테이블_객단가_target": 0, "저녁_영수건수_target": 0,
    }

    if ym not in monthly_targets:
        logger.warning("MONTHLY_TARGETS 미정의: %s (%d일 목표 0으로 처리)", ym, cnt)
        return result

    t   = monthly_targets[ym]
    dim = calendar.monthrange(int(ym[:4]), int(ym[5:]))[1]

    # 테이블단가는 고정값 (비율 분할 없음)
    result["테이블_객단가_target"]      = t.get("aov",        0)
    result["점심_테이블_객단가_target"] = t.get("lunch_aov",  0)
    result["저녁_테이블_객단가_target"] = t.get("dinner_aov", 0)

    for key, tkey in [
        ("매출_target",          "sale"),
        ("영수건수_target",      "orders"),
        ("점심_매출_target",     "lunch_sale"),
        ("점심_영수건수_target", "lunch_orders"),
        ("저녁_매출_target",     "dinner_sale"),
        ("저녁_영수건수_target", "dinner_orders"),
    ]:
        result[key] = round(t.get(tkey, 0) / dim * cnt)

    return result


def build_hall_sales_target(monthly_targets: dict) -> str:
    df = _load_hall_df()
    if df.empty:
        return "홀 데이터 없음, CSV 미생성"

    weekly = _weekly_agg(df)
    if weekly.empty:
        return "완료된 주 없음"

    rows = []
    for _, row in weekly.iterrows():
        ym = row["기준월"]
        t  = _calc_weekly_targets(row["week_start"].date(), ym, monthly_targets)
        rows.append({
            "입력날짜":                  row["입력날짜"].strftime("%Y-%m-%d"),
            "기준주":                   _week_label(ym, row["week_start"]),
            "기준월":                   ym,
            "매출":                     int(row["매출"]),
            "테이블_객단가":            int(row["테이블_객단가"]),
            "영수건수":                 int(row["영수건수"]),
            "매출_target":              t["매출_target"],
            "테이블_객단가_target":     t["테이블_객단가_target"],
            "영수건수_target":          t["영수건수_target"],
            "점심_매출":                int(row["점심_매출"]),
            "점심_테이블_객단가":       int(row["점심_테이블_객단가"]),
            "점심_영수건수":            int(row["점심_영수건수"]),
            "점심_매출_target":         t["점심_매출_target"],
            "점심_테이블_객단가_target":t["점심_테이블_객단가_target"],
            "점심_영수건수_target":     t["점심_영수건수_target"],
            "저녁_매출":                int(row["저녁_매출"]),
            "저녁_테이블_객단가":       int(row["저녁_테이블_객단가"]),
            "저녁_영수건수":            int(row["저녁_영수건수"]),
            "저녁_매출_target":         t["저녁_매출_target"],
            "저녁_테이블_객단가_target":t["저녁_테이블_객단가_target"],
            "저녁_영수건수_target":     t["저녁_영수건수_target"],
        })

    out = pd.DataFrame(rows)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    logger.info("hall_sale_target.csv 저장 완료: %d행 → %s", len(out), OUTPUT_PATH)
    return f"완료 {len(out)}행"
