"""
월별 폐점율 KPI 스냅샷 파이프라인
GSheet 직원관리 시트 → 매장×월 스냅샷 + 월별 KPI 집계 CSV 저장

운영중 정의: 실오픈일 있음 + 담당 SV 있음 + 해지일 없음
해지:        해지일 입력 시점

closing_rate = closed / active_count
"""

import logging
import pandas as pd
from datetime import datetime

from modules.extract.extract_gsheet import extract_gsheet
from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

# ============================================================
# GSheet 설정
# ============================================================
EMPLOYEE_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1a6-20U1-FYCQEfbOOVSDG3M0q6G2me5f/edit"
EMPLOYEE_SHEET_NAME = None
CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

SNAPSHOT_START_YM = "2023-09"  # 집계 시작 월

# ============================================================
# 출력 경로
# ============================================================
CLOSING_RATE_DIR = MART_DB / "closing_rate"
SNAPSHOT_CSV     = CLOSING_RATE_DIR / "employee_store_snapshot_history.csv"
KPI_CSV          = CLOSING_RATE_DIR / "closing_rate_monthly_kpi.csv"


# ============================================================
# Task 1: GSheet 추출 (노트북 df_employee와 동일)
# ============================================================
def extract_employee(**context):
    """GSheet 직원관리 시트에서 df_employee 생성

    노트북(000_analysis_sample)과 동일한 B3 시작 방식:
      df_all.iloc[1:, 1:] → 첫 행(요약) 및 A열(빈 컬럼) 제외
    필터: 실오픈일 not null
    운영 판별: 담당 SV 있음 + 해지일 없음
    """
    logger.info("[extract] GSheet 직원관리 시트 로드 시작")

    df_all = extract_gsheet(
        url=EMPLOYEE_GSHEET_URL,
        sheet_name=EMPLOYEE_SHEET_NAME,
        credentials_path=CREDENTIALS_PATH,
    )

    # B3 기준: 첫 행(요약) + 첫 컬럼(A열) 제외
    df_raw = df_all.iloc[1:, 1:].copy().reset_index(drop=True)
    raw_header = [
        str(col).strip().replace("\n", "") if pd.notna(col) else ""
        for col in df_raw.iloc[0].tolist()
    ]
    df = df_raw.iloc[1:].copy()
    df.columns = raw_header
    df = df.reset_index(drop=True)

    # 컬럼명 표준화
    col_map = {"담당 S.V": "담당 SV", "담당SV": "담당 SV"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["매장명", "실오픈일", "해지일", "담당 SV"]:
        if col not in df.columns:
            raise ValueError(f"[extract] '{col}' 컬럼이 GSheet에 없습니다.")

    # 실오픈일 있는 것만 (= df_employee)
    df = df[df["실오픈일"].notna() & (df["실오픈일"].astype(str).str.strip() != "")].copy()

    # 제외 조건: 담당 SV 없음 AND 해지일 없음 (비공식 폐점 — 6개)
    # 포함 조건: 담당 SV 있음 (운영중 68개) OR 해지일 있음 (공식 폐점 11개)
    df["담당 SV"] = df["담당 SV"].astype(str).str.strip()
    has_sv    = ~df["담당 SV"].isin(["", "nan", "None", "NaN"])
    has_haejil = df["해지일"].notna() & (df["해지일"].astype(str).str.strip() != "")
    df = df[has_sv | has_haejil].copy()

    # 필요 컬럼 추출 + 날짜 파싱
    df = df[["매장명", "실오픈일", "해지일"]].copy()
    df["실오픈일"] = pd.to_datetime(df["실오픈일"], errors="coerce")
    df["해지일"]   = pd.to_datetime(df["해지일"],   errors="coerce")
    df = df[df["실오픈일"].notna()].copy()
    df = df.reset_index(drop=True)

    logger.info(f"[extract] 유효 매장 {len(df)}건 (담당SV 있음 + 실오픈일 있음)")
    return df.to_json(orient="records", date_format="iso", force_ascii=False)


# ============================================================
# Task 2: 스냅샷 빌드 + KPI 집계 + CSV 저장
# ============================================================
def transform_and_save(**context):
    """매장×월 스냅샷 생성 및 월별 폐점율 KPI 집계 후 CSV 저장

    is_active = 실오픈일 <= 월말 AND (해지일 없음 OR 해지일 >= 월초)
    closing_rate = closed / active_count
    """
    raw_json = context["ti"].xcom_pull(task_ids="extract_employee")
    df = pd.read_json(raw_json, orient="records")
    df["실오픈일"] = pd.to_datetime(df["실오픈일"])
    df["해지일"]   = pd.to_datetime(df["해지일"], errors="coerce")

    # 월 범위: SNAPSHOT_START_YM ~ 현재월
    current_ym = datetime.now().strftime("%Y-%m")
    months = pd.period_range(SNAPSHOT_START_YM, current_ym, freq="M")
    months_df = pd.DataFrame({"ym": months, "_key": 1})

    # cross join (매장 × 월)
    df["_key"] = 1
    cross = df.merge(months_df, on="_key").drop("_key", axis=1)

    # 월 경계 계산 (tz-naive)
    month_start = cross["ym"].dt.start_time.dt.tz_localize(None)
    month_end   = cross["ym"].dt.end_time.dt.tz_localize(None)

    open_date  = cross["실오픈일"]
    close_date = cross["해지일"]

    # 해당 월에 운영한 매장 (오픈 포함)
    cross["is_active"] = (
        (open_date <= month_end) &
        (close_date.isna() | (close_date >= month_start))
    ).astype(int)

    # 해당 월에 오픈한 신규 매장
    cross["is_new"] = (
        (open_date >= month_start) & (open_date <= month_end)
    ).astype(int)

    # 해당 월에 해지한 매장
    cross["is_closed"] = (
        close_date.notna() &
        (close_date >= month_start) &
        (close_date <= month_end)
    ).astype(int)

    cross["ym"] = cross["ym"].astype(str)

    # ① 스냅샷 저장
    snapshot_cols = ["ym", "매장명", "실오픈일", "해지일", "is_active", "is_new", "is_closed"]
    snapshot_df = cross[snapshot_cols].sort_values(["ym", "매장명"]).reset_index(drop=True)

    CLOSING_RATE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_df.to_csv(SNAPSHOT_CSV, index=False, encoding="utf-8-sig")
    logger.info(f"[transform] 스냅샷 저장 완료: {len(snapshot_df)}행 → {SNAPSHOT_CSV}")

    # ② KPI 집계
    kpi = (
        cross.groupby("ym")
        .agg(
            active_count=("is_active", "sum"),
            new_open    =("is_new",    "sum"),
            closed      =("is_closed", "sum"),
        )
        .reset_index()
    )
    kpi["closing_rate"] = (
        kpi["closed"] / kpi["active_count"].replace(0, pd.NA)
    ).fillna(0.0).round(4)

    kpi.to_csv(KPI_CSV, index=False, encoding="utf-8-sig")
    logger.info(f"[transform] KPI 저장 완료: {len(kpi)}개월 → {KPI_CSV}")

    latest = kpi.iloc[-1]
    return (
        f"완료: {len(kpi)}개월 | "
        f"최신({latest['ym']}) active={int(latest['active_count'])} "
        f"closed={int(latest['closed'])} "
        f"closing_rate={latest['closing_rate']:.2%}"
    )
