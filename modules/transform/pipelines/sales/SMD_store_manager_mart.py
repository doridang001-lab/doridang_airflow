"""
매장 담당자/지역 마스터 + 전월 매출 스냅샷 마트

sales_employee.csv(담당자/광역/시군구/읍면동) + unified_sales_grp(전월 총매출)
→ store_manager.csv (매일 재계산 후 덮어쓰는 스냅샷, 누적 이력 아님)
"""

import logging
import re
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    LOCAL_DB,
    STORE_MANAGER_MART_CSV,
    STORE_MANAGER_MART_DIR,
)
from modules.transform.utility.store_normalize import normalize, normalize_for_join, strip_brand
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files

logger = logging.getLogger(__name__)

EMPLOYEE_CSV_PATH = LOCAL_DB / "영업관리부_DB" / "sales_employee.csv"
_UNIFIED_DAILY_NAME_RE = re.compile(r"^unified_sales_(\d{2})(\d{2})(\d{2})\.parquet$")


def _load_store_master() -> pd.DataFrame:
    """sales_employee.csv → 매장당 1행 담당자/주소 마스터"""
    df = pd.read_csv(
        EMPLOYEE_CSV_PATH,
        usecols=["매장명", "담당자", "광역", "시군구", "읍면동"],
        dtype=str,
        encoding="utf-8-sig",
    )
    df = df.drop_duplicates(subset=["매장명"], keep="first")

    store_name = normalize(df["매장명"])
    df["store"] = strip_brand(store_name)
    df["_join_key"] = normalize_for_join(df["매장명"])
    df = df.rename(columns={"담당자": "manager", "광역": "region", "시군구": "city", "읍면동": "town"})

    for col in ["manager", "region", "city", "town"]:
        df[col] = df[col].fillna("").str.strip()

    return df[["store", "_join_key", "manager", "region", "city", "town"]]


def _previous_month_str(ds: str) -> str:
    """DAG 실행일(ds, YYYY-MM-DD) 기준 전월을 unified_sales 파일명 형식(YYMM)으로 반환"""
    year, month = int(ds[:4]), int(ds[5:7])
    if month == 1:
        year, month = year - 1, 12
    else:
        month -= 1
    return f"{year % 100:02d}{month:02d}"


def _select_previous_month_files(prev_yymm: str) -> list[Path]:
    """iter_unified_sales_files() 결과 중 파일명이 전월(YYMM)에 해당하는 것만 필터링"""
    selected = []
    for path in iter_unified_sales_files():
        m = _UNIFIED_DAILY_NAME_RE.match(path.name)
        if m and f"{m.group(1)}{m.group(2)}" == prev_yymm:
            selected.append(path)
    return selected


def _load_previous_month_sales(prev_yymm: str) -> pd.Series:
    """전월 unified_sales parquet들 → store별 총매출 합계 (브랜드 무관)"""
    files = _select_previous_month_files(prev_yymm)
    if not files:
        logger.warning("전월(%s) unified_sales parquet 없음", prev_yymm)
        return pd.Series(dtype=float)

    parts = []
    for path in files:
        try:
            parts.append(pd.read_parquet(path, columns=["store", "total_price"]))
        except Exception as e:
            logger.warning("parquet 읽기 실패 %s: %s", path.name, e)

    if not parts:
        return pd.Series(dtype=float)

    raw = pd.concat(parts, ignore_index=True)
    raw["_join_key"] = normalize_for_join(raw["store"])
    raw["total_price"] = pd.to_numeric(raw["total_price"], errors="coerce").fillna(0)
    return raw.groupby("_join_key")["total_price"].sum()


def _build_store_manager_table(master: pd.DataFrame, sales: pd.Series) -> pd.DataFrame:
    """마스터 기준 left join. 미매칭 매장은 last_month_sales=0"""
    out = master.copy()
    out["last_month_sales"] = out["_join_key"].map(sales).fillna(0).round(0).astype(int)
    return out[["store", "manager", "region", "city", "town", "last_month_sales"]]


def run_store_manager_mart(**context) -> str:
    ds = context["ds"]
    prev_yymm = _previous_month_str(ds)

    master = _load_store_master()
    sales = _load_previous_month_sales(prev_yymm)
    table = _build_store_manager_table(master, sales)

    STORE_MANAGER_MART_DIR.mkdir(parents=True, exist_ok=True)
    table.to_csv(STORE_MANAGER_MART_CSV, index=False, encoding="utf-8-sig")

    matched = int((table["last_month_sales"] > 0).sum())
    logger.info(
        "store_manager.csv 저장: %d개 매장 / 전월=%s / 매출매칭 %d개 / %s",
        len(table), prev_yymm, matched, STORE_MANAGER_MART_CSV,
    )
    return f"OK: {len(table)}개 매장 저장 | 전월={prev_yymm} | 매출매칭={matched} | {STORE_MANAGER_MART_CSV}"
