"""
네이버 자동수집 값을 hall_marketing_target.csv에 반영한다.
"""

import logging

import pandas as pd

from modules.transform.pipelines.db.DB_Hall_Sales_Excel import MKT_CSV
from modules.transform.utility.paths import NAVER_CORP_STORE_MKT_CSV_PATH

logger = logging.getLogger(__name__)

STORE_NAME = "송파삼전점"


def _norm_date(value) -> str:
    ts = pd.to_datetime(str(value).strip(), errors="coerce")
    return "" if pd.isna(ts) else ts.strftime("%Y-%m-%d")


def sync_naver_marketing() -> None:
    if not NAVER_CORP_STORE_MKT_CSV_PATH.exists():
        logger.warning("네이버 수집 소스 없음, sync 생략: %s", NAVER_CORP_STORE_MKT_CSV_PATH)
        return
    if not MKT_CSV.exists():
        logger.warning("hall_marketing_target.csv 없음, sync 생략: %s", MKT_CSV)
        return

    src = pd.read_csv(
        NAVER_CORP_STORE_MKT_CSV_PATH,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )
    if "store" in src.columns:
        src = src[src["store"].astype(str).str.strip() == STORE_NAME]

    by_date: dict[str, tuple[str, str]] = {}
    for _, row in src.iterrows():
        target_date = _norm_date(row.get("date", ""))
        if target_date:
            by_date[target_date] = (
                str(row.get("place_inflow", "")).strip(),
                str(row.get("reservations", "")).strip(),
            )

    tgt = pd.read_csv(MKT_CSV, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    columns = list(tgt.columns)
    tgt["입력날짜"] = tgt["입력날짜"].map(_norm_date)

    updated = 0
    existing_dates = set(tgt["입력날짜"])
    for i in tgt.index:
        target_date = tgt.at[i, "입력날짜"]
        if target_date in by_date:
            place_inflow, reservations = by_date[target_date]
            tgt.at[i, "플레이스_유입"] = place_inflow
            tgt.at[i, "네이버_오더"] = reservations
            updated += 1

    new_rows = []
    for target_date, (place_inflow, reservations) in by_date.items():
        if target_date not in existing_dates:
            row = {column: "" for column in columns}
            row["입력날짜"] = target_date
            row["플레이스_유입"] = place_inflow
            row["네이버_오더"] = reservations
            new_rows.append(row)

    if new_rows:
        tgt = pd.concat([tgt, pd.DataFrame(new_rows, columns=columns)], ignore_index=True)

    tgt = tgt.sort_values("입력날짜").reset_index(drop=True)
    tgt = tgt[columns]
    tgt.to_csv(MKT_CSV, index=False, encoding="utf-8-sig")
    logger.info(
        "네이버 마케팅 sync 완료: 갱신 %d건 / 추가 %d건 -> %s",
        updated,
        len(new_rows),
        MKT_CSV,
    )
