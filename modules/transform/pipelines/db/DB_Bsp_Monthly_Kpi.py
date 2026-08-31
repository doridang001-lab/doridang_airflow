"""브랜드전략기획팀 월간 KPI 시트(monthly_kpi.xlsx) 실적 자동 채움.

입력:
  - MART_DB/unified_sales_grp/unified_sales_*.parquet (영수건수/주문건수)
  - ANALYTICS_DB/Instagram/instagram_snapshot.csv (팔로워 스냅샷)
  - ANALYTICS_DB/Kakao/Friends/kakao_friends.csv (친구수 스냅샷)
출력:
  - BSP_MONTHLY_KPI_XLSX Sheet1 의 실적 4개 열만 갱신 (목표/수기 열은 건드리지 않음)

시트는 주차 단위 행이므로 실적도 주시작일 기준으로 채운다.
매출 지표는 해당 주 월~일 합계, 스냅샷 지표는 해당 주 안의 최신 collect_date를 쓴다.
시트에 없는 ym(행이 아직 추가되지 않은 미래 달)은 건너뛰고 행을 추가하지 않는다.
"""

import logging
from datetime import date, datetime, timedelta

import pandas as pd
from openpyxl import load_workbook

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    BSP_MONTHLY_KPI_XLSX,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files

logger = logging.getLogger(__name__)

SHEET_NAME = "Sheet1"
WEEK_START_HEADER = "주시작일"
YM_HEADER = "ym"
HALL_ORDER_CNT_HEADER = "송파삼전점 영수건수"
TOTAL_ORDER_CNT_HEADER = "가맹점 주문건수"
INSTAGRAM_FOLLOWERS_HEADER = "인스타그램 팔로워"
KAKAO_FRIENDS_HEADER = "카카오채널 친구"

INSTAGRAM_SNAPSHOT_CSV_PATH = ANALYTICS_DB / "Instagram" / "instagram_snapshot.csv"
INSTAGRAM_ACCOUNT = "doridang_official"
KAKAO_FRIENDS_CSV_PATH = ANALYTICS_DB / "Kakao" / "Friends" / "kakao_friends.csv"
KAKAO_CHANNEL_ID = "_UxiaxiG"
STORE_NAME = "송파삼전점"

INT_NUMBER_FORMAT = '0_);[Red]\\(0\\)'

# 주간 집계가 100건 미만이면 수집 초기 구간 또는 부분 수집 의심으로 경고만 남긴다.
TOTAL_ORDER_CNT_SANITY_MIN = 100


def _as_date(value) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _load_sales_by_week_start() -> pd.DataFrame:
    """unified_sales parquet에서 주시작일별 홀 영수건수/전체 주문건수를 집계한다."""
    files = iter_unified_sales_files()
    if not files:
        logger.warning("unified_sales parquet 없음, 매출 지표 스킵")
        return pd.DataFrame(columns=["week_start", "hall_order_cnt", "total_order_cnt"])

    df = pd.concat(
        [pd.read_parquet(f, columns=["sale_date", "store", "order_type", "order_cnt"]) for f in files],
        ignore_index=True,
    )
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df[df["sale_date"].notna()].copy()
    df["week_start"] = (
        df["sale_date"] - pd.to_timedelta(df["sale_date"].dt.weekday, unit="D")
    ).dt.date

    # order_cnt는 {-1, 0, 1}: 취소 주문이 -1이라 sum()이 자동으로 상계된다.
    # 여기에 취소 제외 필터를 추가로 걸면 이중 차감이 되므로 걸지 않는다.
    total = df.groupby("week_start")["order_cnt"].sum().rename("total_order_cnt")

    hall_mask = (df["store"] == STORE_NAME) & df["order_type"].astype(str).str.contains("홀")
    hall = df[hall_mask].groupby("week_start")["order_cnt"].sum().rename("hall_order_cnt")

    out = pd.concat([hall, total], axis=1).fillna(0).reset_index()
    out["hall_order_cnt"] = out["hall_order_cnt"].astype(int)
    out["total_order_cnt"] = out["total_order_cnt"].astype(int)

    low = out[out["total_order_cnt"] < TOTAL_ORDER_CNT_SANITY_MIN]
    if not low.empty:
        logger.warning(
            "가맹점 주문건수 이상치 의심(< %d): %s",
            TOTAL_ORDER_CNT_SANITY_MIN,
            low.set_index("week_start")["total_order_cnt"].to_dict(),
        )
    return out


def _load_snapshot_latest_by_week_start(
    csv_path,
    *,
    id_col: str,
    id_value: str,
    value_col: str,
) -> dict[date, int]:
    """스냅샷 CSV에서 계정/채널을 고정 필터링 후 주별 최신 collect_date 값을 반환한다."""
    if not csv_path.exists():
        logger.warning("스냅샷 CSV 없음, 스킵: %s", csv_path)
        return {}

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df = df[df[id_col] == id_value]
    if df.empty:
        logger.warning("스냅샷 CSV에 %s=%s 행 없음: %s", id_col, id_value, csv_path)
        return {}

    df = df.copy()
    df["collect_date"] = pd.to_datetime(df["collect_date"], errors="coerce")
    df = df[df["collect_date"].notna()].copy()
    if df.empty:
        logger.warning("스냅샷 CSV에 유효한 collect_date 없음: %s", csv_path)
        return {}
    df["week_start"] = (
        df["collect_date"] - pd.to_timedelta(df["collect_date"].dt.weekday, unit="D")
    ).dt.date
    latest = df.sort_values("collect_date").groupby("week_start").tail(1)
    return dict(zip(latest["week_start"], latest[value_col].astype(int)))


def _col_by_header(ws, *headers: str) -> int | None:
    for cell in ws[1]:
        if cell.value is not None and str(cell.value).strip() in headers:
            return cell.column
    return None


def _write_cell(ws, row: int, col: int, value: int, *, number_format: str | None = None) -> bool:
    cell = ws.cell(row=row, column=col)
    changed = cell.value != value
    if changed:
        cell.value = value
    if number_format is not None and cell.number_format != number_format:
        cell.number_format = number_format
        changed = True
    return changed


def _clear_cell(ws, row: int, col: int) -> bool:
    cell = ws.cell(row=row, column=col)
    changed = cell.value is not None
    if changed:
        cell.value = None
    return changed


def sync_monthly_kpi() -> str:
    """monthly_kpi.xlsx의 실적 4개 열(E/G/I/K)을 주시작일 기준으로 채운다."""
    path = BSP_MONTHLY_KPI_XLSX
    if not path.exists():
        msg = f"SKIP: monthly_kpi.xlsx 없음: {path}"
        logger.warning(msg)
        return msg

    sales = _load_sales_by_week_start().set_index("week_start")
    instagram = _load_snapshot_latest_by_week_start(
        INSTAGRAM_SNAPSHOT_CSV_PATH,
        id_col="account",
        id_value=INSTAGRAM_ACCOUNT,
        value_col="followers",
    )
    kakao = _load_snapshot_latest_by_week_start(
        KAKAO_FRIENDS_CSV_PATH,
        id_col="channel_id",
        id_value=KAKAO_CHANNEL_ID,
        value_col="friends",
    )

    try:
        wb = load_workbook(path)
    except PermissionError as exc:
        raise PermissionError(f"monthly_kpi.xlsx 가 열려 있습니다: {path}") from exc

    ws = wb[SHEET_NAME]

    week_start_col = _col_by_header(ws, WEEK_START_HEADER, "주 시작일", "주 시작 일")
    ym_col = _col_by_header(ws, YM_HEADER)
    hall_col = _col_by_header(ws, HALL_ORDER_CNT_HEADER)
    total_col = _col_by_header(ws, TOTAL_ORDER_CNT_HEADER)
    insta_col = _col_by_header(ws, INSTAGRAM_FOLLOWERS_HEADER)
    kakao_col = _col_by_header(ws, KAKAO_FRIENDS_HEADER)
    missing = [
        name
        for name, col in [
            (YM_HEADER, ym_col),
            (HALL_ORDER_CNT_HEADER, hall_col),
            (TOTAL_ORDER_CNT_HEADER, total_col),
            (INSTAGRAM_FOLLOWERS_HEADER, insta_col),
            (KAKAO_FRIENDS_HEADER, kakao_col),
            (WEEK_START_HEADER, week_start_col),
        ]
        if col is None
    ]
    if missing:
        raise ValueError(f"monthly_kpi.xlsx 헤더 없음: {missing}")

    changed = 0
    missing_ym: set[str] = set()
    missing_week_start: set[str] = set()

    for row in range(2, ws.max_row + 1):
        ym_raw = ws.cell(row=row, column=ym_col).value
        if ym_raw is None or not str(ym_raw).strip():
            continue
        ym = str(ym_raw).strip().replace("_", "-")
        week_start = _as_date(ws.cell(row=row, column=week_start_col).value)
        if week_start is None:
            missing_week_start.add(f"row={row}")
            continue

        if week_start in sales.index:
            hall_val = int(sales.at[week_start, "hall_order_cnt"])
            total_val = int(sales.at[week_start, "total_order_cnt"])
            changed += _write_cell(ws, row, hall_col, hall_val, number_format=INT_NUMBER_FORMAT)
            changed += _write_cell(ws, row, total_col, total_val, number_format=INT_NUMBER_FORMAT)
        else:
            missing_ym.add(ym)
            changed += _clear_cell(ws, row, hall_col)
            changed += _clear_cell(ws, row, total_col)

        if week_start in instagram:
            changed += _write_cell(ws, row, insta_col, instagram[week_start])
        else:
            changed += _clear_cell(ws, row, insta_col)
        if week_start in kakao:
            changed += _write_cell(ws, row, kakao_col, kakao[week_start])
        else:
            changed += _clear_cell(ws, row, kakao_col)

    if missing_ym:
        logger.info("매출 데이터 없는 ym(스킵, 행 추가 안 함): %s", sorted(missing_ym))
    if missing_week_start:
        logger.info("주시작일 없는 행(스킵): %s", sorted(missing_week_start))

    if changed:
        try:
            wb.save(path)
        except PermissionError as exc:
            raise PermissionError(f"monthly_kpi.xlsx 저장 실패(파일 열려있음): {path}") from exc

    result = (
        f"OK: monthly_kpi.xlsx 변경 {changed}건 | "
        f"데이터없는ym {sorted(missing_ym)} | 주시작일없는행 {sorted(missing_week_start)}"
    )
    logger.info(result)
    return result
