"""
홀 매장 일단위 트래킹 Excel 생성 파이프라인

디자인: "일단위 트래킹 (매일 입력)" 이미지 기반
입력:
  - MART_DB/unified_sales_grp/unified_sales_*.parquet  (판매 실적, 자동)
  - MART_DB/hall_sales_target/hall_marketing_target.csv (마케팅, 기준일자 join)
출력:
  - MART_DB/hall_sales_target/hall_weekly_report.xlsx  (고정명, 매 실행 덮어씀)

구조:
  [제목] 일단위 트래킹 (매일 입력)
  [헤더] 날짜 | 점심 영수건수 | 점심 테이블단가 | 저녁 영수건수 | 저녁 테이블단가
         | 플레이스 유입수 | 홍보물 배포(누적) | 쿠폰 회수 | 인스타 노출
         | 당근 노출 | 네이버오더 건수 | 일 매출(자동계산)
  [목표행] 일 목표치 (주황색)
  [데이터] 당월 전체 날짜 × 일별 실적
"""

import calendar
import logging
from datetime import date, timedelta

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

STORE_NAME   = "송파삼전점"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
MKT_CSV      = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"
OUTPUT_XLSX  = MART_DB / "hall_sales_target" / "hall_daily_report.xlsx"

# ── 스타일 ─────────────────────────────────────────────────────
_NAVY    = "1F3864"
_ORANGE  = "F4A460"
_RED_TXT = "C00000"
_GRAY_BG = "F5F5F5"
_WHITE   = "FFFFFF"

_H_FILL  = PatternFill("solid", fgColor=_NAVY)
_T_FILL  = PatternFill("solid", fgColor=_ORANGE)
_G_FILL  = PatternFill("solid", fgColor=_GRAY_BG)

_THIN   = Side(style="thin", color="CCCCCC")
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)

_TITLE_FONT  = Font(bold=True, size=14, color=_NAVY)
_SUB_FONT    = Font(size=9,   color="808080")
_H_FONT      = Font(bold=True, size=10, color=_WHITE)
_T_FONT      = Font(bold=True, size=10, color=_WHITE)
_DATE_FONT   = Font(bold=True, size=10)
_NORM_FONT   = Font(size=10)
_SALE_FONT   = Font(size=10, color=_RED_TXT)
_SALE_T_FONT = Font(bold=True, size=10, color=_RED_TXT)

_CTR  = Alignment(horizontal="center", vertical="center", wrap_text=True)
_LEFT = Alignment(horizontal="left",   vertical="center")
_RGHT = Alignment(horizontal="right",  vertical="center")


# ── 헬퍼 ───────────────────────────────────────────────────────

def _c(ws, row, col, val, font=None, fill=None, align=None, fmt=None, border=True):
    cell = ws.cell(row=row, column=col, value=val)
    cell.font      = font  or _NORM_FONT
    cell.alignment = align or _CTR
    if fill:  cell.fill = fill
    if fmt:   cell.number_format = fmt
    if border: cell.border = _BORDER
    return cell


def _extract_time_slot(df: pd.DataFrame) -> pd.DataFrame:
    raw_hour = (
        df["order_time"].astype(str).str.strip()
        .str.extract(r"^(\d{2}):", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )
    raw_hour = raw_hour.where(raw_hour != 0, other=pd.NA)
    df = df.copy()
    df["time_slot"] = pd.cut(
        raw_hour, bins=[5, 15, 23], labels=["점심", "저녁"], right=True,
    )
    return df


# ── 데이터 로드 ────────────────────────────────────────────────

def _load_sales(ym: str) -> pd.DataFrame:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 없음: {UNIFIED_ROOT}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["sale_date"]   = pd.to_datetime(df["sale_date"], errors="coerce")
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0).astype(int)
    df["order_cnt"]   = pd.to_numeric(df["order_cnt"],   errors="coerce").fillna(0).astype(int)
    df["store"]    = df["store"].astype(str).str.strip()
    df["platform"] = df["platform"].fillna("").astype(str).str.strip()
    mask = (
        (df["store"] == STORE_NAME) &
        (df["platform"] == "홀") &
        (df["sale_date"].dt.strftime("%Y-%m") == ym)
    )
    df = df[mask].copy()
    return _extract_time_slot(df)  # 빈 df도 time_slot 컬럼 추가됨


def _load_marketing(ym: str) -> pd.DataFrame:
    if not MKT_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(MKT_CSV, dtype=str)
    df = df.rename(columns={"인스타_노출_traget": "인스타_노출_target"})
    df["기준일자"] = pd.to_datetime(df["기준일자"], errors="coerce").dt.date
    for col in ["플레이스_유입", "홍보물_배포", "쿠폰_회수율", "인스타_노출", "당근_노출", "네이버_오더"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df = df[df["기준일자"].apply(lambda d: d is not None and d.strftime("%Y-%m") == ym if d else False)]
    df = df.sort_values("기준일자").reset_index(drop=True)
    if "홍보물_배포" in df.columns:
        df["홍보물_배포_누적"] = df["홍보물_배포"].cumsum().astype(int)
    return df


# ── 일별 집계 ──────────────────────────────────────────────────

def _daily_rows(sales_df: pd.DataFrame, mkt_df: pd.DataFrame, all_dates: list) -> list:
    rows = []
    mkt_by_date = {}
    if not mkt_df.empty:
        for _, r in mkt_df.iterrows():
            if r["기준일자"]:
                mkt_by_date[r["기준일자"]] = r

    for d in all_dates:
        day_s  = sales_df[sales_df["sale_date"].dt.date == d]
        lunch  = day_s[day_s["time_slot"] == "점심"]
        dinner = day_s[day_s["time_slot"] == "저녁"]

        l_cnt  = int(lunch["order_cnt"].sum())
        l_sale = int(lunch["total_price"].sum())
        l_aov  = l_sale // l_cnt if l_cnt else 0
        d_cnt  = int(dinner["order_cnt"].sum())
        d_sale = int(dinner["total_price"].sum())
        d_aov  = d_sale // d_cnt if d_cnt else 0
        tot    = int(day_s["total_price"].sum())

        m = mkt_by_date.get(d)
        rows.append({
            "date":        d,
            "lunch_cnt":   l_cnt  or None,
            "lunch_aov":   l_aov  or None,
            "dinner_cnt":  d_cnt  or None,
            "dinner_aov":  d_aov  or None,
            "place":       int(m["플레이스_유입"])    if m is not None else None,
            "leaflet_cum": int(m["홍보물_배포_누적"]) if m is not None and "홍보물_배포_누적" in m.index else None,
            "coupon":      int(m["쿠폰_회수율"])      if m is not None else None,
            "insta":       int(m["인스타_노출"])      if m is not None else None,
            "karrot":      int(m["당근_노출"])        if m is not None else None,
            "naver":       int(m["네이버_오더"])      if m is not None else None,
            "daily_sale":  tot,
        })
    return rows


# ── Excel 작성 ─────────────────────────────────────────────────

HEADERS = [
    "날짜",
    "점심\n영수건수",
    "점심\n테이블단가",
    "저녁\n영수건수",
    "저녁\n테이블단가",
    "플레이스\n유입수",
    "홍보물\n배포(누적)",
    "쿠폰\n회수",
    "인스타\n노출",
    "당근\n노출",
    "네이버오더\n건수",
]
N_COLS = len(HEADERS)

COL_WIDTHS = [7, 8, 11, 8, 11, 9, 11, 7, 8, 8, 10]


def _write_excel(rows: list, daily_target: dict, ym: str, monthly_targets: dict) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = ym  # 예: "2026-06"

    # 열 너비
    for i, w in enumerate(COL_WIDTHS, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    mt = monthly_targets.get(ym, {})
    day_sale  = daily_target.get("sale", 0)
    mon_sale  = mt.get("sale", 0)
    day_sale_man = f"{day_sale / 10_000:.1f}만원"
    mon_sale_man = f"{mon_sale / 10_000:.0f}만원"

    # ── 제목 (row 1) ──────────────────────────────────────────
    ws.merge_cells(f"A1:{get_column_letter(N_COLS)}1")
    title = ws["A1"]
    title.value     = "일단위 트래킹 (매일 입력)"
    title.font      = _TITLE_FONT
    title.alignment = _LEFT
    ws.row_dimensions[1].height = 26

    # ── 부제 (row 2) ──────────────────────────────────────────
    ws.merge_cells(f"A2:{get_column_letter(N_COLS)}2")
    sub = ws["A2"]
    sub.value     = f"매일 수치 직접 입력 (매출 목표: 일 {day_sale_man} / 월 {mon_sale_man})"
    sub.font      = _SUB_FONT
    sub.alignment = _LEFT
    ws.row_dimensions[2].height = 14

    # ── 헤더 (row 3) ──────────────────────────────────────────
    for i, h in enumerate(HEADERS, 1):
        _c(ws, 3, i, h, font=_H_FONT, fill=_H_FILL, align=_CTR)
    ws.row_dimensions[3].height = 30

    # ── 목표 (row 4) ──────────────────────────────────────────
    goal = [
        "목표",
        daily_target.get("lunch_orders",  0),
        daily_target.get("lunch_aov",     0),
        daily_target.get("dinner_orders", 0),
        daily_target.get("dinner_aov",    0),
        daily_target.get("place",         0),
        daily_target.get("홍보물_배포",   0),
        daily_target.get("coupon",        0),
        daily_target.get("insta",         0),
        daily_target.get("karrot",        0),
        daily_target.get("naver",         0),
    ]
    for i, val in enumerate(goal, 1):
        if i == 1:
            _c(ws, 4, i, val, font=_T_FONT, fill=_T_FILL, align=_CTR)
        elif i in (3, 5):
            _c(ws, 4, i, val, font=_T_FONT, fill=_T_FILL, align=_RGHT, fmt="#,##0")
        else:
            _c(ws, 4, i, val, font=_T_FONT, fill=_T_FILL, align=_CTR, fmt="#,##0")
    ws.row_dimensions[4].height = 18

    # ── 데이터 행 ─────────────────────────────────────────────
    for idx, row in enumerate(rows):
        r     = 5 + idx
        is_odd = idx % 2 == 0   # 0-indexed → row 5,7,9... = 홀수 → 흰색
        bg    = None if is_odd else _G_FILL
        d     = row["date"]
        label = f"{d.month}/{d.day}"

        def dc(col, val, fmt=None, is_sale=False):
            font  = (_SALE_FONT if is_sale else _NORM_FONT)
            align = (_RGHT if is_sale or (fmt and "0" in fmt) else _CTR)
            _c(ws, r, col, val, font=font, fill=bg, align=align, fmt=fmt, border=True)

        # 날짜
        _c(ws, r, 1, label, font=_DATE_FONT, fill=bg, align=_CTR)

        # 점심 영수건수, 테이블단가
        dc(2,  row["lunch_cnt"],  "#,##0")
        dc(3,  row["lunch_aov"],  "#,##0")
        # 저녁 영수건수, 테이블단가
        dc(4,  row["dinner_cnt"], "#,##0")
        dc(5,  row["dinner_aov"], "#,##0")
        # 마케팅
        dc(6,  row["place"],        "#,##0")
        dc(7,  row["leaflet_cum"],  "#,##0")
        dc(8,  row["coupon"],       "#,##0")
        dc(9,  row["insta"],        "#,##0")
        dc(10, row["karrot"],       "#,##0")
        dc(11, row["naver"],        "#,##0")

        ws.row_dimensions[r].height = 17

    # ── 저장 ─────────────────────────────────────────────────
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUTPUT_XLSX)


# ── 메인 함수 ──────────────────────────────────────────────────

def build_daily_tracking_excel(monthly_targets: dict,
                                marketing_monthly_targets: dict,
                                daily_target: dict) -> str:
    today = date.today()
    ym    = today.strftime("%Y-%m")
    year, month = today.year, today.month
    days_in_month = calendar.monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    sales_df = _load_sales(ym)
    mkt_df   = _load_marketing(ym)

    rows = _daily_rows(sales_df, mkt_df, all_dates)
    _write_excel(rows, daily_target, ym, monthly_targets)

    win_path = (
        r"C:\Users\민준\OneDrive - 주식회사 도리당"
        r"\data\mart\hall_sales_target\hall_daily_report.xlsx"
    )
    logger.info("일단위 트래킹 저장 완료 (%s, %d일치)\nWindows: %s",
                ym, days_in_month, win_path)
    return f"완료: {ym} ({days_in_month}일) → {win_path}"
