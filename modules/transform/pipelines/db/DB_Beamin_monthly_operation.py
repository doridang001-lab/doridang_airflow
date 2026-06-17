"""배민 매장 월간 영업시간·영업일수 요약.

입력:
  analytics/baemin_macro/shop_change/brand={}/store={}/ym={ym}/shop_change.csv  (필수)
  analytics/baemin_macro/shop_operation/brand={}/store={}/ym={ym}/shop_operation.csv (선택)

출력:
  analytics/baemin_macro/monthly_operation/brand={}/store={}/ym={ym}/monthly_operation.csv

컬럼:
  ym, brand, store, store_id,
  기준운영시간h,          # 평균 일일 운영시간 (요일별 가중 평균)
  영업일수,               # 달력일수 - 정기휴무 - 임시휴무
  정기휴무수, 임시휴무수,
  가게중지횟수,           # 가게 전체 임시중지 건수 (가게 영업 임시중지 ≠ 없음)
  가게중지시간h,          # 가게 전체 임시중지 시간 합산
  배민중지횟수,           # 배민only 임시중지 건수 (가게=없음 AND 배민≠없음)
  배민중지시간h,          # 배민only 임시중지 시간 합산
  가게총영업시간h,        # 영업일수×기준시간 - 가게중지시간h
  배민총영업시간h         # 가게총영업시간h - 배민중지시간h

임시휴무 취소 처리:
  SET 이벤트(변후=날짜)  → ADD
  CANCEL 이벤트(변후=없음, 변전=날짜) → 변경시간 이전 날짜는 실제 휴무였으므로 유지,
                                        변경시간 이후 날짜만 REMOVE
  교체 이벤트(변후=새날짜, 변전=구날짜) → ADD 변후 + 위 규칙으로 REMOVE 변전
"""

import calendar
import logging
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.utility.paths import (
    BAEMIN_MONTHLY_OPERATION_DB,
    BAEMIN_SHOP_CHANGE_DB,
    BAEMIN_SHOP_OPERATION_DB,
)
from modules.transform.pipelines.db.beamin_store_io import find_tables, read_file, read_table

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")

MONTHLY_COLUMNS = [
    "ym", "brand", "store", "store_id",
    "기준운영시간h",
    "영업일수", "정기휴무수", "임시휴무수",
    "가게중지횟수", "가게중지시간h",
    "배민중지횟수", "배민중지시간h",
    "가게총영업시간h", "배민총영업시간h",
]

_WEEKDAY_KR = {
    "월요일": 0, "화요일": 1, "수요일": 2, "목요일": 3,
    "금요일": 4, "토요일": 5, "일요일": 6,
    "월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6,
}


# ---------------------------------------------------------------------------
# 메인 함수
# ---------------------------------------------------------------------------

def compute_monthly_operation(brand: str, store: str, store_id: str, ym: str) -> Path | None:
    """store_id/ym 기준으로 월간 영업 요약을 계산해 CSV로 저장한다.

    변경 이력 없는 달 = 정상 운영 전일로 처리.
    """
    ch_df = _load_change_df_for_ym(brand, store, store_id, ym)
    if ch_df is None or ch_df.empty:
        logger.info("변경 이력 없음 (정상 운영으로 처리): %s/%s/%s/%s", brand, store, store_id, ym)
        ch_df = pd.DataFrame(columns=[
            "대분류", "변경시간",
            "변경후_정기휴무일", "변경후_임시휴무일",
            "변경전_임시휴무일",
            "변경후_가게중지", "변경후_배민배달",
        ])

    # 요일별 기준 운영시간
    hours_by_weekday = _get_daily_hours_by_weekday(brand, store, store_id, ym)

    # 휴무일·임시중지 이벤트 파싱
    regular_weekdays: list[int] = []
    temp_holiday_dates: set[date] = set()
    store_closure_entries: list[float] = []
    baemin_only_entries: list[float] = []

    # 휴무일: 교차월 필요 (정기휴무 최신 상태 + 이전 달 설정된 임시휴무 범위)
    holiday_rows = ch_df[ch_df.get("대분류", pd.Series(dtype=str)).str.contains("휴무일", na=False)]
    # 임시중지: 해당 ym 이벤트만 (2월 이벤트가 5월 계산에 포함되지 않도록)
    _ym_time_pat = rf"{ym[:4]}\.\s*0?{int(ym[5:7])}\."
    pause_rows = ch_df[
        ch_df.get("대분류", pd.Series(dtype=str)).str.contains("영업임시중지", na=False)
        & ch_df.get("변경시간", pd.Series(dtype=str)).str.contains(_ym_time_pat, na=False, regex=True)
    ]

    # 정기휴무: 해당 ym 마지막 날 이전(≤)의 이벤트 중 가장 최신 변경후_정기휴무일
    # - CSV는 최신 순(내림차순)이므로 reversed() 없이 순회 = 최신 → 오래된 순
    # - 단, 미래 이벤트(ym 이후)는 제외: 2025-12 계산 시 2026-05 이벤트 미사용
    ym_cutoff = f"{ym[:4]}. {int(ym[5:7]):02d}."  # e.g. "2026. 05."
    for _, hr in holiday_rows.sort_values("변경시간", ascending=False).iterrows():
        # 변경시간이 target ym보다 이후인 이벤트는 건너뜀
        chg = hr.get("변경시간", "")
        chg_date = _parse_change_date(chg)
        ym_last_date = date(int(ym[:4]), int(ym[5:7]), calendar.monthrange(int(ym[:4]), int(ym[5:7]))[1])
        if chg_date and chg_date > ym_last_date:
            continue
        val = hr.get("변경후_정기휴무일", "")
        if val and val != "없음":
            regular_weekdays = _parse_regular_holiday_weekdays(val)
            break

    # 임시휴무: 시간순 정렬 후 SET / CANCEL(부분) 처리
    if not holiday_rows.empty and "변경시간" in holiday_rows.columns:
        holiday_rows_sorted = holiday_rows.sort_values("변경시간", ascending=True)
        for _, row in holiday_rows_sorted.iterrows():
            after = row.get("변경후_임시휴무일", "")
            before = row.get("변경전_임시휴무일", "")
            # SET: 변후에 날짜 있으면 ADD
            if after and after != "없음":
                temp_holiday_dates.update(_parse_temp_holiday_dates(after, ym))
            # CANCEL/교체: 변전에 날짜 있으면 REMOVE — 단, 변경시간 이전 날짜는 이미 지난 휴무이므로 유지
            if before and before != "없음":
                cancel_date = _parse_change_date(row.get("변경시간", ""))
                cancelled = _parse_temp_holiday_dates(before, ym)
                if cancel_date:
                    # 변경시간 이후 날짜만 제거 (이전 날짜는 실제 휴무였음)
                    temp_holiday_dates -= {d for d in cancelled if d >= cancel_date}
                else:
                    temp_holiday_dates -= cancelled

    # 임시중지: 가게중지 vs 배민only 분리
    # 배민은 동일 이벤트를 여러 row로 기록 → (변경시간, 시간범위) 기준으로 중복 제거
    seen_store_closures: set[tuple[str, str]] = set()
    seen_baemin_only: set[tuple[str, str]] = set()

    for _, row in pause_rows.iterrows():
        chg_time = row.get("변경시간", "")
        store_val = row.get("변경후_가게중지", "")
        baemin_val = row.get("변경후_배민배달", "")

        if store_val and store_val != "없음":
            key = (chg_time, store_val)
            if key not in seen_store_closures:
                seen_store_closures.add(key)
                h = _parse_suspension_hours(store_val)
                if h > 0:
                    store_closure_entries.append(h)

        # 배민only: 가게중지=없음 AND 배민≠없음
        if (not store_val or store_val == "없음") and baemin_val and baemin_val != "없음":
            key = (chg_time, baemin_val)
            if key not in seen_baemin_only:
                seen_baemin_only.add(key)
                h = _parse_suspension_hours(baemin_val)
                if h > 0:
                    baemin_only_entries.append(h)

    # 월 기준 날짜 계산 (진행 중인 달은 오늘까지만)
    year, month = int(ym[:4]), int(ym[5:7])
    total_days = calendar.monthrange(year, month)[1]
    today_kst = pendulum.now(KST).date()
    if year == today_kst.year and month == today_kst.month:
        total_days = min(total_days, today_kst.day)
    regular_dates = {
        date(year, month, d)
        for d in range(1, total_days + 1)
        if date(year, month, d).weekday() in regular_weekdays
    }
    regular_off = len(regular_dates)
    temp_off = len(temp_holiday_dates - regular_dates)
    operating_days = total_days - regular_off - temp_off

    # 요일별 총 기준 운영시간
    holiday_day_set = regular_dates | temp_holiday_dates
    base_total = sum(
        hours_by_weekday.get(date(year, month, d).weekday(), 0.0)
        for d in range(1, total_days + 1)
        if date(year, month, d) not in holiday_day_set
    )
    avg_daily_hours = round(base_total / operating_days, 2) if operating_days > 0 else 0.0

    store_closure_h = round(sum(store_closure_entries), 2)
    baemin_only_h = round(sum(baemin_only_entries), 2)
    # 임시중지 시간이 영업일 총시간을 초과할 경우 0으로 클램프
    store_total_h = round(max(0.0, base_total - store_closure_h), 2)
    baemin_total_h = round(max(0.0, store_total_h - baemin_only_h), 2)

    row_data = {
        "ym": ym, "brand": brand, "store": store, "store_id": store_id,
        "기준운영시간h": avg_daily_hours,
        "영업일수": operating_days, "정기휴무수": regular_off, "임시휴무수": temp_off,
        "가게중지횟수": len(store_closure_entries), "가게중지시간h": store_closure_h,
        "배민중지횟수": len(baemin_only_entries), "배민중지시간h": baemin_only_h,
        "가게총영업시간h": store_total_h, "배민총영업시간h": baemin_total_h,
    }

    out_dir = (
        BAEMIN_MONTHLY_OPERATION_DB
        / f"brand={brand}" / f"store={store}"
        / f"ym={ym}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "monthly_operation.csv"
    pd.DataFrame([row_data], columns=MONTHLY_COLUMNS).to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(
        "월간 운영 요약: %s/%s/%s/%s → 영업일수=%d 가게총=%.1fh 배민총=%.1fh",
        brand, store, store_id, ym, operating_days, store_total_h, baemin_total_h,
    )
    return out_path


# ---------------------------------------------------------------------------
# 데이터 로드
# ---------------------------------------------------------------------------

def _load_change_df_for_ym(brand: str, store: str, store_id: str, ym: str) -> pd.DataFrame | None:
    """store_id 기준으로 전체 변경 이력 DataFrame 반환.

    직접 파티션 CSV(ym=) 또는 최신 ym CSV에서 store_id 필터링만 수행.
    ym 시간 필터는 적용하지 않음 — 정기휴무 상태가 이전 달에 설정되었어도 올바르게 참조되도록.
    ym별 시간 필터링은 호출부(임시중지 ym 필터 등)에서 처리.
    """
    direct = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "shop_change"
    direct_df = read_table(direct)
    if direct_df is not None:
        return _filter_by_store_id(direct_df.fillna(""), store_id)

    # 폴백: 가장 최신 ym 파일에서 store_id 필터만 적용 (시간 필터 없음)
    base = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}"
    for csv in find_tables(base, "ym=*/shop_change")[::-1]:
        df = read_file(csv).fillna("")
        if "변경시간" not in df.columns:
            continue
        result = _filter_by_store_id(df, store_id)
        if result is not None and not result.empty:
            return result
    return None


def _filter_by_store_id(df: pd.DataFrame, store_id: str) -> pd.DataFrame | None:
    """DataFrame에서 해당 store_id 행만 반환. store_id 컬럼 없으면 그대로 반환."""
    if df is None or df.empty:
        return None
    if "store_id" in df.columns:
        filtered = df[df["store_id"].astype(str) == str(store_id)]
        return filtered if not filtered.empty else None
    return df


# ---------------------------------------------------------------------------
# 기준 운영시간 (요일별)
# ---------------------------------------------------------------------------

def _get_daily_hours_by_weekday(brand: str, store: str, store_id: str, ym: str) -> dict[int, float]:
    """store_id 기준 요일별 일일 운영시간 반환 {0(월)~6(일): hours}.

    우선순위: shop_operation(store_id 필터) → shop_change 운영시간 히스토리 scan.
    """
    # 1) shop_operation (store_id 필터링)
    op_stem = BAEMIN_SHOP_OPERATION_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "shop_operation"
    op_df = read_table(op_stem)
    if op_df is not None:
        df = op_df.fillna("")
        if "store_id" in df.columns:
            df = df[df["store_id"].astype(str) == str(store_id)]
        if not df.empty:
            h = _parse_hours_by_weekday(df.iloc[-1].get("운영시간", ""))
            if h:
                return h

    # 2) 현재 ym shop_change 운영시간 변경 (store_id 필터)
    ch_paths = find_tables(BAEMIN_SHOP_CHANGE_DB, f"brand={brand}/store={store}/ym={ym}/shop_change")
    if ch_paths:
        h = _extract_weekday_hours_from_change_csv(ch_paths[0], store_id)
        if h:
            return h

    # 3) 히스토리 scan (최신 ym 우선)
    base = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}"
    for csv in find_tables(base, "ym=*/shop_change")[::-1]:
        if csv.parent.name == f"ym={ym}":
            continue
        h = _extract_weekday_hours_from_change_csv(csv, store_id)
        if h:
            return h

    return {}


def _extract_weekday_hours_from_change_csv(csv_path: Path, store_id: str) -> dict[int, float]:
    df = read_file(csv_path).fillna("")
    if "store_id" in df.columns:
        df = df[df["store_id"].astype(str) == str(store_id)]
    rows = df[df.get("대분류", pd.Series(dtype=str)).str.contains("운영시간", na=False)]
    for val in rows["변경후_운영시간"].tolist():
        if val:
            h = _parse_hours_by_weekday(val)
            if h:
                return h
    return {}


def _parse_hours_by_weekday(text: str) -> dict[int, float]:
    """운영시간 텍스트 → {요일index: hours}."""
    if not text:
        return {}
    result: dict[int, float] = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        weekday_idx = _detect_weekday(line)
        h = _parse_single_line_hours(line)
        if h <= 0:
            continue
        if weekday_idx is None:
            return {i: h for i in range(7)}
        result[weekday_idx] = h
    return result


def _detect_weekday(line: str) -> int | None:
    for kor, idx in _WEEKDAY_KR.items():
        if line.startswith(kor):
            return idx
    return None


def _parse_single_line_hours(line: str) -> float:
    line = re.sub(r"\(다음\s*날\)", "", line)
    m12 = re.search(
        r"(오전|오후)\s*(\d{1,2}):(\d{2})\s*[~～]\s*(오전|오후)\s*(\d{1,2}):(\d{2})", line
    )
    if m12:
        ap1, h1, m1, ap2, h2, m2 = m12.groups()
        start = _to_minutes_12h(ap1, int(h1), int(m1))
        end = _to_minutes_12h(ap2, int(h2), int(m2))
        if end <= start:
            end += 24 * 60
        return round((end - start) / 60, 2)
    m24 = re.search(r"(\d{1,2}):(\d{2})\s*[~～]\s*(\d{1,2}):(\d{2})", line)
    if m24:
        h1, m1, h2, m2 = (int(x) for x in m24.groups())
        start = h1 * 60 + m1
        end = h2 * 60 + m2
        if end <= start:
            end += 24 * 60
        return round((end - start) / 60, 2)
    return 0.0


def _to_minutes_12h(ampm: str, hour: int, minute: int) -> int:
    if ampm == "오후" and hour != 12:
        hour += 12
    elif ampm == "오전" and hour == 12:
        hour = 0
    return hour * 60 + minute


# ---------------------------------------------------------------------------
# 파싱 헬퍼
# ---------------------------------------------------------------------------

def _parse_change_date(text: str) -> date | None:
    """'2026. 05. 05 18:43' → date(2026, 5, 5)."""
    m = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def _parse_suspension_hours(text: str) -> float:
    """'26.05.28 22:01 ~ 26.05.28 23:01' → 1.0."""
    if not text or text == "없음":
        return 0.0
    m = re.search(
        r"(\d{2})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})\s*[~～]\s*(\d{2})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})",
        text,
    )
    if not m:
        return 0.0
    y1, mo1, d1, h1, mi1, y2, mo2, d2, h2, mi2 = (int(x) for x in m.groups())
    day_diff = (date(2000 + y2, mo2, d2) - date(2000 + y1, mo1, d1)).days
    total_minutes = day_diff * 24 * 60 + (h2 * 60 + mi2) - (h1 * 60 + mi1)
    return round(max(0, total_minutes) / 60, 2)


def _parse_temp_holiday_dates(text: str, ym: str) -> set[date]:
    """'2026-05-27 ~ 2026-05-28' → {date(...)} (해당 ym만)."""
    if not text or text == "없음":
        return set()
    year, month = int(ym[:4]), int(ym[5:7])
    result: set[date] = set()
    for segment in text.split(","):
        segment = segment.strip()
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})\s*[~～]\s*(\d{4})-(\d{2})-(\d{2})", segment)
        if not m:
            single = re.search(r"(\d{4})-(\d{2})-(\d{2})", segment)
            if single:
                y, mo, d = int(single.group(1)), int(single.group(2)), int(single.group(3))
                if y == year and mo == month:
                    result.add(date(y, mo, d))
            continue
        y1, mo1, d1, y2, mo2, d2 = (int(x) for x in m.groups())
        cur = date(y1, mo1, d1)
        end = date(y2, mo2, d2)
        while cur <= end:
            if cur.year == year and cur.month == month:
                result.add(cur)
            cur += timedelta(days=1)
    return result


def _parse_regular_holiday_weekdays(text: str) -> list[int]:
    """'매주 화요일, 매주 일요일' → [1, 6]."""
    if not text or text == "없음":
        return []
    weekdays = []
    for token in re.split(r"[,，\s]+", text):
        for kor, idx in _WEEKDAY_KR.items():
            if kor in token:
                if idx not in weekdays:
                    weekdays.append(idx)
                break
    return weekdays


def _count_weekdays_in_month(year: int, month: int, weekdays: list[int]) -> int:
    if not weekdays:
        return 0
    total = calendar.monthrange(year, month)[1]
    return sum(1 for d in range(1, total + 1) if date(year, month, d).weekday() in weekdays)
