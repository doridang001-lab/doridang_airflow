"""
리뷰 데이터마트 집계 파이프라인.

입력:
- ANALYTICS_DB/toorder_review/toorder_voc_YYYYMM.parquet (월별 누적)
- ANALYTICS_DB/toorder_review/toorder_voc_YYYYMMDD.parquet (레거시 일별 누적)

출력:
- MART_DB/unified_review/unified_review.parquet
  컬럼: 작성일자, 매장명, 토픽, 감정수준, 언급수

실행 모드:
- mode="all"      → 전체 parquet 재집계 (conf backfill=true)
- mode="lookback" → 오늘 기준 최근 days일치 파일만 (기본 30일)
- mode="date"     → 특정 날짜 1개 파일 (conf sale_date)
- mode="range"    → 지정 기간 파일 (conf start_date + end_date)
"""

import logging
import numbers
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    TOORDER_REVIEW_ANALYTICS_DIR,
    UNIFIED_REVIEW_MART_DIR,
)


logger = logging.getLogger(__name__)

_FILE_DATE_RE = re.compile(r"toorder_voc_(\d{8})\.parquet$")
_FILE_MONTH_RE = re.compile(r"toorder_voc_(\d{6})\.parquet$")
_GROUP_KEYS = ["작성일자", "매장명", "토픽", "감정수준"]
_REVIEW_COUNT_DEDUP_KEYS = ["작성일자", "매장명", "채널", "작성자", "리뷰내용"]
_MIN_REVIEW_DATE = date(2020, 1, 1)
_MAX_REVIEW_DATE = date(2100, 12, 31)


def _parse_file_date(path: Path) -> date | None:
    m = _FILE_DATE_RE.search(path.name)
    if not m:
        return None
    try:
        return date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
    except ValueError:
        return None


def _parse_file_month_range(path: Path) -> tuple[date, date] | None:
    m = _FILE_MONTH_RE.search(path.name)
    if not m:
        return None
    try:
        year = int(m.group(1)[:4])
        month = int(m.group(1)[4:6])
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date(year, month + 1, 1) - timedelta(days=1)
        return start, end
    except ValueError:
        return None


def _file_overlaps_range(path: Path, start_dt: date, end_dt: date) -> bool:
    parsed_date = _parse_file_date(path)
    if parsed_date is not None:
        return start_dt <= parsed_date <= end_dt
    parsed_month = _parse_file_month_range(path)
    if parsed_month is None:
        return False
    month_start, month_end = parsed_month
    return month_start <= end_dt and month_end >= start_dt


def _normalize_date_only(value: str | date | int | float | None) -> date | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        if isinstance(value, numbers.Real) and not isinstance(value, bool):
            n = int(value)
            if float(value) == float(n) and 10_000_000 <= n <= 99_999_999:
                s = str(n)
                return date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        s = str(value).strip()
        if not s or s == "nan":
            return None
        if len(s) == 8 and s.isdigit():
            return date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        return date.fromisoformat(s)
    except ValueError:
        return None


def _format_written_date_text(value: str | date | int | float | None) -> str | None:
    normalized = _normalize_date_only(value)
    if normalized is not None:
        return normalized.isoformat()

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _coerce_written_date_to_text(df: pd.DataFrame) -> pd.DataFrame:
    if "작성일자" not in df.columns:
        return df

    df = df.copy()
    df["작성일자"] = df["작성일자"].map(_format_written_date_text).astype("string")
    return df


def _drop_invalid_written_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "작성일자" not in df.columns:
        return df

    normalized = df["작성일자"].map(_normalize_date_only)
    valid_mask = normalized.map(
        lambda value: value is not None and _MIN_REVIEW_DATE <= value <= _MAX_REVIEW_DATE
    )
    dropped = int((~valid_mask).sum())
    if dropped:
        logger.warning("비정상 작성일자 행 제거: %d행", dropped)
    return df[valid_mask].copy()


def _filter_written_date_range(
    df: pd.DataFrame,
    start_dt: date | None = None,
    end_dt: date | None = None,
) -> pd.DataFrame:
    if "작성일자" not in df.columns or (start_dt is None and end_dt is None):
        return df
    normalized = df["작성일자"].map(_normalize_date_only)
    mask = pd.Series(True, index=df.index)
    if start_dt is not None:
        mask &= normalized.map(lambda value: value is not None and value >= start_dt)
    if end_dt is not None:
        mask &= normalized.map(lambda value: value is not None and value <= end_dt)
    return df[mask].copy()


def _ensure_target_date_rows(df: pd.DataFrame, mode: str, target_date: str | None) -> pd.DataFrame:
    """mode=date에서 target_date에 행이 없으면 전날 데이터를 복제해 target_date 한 줄로 보정."""
    if mode != "date" or "작성일자" not in df.columns:
        return df

    target_dt = _normalize_date_only(target_date)
    if target_dt is None:
        return df

    current_dates = pd.to_datetime(df["작성일자"], errors="coerce").dt.date
    if current_dates.isna().all():
        return df

    if target_dt in set(current_dates.dropna()):
        return df

    fallback_dt = target_dt - timedelta(days=1)
    fallback_rows = df[current_dates == fallback_dt].copy()
    if fallback_rows.empty:
        return df

    fallback_rows["작성일자"] = target_dt
    return pd.concat([df, fallback_rows], ignore_index=True)


def _require_date(value: str | date | int | float | None, field_name: str) -> date:
    parsed = _normalize_date_only(value)
    if parsed is None:
        raise ValueError(f"{field_name} must be YYYY-MM-DD format: {value}")
    return parsed


def _collect_files(
    mode: str,
    days: int = 30,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[Path]:
    all_files = sorted(TOORDER_REVIEW_ANALYTICS_DIR.glob("toorder_voc_*.parquet"))
    if not all_files:
        return []

    if mode == "all":
        return all_files

    if mode == "date" and target_date:
        target_dt = _require_date(target_date, "target_date")
        return [f for f in all_files if _file_overlaps_range(f, target_dt, target_dt)]

    if mode == "range" and start_date and end_date:
        start_dt = _require_date(start_date, "start_date")
        end_dt = _require_date(end_date, "end_date")
        if start_dt > end_dt:
            raise ValueError(f"start_date > end_date: {start_dt} > {end_dt}")
        matched = [
            f
            for f in all_files
            if _file_overlaps_range(f, start_dt, end_dt)
        ]
        return matched

    # lookback: 오늘 기준 최근 days일
    cutoff = date.today() - timedelta(days=days)
    return [f for f in all_files if _file_overlaps_range(f, cutoff, date.max)]


def _file_target_date(path: Path) -> str | None:
    parsed = _parse_file_date(path)
    return parsed.isoformat() if parsed else None


def _resolve_filter_range(
    mode: str,
    days: int = 30,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[date | None, date | None]:
    if mode == "date" and target_date:
        target_dt = _require_date(target_date, "target_date")
        return target_dt, target_dt
    if mode == "range" and start_date and end_date:
        return _require_date(start_date, "start_date"), _require_date(end_date, "end_date")
    if mode == "lookback":
        return date.today() - timedelta(days=days), None
    return None, None


def _load_and_aggregate(
    files: list[Path],
    mode: str = "lookback",
    days: int = 30,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f, columns=["번호", *_GROUP_KEYS]))
        except Exception as e:
            logger.warning("파일 로드 실패 %s: %s", f.name, e)

    if not dfs:
        return pd.DataFrame(columns=[*_GROUP_KEYS, "언급수"])

    df = pd.concat(dfs, ignore_index=True)
    if mode == "date":
        df = _ensure_target_date_rows(df, mode=mode, target_date=target_date)
    df = _coerce_written_date_to_text(df)
    df = _drop_invalid_written_dates(df)
    start_dt, end_dt = _resolve_filter_range(
        mode=mode,
        days=days,
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _filter_written_date_range(df, start_dt=start_dt, end_dt=end_dt)

    result = (
        df.groupby(_GROUP_KEYS, dropna=False)["번호"]
        .count()
        .reset_index(name="언급수")
    )
    return result


def _save_mart(df: pd.DataFrame) -> int:
    """작성일자별로 분리하여 unified_review_YYMMDD.parquet 저장. 저장 파일 수 반환."""
    UNIFIED_REVIEW_MART_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0
    for date_val, group in df.groupby("작성일자", dropna=False):
        normalized_date = _normalize_date_only(date_val)
        date_str = normalized_date.strftime("%Y%m%d") if normalized_date else "0"
        date_str = date_str.zfill(8)
        short = date_str[2:]
        out_path = UNIFIED_REVIEW_MART_DIR / f"unified_review_{short}.parquet"
        group.to_parquet(out_path, index=False)
        logger.info("저장: %s (%d행)", out_path.name, len(group))
        saved += 1
    return saved


def run_review(
    mode: str = "lookback",
    days: int = 30,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    files = _collect_files(
        mode=mode,
        days=days,
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
    )
    if not files:
        logger.warning("집계할 parquet 파일 없음 (mode=%s, days=%d)", mode, days)
        return f"스킵: 파일 없음 (mode={mode})"

    logger.info("집계 대상 파일 %d개 (mode=%s)", len(files), mode)
    df = _load_and_aggregate(
        files,
        mode=mode,
        days=days,
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty:
        logger.warning("집계 결과 없음")
        return f"스킵: 집계 결과 없음 (mode={mode})"

    saved_files = _save_mart(df)
    return f"저장 완료: {len(df)}행, {saved_files}개 파일 (mode={mode}, 원천 {len(files)}개)"


def run_lookback_review(days: int = 30) -> str:
    return run_review(mode="lookback", days=days)


def backfill_review() -> str:
    return run_review(mode="all")


def build_daily_review_count(
    mode: str = "lookback",
    days: int = 30,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """일별 매장별 리뷰 수 집계 (실제 리뷰키 기준 중복 제거). unified_review_count.parquet upsert 저장."""
    files = _collect_files(
        mode=mode,
        days=days,
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
    )
    if not files:
        return f"스킵: 파일 없음 (mode={mode})"

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f, columns=_REVIEW_COUNT_DEDUP_KEYS))
        except Exception as e:
            logger.warning("파일 로드 실패 %s: %s", f.name, e)

    if not dfs:
        return "스킵: 로드 실패"

    df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=_REVIEW_COUNT_DEDUP_KEYS)
    if mode == "date":
        df = _ensure_target_date_rows(df, mode=mode, target_date=target_date)
    df = _coerce_written_date_to_text(df)
    df = _drop_invalid_written_dates(df)
    start_dt, end_dt = _resolve_filter_range(
        mode=mode,
        days=days,
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _filter_written_date_range(df, start_dt=start_dt, end_dt=end_dt)
    count_df = (
        df.groupby(["작성일자", "매장명", "채널"], dropna=False)
        .size()
        .reset_index(name="리뷰수")
    )

    out_path = UNIFIED_REVIEW_MART_DIR / "unified_review_count.parquet"
    UNIFIED_REVIEW_MART_DIR.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and mode != "all":
        existing = pd.read_parquet(out_path)
        existing = _coerce_written_date_to_text(existing)
        existing = _drop_invalid_written_dates(existing)
        new_dates = count_df["작성일자"].unique()
        existing = existing[~existing["작성일자"].isin(new_dates)]
        count_df = pd.concat([existing, count_df], ignore_index=True)
        count_df = count_df.sort_values(["작성일자", "매장명", "채널"]).reset_index(drop=True)

    count_df = _coerce_written_date_to_text(count_df)
    count_df.to_parquet(out_path, index=False)

    total = int(count_df["리뷰수"].sum())
    logger.info("일별 리뷰수 집계 완료: %d행, 총 %d건", len(count_df), total)
    return f"일별 리뷰수 저장: {len(count_df)}행, 총 {total:,}건 (원천 {len(files)}개 파일)"
