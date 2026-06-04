"""
리뷰 데이터마트 집계 파이프라인.

입력:
- ANALYTICS_DB/toorder_review/toorder_voc_YYYYMMDD.parquet (일별 누적)

출력:
- MART_DB/unified_review/unified_review.parquet
  컬럼: 작성일자, 매장명, 토픽, 감정수준, 언급수

실행 모드:
- mode="all"      → 전체 parquet 재집계 (conf backfill=true)
- mode="lookback" → 오늘 기준 최근 days일치 파일만 (기본 30일)
- mode="date"     → 특정 날짜 1개 파일 (conf sale_date)
"""

import logging
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    TOORDER_REVIEW_ANALYTICS_DIR,
    UNIFIED_REVIEW_MART_DIR,
)

logger = logging.getLogger(__name__)

_FILE_DATE_RE = re.compile(r"toorder_voc_(\d{8})\.parquet$")
_GROUP_KEYS = ["작성일자", "매장명", "토픽", "감정수준"]


def _parse_file_date(path: Path) -> date | None:
    m = _FILE_DATE_RE.search(path.name)
    if not m:
        return None
    try:
        return date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
    except ValueError:
        return None


def _collect_files(mode: str, days: int = 30, target_date: str | None = None) -> list[Path]:
    all_files = sorted(TOORDER_REVIEW_ANALYTICS_DIR.glob("toorder_voc_*.parquet"))
    if not all_files:
        return []

    if mode == "all":
        return all_files

    if mode == "date" and target_date:
        date_str = target_date.replace("-", "")
        matched = [f for f in all_files if date_str in f.name]
        return matched

    # lookback: 오늘 기준 최근 days일
    cutoff = date.today() - timedelta(days=days)
    return [f for f in all_files if (d := _parse_file_date(f)) is not None and d >= cutoff]


def _load_and_aggregate(files: list[Path]) -> pd.DataFrame:
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f, columns=["번호", *_GROUP_KEYS]))
        except Exception as e:
            logger.warning("파일 로드 실패 %s: %s", f.name, e)

    if not dfs:
        return pd.DataFrame(columns=[*_GROUP_KEYS, "언급수"])

    df = pd.concat(dfs, ignore_index=True)
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
        # 작성일자 → YYMMDD (date/Timestamp/str 모두 처리)
        date_str = str(date_val).replace("-", "")  # "2026-06-01" → "20260601"
        short = date_str[2:]                        # "260601"
        out_path = UNIFIED_REVIEW_MART_DIR / f"unified_review_{short}.parquet"
        group.to_parquet(out_path, index=False)
        logger.info("저장: %s (%d행)", out_path.name, len(group))
        saved += 1
    return saved


def run_review(mode: str = "lookback", days: int = 30, target_date: str | None = None) -> str:
    files = _collect_files(mode=mode, days=days, target_date=target_date)
    if not files:
        logger.warning("집계할 parquet 파일 없음 (mode=%s, days=%d)", mode, days)
        return f"스킵: 파일 없음 (mode={mode})"

    logger.info("집계 대상 파일 %d개 (mode=%s)", len(files), mode)
    df = _load_and_aggregate(files)

    if df.empty:
        logger.warning("집계 결과 없음")
        return f"스킵: 집계 결과 없음 (mode={mode})"

    saved_files = _save_mart(df)
    return f"저장 완료: {len(df)}행, {saved_files}개 파일 (mode={mode}, 원천 {len(files)}개)"


def run_lookback_review(days: int = 30) -> str:
    return run_review(mode="lookback", days=days)


def backfill_review() -> str:
    return run_review(mode="all")
