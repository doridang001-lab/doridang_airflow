"""배민 수집 저장소 IO 헬퍼 — OneDrive 적재는 parquet, 읽기는 parquet 우선·CSV 폴백.

수집/다운로드 단계는 CSV를 그대로 쓰더라도, OneDrive analytics 파티션에 적재할 때는
parquet(zstd)로 저장해 용량을 줄인다. 과도기(기존 CSV가 남아있는 상태)에도 안전하도록
읽기는 parquet가 있으면 우선 사용하고 없으면 CSV로 폴백한다.

기존 동작과의 호환을 위해 모든 셀은 **문자열(dtype=str)** 로 다룬다
(주문번호 앞자리 0, 금액 문자열 보존).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "zstd"


def _as_str_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("").astype(str)


def order_ym(series: pd.Series) -> pd.Series:
    """배민 주문시각에서 주문 월(YYYY-MM)을 추출한다. 파싱 실패는 빈 문자열."""
    matched = series.fillna("").astype(str).str.extract(r"^\s*(\d{4})\.\s*(\d{1,2})\.")
    return (matched[0] + "-" + matched[1].str.zfill(2)).fillna("")


def order_date(series: pd.Series) -> pd.Series:
    """배민 주문시각에서 주문일자(YYYY-MM-DD)를 추출한다. 파싱 실패는 빈 문자열."""
    matched = series.fillna("").astype(str).str.extract(
        r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\."
    )
    return (
        matched[0]
        + "-"
        + matched[1].str.zfill(2)
        + "-"
        + matched[2].str.zfill(2)
    ).fillna("")


def replace_covered_date_range(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
    existing_dates: pd.Series,
    new_dates: pd.Series,
    order_key: str = "주문번호",
) -> tuple[pd.DataFrame, dict]:
    """새 데이터가 커버하는 주문일자 구간의 기존 행을 통째로 교체한다."""
    new_data = _as_str_df(new_df.copy())
    coverage_dates = new_dates.fillna("").astype(str).str.strip().reset_index(drop=True)
    valid_coverage = coverage_dates[coverage_dates.str.fullmatch(r"\d{4}-\d{2}-\d{2}")]
    info = {
        "range": None,
        "removed": 0,
        "covered_dates": [],
        "shrunk_dates": [],
        "partial_shrunk_dates": [],
        "shrink_details": {},
    }

    if valid_coverage.empty:
        if existing is None or existing.empty:
            return new_data, info
        return pd.concat([_as_str_df(existing.copy()), new_data], ignore_index=True), info

    min_date = valid_coverage.min()
    max_date = valid_coverage.max()
    covered_dates = pd.date_range(min_date, max_date).strftime("%Y-%m-%d").tolist()
    info["range"] = (min_date, max_date)
    info["covered_dates"] = covered_dates

    if existing is None or existing.empty:
        return new_data, info

    existing_data = _as_str_df(existing.copy()).reset_index(drop=True)
    existing_date_values = existing_dates.fillna("").astype(str).str.strip().reset_index(drop=True)
    if len(existing_date_values) != len(existing_data):
        raise ValueError("existing_dates 길이가 existing 행 수와 일치하지 않습니다")

    if len(coverage_dates) == len(new_data):
        new_row_dates = coverage_dates
    elif "주문시각" in new_data.columns:
        new_row_dates = order_date(new_data["주문시각"]).reset_index(drop=True)
    elif "order_date" in new_data.columns:
        matched = new_data["order_date"].fillna("").astype(str).str.extract(
            r"(\d{4})\.(\d{2})\.(\d{2})"
        )
        new_row_dates = (
            matched[0] + "-" + matched[1] + "-" + matched[2]
        ).fillna("").reset_index(drop=True)
    else:
        new_row_dates = pd.Series("", index=new_data.index, dtype=str).reset_index(drop=True)

    in_covered_range = existing_date_values.isin(covered_dates)
    info["removed"] = int(in_covered_range.sum())

    if order_key in existing_data.columns:
        existing_keys = existing_data[order_key].fillna("").astype(str).str.strip()
        existing_counts = (
            pd.DataFrame({"date": existing_date_values, "key": existing_keys})
            .loc[lambda frame: frame["date"].isin(covered_dates) & frame["key"].ne("")]
            .groupby("date")["key"]
            .nunique()
        )
    else:
        existing_counts = pd.Series(dtype="int64")

    if not new_data.empty and order_key in new_data.columns:
        new_keys = new_data[order_key].fillna("").astype(str).str.strip().reset_index(drop=True)
        new_counts = (
            pd.DataFrame({"date": new_row_dates, "key": new_keys})
            .loc[lambda frame: frame["date"].isin(covered_dates) & frame["key"].ne("")]
            .groupby("date")["key"]
            .nunique()
        )
    else:
        new_counts = pd.Series(dtype="int64")

    info["shrunk_dates"] = [
        date
        for date in covered_dates
        if int(existing_counts.get(date, 0)) > 0 and int(new_counts.get(date, 0)) == 0
    ]
    info["partial_shrunk_dates"] = [
        date
        for date in covered_dates
        if 0 < int(new_counts.get(date, 0)) < int(existing_counts.get(date, 0))
    ]
    info["shrink_details"] = {
        date: {
            "existing_orders": int(existing_counts.get(date, 0)),
            "new_orders": int(new_counts.get(date, 0)),
        }
        for date in covered_dates
        if int(existing_counts.get(date, 0)) > int(new_counts.get(date, 0))
    }

    kept = existing_data.loc[~in_covered_range].copy()
    return pd.concat([kept, new_data], ignore_index=True), info


def read_table(stem_path: Path, columns: list[str] | None = None) -> pd.DataFrame | None:
    """확장자 없는 경로(stem)를 받아 parquet 우선·CSV 폴백으로 읽는다.

    예: stem_path=.../ym=2026-06/orders_2026-06 → orders_2026-06.parquet 또는 .csv
    둘 다 없으면 None.
    """
    pq = stem_path.with_suffix(".parquet")
    csv = stem_path.with_suffix(".csv")
    if pq.exists():
        return _as_str_df(pd.read_parquet(pq, columns=columns))
    if csv.exists():
        return _as_str_df(pd.read_csv(csv, dtype=str, encoding="utf-8-sig", usecols=columns))
    return None


def read_file(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """확장자가 있는 실제 파일(parquet 또는 csv)을 문자열 DataFrame으로 읽는다."""
    if path.suffix == ".parquet":
        return _as_str_df(pd.read_parquet(path, columns=columns))
    return _as_str_df(pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=columns))


def write_table(df: pd.DataFrame, stem_path: Path) -> Path:
    """문자열 DataFrame을 parquet(zstd)로 저장하고, 같은 위치의 레거시 CSV는 제거한다."""
    stem_path.parent.mkdir(parents=True, exist_ok=True)
    pq = stem_path.with_suffix(".parquet")
    _as_str_df(df).to_parquet(pq, engine="pyarrow", compression=PARQUET_COMPRESSION, index=False)
    csv = stem_path.with_suffix(".csv")
    if csv.exists():
        try:
            csv.unlink()
        except OSError as exc:
            logger.warning("레거시 CSV 삭제 실패: %s / %s", csv, exc)
    return pq


def find_tables(base: Path, rel_glob_stem: str) -> list[Path]:
    """parquet·csv 양쪽을 glob하고, 같은 stem이면 parquet를 우선해 실제 파일 경로를 반환한다.

    rel_glob_stem: 확장자 없는 glob 패턴
        예: "brand=*/store=*/ym=2026-06/orders_2026-06"
    """
    found: dict[Path, Path] = {}
    for suffix in (".csv", ".parquet"):  # parquet를 뒤에 둬서 같은 stem이면 덮어씀
        for p in base.glob(rel_glob_stem + suffix):
            found[p.with_suffix("")] = p
    return sorted(found.values())
