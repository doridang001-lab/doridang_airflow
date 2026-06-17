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
