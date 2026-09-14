from pathlib import Path

import pandas as pd
import pytest

from modules.extract import crawling_toorder_sales_report as crawler
from modules.transform.pipelines.db import DB_CollectionCompare as compare


def _write_toorder_dates(path: Path, dates: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": dates}).to_parquet(path, index=False, engine="pyarrow")


def test_validate_toorder_freshness_passes_when_source_has_expected_date(tmp_path):
    path = tmp_path / "toorder_store_platform_daily.parquet"
    _write_toorder_dates(path, ["2026-08-30", "2026-08-31"])

    assert compare.validate_toorder_freshness(path, expected_date="2026-08-31") == "2026-08-31"


def test_validate_toorder_freshness_fails_when_source_is_stale(tmp_path):
    path = tmp_path / "toorder_store_platform_daily.parquet"
    _write_toorder_dates(path, ["2026-08-28"])

    with pytest.raises(RuntimeError, match="ToOrder 원천 최신일 지연"):
        compare.validate_toorder_freshness(path, expected_date="2026-08-31")


def test_validate_toorder_freshness_fails_when_source_is_missing(tmp_path):
    path = tmp_path / "missing.parquet"

    with pytest.raises(RuntimeError, match="ToOrder 원천 parquet 없음"):
        compare.validate_toorder_freshness(path, expected_date="2026-08-31")


def test_toorder_datedetail_debug_dir_uses_temp_dir(monkeypatch, tmp_path):
    monkeypatch.setattr("modules.transform.utility.paths.TEMP_DIR", tmp_path)

    assert crawler._toorder_debug_dir("toorder_datedetail_debug") == tmp_path / "toorder_datedetail_debug"
