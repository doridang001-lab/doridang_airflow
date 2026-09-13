import sys
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_01_now as now


def test_collect_now_for_driver_saves_no_data_status(tmp_path: Path):
    store_info = {"store_id": "1", "brand": "나홀로", "store": "store"}

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(
             now,
             "navigate_to_store_now",
             return_value={"status": "no_data", "reason": "metric_labels_without_values"},
         ) as mock_nav, \
         patch.object(
             now,
             "collect_single_store_stats",
             return_value={"account_id": "acct", "store_id": "1", "platform": "baemin", "조리소요시간": ""},
         ):
        now.collect_now_for_driver(MagicMock(), "acct", [store_info])

    mock_nav.assert_called_once_with(ANY, "1", return_state=True)
    out_file = next(tmp_path.rglob("baemin_now.csv"))
    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig", keep_default_na=False)

    assert "collection_status" not in df.columns
    assert df.loc[0, "collection_note"] == "metric_labels_without_values"
    assert df.loc[0, "store"] == "store"
    assert df.loc[0, "brand_store"] == "나홀로|store"
    assert df.loc[0, "store_name"] == "나홀로 store"
    assert df.loc[0, "조리소요시간"] == ""
    assert df.loc[0, "최근별점"] == ""
    assert list(df.columns) == now.NOW_CANONICAL_COLUMNS


def test_collect_now_for_driver_saves_ok_status(tmp_path: Path):
    store_info = {"store_id": "1", "brand": "도리당", "store": "store"}

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(
             now,
             "navigate_to_store_now",
             return_value={"status": "loaded", "reason": "metric_values"},
         ), \
         patch.object(
             now,
             "collect_single_store_stats",
             return_value={"account_id": "acct", "store_id": "1", "platform": "baemin", "조리소요시간": "12"},
         ):
        now.collect_now_for_driver(MagicMock(), "acct", [store_info])

    out_file = next(tmp_path.rglob("baemin_now.csv"))
    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig")

    assert "collection_status" not in df.columns
    assert df.loc[0, "collection_note"] == "metric_values"
    assert df.loc[0, "store_name"] == "도리당 store"
    assert df.loc[0, "brand_store"] == "도리당|store"
    assert df.loc[0, "조리소요시간"] == "12"
    assert list(df.columns[-3:]) == ["brand", "store", "brand_store"]
    assert list(df.columns) == now.NOW_CANONICAL_COLUMNS


def test_collect_now_for_driver_saves_improve_status(tmp_path: Path):
    store_info = {"store_id": "1", "brand": "도리당", "store": "store"}

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(
             now,
             "navigate_to_store_now",
             return_value={"status": "loaded", "reason": "metric_values"},
         ), \
         patch.object(
             now,
             "collect_single_store_stats",
             return_value={
                 "account_id": "acct",
                 "store_id": "1",
                 "platform": "baemin",
                 "준비시간정확도": "6",
                 "준비시간정확도_상태": "개선해요",
             },
         ):
        now.collect_now_for_driver(MagicMock(), "acct", [store_info])

    out_file = next(tmp_path.rglob("baemin_now.csv"))
    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig", keep_default_na=False)

    assert "collection_status" not in df.columns
    assert df.loc[0, "준비시간정확도_상태"] == "개선해요"
    assert df.loc[0, "brand_store"] == "도리당|store"
    assert list(df.columns) == now.NOW_CANONICAL_COLUMNS


def test_collect_now_for_driver_propagates_navigation_failure():
    store_info = {"store_id": "1", "brand": "도리당", "store": "store"}

    with patch.object(
        now,
        "navigate_to_store_now",
        return_value={"status": "missing", "reason": "navigation_failed"},
    ):
        with pytest.raises(RuntimeError, match="NOW render/metrics failed"):
            now.collect_now_for_driver(MagicMock(), "acct", [store_info])


def test_collect_now_for_driver_rejects_loaded_empty_metrics(tmp_path: Path):
    store_info = {"store_id": "1", "brand": "도리당", "store": "store"}

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(
             now,
             "navigate_to_store_now",
             return_value={"status": "loaded", "reason": "metric_values"},
         ) as mock_nav, \
         patch.object(
             now,
             "collect_single_store_stats",
             return_value={"account_id": "acct", "store_id": "1", "platform": "baemin"},
         ):
        with pytest.raises(RuntimeError, match="NOW loaded but metrics empty"):
            now.collect_now_for_driver(MagicMock(), "acct", [store_info])

    assert mock_nav.call_count == 2
    assert list(tmp_path.rglob("baemin_now.csv")) == []


def test_collect_now_for_driver_retries_loaded_empty_then_saves(tmp_path: Path):
    store_info = {"store_id": "1", "brand": "도리당", "store": "store"}

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(
             now,
             "navigate_to_store_now",
             return_value={"status": "loaded", "reason": "metric_values"},
         ) as mock_nav, \
         patch.object(
             now,
             "collect_single_store_stats",
             side_effect=[
                 {"account_id": "acct", "store_id": "1", "platform": "baemin"},
                 {
                     "account_id": "acct",
                     "store_id": "1",
                     "platform": "baemin",
                     "조리소요시간": "12",
                 },
             ],
         ):
        now.collect_now_for_driver(MagicMock(), "acct", [store_info])

    assert mock_nav.call_count == 2
    out_file = next(tmp_path.rglob("baemin_now.csv"))
    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    assert "collection_status" not in df.columns
    assert df.loc[0, "조리소요시간"] == "12"
    assert list(df.columns) == now.NOW_CANONICAL_COLUMNS


def test_save_metrics_csv_normalizes_existing_schema(tmp_path: Path):
    out_dir = tmp_path / "brand=도리당" / "store=store" / "ym=2026-08"
    out_dir.mkdir(parents=True)
    out_file = out_dir / "baemin_now.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-08-01T00:00:00",
                "store_id": "1",
                "store_name": "",
                "조리소요시간": "10",
                "date": "2026-08-01",
            }
        ]
    ).to_csv(out_file, index=False, encoding="utf-8-sig")

    with patch.object(now, "BAEMIN_METRICS_DB", tmp_path), \
         patch.object(now.pendulum, "now") as mock_now:
        mock_now.return_value.format.side_effect = lambda fmt: {"YYYY-MM-DD": "2026-08-02", "YYYY-MM": "2026-08"}[fmt]
        now._save_metrics_csv(
            {
                "account_id": "acct",
                "store_id": "1",
                "platform": "baemin",
                "collected_at": "2026-08-02T00:00:00",
                "조리소요시간": "12",
            },
            "도리당",
            "store",
        )

    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    assert list(df.columns) == now.NOW_CANONICAL_COLUMNS
    assert len(df) == 2
    assert df.loc[0, "store_name"] == "도리당 store"
    assert "collection_status" not in df.columns
    assert df.loc[1, "store_name"] == "도리당 store"
    assert df.loc[1, "brand_store"] == "도리당|store"
