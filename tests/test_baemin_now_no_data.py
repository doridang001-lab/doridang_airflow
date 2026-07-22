import sys
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pandas as pd

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
    df = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig")

    assert df.loc[0, "collection_status"] == "no_data"
    assert df.loc[0, "collection_note"] == "metric_labels_without_values"
    assert df.loc[0, "store"] == "store"


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

    assert df.loc[0, "collection_status"] == "ok"
    assert df.loc[0, "collection_note"] == "metric_values"
    assert df.loc[0, "조리소요시간"] == "12"
