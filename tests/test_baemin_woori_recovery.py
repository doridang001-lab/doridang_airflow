import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_02_woori_shop_click as woori


def test_collect_woori_retries_revisit_after_empty_and_saves():
    store_info = {"store_id": "14778331", "brand": "brand", "store": "송파삼전점"}
    rows = [{"날짜": "2026-05-01"}]

    with patch.object(woori, "KNOWN_BRANDS", ["brand"]), \
         patch.object(woori, "_get_target_months", return_value=["2026-05"]), \
         patch.object(woori, "_navigate_to_woori_url"), \
         patch.object(woori, "_extract_table_rows", side_effect=[[], [], rows]), \
         patch.object(woori, "_save_csv", return_value=Path("saved.csv")) as mock_save, \
         patch.object(woori, "time") as mock_time:
        mock_time.sleep.return_value = None
        driver = MagicMock()
        woori.collect_woori_for_driver(driver, [store_info])

    assert driver.refresh.call_count == 1
    mock_save.assert_called_once()


def test_collect_woori_raises_when_previous_month_was_previously_collected_but_now_empty():
    store_info = {"store_id": "14778331", "brand": "brand", "store": "송파삼전점"}

    with patch.object(woori, "KNOWN_BRANDS", ["brand"]), \
         patch.object(woori, "_get_target_months", return_value=["2026-05"]), \
         patch.object(woori, "_navigate_to_woori_url"), \
         patch.object(woori, "_extract_table_rows", side_effect=[[], [], []]), \
         patch.object(woori, "_existing_woori_row_count", return_value=12), \
         patch.object(woori, "time") as mock_time:
        mock_time.sleep.return_value = None
        driver = MagicMock()
        try:
            woori.collect_woori_for_driver(driver, [store_info])
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "unexpectedly empty" in str(e)
