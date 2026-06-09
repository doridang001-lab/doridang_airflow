import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_02_woori_shop_click as woori


class _WaitUntil:
    def __init__(self, _driver, _timeout):
        self.timeout = _timeout

    def until(self, predicate):
        return predicate(MagicMock())


def test_extract_table_rows_fast_empty_returns_without_long_wait():
    driver = MagicMock()
    driver.find_elements.return_value = []
    driver.execute_script.side_effect = [
        {"trs": 0, "busy": 0, "ready": "complete"},
        '{"tr":0,"fast_empty":true}',
    ]
    driver.current_url = "https://self.baemin.com/empty"

    with patch.object(woori, "WebDriverWait", _WaitUntil):
        rows = woori._extract_table_rows(driver, "1", "store", "2026-06")

    assert rows == []


def test_collect_woori_current_month_empty_does_not_retry():
    store = {"store_id": "1", "brand": woori.KNOWN_BRANDS[0], "store": "store"}
    driver = MagicMock()

    with patch.object(woori, "_get_target_months", return_value=["2026-06"]), \
         patch.object(woori, "_navigate_to_woori_url"), \
         patch.object(woori, "_extract_table_rows", return_value=[]) as mock_extract, \
         patch.object(woori, "_retry_woori_after_empty") as mock_retry, \
         patch.object(woori, "_existing_woori_row_count", return_value=0), \
         patch.object(woori, "time") as mock_time:
        mock_time.sleep.return_value = None
        woori.collect_woori_for_driver(driver, [store])

    assert mock_extract.call_count == 1
    mock_retry.assert_not_called()


def test_nahollo_woori_empty_is_expected():
    store = {"store_id": "1", "brand": "나홀로", "store": "store"}

    assert woori._woori_empty_is_expected(store) is True


def test_non_nahollo_woori_empty_is_not_expected():
    store = {"store_id": "1", "brand": "도리당", "store": "store"}

    assert woori._woori_empty_is_expected(store) is False
