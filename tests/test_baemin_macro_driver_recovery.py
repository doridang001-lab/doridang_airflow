import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_03_shop_change as shop_change
from modules.transform.pipelines.db import DB_Beamin_02_woori_shop_click as woori
from modules.transform.pipelines.db import DB_Beamin_combined as combined


def _shop_change_row(**overrides):
    row = {column: "" for column in shop_change.CSV_COLUMNS}
    row.update(
        {
            "수집일시": "2026-06-01T00:00:00+09:00",
            "매장명": "store",
            "store_id": "1",
            "지역명": "area",
            "대분류": "운영시간 변경",
            "변경시간": "2026-05-31 12:00:00",
            "작업자": "u1",
            "변경후_운영시간": "09:00~21:00",
            "변경전_운영시간": "10:00~21:00",
        }
    )
    row.update(overrides)
    return row


def test_collect_shop_change_retries_dead_store_and_continues():
    account = {"account_id": "acct1", "password": "pw"}
    stores = [
        {"store_id": "1", "brand": "brand", "store": "s1"},
        {"store_id": "2", "brand": "brand", "store": "s2"},
    ]
    drivers = [
        MagicMock(name="bootstrap"),
        MagicMock(name="store1_try1"),
        MagicMock(name="store1_try2"),
        MagicMock(name="store2_try1"),
    ]

    with patch.object(shop_change, "launch_browser", side_effect=drivers), \
         patch.object(shop_change, "login_baemin", return_value=True), \
         patch.object(shop_change, "clean_chrome_profile"), \
         patch.object(shop_change, "_force_kill_chrome"), \
         patch.object(shop_change, "logout_baemin"), \
         patch.object(shop_change, "time") as mock_time, \
         patch.object(shop_change, "_load_shop_change_store_list", return_value=stores), \
         patch.object(
             shop_change,
             "collect_shop_change_for_driver",
             side_effect=[Exception("Connection refused"), None, None],
         ):
        mock_time.sleep.return_value = None
        result = shop_change.collect_shop_change([account])

    assert "store_fail=0" in result["summary"]


def test_collect_now_and_woori_bootstrap_recovery_failure_marks_account_failed():
    account = {"account_id": "acct1", "password": "pw"}
    with patch.object(combined, "_build_account_session", return_value=None):
        with pytest.raises(RuntimeError):
            combined.collect_now_and_woori([account])


def test_collect_now_and_woori_runs_all_now_before_woori_in_brand_order():
    account = {
        "account_id": "acct1",
        "password": "pw",
        "store_name": "도리당 송파삼전점",
    }
    driver = MagicMock()
    driver.current_url = "about:blank"
    raw_options = [
        {"store_id": "2", "text": "나홀로 송파삼전점"},
        {"store_id": "1", "text": "도리당 송파삼전점"},
    ]
    calls = []

    def record_now(_driver, _account_id, stores):
        calls.append(("now", stores[0]["brand"], stores[0]["store"]))

    def record_woori(_driver, stores):
        calls.append(("woori", stores[0]["brand"], stores[0]["store"]))

    with patch.object(
        combined,
        "resolve_stability_profile",
        return_value={
            "name": "test",
            "max_session_recovery_per_account": 2,
            "driver_restart_every_stores": 999,
            "account_wait_range": (0, 0),
        },
    ), \
         patch.object(combined, "_build_account_session", return_value=driver), \
         patch.object(combined, "wait_for_page", return_value=True), \
         patch.object(combined, "get_store_options", return_value=raw_options), \
         patch.object(combined, "collect_now_for_driver", side_effect=record_now), \
         patch.object(combined, "collect_woori_for_driver", side_effect=record_woori), \
         patch.object(combined, "collect_shop_change_for_driver"), \
         patch.object(combined, "collect_orders_for_driver", return_value={"ok": True, "validation": []}), \
         patch.object(combined, "collect_ad_funnel_for_driver", return_value=True), \
         patch.object(combined, "logout_baemin"), \
         patch.object(combined.random, "uniform", return_value=0), \
         patch.object(combined.time, "sleep"):
        result = combined.collect_now_and_woori([account], stability_profile="test")

    assert result["summary"] == "성공 1/1 계정"
    assert calls == [
        ("now", "도리당", "송파삼전점"),
        ("now", "나홀로", "송파삼전점"),
        ("woori", "도리당", "송파삼전점"),
        ("woori", "나홀로", "송파삼전점"),
    ]


def test_renderer_timeout_is_treated_as_driver_crash():
    exc = Exception("timeout: Timed out receiving message from renderer: 44.302")

    assert combined._is_recoverable_session_issue(exc)
    assert woori._is_driver_crash_error(exc)


def test_shop_change_save_csv_keeps_same_timestamp_multiple_rows(tmp_path):
    rows = [
        _shop_change_row(변경후_운영시간="09:00~21:00", 변경전_운영시간="10:00~21:00"),
        _shop_change_row(변경후_운영시간="11:00~22:00", 변경전_운영시간="09:00~21:00"),
    ]

    with patch.object(shop_change, "BAEMIN_SHOP_CHANGE_DB", tmp_path):
        out_path = shop_change._save_csv(rows, "brand", "store")

    df = pd.read_csv(out_path, dtype=str)
    assert len(df) == 2


def test_expanded_content_text_falls_back_to_text_content_and_js():
    content = MagicMock()
    content.get_attribute.side_effect = ["", "detail text"]

    item = MagicMock()
    item.find_element.return_value = content
    item.parent.execute_script.return_value = ""

    assert shop_change._expanded_content_text(item) == "detail text"

    content2 = MagicMock()
    content2.get_attribute.side_effect = ["", ""]
    item2 = MagicMock()
    item2.find_element.return_value = content2
    item2.parent.execute_script.return_value = "js detail text"

    assert shop_change._expanded_content_text(item2) == "js detail text"


def test_extract_change_rows_skips_existing_summary_key(tmp_path):
    store_info = {"store_id": "1", "brand": "brand", "store": "store"}
    out_dir = tmp_path / "brand=brand" / "store=store" / "ym=2026-06"
    out_dir.mkdir(parents=True)
    pd.DataFrame(
        [{"변경시간": "2026-06-01 12:00:00", "대분류": "운영시간 변경"}]
    ).to_csv(out_dir / "shop_change.csv", index=False, encoding="utf-8-sig")

    item = MagicMock()
    driver = MagicMock()
    driver.find_elements.return_value = [item]

    with patch.object(shop_change, "BAEMIN_SHOP_CHANGE_DB", tmp_path), \
         patch.object(
             shop_change,
             "_get_item_summary",
             return_value=("운영시간 변경", "2026-06-01 12:00:00", "key1"),
         ), \
         patch.object(shop_change, "_scroll_to_top"), \
         patch.object(shop_change, "_scroll_by"), \
         patch.object(shop_change, "human_click") as mock_click, \
         patch.object(shop_change, "time") as mock_time:
        mock_time.sleep.return_value = None
        rows = shop_change._extract_change_rows(driver, store_info, "store")

    assert rows == []
    mock_click.assert_not_called()
