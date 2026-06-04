import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_03_shop_change as shop_change


def test_collect_shop_change_sends_partial_failure_alert():
    account = {"account_id": "acct1", "password": "pw"}
    stores = [{"store_id": "1", "brand": "brand", "store": "s1"}]

    with patch.object(shop_change, "launch_browser", return_value=MagicMock()), \
         patch.object(shop_change, "login_baemin", return_value=True), \
         patch.object(shop_change, "clean_chrome_profile"), \
         patch.object(shop_change, "_force_kill_chrome"), \
         patch.object(shop_change, "logout_baemin"), \
         patch.object(shop_change, "time") as mock_time, \
         patch.object(shop_change, "_load_shop_change_store_list", return_value=stores), \
         patch.object(shop_change, "collect_shop_change_for_driver", side_effect=RuntimeError("parse exploded")), \
         patch.object(shop_change, "_send_email_alert") as mock_email, \
         patch.object(shop_change, "send_telegram") as mock_telegram:
        mock_time.sleep.return_value = None
        result = shop_change.collect_shop_change([account])

    assert "store_fail=1" in result
    mock_email.assert_called_once()
    mock_telegram.assert_called_once()
    alert_body = mock_email.call_args.args[1]
    assert "collection_failures=1" in alert_body
    assert "parse exploded" in alert_body


def test_parse_failure_is_recorded_for_missing_detail_text():
    shop_change._reset_shop_change_alerts()
    store_info = {"store_id": "1", "brand": "brand", "store": "store"}

    with patch.object(shop_change, "_get_item_summary", return_value=("운영시간 변경", "2026-05-31 12:00:00", "key1")), \
         patch.object(shop_change, "_expanded_content_text", return_value=""), \
         patch.object(shop_change, "_format_store_target", return_value="brand/store/1"):
        row = shop_change._parse_change_history_item(
            MagicMock(),
            "2026-06-01T00:00:00+09:00",
            store_info,
            "brand/store",
            "store",
        )

    assert row is not None
    assert len(shop_change._SHOP_CHANGE_ALERT_STATE["parse_failures"]) == 1
    assert shop_change._SHOP_CHANGE_ALERT_STATE["parse_failures"][0]["target"] == "brand/store/1"
