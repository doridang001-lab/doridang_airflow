import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_03_shop_change as shop_change
from modules.transform.pipelines.db import DB_Beamin_combined as combined


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

    assert "성공 1/1 계정" in result
    assert "store_fail=0" in result


def test_collect_now_and_woori_bootstrap_recovery_failure_marks_account_failed():
    account = {"account_id": "acct1", "password": "pw"}
    driver = MagicMock()
    type(driver).current_url = property(lambda _self: (_ for _ in ()).throw(Exception("Connection refused")))

    with patch.object(combined, "launch_browser", return_value=driver), \
         patch.object(combined, "login_baemin", return_value=True), \
         patch.object(combined, "_restart_driver_if_dead", return_value=None):
        result = combined.collect_now_and_woori([account])

    assert result["summary"] == "성공 0/1 계정"
    assert result["failed"]["accounts"] == [account]
    assert result["failed"]["stores"] == []


def test_shop_change_save_csv_keeps_same_timestamp_multiple_rows(tmp_path):
    rows = [
        {
            "?섏쭛?쇱떆": "2026-06-01T00:00:00+09:00",
            "留ㅼ옣紐?": "store",
            "store_id": "1",
            "吏??챸": "area",
            "?遺꾨쪟": "운영시간 변경",
            "遺꾨쪟": "",
            "蹂寃쎌떆媛?": "2026-05-31 12:00:00",
            "?묒뾽??": "u1",
            "蹂寃쏀썑_?뺢린?대Т??": "",
            "蹂寃쏀썑_?꾩떆?대Т??": "",
            "蹂寃쏀썑_?ъ쑀": "",
            "蹂寃쏀썑_媛寃뚯쨷吏": "",
            "蹂寃쏀썑_諛곕?諛곕떖": "",
            "蹂寃쏀썑_?댁쁺?쒓컙": "09:00~21:00",
            "蹂寃쎌쟾_?뺢린?대Т??": "",
            "蹂寃쎌쟾_?꾩떆?대Т??": "",
            "蹂寃쎌쟾_?ъ쑀": "",
            "蹂寃쎌쟾_媛寃뚯쨷吏": "",
            "蹂寃쎌쟾_諛곕?諛곕떖": "",
            "蹂寃쎌쟾_?댁쁺?쒓컙": "10:00~21:00",
        },
        {
            "?섏쭛?쇱떆": "2026-06-01T00:00:00+09:00",
            "留ㅼ옣紐?": "store",
            "store_id": "1",
            "吏??챸": "area",
            "?遺꾨쪟": "운영시간 변경",
            "遺꾨쪟": "",
            "蹂寃쎌떆媛?": "2026-05-31 12:00:00",
            "?묒뾽??": "u1",
            "蹂寃쏀썑_?뺢린?대Т??": "",
            "蹂寃쏀썑_?꾩떆?대Т??": "",
            "蹂寃쏀썑_?ъ쑀": "",
            "蹂寃쏀썑_媛寃뚯쨷吏": "",
            "蹂寃쏀썑_諛곕?諛곕떖": "",
            "蹂寃쏀썑_?댁쁺?쒓컙": "11:00~22:00",
            "蹂寃쎌쟾_?뺢린?대Т??": "",
            "蹂寃쎌쟾_?꾩떆?대Т??": "",
            "蹂寃쎌쟾_?ъ쑀": "",
            "蹂寃쎌쟾_媛寃뚯쨷吏": "",
            "蹂寃쎌쟾_諛곕?諛곕떖": "",
            "蹂寃쎌쟾_?댁쁺?쒓컙": "09:00~21:00",
        },
    ]

    with patch.object(shop_change, "BAEMIN_SHOP_CHANGE_DB", tmp_path):
        out_path = shop_change._save_csv(rows, "brand", "store")

    df = pd.read_csv(out_path, dtype=str)
    assert len(df) == 2


def test_expanded_content_text_falls_back_to_text_content_and_js():
    content = MagicMock()
    content.get_attribute.side_effect = ["", "상세 텍스트"]

    item = MagicMock()
    item.find_element.return_value = content
    item.parent.execute_script.return_value = ""

    assert shop_change._expanded_content_text(item) == "상세 텍스트"

    content2 = MagicMock()
    content2.get_attribute.side_effect = ["", ""]
    item2 = MagicMock()
    item2.find_element.return_value = content2
    item2.parent.execute_script.return_value = "js 상세 텍스트"

    assert shop_change._expanded_content_text(item2) == "js 상세 텍스트"
