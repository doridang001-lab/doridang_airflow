import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_05_ad_funnel as ad_funnel


def test_collect_ad_funnel_no_ads_saves_status_and_succeeds():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "나홀로", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_set_ad_filter_yesterday", return_value=True), \
         patch.object(ad_funnel, "_extract_ad_metrics", return_value={"status": "no_ads", "metrics": None}), \
         patch.object(ad_funnel, "_save_ad_funnel_csv", return_value=Path("tmp.csv")) as mock_save:
        assert ad_funnel.collect_ad_funnel_for_driver(driver, store_info, target_date="2026-06-05") is True

    mock_save.assert_called_once_with(None, "나홀로", "store", "2026-06-05", status="no_ads")


def test_collect_ad_funnel_parse_error_returns_false():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "나홀로", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_set_ad_filter_yesterday", return_value=True), \
         patch.object(ad_funnel, "_extract_ad_metrics", side_effect=[
             {"status": "parse_error", "metrics": None, "reason": "bad dom"},
             {"status": "parse_error", "metrics": None, "reason": "bad dom"},
         ]):
        assert ad_funnel.collect_ad_funnel_for_driver(driver, store_info, target_date="2026-06-05") is False


def test_validate_and_retry_ad_funnel_skips_no_ads_rows(tmp_path: Path):
    store_info = {
        "account_id": "acct",
        "password": "pw",
        "store_id": "1",
        "brand": "나홀로",
        "store": "store",
    }
    out_dir = tmp_path / "brand=나홀로" / "store=store" / "ym=2026-06"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "collected_at": "2026-06-06T00:00:00+09:00",
                "target_date": "2026-06-05",
                "store_name": "store",
                "collection_status": "no_ads",
                "노출수": "",
                "클릭수": "",
                "주문수": "",
                "주문금액": "",
            }
        ]
    ).to_csv(out_dir / "baemin_ad_funnel.csv", index=False, encoding="utf-8-sig")

    with patch.object(ad_funnel, "BAEMIN_AD_FUNNEL_DB", tmp_path), \
         patch.object(ad_funnel, "collect_ad_funnel_for_account") as mock_retry:
        result = ad_funnel._validate_and_retry_ad_funnel([store_info], "2026-06-05")

    assert result == {"empty_stores": [], "retried": [], "still_empty": []}
    mock_retry.assert_not_called()


def test_collect_ad_funnel_parse_error_refreshes_before_failing():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "brand", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_set_ad_filter_yesterday", return_value=True), \
         patch.object(ad_funnel, "_extract_ad_metrics", return_value={
             "status": "parse_error",
             "metrics": None,
             "reason": "blank selenium message",
         }), \
         patch.object(ad_funnel, "_reload_and_retry_extract", return_value={
             "status": "parse_error",
             "metrics": None,
             "reason": "refresh wait_for_page failed",
         }) as mock_refresh:
        assert ad_funnel.collect_ad_funnel_for_driver(driver, store_info, target_date="2026-06-05") is False

    mock_refresh.assert_called_once_with(driver, "1", "store")
