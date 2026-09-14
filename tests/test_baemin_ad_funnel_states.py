import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_05_ad_funnel as ad_funnel
from modules.transform.pipelines.db import DB_Beamin_04_orders as orders


def test_ad_filter_extracts_label_keyed_metrics():
    driver = MagicMock()
    driver.execute_script.return_value = {
        "노출수": "2,376",
        "클릭수": "63",
        "주문수": "40",
        "주문금액": "900,000",
    }

    assert ad_funnel._extract_filter_vals(driver, 1, "강동점") == {
        "노출수": "2,376",
        "클릭수": "63",
        "주문수": "40",
        "주문금액": "900,000",
    }


def test_empty_filter_metrics_are_failure_not_zeroes():
    driver = MagicMock()
    driver.execute_script.side_effect = [[], {"noDataCount": 0, "btns": []}]

    with patch.object(ad_funnel, "_dump_ad_dom_diagnostics"):
        assert ad_funnel._extract_filter_vals(driver, 1, "강동점") is None


def test_no_period_data_filter_metrics_fallback_to_zeroes():
    driver = MagicMock()
    driver.execute_script.side_effect = [
        {},
        {"noDataCount": 2, "btns": ["어제", "어제"]},
    ]

    assert ad_funnel._extract_filter_vals(driver, 1, "광주태전점") == {
        "주문수": "0",
        "주문금액": "0",
    }


def test_order_filter_zero_impression_metrics_fallback_to_order_zeroes():
    driver = MagicMock()
    driver.execute_script.return_value = {"노출수": "0", "클릭수": "0"}

    assert ad_funnel._extract_filter_vals(driver, 1, "백석점") == {
        "주문수": "0",
        "주문금액": "0",
    }


def test_ad_metrics_plausibility_rejects_swapped_or_missing_values():
    assert ad_funnel._metrics_are_plausible(
        {"노출수": "10,000", "클릭수": "500", "주문수": "41", "주문금액": "1,210,410"}
    )
    assert not ad_funnel._metrics_are_plausible(
        {"노출수": "411", "클릭수": "12,104,100", "주문수": "0", "주문금액": "0"}
    )
    assert not ad_funnel._metrics_are_plausible(
        {"노출수": "10,000", "클릭수": "500", "주문수": "", "주문금액": "0"}
    )
    assert not ad_funnel._metrics_are_plausible(
        {"노출수": "0", "클릭수": "0", "주문수": "05101520", "주문금액": "05101520"}
    )
    assert not ad_funnel._metrics_are_plausible(
        {"노출수": "0", "클릭수": "0", "주문수": "0", "주문금액": "0102030"}
    )
    assert not ad_funnel._metrics_are_plausible(
        {"노출수": "0", "클릭수": "0", "주문수": "0", "주문금액": "02468"}
    )


def test_collect_ad_funnel_no_ads_saves_status_and_succeeds():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "나홀로", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_snapshot_ad_metric_state", return_value={"no_ads": True}), \
         patch.object(ad_funnel, "_save_ad_funnel_csv", return_value=Path("tmp.csv")) as mock_save:
        assert ad_funnel.collect_ad_funnel_for_driver(driver, store_info, target_date="2026-06-05") is True

    mock_save.assert_called_once_with(None, "나홀로", "store", "2026-06-05", status="no_ads")


def test_collect_ad_funnel_parse_error_returns_false():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "나홀로", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_snapshot_ad_metric_state", return_value={"no_ads": False}), \
         patch.object(ad_funnel, "_set_ad_filter", return_value=None), \
         patch.object(ad_funnel, "_reload_and_collect", return_value=None):
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


def _write_ad_funnel_row(root: Path, store_info: dict, target_date: str, status: str) -> None:
    out_dir = (
        root
        / f"brand={store_info['brand']}"
        / f"store={store_info['store']}"
        / f"ym={target_date[:7]}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "collected_at": "2026-06-06T00:00:00+09:00",
                "target_date": target_date,
                "store_name": store_info["store"],
                "collection_status": status,
                "노출수": "",
                "클릭수": "",
                "주문수": "",
                "주문금액": "",
            }
        ]
    ).to_csv(out_dir / "baemin_ad_funnel.csv", index=False, encoding="utf-8-sig")


def _write_orders_rows(root: Path, brand: str, store: str, target_date: str, rows: list[dict]) -> None:
    out_dir = root / f"brand={brand}" / f"store={store}" / f"ym={target_date[:7]}"
    out_dir.mkdir(parents=True, exist_ok=True)
    data = rows or [
        {
            "주문상태": "배달완료",
            "주문번호": "old-order",
            "주문시각": "2026. 06. 04. 12:00",
            "결제금액": "12000",
        }
    ]
    pd.DataFrame(data).to_csv(out_dir / f"orders_{target_date[:7]}.csv", index=False, encoding="utf-8-sig")


def test_validate_ad_funnel_parse_error_becomes_zero_sales_when_orders_are_zero(tmp_path: Path):
    target_date = "2026-06-05"
    ad_root = tmp_path / "ad"
    orders_root = tmp_path / "orders"
    store_info = {
        "account_id": "acct",
        "password": "pw",
        "store_id": "1",
        "brand": "도리당",
        "store": "휴무점",
    }
    _write_ad_funnel_row(ad_root, store_info, target_date, "parse_error")
    _write_orders_rows(orders_root, "도리당", "휴무점", target_date, rows=[])

    with patch.object(ad_funnel, "BAEMIN_AD_FUNNEL_DB", ad_root), \
         patch.object(ad_funnel, "BAEMIN_ORDERS_DB", orders_root), \
         patch.object(ad_funnel, "collect_ad_funnel_for_account") as mock_retry:
        result = ad_funnel._validate_and_retry_ad_funnel([store_info], target_date)

    assert result == {"empty_stores": [], "retried": [], "still_empty": []}
    mock_retry.assert_not_called()
    out_file = next(ad_root.rglob("baemin_ad_funnel.csv"))
    row = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig").iloc[0]
    assert row["collection_status"] == "zero_sales"
    assert row["노출수"] == "0"
    assert row["클릭수"] == "0"
    assert row["주문수"] == "0"
    assert row["주문금액"] == "0"


def test_validate_ad_funnel_parse_error_becomes_zero_sales_with_parquet_no_data_marker(tmp_path: Path):
    target_date = "2026-06-05"
    ad_root = tmp_path / "ad"
    orders_root = tmp_path / "orders"
    store_info = {
        "account_id": "acct",
        "password": "pw",
        "store_id": "1",
        "brand": "도리당",
        "store": "파케이마커점",
    }
    _write_ad_funnel_row(ad_root, store_info, target_date, "parse_error")
    marker_dir = (
        orders_root
        / "_no_data"
        / "brand=도리당"
        / "store=파케이마커점"
        / "ym=2026-06"
    )
    marker_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "target_date": target_date,
                "brand": "도리당",
                "store": "파케이마커점",
                "store_id": "1",
                "status": "no_data",
                "reason": "total_summary_zero",
                "collected_at": "2026-06-06T00:00:00+09:00",
            }
        ]
    ).to_parquet(marker_dir / "orders_no_data.parquet", index=False)

    with patch.object(ad_funnel, "BAEMIN_AD_FUNNEL_DB", ad_root), \
         patch.object(ad_funnel, "BAEMIN_ORDERS_DB", orders_root), \
         patch.object(orders, "BAEMIN_ORDERS_DB", orders_root), \
         patch.object(ad_funnel, "collect_ad_funnel_for_account") as mock_retry:
        result = ad_funnel._validate_and_retry_ad_funnel([store_info], target_date)

    assert result == {"empty_stores": [], "retried": [], "still_empty": []}
    mock_retry.assert_not_called()
    out_file = next(ad_root.rglob("baemin_ad_funnel.csv"))
    row = pd.read_csv(out_file, dtype=str, encoding="utf-8-sig").iloc[0]
    assert row["collection_status"] == "zero_sales"


def test_validate_ad_funnel_keeps_parse_error_retryable_when_orders_have_sales(tmp_path: Path):
    target_date = "2026-06-05"
    ad_root = tmp_path / "ad"
    orders_root = tmp_path / "orders"
    store_info = {
        "account_id": "acct",
        "password": "pw",
        "store_id": "1",
        "brand": "도리당",
        "store": "영업점",
    }
    _write_ad_funnel_row(ad_root, store_info, target_date, "parse_error")
    _write_orders_rows(
        orders_root,
        "도리당",
        "영업점",
        target_date,
        rows=[
            {
                "주문상태": "배달완료",
                "주문번호": "order-1",
                "주문시각": "2026. 06. 05. 12:00",
                "결제금액": "12,000",
            }
        ],
    )

    with patch.object(ad_funnel, "BAEMIN_AD_FUNNEL_DB", ad_root), \
         patch.object(ad_funnel, "BAEMIN_ORDERS_DB", orders_root), \
         patch.object(ad_funnel, "collect_ad_funnel_for_account") as mock_retry:
        result = ad_funnel._validate_and_retry_ad_funnel([store_info], target_date)

    assert result["empty_stores"] == [store_info]
    assert result["retried"] == [store_info]
    assert result["still_empty"] == [store_info]
    mock_retry.assert_called_once()


def test_ad_funnel_zero_sales_filter_is_brand_specific(tmp_path: Path):
    target_date = "2026-06-05"
    ad_root = tmp_path / "ad"
    orders_root = tmp_path / "orders"
    failed = {
        "accounts": [],
        "stores": [],
        "orders": [],
        "ads": [
            {
                "account": {"account_id": "acct"},
                "stores": [
                    {"store_id": "1", "brand": "도리당", "store": "공유점"},
                    {"store_id": "2", "brand": "나홀로", "store": "공유점"},
                ],
            }
        ],
        "stages": [],
    }
    _write_orders_rows(
        orders_root,
        "도리당",
        "공유점",
        target_date,
        rows=[
            {
                "주문상태": "배달완료",
                "주문번호": "order-1",
                "주문시각": "2026. 06. 05. 12:00",
                "결제금액": "12,000",
            }
        ],
    )
    _write_orders_rows(orders_root, "나홀로", "공유점", target_date, rows=[])

    with patch.object(ad_funnel, "BAEMIN_AD_FUNNEL_DB", ad_root), \
         patch.object(ad_funnel, "BAEMIN_ORDERS_DB", orders_root):
        filtered = ad_funnel.filter_ad_funnel_zero_sales_failures(failed, target_date)

    assert filtered["ads"] == [
        {
            "account": {"account_id": "acct"},
            "stores": [{"store_id": "1", "brand": "도리당", "store": "공유점"}],
        }
    ]
    zero_file = ad_root / "brand=나홀로" / "store=공유점" / "ym=2026-06" / "baemin_ad_funnel.csv"
    assert pd.read_csv(zero_file, dtype=str, encoding="utf-8-sig").iloc[0]["collection_status"] == "zero_sales"


def test_collect_ad_funnel_parse_error_refreshes_before_failing():
    driver = MagicMock()
    store_info = {"store_id": "1", "brand": "brand", "store": "store"}

    with patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(ad_funnel, "_snapshot_ad_metric_state", return_value={"no_ads": False}), \
         patch.object(ad_funnel, "_set_ad_filter", return_value=None), \
         patch.object(ad_funnel, "_reload_and_collect", return_value=None) as mock_refresh:
        assert ad_funnel.collect_ad_funnel_for_driver(driver, store_info, target_date="2026-06-05") is False

    mock_refresh.assert_called_once_with(driver, "1", "store", "2026-06-05")


def test_ad_funnel_dom_circuit_opens_after_three_missing_metrics():
    ad_funnel._record_filter_extract_success()
    driver = MagicMock()
    driver.execute_script.return_value = {}

    with patch.object(ad_funnel, "_dump_ad_dom_diagnostics"):
        assert ad_funnel._extract_filter_vals(driver, 0, "store") is None
        assert not ad_funnel._ad_dom_circuit_open()
        assert ad_funnel._extract_filter_vals(driver, 0, "store") is None
        assert not ad_funnel._ad_dom_circuit_open()
        assert ad_funnel._extract_filter_vals(driver, 0, "store") is None
        assert ad_funnel._ad_dom_circuit_open()

    ad_funnel._record_filter_extract_success()


def test_ad_funnel_dom_circuit_does_not_skip_next_retry_store():
    ad_funnel._record_filter_extract_success()
    store_infos = [
        {"account_id": "acct", "password": "pw", "store_id": "1", "brand": "brand", "store": "store1"},
        {"account_id": "acct", "password": "pw", "store_id": "2", "brand": "brand", "store": "store2"},
    ]

    calls = {"count": 0}

    def collect_side_effect(*_args, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            ad_funnel._record_filter_extract_failure()
            ad_funnel._record_filter_extract_failure()
            ad_funnel._record_filter_extract_failure()
            return False
        return True

    driver = MagicMock()
    with patch.object(ad_funnel, "launch_browser", return_value=driver) as mock_launch, \
         patch.object(ad_funnel, "login_baemin", return_value=True), \
         patch.object(ad_funnel, "wait_for_page", return_value=True), \
         patch.object(
             ad_funnel,
             "_collect_ad_funnel_metrics",
             side_effect=collect_side_effect,
         ):
        failed = ad_funnel.collect_ad_funnel_for_account(
            "acct",
            "pw",
            store_infos,
            target_date="2026-06-05",
            max_attempts=1,
        )

    assert failed == [store_infos[0]]
    assert mock_launch.call_count == 2
    ad_funnel._record_filter_extract_success()
