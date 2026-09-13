import pytest
from unittest.mock import MagicMock, patch

from modules.transform.pipelines.db import DB_Beamin_04_orders as orders
from modules.transform.pipelines.db import DB_Beamin_combined as combined


def _profile() -> dict:
    return {
        "name": "test",
        "max_session_recovery_per_account": 2,
        "driver_restart_every_stores": 999,
        "account_wait_range": (0, 0),
    }


def test_collect_orders_for_driver_returns_date_filter_reason(monkeypatch):
    monkeypatch.setattr(orders, "_open_orders_history", lambda driver, target_date=None: None)
    monkeypatch.setattr(orders, "_wait_for_orders_page_shell", lambda driver: True)
    monkeypatch.setattr(orders, "_select_order_store", lambda driver, store_id, store: True)
    monkeypatch.setattr(orders, "_set_date", lambda driver, target_date: False)

    result = orders.collect_orders_for_driver(
        MagicMock(),
        {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        target_date="2026-07-26",
    )

    assert result == {"ok": False, "reason": "date_filter", "validation": []}


def test_collect_orders_for_driver_saves_partial_date_rows_but_keeps_failure(monkeypatch):
    rows = [{"주문번호": "A", "주문시각": "2026. 07. 26. (일) 오후 1:00:00", "결제금액": "10000"}]
    validation = {
        "matched": False,
        "store": "테스트점",
        "reason": "date_filtered_rows",
        "save_partial": True,
    }
    saved = []

    monkeypatch.setattr(orders, "_open_orders_history", lambda driver, target_date=None: None)
    monkeypatch.setattr(orders, "_wait_for_orders_page_shell", lambda driver: True)
    monkeypatch.setattr(orders, "_select_order_store", lambda driver, store_id, store: True)
    monkeypatch.setattr(orders, "_set_date", lambda driver, target_date: True)
    monkeypatch.setattr(orders, "_page_signature", lambda driver: "sig")
    monkeypatch.setattr(orders, "_read_total_summary", lambda driver: {"count": 1, "amount": 10000})
    monkeypatch.setattr(orders, "_wait_for_filter_settle", lambda driver, prev_sig, prev_summary: True)
    monkeypatch.setattr(
        orders,
        "_collect_with_retry_on_mismatch",
        lambda *args, **kwargs: (rows, validation),
    )
    monkeypatch.setattr(
        orders,
        "_save_orders_csv",
        lambda rows_arg, brand, store, target_date: saved.append((rows_arg, brand, store, target_date)) or "saved.csv",
    )

    result = orders.collect_orders_for_driver(
        MagicMock(),
        {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        target_date="2026-07-26",
    )

    assert result["ok"] is False
    assert result["reason"] == "date_filtered_rows"
    assert saved == [(rows, "도리당", "테스트점", "2026-07-26")]


def test_relative_date_label_uses_popup_date_not_runtime_label():
    class FakeDriver:
        def execute_script(self, *_args):
            return [
                "오늘",
                "2026. 08. 04. (화)",
                "어제",
                "2026. 08. 03. (월)",
            ]

    driver = FakeDriver()

    assert orders._relative_date_label_for_target(driver, "2026-08-04", "어제") == "오늘"
    assert orders._relative_date_label_for_target(driver, "2026-08-05", "오늘") is None


def test_calendar_yms_from_texts_accepts_dual_month_headers_without_range_noise():
    yms = orders._calendar_yms_from_texts(
        [
            "직접 선택 2026. 08. 31 ~ 2026. 09. 06",
            "2026. 8",
            "2026. 9",
        ]
    )

    assert yms == [(2026, 8), (2026, 9)]


def test_date_filter_leakage_is_not_saveable_even_with_target_rows():
    rows = [
        {"주문시각": "2026. 08. 04. (화) 오후 1:00:00"},
        {"주문시각": "2026. 08. 03. (월) 오후 1:00:00"},
    ]

    assert orders._date_filter_leakage_is_saveable(rows, "2026-08-04") is False


def test_orders_history_url_includes_target_date():
    assert orders._orders_history_url("2026-08-04") == (
        "https://self.baemin.com/orders/history?startDate=2026-08-04&endDate=2026-08-04"
    )


def test_rows_from_extension_csv_uses_pipeline_columns():
    csv = (
        "collected_at,store_name,주문상태,주문번호,주문시각,결제금액\n"
        "2026-08-04T01:00:00,도리당 테스트점,배달완료,A1,2026. 08. 04. (화) 오후 1:00:00,10000\n"
    )

    rows = orders._rows_from_extension_csv(csv)

    assert rows[0]["주문번호"] == "A1"
    assert rows[0]["결제금액"] == "10000"
    assert "입금예정금액" in rows[0]


def test_extension_bundle_exposes_collector_globals(monkeypatch):
    monkeypatch.setattr(orders, "_EXTENSION_CONTENT_FILES", ())
    monkeypatch.setattr(orders, "_EXTENSION_SCRIPT_CACHE", None)

    script = orders._read_extension_script_bundle()

    assert "window.Utils = Utils" in script
    assert "window.Sites = Sites" in script


def test_extension_collector_failure_falls_back_to_selenium(monkeypatch):
    expected_rows = [{"주문번호": "A1", "주문시각": "2026. 08. 04. (화) 오후 1:00:00"}]

    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(
        orders,
        "_collect_all_pages_with_extension",
        lambda _driver, _store_info: (_ for _ in ()).throw(RuntimeError("extension failed")),
    )
    monkeypatch.setattr(orders, "_collect_all_pages", lambda _driver, _store_info: expected_rows)

    rows, collector = orders._collect_all_pages_for_mode(
        MagicMock(),
        {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
    )

    assert rows == expected_rows
    assert collector == "selenium"


def test_extension_collector_unavailable_does_not_fallback_to_selenium(monkeypatch):
    selenium_collector = MagicMock(return_value=[])

    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(
        orders,
        "_collect_all_pages_with_extension",
        lambda _driver, _store_info: (_ for _ in ()).throw(
            orders.ExtensionCollectorUnavailable("배민 확장 orders collector 준비 실패")
        ),
    )
    monkeypatch.setattr(orders, "_collect_all_pages", selenium_collector)

    with pytest.raises(orders.ExtensionCollectorUnavailable):
        orders._collect_all_pages_for_mode(
            MagicMock(),
            {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        )

    selenium_collector.assert_not_called()


def test_extension_driver_read_timeout_does_not_fallback_to_selenium(monkeypatch):
    selenium_collector = MagicMock(return_value=[])

    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(
        orders,
        "_collect_all_pages_with_extension",
        lambda _driver, _store_info: (_ for _ in ()).throw(
            RuntimeError("HTTPConnectionPool(host='localhost', port=57937): Read timed out. (read timeout=90)")
        ),
    )
    monkeypatch.setattr(orders, "_collect_all_pages", selenium_collector)

    with pytest.raises(RuntimeError):
        orders._collect_all_pages_for_mode(
            MagicMock(),
            {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        )

    selenium_collector.assert_not_called()


def test_extension_validation_mismatch_retries_with_selenium(monkeypatch):
    extension_rows = [
        {
            "주문번호": "A1",
            "주문시각": "2026. 08. 04. (화) 오후 1:00:00",
            "결제금액": "10000",
            "입금예정금액": "9000",
        }
    ]
    selenium_rows = [
        {
            "주문번호": "A1",
            "주문시각": "2026. 08. 04. (화) 오후 1:00:00",
            "결제금액": "10000",
            "입금예정금액": "9000",
        },
        {
            "주문번호": "A2",
            "주문시각": "2026. 08. 04. (화) 오후 2:00:00",
            "결제금액": "20000",
            "입금예정금액": "18000",
        },
    ]

    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(orders, "_EXTENSION_VALIDATION_SELENIUM_FALLBACK", True)
    monkeypatch.setattr(orders, "_csv_already_covers", lambda *args, **kwargs: False)
    monkeypatch.setattr(orders, "_go_to_first_page", lambda _driver, _store: None)
    monkeypatch.setattr(orders, "_read_total_summary", lambda _driver: {"count": 2, "amount": 30000})
    monkeypatch.setattr(orders, "_collect_all_pages_with_extension", lambda _driver, _store_info: extension_rows)
    monkeypatch.setattr(orders, "_collect_all_pages", lambda _driver, _store_info: selenium_rows)
    monkeypatch.setattr(orders, "_open_orders_history", lambda _driver, _target_date=None: None)
    monkeypatch.setattr(orders, "_wait_for_orders_page_shell", lambda _driver: True)

    rows, validation = orders._collect_with_retry_on_mismatch(
        MagicMock(),
        {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        "배달완료",
        lambda _driver: True,
        target_date="2026-08-04",
    )

    assert rows == selenium_rows
    assert validation["matched"] is True
    assert validation["collector"] == "selenium"


def test_extension_validation_mismatch_keeps_failure_without_selenium_fallback(monkeypatch):
    extension_rows = [
        {
            "주문번호": "A1",
            "주문시각": "2026. 08. 04. (화) 오후 1:00:00",
            "결제금액": "10000",
            "입금예정금액": "9000",
        }
    ]
    selenium_collector = MagicMock(return_value=[])

    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(orders, "_EXTENSION_VALIDATION_SELENIUM_FALLBACK", False)
    monkeypatch.setattr(orders, "_csv_already_covers", lambda *args, **kwargs: False)
    monkeypatch.setattr(orders, "_go_to_first_page", lambda _driver, _store: None)
    monkeypatch.setattr(orders, "_read_total_summary", lambda _driver: {"count": 2, "amount": 30000})
    monkeypatch.setattr(orders, "_collect_all_pages_with_extension", lambda _driver, _store_info: extension_rows)
    monkeypatch.setattr(orders, "_collect_all_pages", selenium_collector)
    monkeypatch.setattr(orders, "_open_orders_history", lambda _driver, _target_date=None: None)
    monkeypatch.setattr(orders, "_wait_for_orders_page_shell", lambda _driver: True)

    rows, validation = orders._collect_with_retry_on_mismatch(
        MagicMock(),
        {"store_id": "1001", "brand": "도리당", "store": "테스트점"},
        "배달완료",
        lambda _driver: True,
        target_date="2026-08-04",
    )

    assert rows == extension_rows
    assert validation["matched"] is False
    assert validation["collector"] == "extension"
    selenium_collector.assert_not_called()


def test_collect_now_and_woori_preserves_failures_after_repeated_order_date_filter_failures():
    accounts = [
        {
            "account_id": f"acct-{idx}",
            "password": "pw",
            "store_name": f"도리당 테스트{idx}점",
            "store_id": str(1000 + idx),
        }
        for idx in range(1, 6)
    ]
    driver = MagicMock()
    driver.current_url = "https://self.baemin.com/"

    with patch.object(combined, "resolve_stability_profile", return_value=_profile()), \
         patch.object(combined, "_build_account_session", return_value=driver), \
         patch.object(combined, "is_on_main_dashboard", return_value=True), \
         patch.object(combined, "wait_for_page", return_value=True), \
         patch.object(combined, "collect_woori_for_driver"), \
         patch.object(combined, "collect_shop_operation_for_driver"), \
         patch.object(
             combined,
             "collect_orders_for_driver",
             return_value={"ok": False, "reason": "date_filter", "validation": []},
         ), \
         patch.object(combined, "logout_baemin"), \
         patch.object(combined, "quit_driver_safely"), \
         patch.object(combined.random, "uniform", return_value=0), \
         patch.object(combined.time, "sleep"):
        result = combined.collect_now_and_woori(
            accounts,
            target_date="2026-07-26",
            stability_profile="test",
            _allow_login_second_pass=False,
        )

    assert result["metrics"]["orders_date_filter_abort"] is True
    assert len(result["failed"]["orders"]) == 5


def test_collect_orders_only_preserves_failures_after_repeated_order_date_filter_failures():
    accounts = [
        {
            "account_id": f"acct-{idx}",
            "password": "pw",
            "store_name": f"도리당 테스트{idx}점",
        }
        for idx in range(1, 6)
    ]
    drivers = [MagicMock(name=f"driver-{idx}") for idx in range(1, 6)]
    for driver in drivers:
        driver.current_url = "https://self.baemin.com/"

    def store_options(driver):
        idx = drivers.index(driver) + 1
        return [{"store_id": str(1000 + idx), "text": f"도리당 테스트{idx}점"}]

    with patch.object(combined, "resolve_stability_profile", return_value=_profile()), \
         patch.object(combined, "_build_dashboard_session", side_effect=drivers), \
         patch.object(combined, "_ensure_dashboard_store_select", side_effect=lambda _account, driver, _metrics, _profile: driver), \
         patch.object(combined, "get_store_options", side_effect=store_options), \
         patch.object(
             combined,
             "collect_orders_for_driver",
             return_value={"ok": False, "reason": "date_filter", "validation": []},
         ), \
         patch.object(combined, "quit_driver_safely"), \
         patch.object(combined.random, "uniform", return_value=0), \
         patch.object(combined.time, "sleep"):
        result = combined.collect_orders_only(
            accounts,
            target_date="2026-07-26",
            stability_profile="test",
        )

    assert result["metrics"]["orders_date_filter_abort"] is True
    assert len(result["failed"]["orders"]) == 5
