from modules.transform.pipelines.db import Food_Guide_orders_collect as food_guide


class _FakeElement:
    def __init__(self, *, displayed: bool = True, enabled: bool = True):
        self._displayed = displayed
        self._enabled = enabled

    def is_displayed(self):
        return self._displayed

    def is_enabled(self):
        return self._enabled


class _FakeDriver:
    def __init__(self, elements):
        self.elements = elements
        self.calls = []

    def find_elements(self, by, value):
        self.calls.append((by, value))
        return self.elements


class _FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_food_guide_button_selectors_are_scoped_to_selected_tab():
    for key in ("search_button", "export_button"):
        _, selector = food_guide.SELECTORS[key]
        assert "w2tabcontrol_contents_wrapper_selected" in selector


def test_wait_clickable_skips_hidden_candidates():
    hidden = _FakeElement(displayed=False)
    visible = _FakeElement(displayed=True)
    driver = _FakeDriver([hidden, visible])

    assert food_guide._wait_clickable(driver, "search_button", timeout=0.1) is visible


def test_grid_state_indicates_no_data_from_total_count():
    assert food_guide._parse_grid_total_count("5,345") == 5345
    assert food_guide._parse_grid_total_count("(총5,345건)", allow_bare_number=False) == 5345
    assert food_guide._grid_state_indicates_no_data({"total_count": "0"})
    assert food_guide._grid_state_indicates_no_data({"bodyText": "사업장주문내역(총0건)"})
    assert food_guide._grid_state_indicates_no_data({"bodyText": "배송기간 2026-08-27 ~ 사업장주문내역(총0건)"})
    assert not food_guide._grid_state_indicates_no_data({"total_count": "12"})
    assert not food_guide._grid_state_indicates_no_data({"bodyText": "배송기간 2026-08-27 ~ 사업장주문내역(총12건)"})
    assert not food_guide._grid_state_indicates_no_data(
        {
            "total_count": "5,345",
            "total_group_text": "(총5,345건)",
            "bodyText": "이전 탭 사업장주문내역(총0건)",
        }
    )
    assert not food_guide._grid_state_indicates_no_data(
        {
            "total_count": "",
            "total_group_text": "(총5,345건)",
            "bodyText": "이전 탭 사업장주문내역(총0건)",
        }
    )


def test_save_food_guide_orders_noops_without_downloaded_path():
    result = food_guide.save_food_guide_orders(
        downloaded_path="",
        date_from="2026-08-27",
        date_to="2026-08-27",
    )

    assert result["success"] is True
    assert result["no_data"] is True
    assert result["rows"] == 0
    assert result["parquet_files"] == []


def test_wait_for_order_grid_ready_waits_through_initial_no_data(monkeypatch):
    states = [
        {"total_count": "0", "visibleLoading": 0, "jqueryActive": 0, "grid_text": "데이터 없음"},
        {"total_count": "0", "visibleLoading": 0, "jqueryActive": 0, "grid_text": "데이터 없음"},
        {"total_count": "5,345", "visibleLoading": 0, "jqueryActive": 0, "grid_text": "배송일\t사업장"},
    ]
    clock = _FakeClock()

    monkeypatch.setattr(food_guide, "_wait_order_history_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_read_order_grid_state", lambda driver: states.pop(0))

    result = food_guide._wait_for_order_grid_ready(
        object(),
        timeout=20,
        poll_sec=5,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["grid_ready_reason"] == "data"
    assert clock.sleeps == [5, 5]


def test_wait_for_order_grid_ready_returns_immediately_when_rows_exist(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(food_guide, "_wait_order_history_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        food_guide,
        "_read_order_grid_state",
        lambda driver: {"total_count": "3", "visibleLoading": 0, "jqueryActive": 0, "has_data_rows": True},
    )

    result = food_guide._wait_for_order_grid_ready(
        object(),
        timeout=20,
        poll_sec=5,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["grid_ready_reason"] == "data"
    assert clock.sleeps == []


def test_wait_for_order_grid_ready_uses_total_group_text(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(food_guide, "_wait_order_history_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        food_guide,
        "_read_order_grid_state",
        lambda driver: {
            "total_count": "",
            "total_group_text": "(총5,345건)",
            "visibleLoading": 0,
            "jqueryActive": 0,
            "has_data_rows": False,
            "bodyText": "이전 탭 사업장주문내역(총0건)",
        },
    )

    result = food_guide._wait_for_order_grid_ready(
        object(),
        timeout=20,
        poll_sec=5,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["grid_ready_reason"] == "data"
    assert clock.sleeps == []


def test_download_food_guide_orders_clicks_search_before_waiting_for_grid(monkeypatch, tmp_path):
    clicked = []
    downloaded = tmp_path / "orders.xlsx"
    downloaded.write_bytes(b"fake")

    monkeypatch.setattr(food_guide, "_launch_browser", lambda download_dir: object())
    monkeypatch.setattr(food_guide, "_login", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_wait_ajax_idle", lambda *args, **kwargs: {})
    monkeypatch.setattr(food_guide, "_wait_visible", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_wait_order_history_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_set_date_range", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_wait_for_order_grid_ready", lambda *args, **kwargs: {"total_count": "1"})
    monkeypatch.setattr(food_guide, "_wait_for_download", lambda *args, **kwargs: downloaded)
    monkeypatch.setattr(food_guide, "_validate_download", lambda path: {"path": str(path), "size": 1})
    monkeypatch.setattr(food_guide, "_close_driver", lambda driver: None)
    monkeypatch.setattr(food_guide, "_click", lambda driver, key, timeout=food_guide.WAIT_TIMEOUT: clicked.append(key))

    result = food_guide.download_food_guide_orders(
        date_from="2026-08-27",
        date_to="2026-08-27",
        download_dir=tmp_path,
        food_guide_id="id",
        food_guide_pw="pw",
    )

    assert result["success"] is True
    assert clicked == ["order_menu", "hq_order_history", "search_button", "export_button"]


def test_download_food_guide_orders_does_not_success_no_data_by_default(monkeypatch, tmp_path):
    clicked = []

    monkeypatch.setattr(food_guide, "ALLOW_NO_DATA_SUCCESS", False)
    monkeypatch.setattr(food_guide, "_launch_browser", lambda download_dir: object())
    monkeypatch.setattr(food_guide, "_login", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_wait_ajax_idle", lambda *args, **kwargs: {})
    monkeypatch.setattr(food_guide, "_wait_visible", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_wait_order_history_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_set_date_range", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        food_guide,
        "_wait_for_order_grid_ready",
        lambda *args, **kwargs: {"total_count": "0", "total_group_text": "(총0건)", "grid_ready_reason": "stable_no_data"},
    )
    monkeypatch.setattr(food_guide, "_wait_for_download", lambda *args, **kwargs: None)
    monkeypatch.setattr(food_guide, "_save_debug_artifacts", lambda *args, **kwargs: {"html": "debug.html"})
    monkeypatch.setattr(food_guide, "_close_driver", lambda driver: None)
    monkeypatch.setattr(food_guide, "_click", lambda driver, key, timeout=food_guide.WAIT_TIMEOUT: clicked.append(key))

    try:
        food_guide.download_food_guide_orders(
            date_from="2026-08-27",
            date_to="2026-08-27",
            download_dir=tmp_path,
            food_guide_id="id",
            food_guide_pw="pw",
        )
    except RuntimeError as exc:
        assert "no_data_success_disabled=True" in str(exc)
    else:
        raise AssertionError("RuntimeError was not raised")

    assert clicked == ["order_menu", "hq_order_history", "search_button", "export_button"]
