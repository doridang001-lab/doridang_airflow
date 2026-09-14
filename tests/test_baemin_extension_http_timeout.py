"""확장 orders collector 호출 동안만 chromedriver HTTP timeout을 올리고 되돌리는지 검증."""

from types import SimpleNamespace

import pytest

from modules.transform.pipelines.db import DB_Beamin_04_orders as mod


class _FakeDriver:
    def __init__(self, timeout: int):
        self.command_executor = SimpleNamespace(
            _client_config=SimpleNamespace(timeout=timeout, init_args_for_pool_manager={}),
            _get_connection_manager=lambda: object(),
        )
        self.seen_timeouts: list[int] = []

    def set_script_timeout(self, value):
        pass


def _install_fake_apply(monkeypatch, driver):
    from modules.transform.utility import selenium_uc

    def fake_apply(d, timeout_sec, log_fn=None):
        d.command_executor._client_config.timeout = timeout_sec
        d.seen_timeouts.append(timeout_sec)

    monkeypatch.setattr(selenium_uc, "_apply_failfast_client", fake_apply)


def test_http_timeout_raised_during_collect_and_restored(monkeypatch):
    driver = _FakeDriver(timeout=90)
    _install_fake_apply(monkeypatch, driver)
    monkeypatch.setattr(mod, "_inject_baemin_extension_collector", lambda d: None)
    monkeypatch.setattr(mod, "_EXTENSION_COLLECT_TIMEOUT_SEC", 300)

    def fake_script(d, store_info, timeout_sec):
        assert d.command_executor._client_config.timeout == 360
        return {"success": True, "rows": 1, "csv": "주문번호\n1\n"}

    monkeypatch.setattr(mod, "_run_extension_collect_script", fake_script)
    monkeypatch.setattr(mod, "_rows_from_extension_csv", lambda text: [{"주문번호": "1"}])

    rows = mod._collect_all_pages_with_extension(driver, {"brand": "도리당", "store": "미사점", "store_id": "1"})

    assert rows == [{"주문번호": "1"}]
    assert driver.seen_timeouts == [360, 90]
    assert driver.command_executor._client_config.timeout == 90


def test_http_timeout_restored_even_when_collect_raises(monkeypatch):
    driver = _FakeDriver(timeout=90)
    _install_fake_apply(monkeypatch, driver)
    monkeypatch.setattr(mod, "_inject_baemin_extension_collector", lambda d: None)

    def boom(d, store_info, timeout_sec):
        raise RuntimeError("collector died")

    monkeypatch.setattr(mod, "_run_extension_collect_script", boom)

    with pytest.raises(RuntimeError):
        mod._collect_all_pages_with_extension(driver, {"store": "대화점"})

    assert driver.seen_timeouts[-1] == 90


def test_http_timeout_untouched_when_already_long(monkeypatch):
    driver = _FakeDriver(timeout=600)
    _install_fake_apply(monkeypatch, driver)
    monkeypatch.setattr(mod, "_inject_baemin_extension_collector", lambda d: None)
    monkeypatch.setattr(mod, "_run_extension_collect_script", lambda d, s, t: {"success": True, "rows": 0, "csv": ""})
    monkeypatch.setattr(mod, "_read_total_summary", lambda d: {"count": 0, "amount": 0})

    assert mod._collect_all_pages_with_extension(driver, {"store": "x"}) == []
    assert driver.seen_timeouts == []
