import pytest

from modules.extract import crawling_toorder_sales_report as target


class DummyDriver:
    def __init__(self):
        self.cookies_deleted = 0

    def delete_all_cookies(self):
        self.cookies_deleted += 1


def test_do_login_retries_with_company_unchecked_for_account_type_failure(monkeypatch):
    driver = DummyDriver()
    attempts = []

    def fake_login_once(driver, account_id, password, *, is_company):
        attempts.append(is_company)
        if is_company:
            driver._toorder_login_error = "계정유형 또는 아이디 또는 비밀번호 오류"
            return False
        return True

    monkeypatch.setattr(target, "_do_login_once", fake_login_once)

    assert target._do_login(driver, "doridang100001", "pw") is True
    assert attempts == [True, False]
    assert driver.cookies_deleted == 1


def test_do_login_returns_false_after_both_company_attempts_fail(monkeypatch):
    driver = DummyDriver()
    attempts = []

    def fake_login_once(driver, account_id, password, *, is_company):
        attempts.append(is_company)
        driver._toorder_login_error = "아이디 또는 비밀번호가 잘못 입력 되었습니다"
        return False

    monkeypatch.setattr(target, "_do_login_once", fake_login_once)

    assert target._do_login(driver, "doridang100001", "pw") is False
    assert attempts == [True, False]
    assert "isCompany attempts=[True, False]" in driver._toorder_login_error


def test_do_login_skips_toggle_retry_for_non_account_type_failure(monkeypatch):
    driver = DummyDriver()
    attempts = []

    def fake_login_once(driver, account_id, password, *, is_company):
        attempts.append(is_company)
        driver._toorder_login_error = "React 앱 로드 타임아웃"
        return False

    monkeypatch.setattr(target, "_do_login_once", fake_login_once)

    assert target._do_login(driver, "doridang100001", "pw") is False
    assert attempts == [True]
    assert driver.cookies_deleted == 0


def test_do_login_propagates_retriable_driver_exception(monkeypatch):
    driver = DummyDriver()
    attempts = []

    def fake_login_once(driver, account_id, password, *, is_company):
        attempts.append(is_company)
        raise RuntimeError("chrome not reachable")

    monkeypatch.setattr(target, "_do_login_once", fake_login_once)

    with pytest.raises(RuntimeError, match="chrome not reachable"):
        target._do_login(driver, "doridang100001", "pw")
    assert attempts == [True]
