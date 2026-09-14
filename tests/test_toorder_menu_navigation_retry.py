from modules.extract import crawling_toorder_menu as target


def test_toorder_menu_chrome_runtime_defaults_to_container_tmp():
    constants = target._launch_browser.__code__.co_consts
    assert "TOORDER_CHROME_RUNTIME_ROOT" in constants
    assert "/tmp/toorder_chrome_runtime" in constants


def test_toorder_menu_date_input_selectors_include_generic_mui_and_text():
    assert "input.MuiInputBase-input" in target.DATE_INPUT_SELECTORS
    assert "input[type='text']" in target.DATE_INPUT_SELECTORS


def test_toorder_menu_analysis_navigation_labels_are_mapped():
    assert target.ANALYSIS_NAV_LABELS[target.MENU_ANALYSIS_URL] == "메뉴별 판매량"
    assert target.ANALYSIS_NAV_LABELS[target.OPTION_ANALYSIS_URL] == "옵션 메뉴 판매량"


def test_toorder_menu_login_navigation_retry_handles_chrome_tab_crash():
    assert target._is_retriable_navigation_error("Message: tab crashed")
    assert target._is_retriable_navigation_error("chrome not reachable")
    assert target._is_retriable_navigation_error("disconnected: not connected to DevTools")
    assert target._is_retriable_navigation_error("HTTPConnectionPool read timed out")
    assert not target._is_retriable_navigation_error("아이디 또는 비밀번호가 잘못 입력되었습니다")


def test_toorder_menu_navigation_uses_cdp_before_driver_get():
    calls = []

    class Driver:
        def execute_cdp_cmd(self, command, payload):
            calls.append((command, payload))

        def get(self, url):
            calls.append(("get", url))

    target._navigate_without_page_load_wait(Driver(), "https://example.test/login")

    assert calls == [("Page.navigate", {"url": "https://example.test/login"})]


def test_toorder_menu_navigation_falls_back_to_driver_get():
    calls = []

    class Driver:
        def execute_cdp_cmd(self, command, payload):
            raise RuntimeError("cdp down")

        def get(self, url):
            calls.append(("get", url))

    target._navigate_without_page_load_wait(Driver(), "https://example.test/login")

    assert calls == [("get", "https://example.test/login")]


def test_toorder_menu_sidebar_failure_falls_back_without_raising(monkeypatch):
    monkeypatch.setattr(target, "_click_visible_text", lambda driver, label: False)

    assert target._navigate_by_sidebar(object(), "account", target.MENU_ANALYSIS_URL) is False


def test_toorder_menu_direct_navigation_requires_date_inputs(monkeypatch):
    calls = []

    class Driver:
        current_url = target.MENU_ANALYSIS_URL

    class Wait:
        def __init__(self, driver, timeout):
            self.driver = driver

        def until(self, condition):
            result = condition(self.driver)
            if not result:
                raise RuntimeError("wait failed")
            return result

    monkeypatch.setattr(target, "_navigate_by_sidebar", lambda driver, account_id, url: False)
    monkeypatch.setattr(target, "_navigate_without_page_load_wait", lambda driver, url: calls.append(url))
    monkeypatch.setattr(target, "_find_date_inputs", lambda driver: [])
    monkeypatch.setattr(target, "WebDriverWait", Wait)

    assert target._navigate_to_analysis_page(Driver(), "account", target.MENU_ANALYSIS_URL) is False
    assert calls == [target.MENU_ANALYSIS_URL]


def test_toorder_menu_quit_driver_removes_profile_dir(tmp_path):
    profile_dir = tmp_path / "profile"
    profile_dir.mkdir()
    (profile_dir / "Preferences").write_text("{}", encoding="utf-8")

    class Driver:
        _toorder_profile_dir = str(profile_dir)

        def __init__(self):
            self.quit_called = False

        def quit(self):
            self.quit_called = True

    driver = Driver()

    target._quit_driver(driver, "account")

    assert driver.quit_called
    assert not profile_dir.exists()
