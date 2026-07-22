# modules/extract/crawling_relay_cs_fdam.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import os
import glob
import shutil
import time
import datetime as dt

try:
    from urllib3.exceptions import MaxRetryError  # type: ignore
except Exception:  # pragma: no cover
    MaxRetryError = Exception  # type: ignore[misc,assignment]


def _safe_current_url(driver) -> str:
    try:
        return driver.current_url
    except Exception:
        return "<unavailable>"


def _is_driver_disconnected_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if current is not exc and _is_driver_disconnected_error(current):
            return True
        current = current.__cause__ or current.__context__

    if isinstance(exc, (MaxRetryError, ConnectionRefusedError, BrokenPipeError)):
        return True

    msg = str(exc)
    markers = [
        "Selenium 드라이버 연결이 끊어졌습니다",
        "Connection refused",
        "Max retries exceeded with url: /session/",
        "Failed to establish a new connection",
        "chrome not reachable",
        "disconnected:",
        "invalid session id",
    ]
    return any(m in msg for m in markers)


def _is_driver_session_alive(driver) -> bool:
    try:
        driver.execute_script("return 1")
        return True
    except Exception as exc:
        if _is_driver_disconnected_error(exc):
            return False
        return False


def _create_chrome_service_with_retry(max_attempts: int = 3, base_sleep_sec: int = 5) -> Service:
    """webdriver-manager 다운로드를 재시도하여 Service 생성"""
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            if attempt > 1:
                print(f"🔁 ChromeDriver 재시도 {attempt}/{max_attempts}")

            driver_path = ChromeDriverManager().install()
            print(f"✅ ChromeDriver 경로 확보: {driver_path}")
            return Service(driver_path)

        except Exception as e:
            last_error = e
            print(f"⚠️ ChromeDriver 다운로드 실패 ({attempt}/{max_attempts}): {e}")

            if attempt < max_attempts:
                wait_sec = base_sleep_sec * attempt
                print(f"⏳ {wait_sec}초 후 재시도")
                time.sleep(wait_sec)

    raise RuntimeError(f"ChromeDriver 다운로드 재시도 모두 실패: {last_error}") from last_error


def _find_local_chromedriver_path() -> str | None:
    candidates = [
        os.environ.get("CHROMEDRIVER_PATH"),
        "/usr/local/bin/chromedriver",
        "/usr/bin/chromedriver",
    ]

    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None

def setup_chrome_driver(headless: bool = True, download_dir: str = None):
    options = Options()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')  # ✅ 추가
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

    # ✅ 매 실행마다 새 임시 프로파일 사용 → 서버 목록 등 캐시 초기화
    import tempfile
    tmp_profile = tempfile.mkdtemp()
    options.add_argument(f'--user-data-dir={tmp_profile}')

    if download_dir:
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)

    local_error = None
    manager_error = None

    local_driver_path = _find_local_chromedriver_path()
    if local_driver_path:
        try:
            service = Service(local_driver_path)
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_window_size(1920, 1080)
            print(f"✅ Chrome 드라이버 초기화 성공 (로컬 경로: {local_driver_path})")
            return driver
        except Exception as e:
            local_error = e
            print(f"⚠️ 로컬 chromedriver 경로 실패: {e}")

    try:
        driver = webdriver.Chrome(options=options)
        driver.set_window_size(1920, 1080)
        print("✅ Chrome 드라이버 초기화 성공 (Selenium Manager)")
        return driver
    except Exception as e:
        manager_error = e
        print(f"⚠️ Selenium Manager 실패, webdriver-manager 폴백 시도: {e}")

    try:
        service = _create_chrome_service_with_retry(max_attempts=3, base_sleep_sec=5)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_window_size(1920, 1080)  # ✅ 드라이버 생성 후에도 명시적 설정
        print(f"✅ Chrome 드라이버 초기화 성공 (webdriver-manager 폴백)")
        return driver
    except Exception as fallback_error:
        print(f"❌ Chrome 드라이버 초기화 최종 실패: {fallback_error}")
        raise RuntimeError(
            "Chrome 드라이버 초기화 실패 "
            f"(local: {local_error}, selenium_manager: {manager_error}, webdriver_manager: {fallback_error})"
        ) from fallback_error


def wait_page_ready(driver, timeout: int = 30):
    wait = WebDriverWait(driver, timeout)
    try:
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        spinner_selectors = [
            "//div[contains(@class,'ant-spin') and contains(@class,'ant-spin-spinning')]",
            "//div[contains(@class,'ant-spin-nested-loading') and .//div[contains(@class,'ant-spin-spinning')]]",
            "//div[contains(@class,'ant-modal') and contains(@class,'ant-modal-confirm')]",
            "//div[contains(@class,'ant-message') and contains(@class,'ant-message-notice')]",
        ]
        for selector in spinner_selectors:
            try:
                wait.until_not(EC.presence_of_element_located((By.XPATH, selector)))
            except Exception:
                pass
        time.sleep(0.5)
    except Exception as e:
        print(f"⚠️ 페이지 로딩 대기 중 이슈: {e}")


def _is_visible(element) -> bool:
    try:
        return (
            element.is_displayed()
            and element.is_enabled()
            and element.size.get("width", 0) > 0
            and element.size.get("height", 0) > 0
        )
    except Exception:
        return False


def _iter_elements_in_frames(driver, by: str, value: str):
    elements = []
    try:
        elements.extend(driver.find_elements(by, value))
    except Exception:
        pass

    for frame in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.frame(frame)
            elements.extend(driver.find_elements(by, value))
        except Exception:
            pass
        finally:
            try:
                driver.switch_to.default_content()
            except Exception:
                pass
    return elements


def _is_login_spinner_only(driver) -> bool:
    """초기 화면이 로그인 폼 없이 스피너만 보이는 상태인지 판별."""
    try:
        html = driver.page_source.lower()
        if "login_form_id" not in html:
            return False
    except Exception:
        return False

    try:
        if driver.find_elements(By.CSS_SELECTOR, "input#login_form_id, input[name='login_form_id'], input[placeholder*='ID를 입력해주세요'], input[name='id'], input[type='text'], input[type='password']"):
            return False
    except Exception:
        pass

    try:
        spin_el = driver.find_elements(By.CSS_SELECTOR, "#root .ant-spin-spinning")
        return any(el.is_displayed() for el in spin_el)
    except Exception:
        return False


def _collect_console_logs(driver, download_dir: str | None, tag: str) -> None:
    if not download_dir:
        return

    try:
        logs = driver.get_log("browser")
    except Exception:
        return

    if not logs:
        return

    try:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(download_dir, f"debug_console_{tag}_{ts}.log")
        with open(log_path, "w", encoding="utf-8") as f:
            for item in logs:
                f.write(f"{item.get('level', '')} {item.get('timestamp', '')} {item.get('message', '')}\n")
        print(f"  📋 브라우저 콘솔 로그 저장: {log_path}")
    except Exception as err:
        print(f"  ⚠️ 콘솔 로그 저장 실패: {err}")


def _collect_debug_page(driver, download_dir: str | None, tag: str) -> None:
    if not download_dir:
        return

    try:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(download_dir, f"debug_{tag}_{ts}.png")
        driver.save_screenshot(screenshot_path)
        print(f"  📸 디버그 스크린샷 저장: {screenshot_path}")

        html_path = os.path.join(download_dir, f"debug_{tag}_{ts}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"  📄 페이지 소스 저장: {html_path}")

        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            try:
                body_text = driver.execute_script("return document.body ? document.body.innerText : ''")
            except Exception:
                body_text = ""
        print(f"  📝 body 텍스트 (앞 500자):\n{body_text[:500]}")

        _collect_console_logs(driver, download_dir, tag)
    except Exception as debug_err:
        print(f"  ⚠️ 디버그 수집 실패: {debug_err}")


def _find_visible_element_with_wait(
    driver,
    locators: list[tuple[str, str]],
    timeout: int = 40,
    poll_interval: float = 0.8,
    allow_hidden: bool = False,
):
    deadline = time.time() + timeout
    last_candidates = []
    while time.time() < deadline:
        try:
            for by, value in locators:
                candidates = _iter_elements_in_frames(driver, by, value)
                if candidates:
                    last_candidates = candidates
                for element in candidates:
                    if _is_visible(element):
                        return element
        except Exception:
            pass
        time.sleep(poll_interval)

    if allow_hidden and last_candidates:
        return last_candidates[0]
    raise TimeoutException(f"요소 탐색 타임아웃: {locators}")


def _safe_get_value(element) -> str:
    try:
        return (element.get_attribute("value") or "").strip()
    except Exception:
        return ""


def _first_value_from_locators(driver, locators, allow_hidden: bool = False):
    first_candidate = None
    for by, value in locators:
        try:
            for candidate in _iter_elements_in_frames(driver, by, value):
                candidate_value = _safe_get_value(candidate)
                if candidate_value:
                    return candidate_value, candidate
                if allow_hidden and first_candidate is None:
                    first_candidate = candidate
        except Exception:
            pass
    if allow_hidden and first_candidate is not None:
        return "", first_candidate
    return "", None


def _print_login_values(driver, id_locators, pw_locators):
    id_value, id_element = _first_value_from_locators(driver, id_locators, allow_hidden=True)
    pw_value, pw_element = _first_value_from_locators(driver, pw_locators, allow_hidden=True)
    print(f"  🧾 현재 입력값: ID='{id_value}', PW=({len(pw_value)}자리)")
    if id_element is not None:
        try:
            print(f"     └─ id 입력창: visible={id_element.is_displayed()}, class={id_element.get_attribute('class')[:40]}")
        except Exception:
            pass


def _has_login_inputs(driver) -> bool:
    login_locators = [
        (By.ID, "login_form_id"),
        (By.NAME, "login_form_id"),
        (By.CSS_SELECTOR, "input#loginId"),
        (By.CSS_SELECTOR, "input[name='id']"),
        (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
        (By.CSS_SELECTOR, "input[placeholder*='ID']"),
        (By.CSS_SELECTOR, "input[placeholder*='ID를 입력해주세요']"),
        (By.CSS_SELECTOR, "input[name='username']"),
        (By.CSS_SELECTOR, "input[autocomplete='username']"),
        (By.CSS_SELECTOR, "input[type='text']"),
        (By.CSS_SELECTOR, "input#login_form_pw"),
        (By.NAME, "login_form_pw"),
        (By.CSS_SELECTOR, "input[name='password']"),
        (By.CSS_SELECTOR, "input[autocomplete='current-password']"),
        (By.CSS_SELECTOR, "input[type='password']"),
    ]

    for by, value in login_locators:
        try:
            if _iter_elements_in_frames(driver, by, value):
                return True
        except Exception:
            pass
    return False


def _safe_body_text(driver) -> str:
    try:
        return (driver.find_element(By.TAG_NAME, "body").text or "").strip().lower()
    except Exception:
        try:
            txt = driver.execute_script("return document.body ? document.body.innerText : ''")
            return (txt or "").strip().lower()
        except Exception:
            return ""


def _has_login_marker_text(text: str) -> bool:
    lowered = (text or "").lower()
    markers = [
        "login",
        "로그인",
        "server list",
        "서버 목록",
        "id를 입력해주세요",
        "비밀번호를 입력해주세요",
        "login_form_id",
        "login_form_pw",
    ]
    return any(marker in lowered for marker in markers)


def _is_login_page(driver) -> bool:
    if _has_login_inputs(driver):
        return True
    return _has_login_marker_text(_safe_body_text(driver))


def _collect_menu_nodes(driver):
    selectors = [
        "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]",
        "//div[@role='menuitem']",
        "//li[@role='menuitem']",
        "//li[contains(@class,'ant-menu-submenu')]",
        "//li[contains(@class,'ant-menu-item')]",
        "//*[contains(@class,'ant-menu') and contains(@class,'ant-menu-dark')]//li",
        "//*[contains(@class,'ant-menu-submenu') and .//*[contains(@class,'ant-menu-item')]]",
    ]
    nodes = []
    seen = set()
    for selector in selectors:
        try:
            for node in _iter_elements_in_frames(driver, By.XPATH, selector):
                key = id(node)
                if key in seen:
                    continue
                seen.add(key)
                nodes.append(node)
        except Exception:
            pass
    return nodes


def _wait_for_menu_nodes(driver, timeout: int = 60, poll_interval: float = 1.5) -> list:
    end = time.time() + timeout
    while time.time() < end:
        menu_nodes = _collect_menu_nodes(driver)
        if menu_nodes:
            return menu_nodes
        time.sleep(poll_interval)
    return []


def _has_menu_text(nodes, keyword: str) -> bool:
    for node in nodes:
        try:
            text = (node.text or "").strip()
            if keyword in text.replace("\n", ""):
                return True
        except Exception:
            pass
    return False


def _wait_for_login_success(driver, pre_url: str, timeout: int = 30) -> bool:
    end = time.time() + timeout
    stable_non_login = 0
    while time.time() < end:
        if _is_login_page(driver):
            stable_non_login = 0
            time.sleep(0.8)
            continue

        if _collect_menu_nodes(driver):
            return True

        stable_non_login += 1
        if stable_non_login >= 3:
            return True

        time.sleep(0.8)
    return False


def _find_login_input(
    driver,
    locators: list[tuple[str, str]],
    *,
    timeout: int = 70,
    debug_dir: str | None = None,
) -> tuple:
    deadline = time.time() + timeout
    last_refresh = 0
    refresh_wait = 12
    first_spin_seen = None

    while time.time() < deadline:
        try:
            element = _find_visible_element_with_wait(
                driver,
                locators,
                timeout=5,
                poll_interval=0.5,
                allow_hidden=False,
            )
            return element, "visible"
        except TimeoutException:
            pass

        now = time.time()
        if _is_login_spinner_only(driver):
            if first_spin_seen is None:
                first_spin_seen = now
                print("  ⚠️ 로그인 화면에서 스피너만 표시됨")

            if now - last_refresh >= refresh_wait:
                print("  ♻️ 스피너 고착으로 로그인 화면 새로고침")
                try:
                    driver.refresh()
                    wait_page_ready(driver, timeout=30)
                    time.sleep(2.0)
                except Exception as err:
                    print(f"  ⚠️ 새로고침 실패: {err}")
                last_refresh = now

            if now - first_spin_seen >= 45:
                break
        else:
            first_spin_seen = None
        time.sleep(0.5)

    for by, value in locators:
        try:
            candidates = _iter_elements_in_frames(driver, by, value)
            if candidates:
                return candidates[0], "hidden"
        except Exception:
            pass

    _collect_debug_page(driver, debug_dir, "login_input_wait_retry")
    raise TimeoutException(f"로그인 입력창 탐색 타임아웃: {locators}")


def _type_safely(driver, element, value: str) -> None:
    try:
        element.click()
        element.clear()
        element.send_keys(value)
        return
    except Exception:
        pass

    try:
        driver.execute_script(
            """
            const el = arguments[0];
            const value = arguments[1];
            el.focus();
            el.value = '';
            el.value = value;
            el.dispatchEvent(new Event('input', {bubbles: true}));
            el.dispatchEvent(new Event('change', {bubbles: true}));
            el.dispatchEvent(new Event('blur', {bubbles: true}));
            """,
            element,
            value,
        )
    except Exception as e:
        raise RuntimeError(f"입력 필드 입력 실패: {e}") from e


def login_relay_fms(driver, user_id: str, password: str, server_name: str = "도리당", server_number: str = "10625", download_dir: str = None):
    try:
        driver.get("https://erp.relayfms.com/login")
        wait = WebDriverWait(driver, 20)

        wait_page_ready(driver, timeout=20)
        time.sleep(2.0)

        print("📝 ID 입력 중...")
        id_locators = [
            (By.ID, "login_form_id"),
            (By.NAME, "login_form_id"),
            (By.CSS_SELECTOR, "input#loginId"),
            (By.CSS_SELECTOR, "input[name='id']"),
            (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
            (By.CSS_SELECTOR, "input[placeholder*='ID']"),
            (By.CSS_SELECTOR, "input[type='text']"),
            (By.CSS_SELECTOR, "input[autocomplete='username']"),
        ]
        pw_locators = [
            (By.ID, "login_form_pw"),
            (By.NAME, "login_form_pw"),
            (By.CSS_SELECTOR, "input#password"),
            (By.CSS_SELECTOR, "input[name='password']"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[autocomplete='current-password']"),
        ]
        login_button_locators = [
            (By.XPATH, "//button[contains(@class, 'ant-btn-primary') and "
             "(.//span[text()='Login'] or .//span[text()='로그인'] or @type='submit')]"),
            (By.XPATH, "//button[contains(@class, 'ant-btn') and "
             "(.//span[text()='Login'] or .//span[text()='로그인'] or @type='submit')]"),
            (By.XPATH, "//button[contains(normalize-space(.), '로그인')]"),
            (By.XPATH, "//button[contains(normalize-space(.), 'Login')]"),
            (By.XPATH, "//button[@type='submit']"),
            (By.CSS_SELECTOR, "button[type='submit']"),
        ]

        server_name_locators = [
            (By.ID, "login_form_server_name"),
            (By.NAME, "login_form_server_name"),
            (By.CSS_SELECTOR, "input[name='serverName']"),
            (By.CSS_SELECTOR, "input[placeholder*='서버']"),
        ]
        server_number_locators = [
            (By.ID, "login_form_server_number"),
            (By.NAME, "login_form_server_number"),
            (By.CSS_SELECTOR, "input[name='serverNumber']"),
        ]

        login_success = False
        for login_attempt in range(1, 3):
            if login_attempt > 1:
                print(f"  🔄 로그인 페이지 재진입 후 재시도 ({login_attempt}/2)")
                driver.get("https://erp.relayfms.com/login")
                wait_page_ready(driver, timeout=30)
                time.sleep(2.0)

            print(f"📝 ID 입력 중... (시도 {login_attempt}/2)")
            try:
                id_input, id_state = _find_login_input(driver, id_locators, timeout=80, debug_dir=download_dir)
                print(f"  🧭 ID 입력창 탐색 모드: {id_state}")
            except TimeoutException as exc:
                print("  ❌ 로그인 ID 입력창 탐색 실패")
                if not _is_driver_session_alive(driver):
                    raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(ID 입력창 탐색 중).") from exc
                _collect_debug_page(driver, download_dir, "login_id_not_found")
                if _has_login_inputs(driver):
                    print("  ⚠️ 입력창 후보는 존재하나 고정 selector 미탐지. 재검색 모드로 전환")
                    _print_login_values(driver, id_locators, pw_locators)
                raise RuntimeError("로그인 페이지에서 ID 입력창을 찾지 못했습니다.") from exc

            _type_safely(driver, id_input, user_id)
            time.sleep(0.5)

            print("🔒 PW 입력 중...")
            try:
                pw_input, pw_state = _find_login_input(driver, pw_locators, timeout=30, debug_dir=download_dir)
                print(f"  🧭 PW 입력창 탐색 모드: {pw_state}")
            except TimeoutException as exc:
                if not _is_driver_session_alive(driver):
                    raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(PW 입력창 탐색 중).") from exc
                _collect_debug_page(driver, download_dir, "login_pw_not_found")
                raise RuntimeError("로그인 페이지에서 비밀번호 입력창을 찾지 못했습니다.") from exc

            _type_safely(driver, pw_input, password)
            time.sleep(0.5)

            current_server_name, _ = _first_value_from_locators(driver, server_name_locators, allow_hidden=True)
            current_server_number, _ = _first_value_from_locators(driver, server_number_locators, allow_hidden=True)

            if not (
                current_server_name.strip() == server_name.strip()
                and current_server_number.strip() == server_number.strip()
            ):
                print("🔧 서버 목록 버튼 대기 중...")
                try:
                    # 페이지에 있는 모든 버튼 출력 (디버깅)
                    all_buttons = driver.find_elements(By.TAG_NAME, "button")
                    print(f"  현재 버튼 수: {len(all_buttons)}")
                    for btn in all_buttons[:10]:
                        print(f"  버튼: text='{btn.text}' class='{btn.get_attribute('class')[:40]}'")

                    server_list_btn = _find_visible_element_with_wait(
                        driver,
                        [
                            (By.XPATH, "//button[.//span[text()='Server List' or text()='서버 목록']"),
                            (By.XPATH, "//button[contains(normalize-space(.), 'Server List') or contains(normalize-space(.), '서버 목록')]"),
                        ],
                        timeout=15,
                        poll_interval=0.5,
                    )
                    print("🔧 서버 목록 추가 작업 시작...")
                    add_server_to_list(driver, server_name, server_number)
                except Exception as e:
                    print(f"⚠️ 서버 목록 버튼 없음 또는 실패 (스킵): {e.__class__.__name__}")
                    _collect_debug_page(driver, download_dir, "server_list_button_error")
            else:
                print(f"✅ 서버 값 일치 확인으로 Server List 스킵: name='{current_server_name}', number='{current_server_number}'")

            # ✅ 서버 모달 닫힌 후 폼 필드가 리셋될 수 있으므로 재확인·재입력
            time.sleep(1.0)
            try:
                id_field, _ = _find_login_input(driver, id_locators, timeout=10, debug_dir=download_dir)
                pw_field, _ = _find_login_input(driver, pw_locators, timeout=10, debug_dir=download_dir)
                id_val = id_field.get_attribute("value") or ""
                pw_val = pw_field.get_attribute("value") or ""
                if not id_val:
                    print("  ⚠️ ID 필드 비어있음 → 재입력")
                    id_field.clear()
                    id_field.send_keys(user_id)
                    time.sleep(0.3)
                if not pw_val:
                    print("  ⚠️ PW 필드 비어있음 → 재입력")
                    pw_field.clear()
                    pw_field.send_keys(password)
                    time.sleep(0.3)
            except Exception as e:
                print(f"  ⚠️ 폼 필드 재확인 중 오류: {e}")

            pre_url = driver.current_url

            print("🚀 로그인 버튼 클릭 중...")
            try:
                login_button = _find_visible_element_with_wait(driver, login_button_locators, timeout=20)
            except TimeoutException as exc:
                print("  ⚠️ 로그인 버튼 탐색 실패, Enter fallback로 진행")
                if not _is_driver_session_alive(driver):
                    raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(로그인 버튼 탐색 중).") from exc
                _collect_debug_page(driver, download_dir, "login_button_not_found")
                raise RuntimeError("로그인 버튼을 찾지 못했습니다.") from exc

            driver.execute_script("arguments[0].click();", login_button)

            if _wait_for_login_success(driver, pre_url, timeout=60):
                print(f"✅ 로그인 완료 확인: {_safe_current_url(driver)}")
                login_success = True
                break

            # 에러 메시지 확인
            err_els = driver.find_elements(
                By.XPATH,
                "//*[contains(@class,'ant-form-item-explain') or contains(@class,'ant-message-error') or contains(@class,'ant-alert-error')]"
            )
            for el in err_els[:3]:
                print(f"  ❗ 에러 메시지: {el.text}")
            _print_login_values(driver, id_locators, pw_locators)
            _collect_debug_page(driver, download_dir, f"login_not_completed_attempt_{login_attempt}")

            # Enter 키 폴백: 버튼 클릭이 안 먹혔을 경우
            print("  🔄 Enter 키로 로그인 재시도...")
            try:
                pw_field, _ = _find_login_input(driver, pw_locators, timeout=10, debug_dir=download_dir)
                pw_field.send_keys(Keys.RETURN)
                time.sleep(3.0)
                if _wait_for_login_success(driver, pre_url, timeout=15):
                    print(f"  ✅ Enter 키로 로그인 성공: {driver.current_url}")
                    login_success = True
                    break

                print(f"  ⚠️ Enter 키 후에도 로그인 미완료: {driver.current_url}")
                _collect_debug_page(driver, download_dir, f"login_enter_not_completed_attempt_{login_attempt}")
            except Exception as enter_err:
                if isinstance(enter_err, WebDriverException) and _is_driver_disconnected_error(enter_err):
                    raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(Enter 재시도 중).") from enter_err
                print(f"  ⚠️ Enter 키 재시도 실패: {enter_err}")
                _collect_debug_page(driver, download_dir, f"login_enter_failed_attempt_{login_attempt}")

        if not login_success:
            raise RuntimeError(
                "로그인 실패: 인증 미성립(로그인 화면 잔존). "
                "자격증명·서버선택 상태를 확인하세요."
            )

        wait_page_ready(driver, timeout=30)

# login_relay_fms 내 사이드 메뉴 대기 부분 수정

        print("⏳ 사이드 메뉴 로딩 중...")
        menu_loaded = False
        for attempt in range(3):
            try:
                menus = _wait_for_menu_nodes(driver, timeout=45, poll_interval=1.5)
                print(f"  메뉴 후보 수: {len(menus)}")
                if _has_menu_text(menus, "CS관리") or _has_menu_text(menus, "매장CS"):
                    print("✅ 사이드 메뉴 로딩 완료")
                    menu_loaded = True
                    break

                if menus:
                    print("  메뉴는 보이나 CS관리 항목 미탐색. 새로고침 후 재시도")
                else:
                    print("  메뉴 미탐색. 새로고침 후 재시도")
            except Exception as err:
                print(f"  ⚠️ 사이드 메뉴 대기 실패 (시도 {attempt+1}/3): {err}")

            if attempt < 2:
                driver.refresh()
                wait_page_ready(driver, timeout=30)
                time.sleep(3.0)

        if not menu_loaded:
            print("⚠️ 사이드 메뉴 미로딩 → 디버깅 정보 수집")
            
            # 현재 URL 확인
            print(f"  현재 URL: {_safe_current_url(driver)}")
            
            # 스크린샷 저장
            screenshot_path = os.path.join(download_dir, "debug_screenshot.png")
            driver.save_screenshot(screenshot_path)
            print(f"  📸 스크린샷 저장: {screenshot_path}")
            
            # 페이지 소스 일부 저장
            page_source = driver.page_source
            debug_html_path = os.path.join(download_dir, "debug_page.html")
            with open(debug_html_path, 'w', encoding='utf-8') as f:
                f.write(page_source)
            print(f"  📄 페이지 소스 저장: {debug_html_path}")
            
            # body 텍스트 출력 (핵심)
            body_text = _safe_body_text(driver)
            print(f"  📝 body 텍스트 (앞 500자):\n{body_text[:500]}")
            
            # 혹시 에러 메시지나 로그인 페이지로 돌아갔는지 확인
            if _is_login_page(driver) or _has_login_marker_text(body_text):
                print("  ❌ 로그인 화면 잔존 → 로그인 실패")
                raise RuntimeError(
                    "로그인 실패: 인증 미성립(로그인 화면 잔존). "
                    "자격증명·서버선택 상태를 확인하세요."
                )
            if 'login' in _safe_current_url(driver).lower():
                print("  ❌ 로그인 페이지로 돌아감 → 로그인 실패")
                raise RuntimeError(
                    "로그인 실패: 로그인 페이지에서 벗어나지 못함. "
                    "자격증명 또는 서버 선택 상태를 확인하세요."
                )

    except Exception as e:
        print(f"❌ 로그인 실패: {e}")
        raise
    


def add_server_to_list(driver, server_name: str = "도리당", server_number: str = "10625"):
    try:
        wait = WebDriverWait(driver, 15)

        print("  ├─ 📋 Server List 버튼 클릭")
        server_list_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[.//span[text()='Server List' or text()='서버 목록']]")
            )
        )
        driver.execute_script("arguments[0].click();", server_list_btn)
        time.sleep(1.5)

        print("  ├─ ➕ 추가(Add) 버튼 클릭")
        add_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class,'ant-btn-primary') and (.//span[@aria-label='plus-circle'] or .//span[text()='Add' or text()='추가'])]")
            )
        )
        driver.execute_script("arguments[0].click();", add_btn)
        time.sleep(0.8)

        print(f"  ├─ 📝 사용처명 입력: {server_name}")
        name_input = wait.until(EC.presence_of_element_located((By.ID, "edit_form_name")))
        name_input.clear()
        name_input.send_keys(server_name)
        time.sleep(0.3)

        print(f"  ├─ 🔢 연결서버 입력: {server_number}")
        server_input = driver.find_element(By.ID, "edit_form_server_number")
        server_input.clear()
        server_input.send_keys(server_number)
        time.sleep(0.3)

        print("  ├─ ✅ 추가(Add) 제출 버튼 클릭")
        # 폼 내 Add/추가 버튼 (마지막 primary 버튼)
        submit_buttons = driver.find_elements(
            By.XPATH,
            "//button[contains(@class,'ant-btn-primary') and (.//span[text()='Add'] or .//span[text()='추가'])]"
        )
        if submit_buttons:
            driver.execute_script("arguments[0].click();", submit_buttons[-1])
        time.sleep(0.8)

        print("  └─ ✅ 선택(Select) 버튼 클릭")
        select_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class,'ant-btn-primary') and (.//span[text()='Select'] or .//span[text()='선택'])]")
            )
        )
        driver.execute_script("arguments[0].click();", select_btn)
        time.sleep(0.5)
        print("  ✅ 서버 추가 및 선택 완료")

    except Exception as e:
        print(f"  ⚠️ 서버 추가 실패: {e.__class__.__name__}: {str(e)[:100]}")
        try:
            select_btns = driver.find_elements(
                By.XPATH,
                "//button[contains(@class,'ant-btn-primary') and (.//span[text()='Select'] or .//span[text()='선택'])]"
            )
            if select_btns:
                driver.execute_script("arguments[0].click();", select_btns[0])
                print("  └─ Select 버튼 클릭 성공")
                time.sleep(0.5)
            else:
                driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                print("  └─ ESC로 모달 닫기")
                time.sleep(0.3)
        except Exception:
            pass

def expand_submenu_and_click_item(driver, submenu_text: str, item_text: str, timeout: int = 20):
    wait = WebDriverWait(driver, timeout)

    end = time.time() + timeout
    submenu_title = None
    submenu_xpaths = [
        "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]",
        "//div[contains(@class,'ant-menu-submenu-title')]",
        "//li[contains(@class,'ant-menu-submenu')]",
        "//li[@role='menuitem' and contains(@class,'ant-menu-submenu')]",
        "//div[@role='menuitem']",
        "//li[@role='menuitem']",
    ]

    while time.time() < end:
        for xpath in submenu_xpaths:
            try:
                for candidate in _iter_elements_in_frames(driver, By.XPATH, xpath):
                    try:
                        text = (candidate.text or "").replace("\n", "")
                        if submenu_text not in text:
                            continue
                        if _is_visible(candidate):
                            submenu_title = candidate
                            break
                    except Exception:
                        pass
                if submenu_title:
                    break
            except Exception:
                pass
        if submenu_title:
            break
        time.sleep(0.5)

    if submenu_title is None:
        raise TimeoutException(f"사이드메뉴 '{submenu_text}' 탐색 실패")

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submenu_title)
    time.sleep(0.2)

    try:
        expanded = submenu_title.get_attribute("aria-expanded")
    except Exception:
        expanded = None

    if expanded != "true":
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(driver).move_to_element(submenu_title).perform()
            time.sleep(0.1)
        except Exception:
            pass
        driver.execute_script("arguments[0].click();", submenu_title)
        try:
            wait.until(lambda d: submenu_title.get_attribute("aria-expanded") == "true")
        except Exception:
            pass
        wait_page_ready(driver, timeout=15)
        time.sleep(1.0)

    popup_id = submenu_title.get_attribute("aria-controls")

    item = None
    if popup_id:
        popup_path = [
            f"//*[@id='{popup_id}']//li[@role='menuitem' and contains(normalize-space(.), '{item_text}')]",
            f"//*[@id='{popup_id}']//li[contains(@class,'ant-menu-item') and contains(normalize-space(.), '{item_text}')]",
            f"//*[@id='{popup_id}']//a[contains(normalize-space(.), '{item_text}')]",
        ]
        for path in popup_path:
            try:
                item = wait.until(EC.element_to_be_clickable((By.XPATH, path)))
                if item:
                    break
            except Exception:
                pass

    if item is None:
        item_end = time.time() + timeout
        item_xpaths = [
            "//li[@role='menuitem' and contains(normalize-space(.), '{item_text}')]",
            "//li[contains(@class,'ant-menu-item') and contains(normalize-space(.), '{item_text}')]",
            "//a[contains(normalize-space(.), '{item_text}')]",
        ]
        while time.time() < item_end:
            for path_tmpl in item_xpaths:
                try:
                    path = path_tmpl.format(item_text=item_text)
                    for candidate in _iter_elements_in_frames(driver, By.XPATH, path):
                        if _is_visible(candidate):
                            item = candidate
                            break
                    if item:
                        break
                except Exception:
                    pass
            if item:
                break
            time.sleep(0.5)

    if item is None:
        raise TimeoutException(f"사이드 메뉴 항목 '{item_text}' 탐색 실패")

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    time.sleep(0.1)
    try:
        from selenium.webdriver.common.action_chains import ActionChains
        ActionChains(driver).move_to_element(item).perform()
    except Exception:
        pass
    driver.execute_script("arguments[0].click();", item)
    wait_page_ready(driver, timeout=30)
    time.sleep(0.3)


def navigate_to_cs_market(driver, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            print(f"📂 CS관리 메뉴 클릭 중... (시도 {attempt+1}/{max_retries})")
            
            # 메뉴 존재 확인
            menus = _wait_for_menu_nodes(driver, timeout=60, poll_interval=1.5)
            print(f"  🔎 현재 서브메뉴 수: {len(menus)}")
            
            if len(menus) == 0:
                print("  ⚠️ 메뉴 없음 → 새로고침 후 재시도")
                if attempt < max_retries - 1:
                    driver.refresh()
                    wait_page_ready(driver, timeout=30)
                    time.sleep(5.0)
                continue
            
            expand_submenu_and_click_item(driver, submenu_text="CS관리", item_text="매장CS")
            print("✅ 매장CS 페이지 이동 완료")
            return
            
        except Exception as e:
            print(f"  ❌ 시도 {attempt+1} 실패: {e}")
            if attempt < max_retries - 1:
                driver.refresh()
                wait_page_ready(driver, timeout=30)
                time.sleep(5.0)
            else:
                # 디버깅 정보 출력
                try:
                    menus = _collect_menu_nodes(driver)
                    print(f"  🔎 최종 서브메뉴 수: {len(menus)}")
                    for m in menus[:10]:
                        try:
                            txt = (m.text or "").strip().replace("\n", " / ")
                            print(f"   - {txt}")
                        except Exception:
                            pass
                except Exception:
                    pass
                raise


def click_excel_download_and_get_file(
    driver,
    download_dir: str = None,
    timeout: int = 60
) -> str:
    wait = WebDriverWait(driver, timeout)
    wait_page_ready(driver, timeout=20)

    before_files = set(
        glob.glob(os.path.join(download_dir, "*.xlsx")) +
        glob.glob(os.path.join(download_dir, "*.xls"))
    )

    print("📥 엑셀 다운로드 버튼 클릭 중...")
    excel_btn = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//button[contains(@class,'ant-btn') and .//span[contains(text(),'엑셀다운로드')]]"
        ))
    )
    driver.execute_script("arguments[0].click();", excel_btn)
    print("✅ 버튼 클릭 완료, 다운로드 대기 중...")

    deadline = time.time() + timeout
    new_file = None

    while time.time() < deadline:
        time.sleep(2.0)

        # .crdownload 여부 관계없이 새 xlsx 파일 직접 탐지
        after_files = set(
            glob.glob(os.path.join(download_dir, "*.xlsx")) +
            glob.glob(os.path.join(download_dir, "*.xls"))
        )
        new_files = after_files - before_files

        if new_files:
            candidate = max(new_files, key=os.path.getctime)
            # 파일 크기가 안정적(변화 없음)이면 다운로드 완료로 판단
            try:
                size1 = os.path.getsize(candidate)
                time.sleep(2.0)
                size2 = os.path.getsize(candidate)
                if size1 == size2 and size1 > 0:
                    new_file = candidate
                    print(f"✅ 다운로드 완료: {new_file} ({size1:,} bytes)")
                    break
                else:
                    print(f"  ⏳ 다운로드 진행 중... ({size1:,} → {size2:,} bytes)")
            except OSError:
                print("  ⏳ 파일 준비 중...")
        else:
            crdownloads = glob.glob(os.path.join(download_dir, "*.crdownload"))
            if crdownloads:
                print(f"  ⏳ 다운로드 진행 중... (.crdownload 감지됨)")
            else:
                print("  ⏳ 파일 생성 대기 중...")

    if not new_file:
        raise TimeoutError("❌ 엑셀 파일 다운로드 시간 초과")

    return new_file


def run_relay_cs_crawling(
    user_id: str,
    password: str,
    headless: bool = True,
    server_name: str = "도리당",
    server_number: str = "10625",
    download_dir: str = None
) -> str:
    """전체 크롤링 실행 → 다운로드된 파일 경로 반환"""
    if download_dir is None:
        raise ValueError("download_dir 필수")

    download_dir = os.path.abspath(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    last_error: Exception | None = None

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        driver = None
        try:
            if attempt > 1:
                sleep_sec = 5 * attempt
                print(f"🔁 크롤링 재시도 {attempt}/{max_attempts} (대기 {sleep_sec}s)")
                time.sleep(sleep_sec)

            print("=" * 60)
            print("🚀 Relay CS 크롤링 시작")
            print("=" * 60)

            driver = setup_chrome_driver(headless=headless, download_dir=download_dir)
            login_relay_fms(driver, user_id, password, server_name, server_number, download_dir=download_dir)
            navigate_to_cs_market(driver)
            file_path = click_excel_download_and_get_file(driver, download_dir=download_dir, timeout=180)

            print("=" * 60)
            print(f"🎉 크롤링 완료: {file_path}")
            print("=" * 60)
            return file_path

        except Exception as e:
            last_error = e
            if _is_driver_disconnected_error(e):
                print(f"⚠️ Selenium 세션 종료/연결 끊김 감지: {e.__class__.__name__}")
                continue
            raise
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                print("✅ 드라이버 종료")

    raise RuntimeError(f"Relay CS 크롤링 재시도({max_attempts}회) 모두 실패: {last_error}") from last_error

