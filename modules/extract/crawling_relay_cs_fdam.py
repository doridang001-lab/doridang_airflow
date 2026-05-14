# modules/extract/crawling_relay_cs_fdam.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException
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


def login_relay_fms(driver, user_id: str, password: str, server_name: str = "도리당", server_number: str = "10625", download_dir: str = None):
    try:
        driver.get("https://erp.relayfms.com/login")
        wait = WebDriverWait(driver, 20)

        wait_page_ready(driver, timeout=20)
        time.sleep(2.0)

        print("📝 ID 입력 중...")
        id_input = wait.until(EC.presence_of_element_located((By.ID, "login_form_id")))
        id_input.clear()
        id_input.send_keys(user_id)
        time.sleep(0.5)

        print("🔒 PW 입력 중...")
        pw_input = driver.find_element(By.ID, "login_form_pw")
        pw_input.clear()
        pw_input.send_keys(password)
        time.sleep(0.5)

        # ✅ 서버 목록 버튼 대기 - 실패해도 계속 진행
        print("🔧 서버 목록 버튼 대기 중...")
        try:
            # 페이지에 있는 모든 버튼 출력 (디버깅)
            all_buttons = driver.find_elements(By.TAG_NAME, "button")
            print(f"  현재 버튼 수: {len(all_buttons)}")
            for btn in all_buttons[:10]:
                print(f"  버튼: text='{btn.text}' class='{btn.get_attribute('class')[:40]}'")

            WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[.//span[text()='Server List' or text()='서버 목록']]")
                )
            )
            print("🔧 서버 목록 추가 작업 시작...")
            add_server_to_list(driver, server_name, server_number)
        except Exception as e:
            print(f"⚠️ 서버 목록 버튼 없음 (스킵): {e.__class__.__name__}")

        # ✅ 서버 모달 닫힌 후 폼 필드가 리셋될 수 있으므로 재확인·재입력
        time.sleep(1.0)
        try:
            id_field = driver.find_element(By.ID, "login_form_id")
            pw_field = driver.find_element(By.ID, "login_form_pw")
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
        login_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 "//button[contains(@class, 'ant-btn-primary') and "
                 "(.//span[text()='Login'] or .//span[text()='로그인'] or @type='submit')]")
            )
        )
        driver.execute_script("arguments[0].click();", login_button)

        post_wait = WebDriverWait(driver, 60)

        try:
            post_wait.until(lambda d: d.current_url != pre_url)
            print(f"✅ URL 변경 확인: {_safe_current_url(driver)}")
        except Exception as e:
            if isinstance(e, WebDriverException) and _is_driver_disconnected_error(e):
                raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(로그인 직후).") from e

            print(f"⚠️ URL 미변경, 현재: {_safe_current_url(driver)}")
            # ✅ Enter 키 폴백: 버튼 클릭이 안 먹혔을 경우
            print("  🔄 Enter 키로 로그인 재시도...")
            try:
                pw_field = driver.find_element(By.ID, "login_form_pw")
                pw_field.send_keys(Keys.RETURN)
                time.sleep(3.0)
                if driver.current_url != pre_url:
                    print(f"  ✅ Enter 키로 로그인 성공: {driver.current_url}")
                else:
                    # 에러 메시지 확인
                    err_els = driver.find_elements(
                        By.XPATH,
                        "//*[contains(@class,'ant-form-item-explain') or contains(@class,'ant-message-error') or contains(@class,'ant-alert-error')]"
                    )
                    for el in err_els[:3]:
                        print(f"  ❗ 에러 메시지: {el.text}")
                    print(f"  ⚠️ Enter 키 후에도 URL 미변경: {driver.current_url}")
            except Exception as enter_err:
                if isinstance(enter_err, WebDriverException) and _is_driver_disconnected_error(enter_err):
                    raise RuntimeError("Selenium 드라이버 연결이 끊어졌습니다(Enter 재시도 중).") from enter_err
                print(f"  ⚠️ Enter 키 재시도 실패: {enter_err}")

        wait_page_ready(driver, timeout=30)

# login_relay_fms 내 사이드 메뉴 대기 부분 수정

        print("⏳ 사이드 메뉴 로딩 대기 중...")
        menu_loaded = False
        for attempt in range(3):
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]"
                    ))
                )
                print("✅ 사이드 메뉴 로딩 완료")
                menu_loaded = True
                break
            except Exception:
                print(f"  ⚠️ 사이드 메뉴 대기 실패 (시도 {attempt+1}/3), 페이지 새로고침...")
                driver.refresh()
                wait_page_ready(driver, timeout=30)
                time.sleep(3.0)

        if not menu_loaded:
            print("⚠️ 사이드 메뉴 미로딩 → 디버깅 정보 수집")
            
            # 현재 URL 확인
            print(f"  현재 URL: {driver.current_url}")
            
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
            body_text = driver.find_element(By.TAG_NAME, 'body').text
            print(f"  📝 body 텍스트 (앞 500자):\n{body_text[:500]}")
            
            # 혹시 에러 메시지나 로그인 페이지로 돌아갔는지 확인
            if 'login' in driver.current_url.lower():
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

    submenu_title = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')"
            " and .//span[contains(@class,'ant-menu-title-content')]"
            f"[.//span[contains(normalize-space(.), '{submenu_text}')]]]"
        ))
    )
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

    if popup_id:
        popup = wait.until(EC.presence_of_element_located((By.ID, popup_id)))
        item_xpath = (
            f"//*[@id='{popup_id}']"
            "//li[@role='menuitem' and .//span[contains(@class,'ant-menu-title-content')]"
            f"[contains(normalize-space(.), '{item_text}')]]"
        )
        item = wait.until(EC.element_to_be_clickable((By.XPATH, item_xpath)))
    else:
        item_xpath = (
            "//li[contains(@class,'ant-menu-submenu') and .//div[@role='menuitem']"
            "  [.//span[contains(@class,'ant-menu-title-content')]"
            f"   [.//span[contains(normalize-space(.), '{submenu_text}')]]]]"
            "//li[@role='menuitem' and .//span[contains(@class,'ant-menu-title-content')]"
            f"[contains(normalize-space(.), '{item_text}')]]"
        )
        item = wait.until(EC.element_to_be_clickable((By.XPATH, item_xpath)))

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
            menus = driver.find_elements(
                By.XPATH, 
                "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]"
            )
            print(f"  🔎 현재 서브메뉴 수: {len(menus)}")
            
            if len(menus) == 0:
                print("  ⚠️ 메뉴 없음 → 새로고침 후 재시도")
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
                    menus = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]")
                    print(f"  🔎 최종 서브메뉴 수: {len(menus)}")
                    for m in menus[:10]:
                        try:
                            txt = m.find_element(By.XPATH, ".//span[contains(@class,'ant-menu-title-content')]").text
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

    os.makedirs(download_dir, exist_ok=True)
    last_error: Exception | None = None

    for attempt in range(1, 3):
        driver = None
        try:
            if attempt > 1:
                sleep_sec = 5 * attempt
                print(f"🔁 크롤링 재시도 {attempt}/2 (대기 {sleep_sec}s)")
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

    raise RuntimeError(f"Relay CS 크롤링 재시도(2회) 모두 실패: {last_error}") from last_error
