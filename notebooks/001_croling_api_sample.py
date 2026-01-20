"""
투오더(toorder) 플랫폼 크롤링 - 개선 버전
"""
import time
import random
import re
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================================
# 설정
# ============================================================================

PLATFORM_NAME = "toorder"

LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"

LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/dashboard"]

# 타이밍 대폭 단축
TIMING = {
    "page_load": (2.0, 3.0),      # 줄임
    "typing_char": (0.03, 0.08),  # 줄임
    "typing_pause": (0.2, 0.4),   # 줄임
    "before_click": (0.3, 0.5),   # 줄임
    "after_click": (1.5, 2.5),    # 줄임
    "download_wait": (3.0, 5.0),  # 줄임
}

BASE_DIR = Path.cwd()
DOWNLOAD_DIR = BASE_DIR / "download"
LOG_FILE = BASE_DIR / "logs" / "toorder_crawling.txt"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 테스트 설정
# ============================================================================

TEST_ACCOUNT = {
    "id": "doridang1",
    "pw": "ehfl3122",
}

TEST_TARGET_DATE = "2025-12-29"
HEADLESS_MODE = False


# ============================================================================
# 유틸리티
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] [{account_id}] {message}"
    print(log_msg)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_msg + "\n")


def random_delay(timing_key: str):
    min_sec, max_sec = TIMING[timing_key]
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)


def human_type(element, text: str):
    """빠른 타이핑"""
    element.clear()
    time.sleep(0.2)
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    
    time.sleep(0.3)


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser(account_id: str, headless: bool = HEADLESS_MODE):
    log(f"브라우저 실행", account_id)
    
    options = Options()
    
    if headless:
        options.add_argument('--headless=new')
    
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=ko-KR')
    
    # 다운로드 설정
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        log(f"  ✓ 브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"  ✗ 브라우저 실행 실패: {e}", account_id)
        raise


def wait_for_react_load(driver, timeout=10):
    """React 앱 로드 대기"""
    end_time = time.time() + timeout
    while time.time() < end_time:
        # React root 확인
        ready = driver.execute_script("""
            return document.querySelector('input[name="id"]') !== null;
        """)
        if ready:
            return True
        time.sleep(0.5)
    return False


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def do_login(driver, account_id: str, password: str) -> bool:
    log(f"로그인 시도", account_id)
    
    try:
        # 1. 로그인 페이지 이동
        driver.get(LOGIN_URL)
        log(f"  페이지 이동", account_id)
        
        # 2. React 앱 로드 대기
        if not wait_for_react_load(driver, timeout=15):
            log(f"  ✗ React 앱 로드 실패", account_id)
            return False
        
        log(f"  ✓ React 앱 로드 완료", account_id)
        time.sleep(1.0)  # 추가 안정화
        
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False
    
    # 3. ID 입력
    try:
        # WebDriverWait로 명시적 대기
        wait = WebDriverWait(driver, 10)
        id_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']"))
        )
        
        # 요소가 상호작용 가능할 때까지 대기
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='id']")))
        
        human_type(id_input, account_id)
        log(f"  ✓ ID 입력", account_id)
        
    except TimeoutException:
        log(f"  ✗ ID 필드 타임아웃", account_id)
        
        # 디버깅
        try:
            all_inputs = driver.find_elements(By.TAG_NAME, "input")
            log(f"  페이지의 input 수: {len(all_inputs)}", account_id)
            for inp in all_inputs[:3]:
                log(f"    name={inp.get_attribute('name')}, type={inp.get_attribute('type')}", account_id)
        except:
            pass
        
        return False
    
    # 4. 비밀번호 입력
    try:
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        human_type(pw_input, password)
        log(f"  ✓ 비밀번호 입력", account_id)
    except NoSuchElementException:
        log(f"  ✗ 비밀번호 필드 없음", account_id)
        return False
    
    # 5. 기업회원 체크박스
    time.sleep(0.3)
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        driver.execute_script("arguments[0].click();", checkbox)
        log(f"  ✓ 기업회원 체크", account_id)
    except Exception as e:
        log(f"  [경고] 체크박스: {e}", account_id)
    
    # 6. 로그인 버튼 클릭
    time.sleep(0.5)
    
    try:
        # 여러 방법으로 버튼 찾기
        submit_btn = None
        
        # 방법 1: type=submit
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        except:
            pass
        
        # 방법 2: 텍스트
        if not submit_btn:
            try:
                submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), '로그인')]")
            except:
                pass
        
        if not submit_btn:
            log(f"  ✗ 로그인 버튼 없음", account_id)
            return False
        
        # JavaScript 클릭
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit_btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", submit_btn)
        log(f"  ✓ 로그인 버튼 클릭", account_id)
        
    except Exception as e:
        log(f"  버튼 클릭 실패, Enter 시도: {e}", account_id)
        try:
            pw_input.send_keys(Keys.RETURN)
            log(f"  ✓ Enter 제출", account_id)
        except:
            return False
    
    # 7. 로그인 결과 대기
    time.sleep(3.0)  # 고정 대기
    
    current_url = driver.current_url
    log(f"  로그인 후 URL: {current_url}", account_id)
    
    if is_on_success_page(current_url) or not is_on_login_page(current_url):
        log(f"  ✅ 로그인 성공!", account_id)
        return True
    
    # 에러 확인
    try:
        alerts = driver.find_elements(By.CSS_SELECTOR, ".MuiAlert-message")
        for alert in alerts:
            log(f"  [경고] {alert.text}", account_id)
    except:
        pass
    
    log(f"  ❌ 로그인 실패", account_id)
    return False


# ============================================================================
# 보고서 생성
# ============================================================================

def navigate_to_sales_report(driver, account_id: str) -> bool:
    log(f"보고서 페이지 이동", account_id)
    
    try:
        driver.get(SALES_REPORT_URL)
        time.sleep(2.0)
        
        # React 로드 대기
        time.sleep(2.0)
        
        current_url = driver.current_url
        
        if "sales-report/orderkinds" in current_url:
            log(f"  ✓ 보고서 페이지 도착", account_id)
            return True
        else:
            log(f"  ✗ 잘못된 페이지: {current_url}", account_id)
            return False
            
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False


def click_report_button(driver, account_id: str) -> bool:
    log(f"보고서 생성 버튼 클릭", account_id)
    wait = WebDriverWait(driver, 15)
    
    # 버튼 로드 대기
    try:
        report_btn = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        # 클릭 가능할 때까지 대기
        wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        # 스크롤 & 클릭
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        
        log(f"  ✓ 보고서 생성 버튼 클릭", account_id)
        time.sleep(2.0)
        return True
        
    except TimeoutException:
        log(f"  ✗ 보고서 생성 버튼 없음", account_id)
        
        # 디버깅: 버튼 목록
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            log(f"  페이지의 버튼 수: {len(buttons)}", account_id)
            for idx, btn in enumerate(buttons[:5]):
                text = btn.text[:30] if btn.text else "(없음)"
                log(f"    버튼 {idx}: {text}", account_id)
        except:
            pass
        
        return False
    except Exception as e:
        log(f"  ✗ 버튼 클릭 실패: {e}", account_id)
        return False


def wait_for_download(download_dir: Path, timeout: int = 60) -> Path:
    log(f"다운로드 대기 (최대 {timeout}초)")
    
    existing_files = set(download_dir.glob("*"))
    end_time = time.time() + timeout
    
    while time.time() < end_time:
        time.sleep(1)
        
        current_files = set(download_dir.glob("*"))
        new_files = current_files - existing_files
        
        # .crdownload 제외
        completed_files = [f for f in new_files if not f.name.endswith('.crdownload') and f.is_file()]
        
        if completed_files:
            downloaded_file = completed_files[0]
            log(f"  ✓ 다운로드 완료: {downloaded_file.name}")
            return downloaded_file
    
    raise TimeoutError("다운로드 시간 초과")


def rename_downloaded_file(original_file: Path, account_id: str, target_date: str) -> Path:
    file_ext = original_file.suffix
    new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
    
    if new_filename.exists():
        new_filename.unlink()
    
    original_file.rename(new_filename)
    log(f"  파일명 변경: {new_filename.name}")
    
    return new_filename


# ============================================================================
# 메인 실행
# ============================================================================

def run_test():
    account_id = TEST_ACCOUNT["id"]
    password = TEST_ACCOUNT["pw"]
    target_date = TEST_TARGET_DATE
    
    print("\n" + "=" * 70)
    print(f"[{PLATFORM_NAME}] 크롤링 테스트")
    print("=" * 70)
    print(f"  계정: {account_id}")
    print(f"  날짜: {target_date}")
    print(f"  Headless: {HEADLESS_MODE}")
    print(f"  다운로드 경로: {DOWNLOAD_DIR}")
    print("=" * 70 + "\n")
    
    driver = None
    
    try:
        driver = launch_browser(account_id, headless=HEADLESS_MODE)
        
        if not do_login(driver, account_id, password):
            print("\n❌ 로그인 실패")
            return
        
        if not navigate_to_sales_report(driver, account_id):
            print("\n❌ 보고서 페이지 이동 실패")
            return
        
        if not click_report_button(driver, account_id):
            print("\n❌ 보고서 생성 버튼 클릭 실패")
            return
        
        # 다운로드가 시작될 시간만 대기
        log(f"다운로드 시작 대기", account_id)
        time.sleep(3.0)
        
        print(f"\n✅ 보고서 생성 완료!")
        print(f"  다운로드 위치: {DOWNLOAD_DIR}")
        print(f"  파일은 자동으로 저장됩니다.")
        
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            if not HEADLESS_MODE:
                input("\n[Enter]를 눌러 종료...")
            driver.quit()
            log("브라우저 종료", account_id)


def test_login_only():
    account_id = TEST_ACCOUNT["id"]
    password = TEST_ACCOUNT["pw"]
    
    print("\n로그인 테스트")
    
    driver = None
    try:
        driver = launch_browser(account_id, headless=False)
        
        if do_login(driver, account_id, password):
            print(f"\n✅ 로그인 성공!")
            print(f"URL: {driver.current_url}")
        else:
            print("\n❌ 로그인 실패")
        
        input("\n[Enter]를 눌러 종료...")
        
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║  투오더 크롤링 테스트                                         ║
╠══════════════════════════════════════════════════════════════╣
║  1. run_test()        - 전체 테스트                          ║
║  2. test_login_only() - 로그인만 테스트                       ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    run_test()