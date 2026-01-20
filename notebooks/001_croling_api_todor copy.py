"""
투오더(toorder) 플랫폼 크롤링 - 브라우저 버튼 클릭 방식
"""
import time
import random
import re
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import os

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
MAIN_PAGE_URL = "https://ceo.toorder.co.kr/dashboard"
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"

LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/dashboard"]

TIMING = {
    "login_wait": (8.0, 12.0),
    "page_load": (4.0, 6.0),
    "typing_char": (0.05, 0.15),
    "typing_pause": (0.3, 0.6),
    "before_click": (0.5, 1.0),
    "after_click": (2.0, 3.0),
    "download_wait": (5.0, 8.0),
}

BASE_DIR = Path.cwd()
DOWNLOAD_DIR = BASE_DIR / "download"
LOG_FILE = BASE_DIR / "logs" / "toorder_crawling.txt"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_WIN_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Whale/4.35.351.13 Safari/537.36"
)


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
    log(f"  ... {delay:.1f}초 대기 ({timing_key})")
    time.sleep(delay)


def human_type(element, text: str):
    element.clear()
    random_delay("typing_pause")
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(*TIMING["typing_char"]))
    
    random_delay("typing_pause")


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser(account_id: str, headless: bool = HEADLESS_MODE):
    mode_str = "headless" if headless else "GUI"
    log(f"브라우저 실행 시도 ({mode_str})", account_id)
    
    options = Options()
    
    if headless:
        options.add_argument('--headless=new')
    
    # 봇 감지 회피
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=ko-KR')
    options.add_argument(f'--user-agent={DEFAULT_WIN_CHROME_UA}')
    
    # 다운로드 설정
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # ⭐ 프로필 디렉토리 제거 (쿠키 문제 해결)
    # options.add_argument(f'--user-data-dir={profile_path.absolute()}')
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # webdriver 속성 숨기기
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        log(f"브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"브라우저 실행 실패: {e}", account_id)
        raise


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def do_login(driver, account_id: str, password: str) -> bool:
    log(f"로그인 시도", account_id)
    wait = WebDriverWait(driver, 30)
    
    try:
        driver.get(LOGIN_URL)
        random_delay("page_load")
    except WebDriverException as e:
        log(f"  [실패] 로그인 페이지 이동 실패: {e}", account_id)
        return False
    
    # 1. ID 입력
    try:
        id_input = wait.until(EC.presence_of_element_located((By.NAME, "id")))
        human_type(id_input, account_id)
        log(f"  ✓ ID 입력", account_id)
    except TimeoutException:
        log(f"  ✗ ID 필드 없음", account_id)
        return False
    
    # 2. 비밀번호 입력
    try:
        pw_input = driver.find_element(By.NAME, "password")
        human_type(pw_input, password)
        log(f"  ✓ 비밀번호 입력", account_id)
    except NoSuchElementException:
        log(f"  ✗ 비밀번호 필드 없음", account_id)
        return False
    
    # 3. 기업회원 체크박스 클릭
    time.sleep(0.5)
    try:
        checkbox = driver.find_element(By.NAME, "isCompany")
        driver.execute_script("arguments[0].click();", checkbox)
        log(f"  ✓ 기업회원 체크", account_id)
    except Exception as e:
        log(f"  [경고] 체크박스 클릭 실패: {e}", account_id)
    
    time.sleep(1.0)
    
    # 4. 로그인 버튼 클릭
    try:
        submit_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_btn)
        log(f"  ✓ 로그인 버튼 클릭", account_id)
    except Exception as e:
        log(f"  버튼 클릭 실패, Enter 시도: {e}", account_id)
        try:
            pw_input.send_keys(Keys.RETURN)
            log(f"  ✓ Enter로 제출", account_id)
        except Exception as e2:
            log(f"  ✗ 제출 실패: {e2}", account_id)
            return False
    
    # 5. 로그인 결과 대기
    random_delay("login_wait")
    
    current_url = driver.current_url
    log(f"  로그인 후 URL: {current_url}", account_id)
    
    if is_on_success_page(current_url) or not is_on_login_page(current_url):
        log(f"  ✅ 로그인 성공!", account_id)
        return True
    
    log(f"  ❌ 로그인 실패", account_id)
    return False


# ============================================================================
# 보고서 생성 및 다운로드
# ============================================================================

def navigate_to_sales_report(driver, account_id: str) -> bool:
    """매출 보고서 페이지로 이동"""
    log(f"보고서 페이지로 이동", account_id)
    wait = WebDriverWait(driver, 30)
    
    try:
        driver.get(SALES_REPORT_URL)
        random_delay("page_load")
        
        current_url = driver.current_url
        log(f"  현재 URL: {current_url}", account_id)
        
        if "sales-report/orderkinds" in current_url:
            log(f"  ✓ 보고서 페이지 도착", account_id)
            return True
        else:
            log(f"  ✗ 잘못된 페이지", account_id)
            return False
            
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False


def click_report_button(driver, account_id: str) -> bool:
    """'보고서 생성' 버튼 클릭"""
    log(f"보고서 생성 버튼 클릭", account_id)
    wait = WebDriverWait(driver, 30)
    
    button_clicked = False
    
    # 방법 1: 버튼 텍스트로 찾기
    try:
        report_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        # 스크롤하여 버튼 보이게
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
        time.sleep(0.5)
        
        # JavaScript 클릭
        driver.execute_script("arguments[0].click();", report_btn)
        log(f"  ✓ 보고서 생성 버튼 클릭 (텍스트)", account_id)
        button_clicked = True
        
    except Exception as e:
        log(f"  방법 1 실패: {e}", account_id)
    
    # 방법 2: CSS 클래스로 찾기
    if not button_clicked:
        try:
            report_btn = driver.find_element(By.CSS_SELECTOR, "button.MuiButton-containedPrimary")
            driver.execute_script("arguments[0].click();", report_btn)
            log(f"  ✓ 보고서 생성 버튼 클릭 (CSS)", account_id)
            button_clicked = True
        except Exception as e:
            log(f"  방법 2 실패: {e}", account_id)
    
    # 방법 3: ID로 찾기
    if not button_clicked:
        try:
            report_btn = driver.find_element(By.ID, ":r33:")
            driver.execute_script("arguments[0].click();", report_btn)
            log(f"  ✓ 보고서 생성 버튼 클릭 (ID)", account_id)
            button_clicked = True
        except Exception as e:
            log(f"  방법 3 실패: {e}", account_id)
    
    if not button_clicked:
        log(f"  ✗ 보고서 생성 버튼 찾기 실패", account_id)
        
        # 디버깅: 페이지의 버튼들 출력
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            log(f"  페이지의 버튼 수: {len(buttons)}", account_id)
            for idx, btn in enumerate(buttons[:5]):
                text = btn.text[:50] if btn.text else "(텍스트 없음)"
                log(f"  버튼 {idx}: {text}", account_id)
        except:
            pass
        
        return False
    
    # 버튼 클릭 후 대기
    random_delay("after_click")
    return True


def wait_for_download(download_dir: Path, timeout: int = 30) -> Path:
    """다운로드 완료 대기"""
    log(f"다운로드 대기 중... (최대 {timeout}초)")
    
    # 기존 파일 목록
    existing_files = set(download_dir.glob("*"))
    
    end_time = time.time() + timeout
    
    while time.time() < end_time:
        time.sleep(1)
        
        # 현재 파일 목록
        current_files = set(download_dir.glob("*"))
        new_files = current_files - existing_files
        
        # .crdownload 파일 제외하고 새 파일 찾기
        completed_files = [f for f in new_files if not f.name.endswith('.crdownload')]
        
        if completed_files:
            downloaded_file = completed_files[0]
            log(f"  ✓ 다운로드 완료: {downloaded_file.name}")
            return downloaded_file
    
    raise TimeoutError("다운로드 시간 초과")


def rename_downloaded_file(original_file: Path, account_id: str, target_date: str) -> Path:
    """다운로드된 파일 이름 변경"""
    file_ext = original_file.suffix
    new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
    
    # 기존 파일이 있으면 삭제
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
    print("=" * 70 + "\n")
    
    driver = None
    
    try:
        # 1. 브라우저 실행
        driver = launch_browser(account_id, headless=HEADLESS_MODE)
        
        # 2. 로그인
        if not do_login(driver, account_id, password):
            print("\n❌ 로그인 실패")
            return
        
        # 3. 보고서 페이지로 이동
        if not navigate_to_sales_report(driver, account_id):
            print("\n❌ 보고서 페이지 이동 실패")
            return
        
        # 4. 날짜 설정 (필요한 경우)
        # TODO: 날짜 입력 필드가 있다면 여기서 설정
        
        # 5. 보고서 생성 버튼 클릭
        if not click_report_button(driver, account_id):
            print("\n❌ 보고서 생성 버튼 클릭 실패")
            return
        
        # 6. 다운로드 대기
        try:
            downloaded_file = wait_for_download(DOWNLOAD_DIR, timeout=60)
            final_file = rename_downloaded_file(downloaded_file, account_id, target_date)
            
            file_size = final_file.stat().st_size
            print(f"\n✅ 성공!")
            print(f"  파일: {final_file.name}")
            print(f"  크기: {file_size:,} bytes")
            
        except TimeoutError as e:
            print(f"\n❌ 다운로드 실패: {e}")
        
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
    """로그인만 테스트"""
    account_id = TEST_ACCOUNT["id"]
    password = TEST_ACCOUNT["pw"]
    
    print("\n" + "=" * 50)
    print("로그인 테스트")
    print("=" * 50)
    
    driver = None
    try:
        driver = launch_browser(account_id, headless=False)
        
        if do_login(driver, account_id, password):
            print("\n✅ 로그인 성공!")
            print(f"URL: {driver.current_url}")
            
            # 쿠키 확인
            cookies = driver.get_cookies()
            print(f"\n쿠키 수: {len(cookies)}")
            for cookie in cookies[:5]:
                print(f"  - {cookie['name']}: {cookie['value'][:30]}...")
        else:
            print("\n❌ 로그인 실패")
        
        input("\n[Enter]를 눌러 종료...")
        
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║  투오더 크롤링 테스트 도구                                    ║
╠══════════════════════════════════════════════════════════════╣
║  1. run_test()        - 전체 테스트 (로그인 + 보고서 다운)  ║
║  2. test_login_only() - 로그인만 테스트                       ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    run_test()