"""
투오더 크롤링 - Airflow 연동용

============================================================================
사용법
============================================================================

from modules.extract.croling_toorder import run_toorder_crawling

# 보고서 다운로드
result_df = run_toorder_crawling(account_df, target_date="2025-12-29")

============================================================================
"""

import time
import random
import re
import shutil
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# 상수 - URL
# ============================================================================
PLATFORM_NAME = "toorder"
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"


# ============================================================================
# 상수 - 타이밍
# ============================================================================
TIMING = {
    "page_load": (2.0, 3.0),
    "typing_char": (0.03, 0.08),
    "typing_pause": (0.2, 0.4),
    "before_click": (0.3, 0.5),
    "after_click": (1.5, 2.5),
    "download_wait": (3.0, 5.0),
    "account_stagger": (3.0, 7.0),
    "batch_rest": (45.0, 90.0),
}


# ============================================================================
# 상수 - 배치 설정
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)


# ============================================================================
# 상수 - 경로 (Docker 환경 고려)
# ============================================================================
BASE_DIR = os.getenv("AIRFLOW_HOME", Path.cwd())
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", f"{BASE_DIR}/download"))

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 상수 - 기타
# ============================================================================
LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/dashboard"]


# ============================================================================
# 유틸리티 함수
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{account_id}] {message}")


def random_delay(timing_key: str):
    """랜덤 딜레이"""
    min_sec, max_sec = TIMING[timing_key]
    time.sleep(random.uniform(min_sec, max_sec))


def human_type(element, text: str):
    """사람처럼 타이핑"""
    element.clear()
    time.sleep(0.2)
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    
    time.sleep(0.3)


def convert_account_df_to_list(account_df: pd.DataFrame) -> List[Dict]:
    """account_df를 계정 리스트로 변환 (toorder 필터)"""
    filtered_df = account_df[account_df["channel"] == "toorder"].copy()
    
    account_list = []
    for _, row in filtered_df.iterrows():
        account_list.append({
            "id": row["id"],
            "pw": row["pw"],
        })
    
    return account_list


def split_into_random_batches(items: List, size_range: tuple) -> List[List]:
    """랜덤 크기의 배치로 분할"""
    min_size, max_size = size_range
    batches = []
    remaining = list(items)
    
    while remaining:
        batch_size = min(random.randint(min_size, max_size), len(remaining))
        batches.append(remaining[:batch_size])
        remaining = remaining[batch_size:]
    
    return batches


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser(account_id: str, headless: bool = True):
    """브라우저 실행 (Docker/Airflow 환경 대응)"""
    log(f"브라우저 실행 (headless={headless})", account_id)
    
    options = Options()
    
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists():
        options.binary_location = chrome_bin
        log(f"Chrome 바이너리: {chrome_bin}", account_id)
    
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
        log(f"브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"브라우저 실행 실패: {e}", account_id)
        raise


def wait_for_react_load(driver, timeout=10):
    """React 앱 로드 대기"""
    end_time = time.time() + timeout
    while time.time() < end_time:
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
    """투오더 로그인"""
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
        time.sleep(1.0)
        
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False
    
    # 3. ID 입력
    try:
        wait = WebDriverWait(driver, 10)
        id_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']"))
        )
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='id']")))
        
        human_type(id_input, account_id)
        log(f"  ✓ ID 입력", account_id)
        
    except TimeoutException:
        log(f"  ✗ ID 필드 타임아웃", account_id)
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
        submit_btn = None
        
        # type=submit 버튼 찾기
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        except:
            pass
        
        # 텍스트로 찾기
        if not submit_btn:
            try:
                submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), '로그인')]")
            except:
                pass
        
        if not submit_btn:
            log(f"  ✗ 로그인 버튼 없음", account_id)
            return False
        
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
    time.sleep(3.0)
    
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
# 보고서 생성 및 다운로드
# ============================================================================

def navigate_to_sales_report(driver, account_id: str) -> bool:
    """보고서 페이지 이동"""
    log(f"보고서 페이지 이동", account_id)
    
    try:
        driver.get(SALES_REPORT_URL)
        time.sleep(2.0)
        time.sleep(2.0)  # React 로드
        
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
    """보고서 생성 버튼 클릭"""
    log(f"보고서 생성 버튼 클릭", account_id)
    wait = WebDriverWait(driver, 15)
    
    try:
        report_btn = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        
        log(f"  ✓ 보고서 생성 버튼 클릭", account_id)
        time.sleep(2.0)
        return True
        
    except TimeoutException:
        log(f"  ✗ 보고서 생성 버튼 없음", account_id)
        return False
    except Exception as e:
        log(f"  ✗ 버튼 클릭 실패: {e}", account_id)
        return False


def wait_for_download(download_dir: Path, timeout: int = 60, account_id: str = "SYSTEM") -> Path:
    """다운로드 대기"""
    log(f"다운로드 대기 (최대 {timeout}초)", account_id)
    
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
            log(f"  ✓ 다운로드 완료: {downloaded_file.name}", account_id)
            return downloaded_file
    
    raise TimeoutError("다운로드 시간 초과")


def rename_downloaded_file(original_file: Path, account_id: str, target_date: str) -> Path:
    """다운로드 파일명 변경"""
    file_ext = original_file.suffix
    new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
    
    if new_filename.exists():
        new_filename.unlink()
    
    original_file.rename(new_filename)
    log(f"  파일명 변경: {new_filename.name}", account_id)
    
    return new_filename


# ============================================================================
# 단일 계정 처리
# ============================================================================

def create_account_result(account_id: str) -> Dict[str, Any]:
    """계정 결과 딕셔너리 초기화"""
    return {
        "success": False,
        "account_id": account_id,
        "platform": PLATFORM_NAME,
        "downloaded_file": None,
        "file_size_mb": None,
        "collected_at": None,
        "error": None,
    }


def process_single_account(
    account: Dict,
    target_date: str,
) -> Dict[str, Any]:
    """단일 계정 처리"""
    account_id = account["id"]
    password = account["pw"]
    
    result = create_account_result(account_id)
    driver = None
    
    try:
        log(f"처리 시작", account_id)
        
        driver = launch_browser(account_id, headless=True)
        
        # 로그인
        if not do_login(driver, account_id, password):
            result["error"] = "로그인 실패"
            return result
        
        # 보고서 페이지 이동
        if not navigate_to_sales_report(driver, account_id):
            result["error"] = "보고서 페이지 이동 실패"
            return result
        
        # 보고서 생성 버튼 클릭
        if not click_report_button(driver, account_id):
            result["error"] = "보고서 생성 버튼 클릭 실패"
            return result
        
        # 다운로드 대기
        log(f"다운로드 시작 대기", account_id)
        time.sleep(3.0)
        
        try:
            downloaded_file = wait_for_download(DOWNLOAD_DIR, timeout=60, account_id=account_id)
            
            # 파일명 변경
            final_file = rename_downloaded_file(downloaded_file, account_id, target_date)
            
            # 결과 업데이트
            result["success"] = True
            result["downloaded_file"] = str(final_file)
            result["file_size_mb"] = round(final_file.stat().st_size / (1024 * 1024), 2)
            result["collected_at"] = datetime.now().isoformat()
            
            log(f"✅ 처리 완료 (파일: {final_file.name}, {result['file_size_mb']}MB)", account_id)
            
        except TimeoutError:
            result["error"] = "다운로드 타임아웃"
            log(f"❌ 다운로드 실패", account_id)
        
    except Exception as e:
        result["error"] = str(e)
        log(f"❌ 처리 중단: {e}", account_id)
        
    finally:
        if driver:
            try:
                driver.quit()
                log(f"브라우저 종료", account_id)
            except:
                pass
    
    return result


# ============================================================================
# 배치 처리
# ============================================================================

def process_account_with_delay(
    account: Dict,
    target_date: str,
    start_delay: float,
) -> Dict:
    """딜레이 후 계정 처리"""
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    
    return process_single_account(account, target_date)


def process_batch(
    batch: List[Dict],
    target_date: str,
) -> List[Dict]:
    """배치 처리 (병렬)"""
    results = []
    
    with ThreadPoolExecutor(max_workers=len(batch)) as executor:
        futures = []
        cumulative_delay = 0.0
        
        for idx, account in enumerate(batch):
            if idx > 0:
                interval = random.uniform(*TIMING["account_stagger"])
                cumulative_delay += interval
            
            future = executor.submit(
                process_account_with_delay,
                account,
                target_date,
                cumulative_delay,
            )
            futures.append(future)
        
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


# ============================================================================
# 메인 함수 (Airflow에서 호출)
# ============================================================================

def run_toorder_crawling(
    account_df: pd.DataFrame,
    target_date: str,
) -> pd.DataFrame:
    """
    투오더 크롤링 메인 함수
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw)
        target_date: 조회 날짜 (예: "2025-12-29")
    
    Returns:
        result_df: 다운로드 결과 DataFrame
    """
    log("=" * 60)
    log(f"투오더 크롤링 시작")
    log("=" * 60)
    
    account_list = convert_account_df_to_list(account_df)
    
    log(f"  대상 날짜: {target_date}")
    log(f"  계정 수: {len(account_list)}개")
    
    if not account_list:
        log("toorder 계정이 없습니다.")
        return pd.DataFrame()
    
    batches = split_into_random_batches(account_list, BATCH_SIZE_RANGE)
    log(f"  배치 분할: {[len(b) for b in batches]}")
    
    all_results = []
    
    for batch_idx, batch in enumerate(batches):
        log(f"\n{'=' * 40}")
        log(f"배치 {batch_idx + 1}/{len(batches)} 시작 ({len(batch)}개 계정)")
        log(f"{'=' * 40}")
        
        batch_results = process_batch(batch, target_date)
        all_results.extend(batch_results)
        
        is_not_last_batch = batch_idx < len(batches) - 1
        if is_not_last_batch:
            delay = random.uniform(*TIMING["batch_rest"])
            log(f"다음 배치까지 {delay:.1f}초 대기...")
            time.sleep(delay)
    
    result_df = pd.DataFrame(all_results)
    
    total_success = sum(1 for r in all_results if r["success"])
    total_failed = len(all_results) - total_success
    
    log("\n" + "=" * 60)
    log("투오더 크롤링 완료")
    log(f"  계정: 성공 {total_success}개, 실패 {total_failed}개")
    log(f"  결과: {len(result_df)}행")
    log("=" * 60)
    
    return result_df


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    
    test_account_df = pd.DataFrame([
        {
            "channel": "toorder",
            "id": "doridang1",
            "pw": "ehfl3122",
        },
    ])
    
    # 테스트: 보고서 다운로드
    print("\n[테스트] 보고서 다운로드")
    result_df = run_toorder_crawling(test_account_df, target_date="2025-12-29")
    print(result_df)