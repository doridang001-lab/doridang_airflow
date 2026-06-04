"""
투오더 크롤링 - Airflow 연동용 (날짜 범위 버전)

============================================================================
사용법
============================================================================

from modules.extract.croling_toorder_range import run_toorder_crawling_range

# 날짜 범위 지정 다운로드 (각 날짜별로 개별 다운로드)
result_df = run_toorder_crawling_range(
    account_df,
    start_date="2025-10-10",
    end_date="2025-10-30"
)

# 위 예시는 아래처럼 동작:
# 2025-10-10 ~ 2025-10-10 다운로드
# 2025-10-11 ~ 2025-10-11 다운로드
# ...
# 2025-10-30 ~ 2025-10-30 다운로드

============================================================================
"""

import time
import random
import re
import shutil
import os
import platform
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from modules.transform.utility.selenium_uc import launch_uc_chrome


# ============================================================================
# 상수 - 브라우저 설정 (테스트 시 여기서 변경!)
# ============================================================================
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None


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
    "date_interval": (2.0, 4.0),  # 날짜별 다운로드 간격
}


# ============================================================================
# 상수 - 배치 설정
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)


# ============================================================================
# 상수 - 경로 (Windows/WSL/Docker 모두 지원)
# ============================================================================
def get_download_dir():
    """실행 환경에 맞는 다운로드 디렉토리 반환"""
    # Windows 환경 (가장 먼저 확인)
    if platform.system() == "Windows":
        return Path.home() / "Downloads" / "airflow_download"
    
    # Docker 환경 (AIRFLOW_HOME 환경변수 존재)
    if os.getenv("AIRFLOW_HOME"):
        return Path("/opt/airflow/download")
    
    # WSL/Linux 환경
    return Path("/mnt/d/Download")

DOWNLOAD_DIR = get_download_dir()
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


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """
    시작일부터 종료일까지의 날짜 리스트 생성
    
    Parameters:
        start_date: 시작 날짜 (예: "2025-10-10")
        end_date: 종료 날짜 (예: "2025-10-30")
    
    Returns:
        날짜 문자열 리스트 ["2025-10-10", "2025-10-11", ..., "2025-10-30"]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    if start > end:
        raise ValueError(f"시작일({start_date})이 종료일({end_date})보다 늦습니다.")
    
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return date_list


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

def launch_browser(account_id: str):
    """브라우저 실행 (Docker/Airflow 환경 대응)"""
    log(f"🔍 [DEBUG] launch_browser 시작 (headless={HEADLESS_MODE})", account_id)
    
    options = uc.ChromeOptions()
    
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    chrome_path = Path(chrome_bin)
    
    if chrome_path.exists() and isinstance(chrome_bin, str) and chrome_bin.strip():
        options.binary_location = chrome_bin
    
    if HEADLESS_MODE:
        options.add_argument('--headless=new')
    
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-resources')
    
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        driver = launch_uc_chrome(
            options,
            account_id=account_id,
            chrome_bin=chrome_bin,
            log_fn=lambda message: log(message, account_id),
        )
        log(f"✅ 브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"❌ 브라우저 실행 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
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
        driver.get(LOGIN_URL)
        if not wait_for_react_load(driver, timeout=15):
            log(f"  ✗ React 앱 로드 실패", account_id)
            return False
        time.sleep(1.0)
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False
    
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
    
    try:
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        human_type(pw_input, password)
        log(f"  ✓ 비밀번호 입력", account_id)
    except NoSuchElementException:
        log(f"  ✗ 비밀번호 필드 없음", account_id)
        return False
    
    time.sleep(0.3)
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        driver.execute_script("arguments[0].click();", checkbox)
        log(f"  ✓ 기업회원 체크", account_id)
    except Exception as e:
        log(f"  [경고] 체크박스: {e}", account_id)
    
    time.sleep(0.5)
    
    try:
        submit_btn = None
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        except:
            pass
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
        except:
            return False
    
    time.sleep(3.0)
    
    current_url = driver.current_url
    if is_on_success_page(current_url) or not is_on_login_page(current_url):
        log(f"  ✅ 로그인 성공!", account_id)
        return True
    
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
        time.sleep(4.0)
        
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


def set_date_range(driver, account_id: str, target_date: str) -> bool:
    """
    날짜 범위 설정 (시작일, 종료일 모두 target_date로)
    """
    log(f"  📅 날짜 설정: {target_date}", account_id)
    
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%y-%m-%d")  # "25-12-31"
        
        # 날짜 입력 필드 찾기
        date_inputs = driver.find_elements(
            By.CSS_SELECTOR, 
            "input.MuiInputBase-input.MuiOutlinedInput-input.MuiInputBase-inputAdornedStart"
        )
        
        if len(date_inputs) < 2:
            date_inputs = driver.find_elements(
                By.CSS_SELECTOR,
                ".MuiMultiInputDateRangeField-root input"
            )
        
        if len(date_inputs) < 2:
            log(f"    ✗ 날짜 입력 필드를 찾을 수 없음", account_id)
            return False
        
        start_input = date_inputs[0]
        end_input = date_inputs[1]
        
        # 시작일 설정
        start_input.click()
        time.sleep(0.3)
        start_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        start_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        start_input.send_keys(formatted_date)
        
        time.sleep(0.5)
        
        # 종료일 설정
        end_input.click()
        time.sleep(0.3)
        end_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        end_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        end_input.send_keys(formatted_date)
        
        # 외부 클릭하여 확정
        time.sleep(0.3)
        driver.find_element(By.TAG_NAME, "body").click()
        time.sleep(1.0)
        
        log(f"    ✓ 날짜 설정 완료: {formatted_date}", account_id)
        return True
        
    except Exception as e:
        log(f"    ✗ 날짜 설정 실패: {e}", account_id)
        return False


def download_report_for_date(driver, account_id: str, target_date: str) -> Dict[str, Any]:
    """
    특정 날짜의 보고서 다운로드 (브라우저 세션 유지 상태에서)
    
    Returns:
        결과 딕셔너리 {"success": bool, "file": str, "date": str, "error": str}
    """
    result = {
        "success": False,
        "file": None,
        "date": target_date,
        "error": None
    }
    
    try:
        # 날짜 설정
        if not set_date_range(driver, account_id, target_date):
            result["error"] = "날짜 설정 실패"
            return result
        
        # 다운로드 전 파일 목록
        existing_files = set(DOWNLOAD_DIR.glob("*"))
        
        # 보고서 생성 버튼 클릭
        wait = WebDriverWait(driver, 15)
        report_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        
        log(f"  📥 보고서 다운로드 시작: {target_date}", account_id)
        
        # 다운로드 대기 (최대 10초)
        downloaded_file = None
        for i in range(10):
            time.sleep(1)
            current_files = set(DOWNLOAD_DIR.glob("*"))
            new_files = current_files - existing_files
            completed_files = [f for f in new_files if not f.name.endswith('.crdownload') and f.is_file()]
            
            if completed_files:
                downloaded_file = completed_files[0]
                break
        
        if not downloaded_file:
            result["error"] = "다운로드 파일 없음"
            return result
        
        # 파일명 변경
        file_ext = downloaded_file.suffix
        new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
        
        if new_filename.exists():
            new_filename.unlink()
        
        downloaded_file.rename(new_filename)
        
        result["success"] = True
        result["file"] = str(new_filename)
        log(f"  ✅ 다운로드 완료: {new_filename.name}", account_id)
        
    except TimeoutException:
        result["error"] = "보고서 생성 버튼 없음"
    except Exception as e:
        result["error"] = str(e)
    
    return result


# ============================================================================
# 단일 계정 - 다중 날짜 처리
# ============================================================================

def process_single_account_multi_dates(
    account: Dict,
    date_list: List[str],
) -> List[Dict[str, Any]]:
    """
    단일 계정으로 여러 날짜 다운로드 (브라우저 세션 유지)
    
    Returns:
        각 날짜별 결과 리스트
    """
    account_id = account["id"]
    password = account["pw"]
    
    all_results = []
    driver = None
    
    try:
        log(f"처리 시작 (총 {len(date_list)}일)", account_id)
        
        driver = launch_browser(account_id)
        
        if not do_login(driver, account_id, password):
            # 로그인 실패 시 모든 날짜 실패 처리
            for target_date in date_list:
                all_results.append({
                    "success": False,
                    "account_id": account_id,
                    "platform": PLATFORM_NAME,
                    "target_date": target_date,
                    "downloaded_file": None,
                    "file_size_mb": None,
                    "collected_at": None,
                    "error": "로그인 실패",
                })
            return all_results
        
        if not navigate_to_sales_report(driver, account_id):
            for target_date in date_list:
                all_results.append({
                    "success": False,
                    "account_id": account_id,
                    "platform": PLATFORM_NAME,
                    "target_date": target_date,
                    "downloaded_file": None,
                    "file_size_mb": None,
                    "collected_at": None,
                    "error": "보고서 페이지 이동 실패",
                })
            return all_results
        
        # 각 날짜별로 다운로드 (세션 유지!)
        for idx, target_date in enumerate(date_list):
            log(f"📆 [{idx + 1}/{len(date_list)}] {target_date} 처리 중...", account_id)
            
            download_result = download_report_for_date(driver, account_id, target_date)
            
            result = {
                "success": download_result["success"],
                "account_id": account_id,
                "platform": PLATFORM_NAME,
                "target_date": target_date,
                "downloaded_file": download_result["file"],
                "file_size_mb": None,
                "collected_at": datetime.now().isoformat() if download_result["success"] else None,
                "error": download_result["error"],
            }
            
            if download_result["success"] and download_result["file"]:
                result["file_size_mb"] = round(
                    Path(download_result["file"]).stat().st_size / (1024 * 1024), 2
                )
            
            all_results.append(result)
            
            # 다음 날짜 전 딜레이 (마지막 날짜 제외)
            if idx < len(date_list) - 1:
                delay = random.uniform(*TIMING["date_interval"])
                time.sleep(delay)
        
        success_count = sum(1 for r in all_results if r["success"])
        log(f"✅ 완료: {success_count}/{len(date_list)}일 성공", account_id)
        
    except Exception as e:
        log(f"❌ 처리 중단: {e}", account_id)
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            try:
                driver.quit()
                log(f"브라우저 종료", account_id)
            except:
                pass
    
    return all_results


# ============================================================================
# 메인 함수 (날짜 범위 버전)
# ============================================================================

def run_toorder_crawling_range(
    account_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    투오더 크롤링 메인 함수 (날짜 범위 버전)
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw)
        start_date: 시작 날짜 (예: "2025-10-10")
        end_date: 종료 날짜 (예: "2025-10-30")
    
    Returns:
        result_df: 다운로드 결과 DataFrame (각 계정 x 각 날짜)
    
    Example:
        result_df = run_toorder_crawling_range(
            account_df,
            start_date="2025-10-10",
            end_date="2025-10-30"
        )
        # 2025-10-10 ~ 2025-10-10 다운로드
        # 2025-10-11 ~ 2025-10-11 다운로드
        # ...
        # 2025-10-30 ~ 2025-10-30 다운로드
    """
    log("=" * 60)
    log(f"투오더 크롤링 시작 (날짜 범위 버전)")
    log(f"  HEADLESS_MODE={HEADLESS_MODE}")
    log("=" * 60)
    
    # 날짜 리스트 생성
    date_list = generate_date_range(start_date, end_date)
    log(f"  📅 날짜 범위: {start_date} ~ {end_date} ({len(date_list)}일)")
    
    account_list = convert_account_df_to_list(account_df)
    log(f"  👤 계정 수: {len(account_list)}개")
    log(f"  📊 총 다운로드 예정: {len(account_list) * len(date_list)}건")
    
    if not account_list:
        log("toorder 계정이 없습니다.")
        return pd.DataFrame()
    
    all_results = []
    
    for acc_idx, account in enumerate(account_list):
        log(f"\n{'=' * 40}")
        log(f"계정 {acc_idx + 1}/{len(account_list)}: {account['id']}")
        log(f"{'=' * 40}")
        
        account_results = process_single_account_multi_dates(account, date_list)
        all_results.extend(account_results)
        
        # 다음 계정 전 딜레이
        if acc_idx < len(account_list) - 1:
            delay = random.uniform(*TIMING["account_stagger"])
            log(f"다음 계정까지 {delay:.1f}초 대기...")
            time.sleep(delay)
    
    result_df = pd.DataFrame(all_results)
    
    total_success = sum(1 for r in all_results if r["success"])
    total_failed = len(all_results) - total_success
    
    log("\n" + "=" * 60)
    log("투오더 크롤링 완료 (날짜 범위 버전)")
    log(f"  📅 날짜 범위: {start_date} ~ {end_date}")
    log(f"  ✅ 성공: {total_success}건")
    log(f"  ❌ 실패: {total_failed}건")
    log(f"  📊 결과: {len(result_df)}행")
    log("=" * 60)
    
    return result_df


# 기존 단일 날짜 함수도 유지 (호환성)
def run_toorder_crawling(
    account_df: pd.DataFrame,
    target_date: str,
) -> pd.DataFrame:
    """단일 날짜 다운로드 (기존 함수 호환)"""
    return run_toorder_crawling_range(account_df, target_date, target_date)


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
    
    # ============================================
    # 테스트: 날짜 범위 지정
    # ============================================
    START_DATE = "2025-12-01"
    END_DATE = "2025-12-31"
    
    print(f"\n[테스트] 날짜 범위 다운로드: {START_DATE} ~ {END_DATE}")
    result_df = run_toorder_crawling_range(
        test_account_df, 
        start_date=START_DATE,
        end_date=END_DATE
    )
    
    print("\n[결과]")
    print(result_df.to_string())
