"""
투오더 크롤링 - Airflow 연동용 (디버깅 버전)

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
import platform
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException


# ============================================================================
# 상수 - 브라우저 설정 (테스트 시 여기서 변경!)
# ============================================================================
# 로컬 테스트: False (브라우저 보임)
# Docker/Airflow: True (백그라운드)
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None  # Airflow면 자동으로 headless


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
# 상수 - 경로 (Windows/WSL/Docker 모두 지원)
# ============================================================================
import platform

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
    log(f"🔍 [DEBUG] ========== launch_browser 시작 ==========", account_id)
    log(f"🔍 [DEBUG] headless={HEADLESS_MODE}", account_id)
    
    # ChromeOptions 생성
    options = uc.ChromeOptions()
    log(f"🔍 [DEBUG] ChromeOptions 객체 생성 완료", account_id)
    
    # Chrome 바이너리 경로 설정
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    log(f"🔍 [DEBUG] 시도할 Chrome 경로: {chrome_bin}", account_id)
    
    # Chrome 존재 여부 확인
    try:
        chrome_path = Path(chrome_bin)
        chrome_exists = chrome_path.exists()
    except:
        chrome_exists = False
    
    log(f"🔍 [DEBUG] Chrome 파일 존재: {chrome_exists}", account_id)
    
    # binary_location 설정: 문자열로 명시적으로 설정 (None 방지)
    if chrome_exists:
        try:
            options.binary_location = str(chrome_bin)  # 명시적으로 문자열 변환
            log(f"🔍 [DEBUG] binary_location 설정: {str(chrome_bin)}", account_id)
        except Exception as e:
            log(f"⚠️ [DEBUG] binary_location 설정 실패: {e}, 자동 감지 사용", account_id)
    else:
        log(f"⚠️ [DEBUG] Chrome 경로 찾을 수 없음, undetected-chromedriver 자동 감지에 의존", account_id)
    
    # headless 설정
    if HEADLESS_MODE:
        options.add_argument('--headless=new')
        log(f"🔍 [DEBUG] headless 모드 활성화", account_id)
    else:
        log(f"🔍 [DEBUG] 일반 모드 (브라우저 표시)", account_id)
    
    # 기본 옵션들
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-resources')
    log(f"🔍 [DEBUG] 기본 Chrome 옵션 추가 완료", account_id)
    
    # 다운로드 설정
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    log(f"🔍 [DEBUG] 다운로드 디렉토리: {DOWNLOAD_DIR.absolute()}", account_id)
    
    # Driver 실행 시도
    try:
        log(f"🔍 [DEBUG] ========== uc.Chrome 호출 시작 ==========", account_id)
        
        # version_main=None 제거 (자동 감지 사용)
        driver = uc.Chrome(options=options)
        
        log(f"✅ ========== 브라우저 실행 성공! ==========", account_id)
        return driver
        
    except Exception as e:
        log(f"❌ ========== 브라우저 실행 실패! ==========", account_id)
        log(f"🔍 [DEBUG] 에러 메시지: {e}", account_id)
        log(f"🔍 [DEBUG] 에러 타입: {type(e).__name__}", account_id)
        
        # 전체 traceback 출력
        import traceback
        log(f"🔍 [DEBUG] ===== Full Traceback =====", account_id)
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
        log(f"  페이지 이동", account_id)
        
        if not wait_for_react_load(driver, timeout=15):
            log(f"  ✗ React 앱 로드 실패", account_id)
            return False
        
        log(f"  ✓ React 앱 로드 완료", account_id)
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
            log(f"  ✓ Enter 제출", account_id)
        except:
            return False
    
    time.sleep(3.0)
    
    current_url = driver.current_url
    log(f"  로그인 후 URL: {current_url}", account_id)
    
    if is_on_success_page(current_url) or not is_on_login_page(current_url):
        log(f"  ✅ 로그인 성공!", account_id)
        return True
    
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


def set_date_range(driver, account_id: str, target_date: str) -> bool:
    """
    날짜 범위 설정 (시작일, 종료일 모두 target_date로)
    
    Parameters:
        driver: Selenium WebDriver
        account_id: 계정 ID (로깅용)
        target_date: 대상 날짜 (예: "2025-12-31")
    
    Returns:
        bool: 성공 여부
    """
    log(f"날짜 범위 설정: {target_date}", account_id)
    
    try:
        # target_date: "2025-12-31" → "25-12-31" (YY-MM-DD 형식)
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%y-%m-%d")  # "25-12-31"
        
        log(f"  변환된 날짜 형식: {formatted_date}", account_id)
        
        # 날짜 입력 필드 찾기 (MUI DateRangePicker)
        date_inputs = driver.find_elements(
            By.CSS_SELECTOR, 
            "input.MuiInputBase-input.MuiOutlinedInput-input.MuiInputBase-inputAdornedStart"
        )
        
        if len(date_inputs) < 2:
            # 대체 선택자 시도
            date_inputs = driver.find_elements(
                By.CSS_SELECTOR,
                ".MuiMultiInputDateRangeField-root input"
            )
        
        if len(date_inputs) < 2:
            log(f"  ✗ 날짜 입력 필드를 찾을 수 없음 (찾은 개수: {len(date_inputs)})", account_id)
            return False
        
        start_input = date_inputs[0]
        end_input = date_inputs[1]
        
        log(f"  ✓ 날짜 입력 필드 찾음 (시작: id={start_input.get_attribute('id')}, 종료: id={end_input.get_attribute('id')})", account_id)
        
        # 시작일 설정
        start_input.click()
        time.sleep(0.3)
        
        # 기존 값 전체 선택 후 삭제
        start_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        start_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        
        # 새 날짜 입력
        start_input.send_keys(formatted_date)
        log(f"  ✓ 시작일 입력: {formatted_date}", account_id)
        
        time.sleep(0.5)
        
        # 종료일 설정
        end_input.click()
        time.sleep(0.3)
        
        # 기존 값 전체 선택 후 삭제
        end_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        end_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        
        # 새 날짜 입력
        end_input.send_keys(formatted_date)
        log(f"  ✓ 종료일 입력: {formatted_date}", account_id)
        
        # 입력 필드 외부 클릭하여 날짜 확정 (body 클릭)
        time.sleep(0.3)
        driver.find_element(By.TAG_NAME, "body").click()
        
        time.sleep(1.0)  # 날짜 반영 대기
        
        # 입력된 값 확인
        start_value = start_input.get_attribute("value")
        end_value = end_input.get_attribute("value")
        log(f"  ✓ 설정 완료 - 시작: {start_value}, 종료: {end_value}", account_id)
        
        return True
        
    except Exception as e:
        log(f"  ✗ 날짜 설정 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
        return False


def click_report_button_and_get_filename(driver, account_id: str, target_date: str) -> str:
    """보고서 생성 버튼 클릭 후 즉시 파일명 반환"""
    log(f"보고서 생성 버튼 클릭", account_id)
    wait = WebDriverWait(driver, 15)
    
    try:
        # ✅ 날짜 범위 먼저 설정
        if not set_date_range(driver, account_id, target_date):
            log(f"  ✗ 날짜 설정 실패로 중단", account_id)
            return None
        
        # 다운로드 전 파일 목록 저장
        existing_files = set(DOWNLOAD_DIR.glob("*"))
        log(f"  다운로드 전 파일 수: {len(existing_files)}개", account_id)
        
        report_btn = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '보고서 생성')]"))
        )
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        
        log(f"  ✓ 보고서 생성 버튼 클릭 - 다운로드 시작됨", account_id)
        
        # 클릭 후 다운로드 대기 (최대 10초)
        max_wait = 10
        downloaded_file = None
        
        for i in range(max_wait):
            time.sleep(1)
            
            current_files = set(DOWNLOAD_DIR.glob("*"))
            new_files = current_files - existing_files
            
            # .crdownload 제외한 완료된 파일만
            completed_files = [f for f in new_files if not f.name.endswith('.crdownload') and f.is_file()]
            
            if completed_files:
                downloaded_file = completed_files[0]
                log(f"  ✓ 다운로드 완료: {downloaded_file.name} ({i+1}초 소요)", account_id)
                break
        
        if not downloaded_file:
            log(f"  ✗ 다운로드 파일 없음 (10초 경과)", account_id)
            log(f"  💡 설정된 다운로드 경로: {DOWNLOAD_DIR}", account_id)
            return None
        
        # 파일명 변경
        file_ext = downloaded_file.suffix
        new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
        
        if new_filename.exists():
            new_filename.unlink()
        
        downloaded_file.rename(new_filename)
        log(f"  ✓ 파일 저장: {new_filename.name}", account_id)
        
        return str(new_filename)
        
    except TimeoutException:
        log(f"  ✗ 보고서 생성 버튼 없음", account_id)
        return None
    except Exception as e:
        log(f"  ✗ 버튼 클릭 또는 파일 처리 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
        return None


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
        
        driver = launch_browser(account_id)
        
        if not do_login(driver, account_id, password):
            result["error"] = "로그인 실패"
            return result
        
        if not navigate_to_sales_report(driver, account_id):
            result["error"] = "보고서 페이지 이동 실패"
            return result
        
        # 보고서 다운로드 (날짜 설정 후 클릭하면 바로 다운로드됨)
        downloaded_file = click_report_button_and_get_filename(driver, account_id, target_date)
        
        if not downloaded_file:
            result["error"] = "보고서 다운로드 실패"
            return result
        
        # 결과 저장
        result["success"] = True
        result["downloaded_file"] = downloaded_file
        result["file_size_mb"] = round(Path(downloaded_file).stat().st_size / (1024 * 1024), 2)
        result["collected_at"] = datetime.now().isoformat()
        
        log(f"✅ 처리 완료 (파일: {Path(downloaded_file).name}, {result['file_size_mb']}MB)", account_id)
        
    except Exception as e:
        result["error"] = str(e)
        log(f"❌ 처리 중단: {e}", account_id)
        
    finally:
        if driver:
            try:
                driver.quit()
                log(f"브라우저 종료", account_id)
            except Exception as e:
                # 이미 종료된 경우 무시
                pass
            
            # driver 객체 정리
            try:
                del driver
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
    log(f"투오더 크롤링 시작 (HEADLESS_MODE={HEADLESS_MODE})")
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
    
    print("\n[테스트] 보고서 다운로드")
    result_df = run_toorder_crawling(test_account_df, target_date="2025-12-29")
    print(result_df)