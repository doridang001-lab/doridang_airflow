"""
투오더 리뷰 크롤링 - Airflow 연동용

============================================================================
사용법
============================================================================

from modules.extract.croling_toorder_review import run_toorder_review_crawling

# 리뷰 데이터 수집 (날짜 범위)
result_df = run_toorder_review_crawling(
    account_df, 
    start_date="2025-12-01",  # 생략 시 전일
    end_date="2025-12-31"      # 생략 시 전일
)

# 전일 데이터만 수집 (기본값)
result_df = run_toorder_review_crawling(account_df)

============================================================================
"""

import time
import random
import re
import shutil
import os
import platform
import subprocess  # 추가
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException


# ============================================================================
# 상수 - 브라우저 설정
# ============================================================================
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None


# ============================================================================
# 상수 - URL
# ============================================================================
PLATFORM_NAME = "toorder_review"
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
REVIEW_URL = "https://ceo.toorder.co.kr/dashboard/review-status/review-analysis"


# ============================================================================
# 상수 - 타이밍
# ============================================================================
TIMING = {
    "page_load": (2.0, 3.0),
    "typing_char": (0.03, 0.08),
    "typing_pause": (0.2, 0.4),
    "before_click": (0.3, 0.5),
    "after_click": (1.5, 2.5),
    "search_loading": (3.0, 5.0),  # 조회 후 로딩 대기
    "download_wait": (3.0, 5.0),
    "date_change_wait": (1.0, 2.0),
    "account_stagger": (3.0, 7.0),
    "batch_rest": (45.0, 90.0),
}


# ============================================================================
# 상수 - 배치 설정
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)


# ============================================================================
# 상수 - 경로
# ============================================================================
def get_download_dir():
    """실행 환경에 맞는 다운로드 디렉토리 반환"""
    # Airflow 환경 우선 확인
    if os.getenv("AIRFLOW_HOME"):
        return Path("/opt/airflow/download")
    
    # Windows 로컬 환경
    if platform.system() == "Windows":
        return Path("E:/down")
    
    # Linux 기타 환경
    return Path("/mnt/d/down")

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


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """날짜 범위 생성"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return date_list


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

def get_chrome_version():
    """Chrome 버전 감지"""
    try:
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        result = subprocess.run(
            [chrome_bin, "--version"], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        # "Google Chrome 143.0.7499.192" → 143
        version_str = result.stdout.strip()
        version_match = re.search(r'(\d+)\.', version_str)
        if version_match:
            major_version = int(version_match.group(1))
            log(f"Chrome 감지 버전: {major_version} (전체: {version_str})", "SYSTEM")
            return major_version
    except Exception as e:
        log(f"Chrome 버전 감지 실패: {e}", "SYSTEM")
    return None


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser(account_id: str):
    """브라우저 실행"""
    log(f"브라우저 실행 (headless={HEADLESS_MODE})", account_id)
    
    options = uc.ChromeOptions()
    
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    chrome_path = Path(chrome_bin)
    
    if chrome_path.exists():
        options.binary_location = str(chrome_bin)
    
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
        # Chrome 버전 자동 감지
        chrome_version = get_chrome_version()
        
        if chrome_version:
            log(f"ChromeDriver 버전: {chrome_version} 사용", account_id)
            driver = uc.Chrome(
                options=options,
                version_main=chrome_version
            )
        else:
            # 버전 감지 실패 시 자동
            log(f"Chrome 버전 자동 매칭 시도", account_id)
            driver = uc.Chrome(options=options)
            
        log(f"✅ 브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"❌ 브라우저 실행 실패: {e}", account_id)
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
    
    log(f"  ❌ 로그인 실패", account_id)
    return False


# ============================================================================
# 리뷰 페이지 이동 및 날짜 설정
# ============================================================================

def navigate_to_review_page(driver, account_id: str) -> bool:
    """리뷰 분석 페이지 이동"""
    log(f"리뷰 페이지 이동", account_id)
    
    try:
        driver.get(REVIEW_URL)
        time.sleep(2.0)
        time.sleep(2.0)
        
        current_url = driver.current_url
        
        if "review-status/review-analysis" in current_url:
            log(f"  ✓ 리뷰 페이지 도착", account_id)
            return True
        else:
            log(f"  ✗ 잘못된 페이지: {current_url}", account_id)
            return False
            
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False


def get_current_calendar_month(driver) -> tuple:
    """달력의 현재 표시된 월 확인 (왼쪽 달력 기준)"""
    try:
        # 첫 번째 달력의 월 정보 가져오기
        month_text_elements = driver.find_elements(
            By.CSS_SELECTOR, 
            ".MuiPickersArrowSwitcher-root .MuiTypography-subtitle1"
        )
        
        if month_text_elements:
            # "12월 2025" 형식
            text = month_text_elements[0].text
            parts = text.split()
            
            month_str = parts[0].replace("월", "")
            year_str = parts[1]
            
            return int(year_str), int(month_str)
    except:
        pass
    
    return None, None


def navigate_calendar_to_month(driver, account_id: str, target_year: int, target_month: int) -> bool:
    """달력을 목표 월로 이동"""
    log(f"  달력 이동: {target_year}년 {target_month}월", account_id)
    
    max_clicks = 24  # 최대 2년치
    
    for _ in range(max_clicks):
        current_year, current_month = get_current_calendar_month(driver)
        
        if current_year is None:
            log(f"  ✗ 달력 월 정보 읽기 실패", account_id)
            return False
        
        if current_year == target_year and current_month == target_month:
            log(f"  ✓ 목표 월 도달: {target_year}-{target_month:02d}", account_id)
            return True
        
        # 이전/다음 판단
        current_date = datetime(current_year, current_month, 1)
        target_date = datetime(target_year, target_month, 1)
        
        if target_date < current_date:
            # 이전 월로
            try:
                prev_btn = driver.find_element(
                    By.CSS_SELECTOR,
                    "button[aria-label='Previous month']"
                )
                driver.execute_script("arguments[0].click();", prev_btn)
                log(f"  ← 이전 월 ({current_year}-{current_month:02d})", account_id)
                time.sleep(0.5)
            except:
                log(f"  ✗ 이전 월 버튼 클릭 실패", account_id)
                return False
        else:
            # 다음 월로
            try:
                next_btn = driver.find_elements(
                    By.CSS_SELECTOR,
                    "button[aria-label='Next month']"
                )[0]  # 첫 번째 달력의 다음 버튼
                driver.execute_script("arguments[0].click();", next_btn)
                log(f"  → 다음 월 ({current_year}-{current_month:02d})", account_id)
                time.sleep(0.5)
            except:
                log(f"  ✗ 다음 월 버튼 클릭 실패", account_id)
                return False
    
    log(f"  ✗ 목표 월 도달 실패 (최대 시도 횟수 초과)", account_id)
    return False


def select_date_in_calendar(driver, account_id: str, target_date: str) -> bool:
    """
    달력에서 특정 날짜 선택
    """
    log(f"날짜 선택: {target_date}", account_id)
    
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        
        # 1. 날짜 입력 필드 클릭 (달력 열기)
        try:
            date_input = driver.find_element(
                By.CSS_SELECTOR,
                "input.MuiInputBase-input.MuiOutlinedInput-input[placeholder='YY-MM-DD']"
            )
            driver.execute_script("arguments[0].click();", date_input)
            log(f"  ✓ 날짜 입력 필드 클릭 (달력 열림)", account_id)
            time.sleep(1.5)
        except:
            log(f"  ✗ 날짜 입력 필드 찾기 실패", account_id)
            return False
        
        # 2. 달력을 목표 월로 이동
        if not navigate_calendar_to_month(driver, account_id, year, month):
            return False
        
        # 3. 날짜 버튼 찾기 및 클릭
        try:
            day_buttons = driver.find_elements(
                By.CSS_SELECTOR,
                ".MuiDateRangePickerDay-root button.MuiPickersDay-root"
            )
            
            clicked = False
            for btn in day_buttons:
                btn_text = btn.text.strip()
                
                if btn_text == str(day):
                    timestamp = btn.get_attribute("data-timestamp")
                    if timestamp:
                        btn_date = datetime.fromtimestamp(int(timestamp) / 1000)
                        if btn_date.year == year and btn_date.month == month and btn_date.day == day:
                            # 첫 번째 클릭 (시작일)
                            driver.execute_script("arguments[0].click();", btn)
                            log(f"  ✓ 날짜 1차 클릭: {day}일 (시작일)", account_id)
                            time.sleep(1.0)
                            
                            # 두 번째 클릭 (종료일)
                            driver.execute_script("arguments[0].click();", btn)
                            log(f"  ✓ 날짜 2차 클릭: {day}일 (종료일)", account_id)
                            
                            clicked = True
                            break
            
            if not clicked:
                log(f"  ✗ {day}일 버튼을 찾을 수 없음", account_id)
                return False
            
            time.sleep(1.0)
            
            # 4. 달력이 자동으로 닫힐 때까지 대기 (또는 강제로 닫기)
            try:
                # ESC 키로 달력 닫기
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                log(f"  ✓ ESC 키로 달력 닫기", account_id)
                time.sleep(0.8)
            except:
                pass
            
            # 5. 달력이 완전히 사라졌는지 확인
            max_wait = 5
            for i in range(max_wait):
                try:
                    # 달력 요소가 여전히 있는지 확인
                    calendar = driver.find_element(
                        By.CSS_SELECTOR,
                        ".MuiDateRangePickerDay-root"
                    )
                    if calendar.is_displayed():
                        log(f"  ⏳ 달력 닫힘 대기 중... ({i+1}/{max_wait})", account_id)
                        time.sleep(0.5)
                    else:
                        break
                except:
                    # 달력 요소가 없으면 닫힌 것
                    log(f"  ✓ 달력 완전히 닫힘", account_id)
                    break
            
            time.sleep(0.5)
            return True
            
        except Exception as e:
            log(f"  ✗ 날짜 선택 실패: {e}", account_id)
            return False
            
    except Exception as e:
        log(f"  ✗ 날짜 파싱 실패: {e}", account_id)
        return False


def click_search_button(driver, account_id: str) -> bool:
    """조회 버튼 클릭 및 로딩 대기"""
    log(f"  조회 버튼 클릭 시작", account_id)
    
    try:
        # 1. 달력 완전히 닫힐 때까지 충분히 대기
        log(f"  ⏳ 달력 닫힘 대기 (2초)...", account_id)
        time.sleep(2.0)
        
        # 2. 조회 버튼이 나타날 때까지 명시적 대기
        wait = WebDriverWait(driver, 10)
        search_btn = None
        
        # 선택자 리스트 (우선순위 순)
        selectors = [
            ("CSS", "div.css-i9gxme > button", "css-i9gxme 박스 안 버튼"),
            ("CSS", "button.css-slx3eq", "css-slx3eq 버튼"),
            ("CSS", "div.MuiCard-root button[style*='min-width: 36px'][style*='padding: 8px']", "카드 안 작은 버튼"),
            ("XPATH", "//div[contains(@class, 'css-i9gxme')]/button", "XPath css-i9gxme"),
        ]
        
        # 각 선택자 순차적으로 시도
        for method, selector, desc in selectors:
            try:
                log(f"  🔍 시도: {desc}", account_id)
                
                if method == "CSS":
                    search_btn = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                else:  # XPATH
                    search_btn = wait.until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                
                # 요소가 보이는지 확인
                if search_btn.is_displayed():
                    log(f"  ✅ 조회 버튼 찾음: {desc}", account_id)
                    break
                else:
                    search_btn = None
                    
            except Exception as e:
                search_btn = None
                continue
        
        if not search_btn:
            log(f"  ✗ 모든 선택자로 버튼을 찾을 수 없음", account_id)
            return False
        
        # 3. 버튼이 클릭 가능할 때까지 대기
        try:
            wait.until(EC.element_to_be_clickable(search_btn))
            log(f"  ✓ 버튼 클릭 가능 상태", account_id)
        except:
            log(f"  ⚠️ 클릭 가능 대기 실패 (계속 진행)", account_id)
        
        # 4. 스크롤하여 버튼이 화면 중앙에 오도록
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", search_btn)
        time.sleep(1.0)
        
        # 5. 클릭
        try:
            driver.execute_script("arguments[0].click();", search_btn)
            log(f"  ✓ 조회 버튼 클릭 완료", account_id)
        except Exception as e:
            log(f"  ✗ 클릭 실패: {e}", account_id)
            return False
        
        # 6. 데이터 로딩 대기 (고정 시간)
        delay = random.uniform(*TIMING["search_loading"])
        log(f"  ⏳ 데이터 로딩 대기 ({delay:.1f}초)...", account_id)
        time.sleep(delay)
        
        log(f"  ✅ 조회 완료", account_id)
        return True
        
    except Exception as e:
        log(f"  ✗ 조회 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# 검색 및 다운로드
# ============================================================================

def click_search_button(driver, account_id: str) -> bool:
    """조회 버튼 클릭 및 로딩 대기"""
    log(f"  조회 버튼 클릭 시작", account_id)
    
    try:
        # 1. 달력 완전히 닫힐 때까지 충분히 대기
        log(f"  ⏳ 달력 닫힘 대기 (2초)...", account_id)
        time.sleep(2.0)
        
        # 2. 조회 버튼이 나타날 때까지 명시적 대기
        wait = WebDriverWait(driver, 10)
        search_btn = None
        
        # 선택자 리스트 (우선순위 순)
        selectors = [
            ("CSS", "div.css-i9gxme > button", "css-i9gxme 박스 안 버튼"),
            ("CSS", "button.css-slx3eq", "css-slx3eq 버튼"),
            ("CSS", "div.MuiCard-root button[style*='min-width: 36px'][style*='padding: 8px']", "카드 안 작은 버튼"),
            ("XPATH", "//div[contains(@class, 'css-i9gxme')]/button", "XPath css-i9gxme"),
            ("XPATH", "//button[contains(@style, 'min-width: 36px') and contains(@style, 'padding: 8px')]//svg[contains(@class, 'iconify--eva')]/..", "XPath 돋보기 SVG 부모"),
        ]
        
        # 각 선택자 순차적으로 시도
        for method, selector, desc in selectors:
            try:
                log(f"  🔍 시도: {desc}", account_id)
                
                if method == "CSS":
                    search_btn = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                else:  # XPATH
                    search_btn = wait.until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                
                # 요소가 보이는지 확인
                if search_btn.is_displayed():
                    log(f"  ✅ 조회 버튼 찾음: {desc}", account_id)
                    break
                else:
                    log(f"  ⚠️ 버튼이 숨겨져 있음: {desc}", account_id)
                    search_btn = None
                    
            except Exception as e:
                log(f"  ❌ 실패: {desc} - {str(e)[:50]}", account_id)
                search_btn = None
                continue
        
        if not search_btn:
            log(f"  ✗ 모든 선택자로 버튼을 찾을 수 없음", account_id)
            
            # 최후의 수단: 페이지의 모든 작은 버튼 출력
            try:
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                log(f"  📊 페이지 전체 버튼 개수: {len(all_buttons)}개", account_id)
                
                small_buttons = [btn for btn in all_buttons 
                               if btn.get_attribute("style") and "36px" in btn.get_attribute("style")]
                log(f"  📊 작은 버튼(36px) 개수: {len(small_buttons)}개", account_id)
                
                for idx, btn in enumerate(small_buttons[:3]):
                    btn_id = btn.get_attribute("id")
                    btn_class = btn.get_attribute("class") or ""
                    is_displayed = btn.is_displayed()
                    log(f"    버튼{idx+1}: id={btn_id}, displayed={is_displayed}, class={btn_class[:40]}...", account_id)
                    
            except Exception as e:
                log(f"  디버깅 실패: {e}", account_id)
            
            return False
        
        # 3. 버튼 정보 확인
        try:
            btn_id = search_btn.get_attribute("id")
            btn_class = search_btn.get_attribute("class")
            btn_style = search_btn.get_attribute("style")
            log(f"  📍 버튼 정보:", account_id)
            log(f"    - id: {btn_id}", account_id)
            log(f"    - class: {btn_class[:50] if btn_class else 'None'}...", account_id)
            log(f"    - style: {btn_style}", account_id)
        except:
            pass
        
        # 4. 버튼이 클릭 가능할 때까지 대기
        try:
            wait.until(EC.element_to_be_clickable(search_btn))
            log(f"  ✓ 버튼 클릭 가능 상태", account_id)
        except:
            log(f"  ⚠️ 클릭 가능 대기 실패 (계속 진행)", account_id)
        
        # 5. 스크롤하여 버튼이 화면 중앙에 오도록
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", search_btn)
        time.sleep(1.0)
        
        # 6. 클릭 전 네트워크 감지 설정
        driver.execute_script("""
            window.reviewApiCalled = false;
            const originalFetch = window.fetch;
            window.fetch = function(...args) {
                if (args[0] && args[0].includes('getReviewNumAnalysisCompany')) {
                    window.reviewApiCalled = true;
                }
                return originalFetch.apply(this, args);
            };
        """)
        
        # 7. 클릭 시도 (여러 방법)
        clicked = False
        
        # 방법 1: JavaScript 클릭
        try:
            driver.execute_script("arguments[0].click();", search_btn)
            log(f"  ✓ 조회 버튼 클릭 (JS)", account_id)
            clicked = True
        except Exception as e:
            log(f"  ⚠️ JS 클릭 실패: {e}", account_id)
        
        # 방법 2: 일반 클릭
        if not clicked:
            try:
                search_btn.click()
                log(f"  ✓ 조회 버튼 클릭 (일반)", account_id)
                clicked = True
            except Exception as e:
                log(f"  ⚠️ 일반 클릭 실패: {e}", account_id)
        
        # 방법 3: ActionChains 클릭
        if not clicked:
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(driver).move_to_element(search_btn).click().perform()
                log(f"  ✓ 조회 버튼 클릭 (ActionChains)", account_id)
                clicked = True
            except Exception as e:
                log(f"  ⚠️ ActionChains 클릭 실패: {e}", account_id)
        
        if not clicked:
            log(f"  ✗ 모든 클릭 방법 실패", account_id)
            return False
        
        # 8. API 호출 대기
        log(f"  ⏳ API 호출 대기 중...", account_id)
        api_called = False
        for i in range(20):  # 10초
            time.sleep(0.5)
            try:
                api_called = driver.execute_script("return window.reviewApiCalled;")
                if api_called:
                    log(f"  ✅ API 호출 감지 ({(i+1)*0.5:.1f}초)", account_id)
                    break
            except:
                pass
        
        if not api_called:
            log(f"  ⚠️ API 호출 미감지 (계속 진행)", account_id)
        
        # 9. 데이터 로딩 완료 대기
        delay = random.uniform(2.5, 3.5)
        log(f"  ⏳ 데이터 로딩 대기 ({delay:.1f}초)...", account_id)
        time.sleep(delay)
        
        log(f"  ✅ 조회 완료", account_id)
        return True
        
    except Exception as e:
        log(f"  ✗ 조회 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
        return False


def download_excel_for_date(driver, account_id: str, target_date: str) -> str:
    """특정 날짜의 Excel 다운로드"""
    log(f"  Excel 다운로드 시작", account_id)
    
    try:
        # 다운로드 전 파일 목록
        existing_files = set(DOWNLOAD_DIR.glob("*"))
        
        # 1. 내보내기 버튼 클릭
        export_btn = driver.find_element(
            By.XPATH,
            "//button[contains(., '내보내기')][@aria-label='내보내기']"
        )
        driver.execute_script("arguments[0].click();", export_btn)
        log(f"  ✓ 내보내기 버튼 클릭", account_id)
        time.sleep(1.0)
        
        # 2. Excel로 내보내기 메뉴 클릭
        excel_menu = driver.find_element(
            By.XPATH,
            "//li[@role='menuitem'][contains(., 'Excel로 내보내기')]"
        )
        driver.execute_script("arguments[0].click();", excel_menu)
        log(f"  ✓ Excel로 내보내기 클릭", account_id)
        
        # 3. 다운로드 대기 (최대 10초)
        max_wait = 10
        downloaded_file = None
        
        for i in range(max_wait):
            time.sleep(1)
            
            current_files = set(DOWNLOAD_DIR.glob("*"))
            new_files = current_files - existing_files
            
            completed_files = [f for f in new_files if not f.name.endswith('.crdownload') and f.is_file()]
            
            if completed_files:
                downloaded_file = completed_files[0]
                log(f"  ✓ 다운로드 완료: {downloaded_file.name} ({i+1}초 소요)", account_id)
                break
        
        if not downloaded_file:
            log(f"  ✗ 다운로드 파일 없음 (10초 경과)", account_id)
            return None
        
        # 4. 파일명 변경
        file_ext = downloaded_file.suffix
        new_filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{target_date}{file_ext}"
        
        if new_filename.exists():
            new_filename.unlink()
        
        downloaded_file.rename(new_filename)
        log(f"  ✓ 파일 저장: {new_filename.name}", account_id)
        
        return str(new_filename)
        
    except Exception as e:
        log(f"  ✗ Excel 다운로드 실패: {e}", account_id)
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# 단일 계정 처리
# ============================================================================

def create_account_result(account_id: str, target_date: str) -> Dict[str, Any]:
    """계정 결과 딕셔너리 초기화"""
    return {
        "success": False,
        "account_id": account_id,
        "platform": PLATFORM_NAME,
        "target_date": target_date,
        "downloaded_file": None,
        "file_size_mb": None,
        "collected_at": None,
        "error": None,
    }


def process_single_account(
    account: Dict,
    date_list: List[str],
) -> List[Dict[str, Any]]:
    """단일 계정의 모든 날짜 처리"""
    account_id = account["id"]
    password = account["pw"]
    
    results = []
    driver = None
    
    try:
        log(f"처리 시작 (총 {len(date_list)}일)", account_id)
        
        driver = launch_browser(account_id)
        
        if not do_login(driver, account_id, password):
            for target_date in date_list:
                result = create_account_result(account_id, target_date)
                result["error"] = "로그인 실패"
                results.append(result)
            return results
        
        if not navigate_to_review_page(driver, account_id):
            for target_date in date_list:
                result = create_account_result(account_id, target_date)
                result["error"] = "리뷰 페이지 이동 실패"
                results.append(result)
            return results
        
        # 각 날짜별로 처리
        for idx, target_date in enumerate(date_list):
            log(f"\n[{idx+1}/{len(date_list)}] 날짜 처리: {target_date}", account_id)
            
            result = create_account_result(account_id, target_date)
            
            try:
                if not select_date_in_calendar(driver, account_id, target_date):
                    result["error"] = "날짜 선택 실패"
                    results.append(result)
                    continue
                
                if not click_search_button(driver, account_id):
                    result["error"] = "조회 실패"
                    results.append(result)
                    continue
                
                downloaded_file = download_excel_for_date(driver, account_id, target_date)
                
                if not downloaded_file:
                    result["error"] = "다운로드 실패"
                    results.append(result)
                    continue
                
                result["success"] = True
                result["downloaded_file"] = downloaded_file
                result["file_size_mb"] = round(Path(downloaded_file).stat().st_size / (1024 * 1024), 2)
                result["collected_at"] = datetime.now().isoformat()
                
                log(f"✅ {target_date} 완료 ({result['file_size_mb']}MB)", account_id)
                results.append(result)
                
                if idx < len(date_list) - 1:
                    delay = random.uniform(*TIMING["date_change_wait"])
                    time.sleep(delay)
                
            except Exception as e:
                result["error"] = f"처리 오류: {str(e)}"
                results.append(result)
                log(f"❌ {target_date} 실패: {e}", account_id)
        
        log(f"✅ 계정 처리 완료 (성공: {sum(1 for r in results if r['success'])}개)", account_id)
        
    except Exception as e:
        log(f"❌ 계정 처리 중단: {e}", account_id)
        
        processed_dates = {r["target_date"] for r in results}
        for target_date in date_list:
            if target_date not in processed_dates:
                result = create_account_result(account_id, target_date)
                result["error"] = f"계정 처리 중단: {str(e)}"
                results.append(result)
        
    finally:
        if driver:
            try:
                driver.quit()
                log(f"브라우저 종료", account_id)
            except:
                pass  # 이미 종료된 경우 무시
    
    return results


# ============================================================================
# 배치 처리
# ============================================================================

def process_account_with_delay(
    account: Dict,
    date_list: List[str],
    start_delay: float,
) -> List[Dict]:
    """딜레이 후 계정 처리"""
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    
    return process_single_account(account, date_list)


def process_batch(
    batch: List[Dict],
    date_list: List[str],
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
                date_list,
                cumulative_delay,
            )
            futures.append(future)
        
        for future in as_completed(futures):
            batch_results = future.result()
            results.extend(batch_results)
    
    return results


# ============================================================================
# 메인 함수
# ============================================================================

def run_toorder_review_crawling(
    account_df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    투오더 리뷰 크롤링 메인 함수
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw)
        start_date: 시작 날짜 (예: "2025-12-01"), 생략 시 전일
        end_date: 종료 날짜 (예: "2025-12-31"), 생략 시 전일
    
    Returns:
        result_df: 다운로드 결과 DataFrame
    """
    # 기본값 설정: 전일
    if start_date is None or end_date is None:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = start_date or yesterday
        end_date = end_date or yesterday
    
    log("=" * 60)
    log(f"투오더 리뷰 크롤링 시작 (HEADLESS_MODE={HEADLESS_MODE})")
    log("=" * 60)
    
    account_list = convert_account_df_to_list(account_df)
    date_list = generate_date_range(start_date, end_date)
    
    log(f"  날짜 범위: {start_date} ~ {end_date} (총 {len(date_list)}일)")
    log(f"  계정 수: {len(account_list)}개")
    log(f"  총 작업: {len(account_list)} x {len(date_list)} = {len(account_list) * len(date_list)}건")
    
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
        
        batch_results = process_batch(batch, date_list)
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
    log("투오더 리뷰 크롤링 완료")
    log(f"  전체: 성공 {total_success}건, 실패 {total_failed}건")
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
            "pw": "ehfl0109!!",
        },
    ])
    
    # print("\n[테스트 1] 전일 데이터 수집 (기본값)")
    # result_df = run_toorder_review_crawling(test_account_df)
    # print(result_df[["account_id", "target_date", "success", "file_size_mb", "error"]])
    
    print("\n[테스트 2] 특정 기간 수집")
    result_df = run_toorder_review_crawling(
        test_account_df,
        start_date="2026-01-31",
        end_date="2026-02-01"
    )
    print(result_df[["account_id", "target_date", "success", "file_size_mb", "error"]])