"""
투오더 VOC 분석 크롤링 - Airflow 연동용

============================================================================
사용법
============================================================================

from modules.transform.piplines.SMP_crawling_toorder_voc._01 import run_toorder_voc_crawling

# 전일 데이터 수집 (기본값)
result_df = run_toorder_voc_crawling(account_df)

# 특정 날짜 수집
result_df = run_toorder_voc_crawling(
    account_df,
    start_date="2025-12-01",
    end_date="2025-12-31"
)

============================================================================
"""

import time
import random
import re
import os
import platform
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from modules.transform.utility.selenium_uc import configure_uc_data_path


# ============================================================================
# 상수 - 환경
# ============================================================================
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None

# ============================================================================
# 상수 - URL
# ============================================================================
PLATFORM_NAME = "toorder_voc"
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
VOC_URL = "https://ceo.toorder.co.kr/dashboard/review-status/review-voc-analysis"

# ============================================================================
# 상수 - 타이밍
# ============================================================================
TIMING = {
    "page_load":         (2.0, 3.0),
    "after_click":       (1.0, 2.0),
    "download_wait":     (3.0, 5.0),
    "date_change_wait":  (1.0, 2.0),
    "account_stagger":   (3.0, 7.0),
    "batch_rest":        (45.0, 90.0),
    "btn_poll_interval": 1.0,          # 버튼 재활성화 폴링 간격
    "btn_max_wait":      120,          # 다운로드 최대 대기 (초)
}

# ============================================================================
# 상수 - 배치
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)

# ============================================================================
# 상수 - 경로
# ============================================================================
def get_download_dir() -> Path:
    if os.getenv("AIRFLOW_HOME"):
        return Path("/opt/airflow/download")
    if platform.system() == "Windows":
        return Path("E:/down")
    return Path("/mnt/d/down")

DOWNLOAD_DIR = get_download_dir()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 유틸리티
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{account_id}] {message}")


def human_type(element, text: str):
    """사람처럼 타이핑"""
    element.clear()
    time.sleep(0.2)
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    time.sleep(0.3)


def convert_account_df_to_list(account_df: pd.DataFrame) -> List[Dict]:
    filtered = account_df[account_df["channel"] == "toorder"].copy()
    return [{"id": row["id"], "pw": row["pw"]} for _, row in filtered.iterrows()]


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")
    result = []
    cur = start
    while cur <= end:
        result.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return result


def split_into_random_batches(items: List, size_range: tuple) -> List[List]:
    min_s, max_s = size_range
    batches, remaining = [], list(items)
    while remaining:
        size = min(random.randint(min_s, max_s), len(remaining))
        batches.append(remaining[:size])
        remaining = remaining[size:]
    return batches


def get_chrome_version() -> Optional[int]:
    try:
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        result = subprocess.run([chrome_bin, "--version"], capture_output=True, text=True, timeout=5)
        m = re.search(r'(\d+)\.', result.stdout)
        if m:
            ver = int(m.group(1))
            log(f"Chrome 버전: {ver}")
            return ver
    except Exception as e:
        log(f"Chrome 버전 감지 실패: {e}")
    return None


# ============================================================================
# 브라우저
# ============================================================================

def launch_browser(account_id: str):
    log(f"브라우저 실행 (headless={HEADLESS_MODE})", account_id)
    options = uc.ChromeOptions()

    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists():
        options.binary_location = chrome_bin

    if HEADLESS_MODE:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    chrome_version = get_chrome_version()
    try:
        configure_uc_data_path()
        if chrome_version:
            driver = uc.Chrome(options=options, version_main=chrome_version)
        else:
            driver = uc.Chrome(options=options)
        log("✅ 브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"❌ 브라우저 실행 실패: {e}", account_id)
        raise


# ============================================================================
# 로그인
# ============================================================================

def wait_for_react_load(driver, timeout=15) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        ready = driver.execute_script(
            "return document.querySelector('input[name=\"id\"]') !== null;"
        )
        if ready:
            return True
        time.sleep(0.5)
    return False


def do_login(driver, account_id: str, password: str) -> bool:
    log("로그인 시도", account_id)
    try:
        driver.get(LOGIN_URL)
        if not wait_for_react_load(driver):
            log("  ✗ React 앱 로드 실패", account_id)
            return False
        time.sleep(1.0)
    except Exception as e:
        log(f"  ✗ 페이지 이동 실패: {e}", account_id)
        return False

    try:
        wait = WebDriverWait(driver, 10)
        id_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']")))
        human_type(id_input, account_id)
        log("  ✓ ID 입력", account_id)
    except TimeoutException:
        log("  ✗ ID 필드 타임아웃", account_id)
        return False

    try:
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        human_type(pw_input, password)
        log("  ✓ PW 입력", account_id)
    except NoSuchElementException:
        log("  ✗ PW 필드 없음", account_id)
        return False

    time.sleep(0.3)
    # 기업회원 체크박스
    try:
        cb = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        driver.execute_script("arguments[0].click();", cb)
        log("  ✓ 기업회원 체크", account_id)
    except Exception:
        pass

    time.sleep(0.5)
    # 로그인 버튼
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        driver.execute_script("arguments[0].click();", btn)
        log("  ✓ 로그인 버튼 클릭", account_id)
    except Exception:
        try:
            pw_input.send_keys(Keys.RETURN)
        except Exception:
            log("  ✗ 로그인 버튼 클릭 실패", account_id)
            return False

    time.sleep(3.0)
    current_url = driver.current_url
    if "/dashboard" in current_url or "/auth" not in current_url:
        log("  ✅ 로그인 성공", account_id)
        return True

    log(f"  ❌ 로그인 실패 (URL: {current_url})", account_id)
    return False


# ============================================================================
# VOC 페이지 이동
# ============================================================================

def navigate_to_voc_page(driver, account_id: str) -> bool:
    log("VOC 분석 페이지 이동", account_id)
    try:
        driver.get(VOC_URL)
        time.sleep(3.0)
        if "review-voc-analysis" in driver.current_url:
            log("  ✓ VOC 페이지 도착", account_id)
            return True
        log(f"  ✗ 잘못된 URL: {driver.current_url}", account_id)
        return False
    except Exception as e:
        log(f"  ✗ 이동 실패: {e}", account_id)
        return False


# ============================================================================
# 날짜 설정 - 달력 팝업 방식
# ============================================================================

def get_calendar_year_month(driver) -> tuple:
    """달력에 표시된 현재 연/월 반환 (예: '2월 2026' → (2026, 2))"""
    try:
        els = driver.find_elements(
            By.CSS_SELECTOR,
            ".MuiPickersArrowSwitcher-root .MuiTypography-subtitle1"
        )
        if els:
            text = els[0].text.strip()          # "2월 2026" 형식
            parts = text.split()
            month = int(parts[0].replace("월", ""))
            year  = int(parts[1])
            return year, month
    except Exception:
        pass
    return None, None


def navigate_calendar_month(driver, account_id: str, target_year: int, target_month: int) -> bool:
    """달력을 목표 연/월로 이동"""
    from datetime import datetime as dt

    for _ in range(24):
        cur_year, cur_month = get_calendar_year_month(driver)
        if cur_year is None:
            log("  ✗ 달력 월 정보 읽기 실패", account_id)
            return False

        if cur_year == target_year and cur_month == target_month:
            log(f"  ✓ 목표 월 도달: {target_year}-{target_month:02d}", account_id)
            return True

        target_dt  = dt(target_year, target_month, 1)
        current_dt = dt(cur_year, cur_month, 1)

        if target_dt < current_dt:
            btns = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Previous month']")
            if not btns:
                log("  ✗ 이전 월 버튼 없음", account_id)
                return False
            driver.execute_script("arguments[0].click();", btns[0])
            log(f"  ← 이전 월 클릭 ({cur_year}-{cur_month:02d})", account_id)
        else:
            btns = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Next month']")
            if not btns:
                log("  ✗ 다음 월 버튼 없음", account_id)
                return False
            driver.execute_script("arguments[0].click();", btns[0])
            log(f"  → 다음 월 클릭 ({cur_year}-{cur_month:02d})", account_id)

        time.sleep(0.5)

    log("  ✗ 달력 월 이동 실패 (최대 횟수 초과)", account_id)
    return False


def set_date_input(driver, account_id: str, target_date: str) -> bool:
    """
    달력 팝업에서 날짜 선택.
    - 날짜 입력 필드 클릭 → 달력 팝업 오픈
    - 목표 월로 이동
    - 해당 날짜 더블클릭 (단일 날짜 선택 확정)
    """
    log(f"날짜 선택 시작: {target_date}", account_id)

    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        year, month, day = date_obj.year, date_obj.month, date_obj.day
    except Exception as e:
        log(f"  ✗ 날짜 파싱 실패: {e}", account_id)
        return False

    # ── 1. 날짜 입력 필드 클릭 (달력 오픈) ──────────────────────────────────
    try:
        wait = WebDriverWait(driver, 10)
        date_input = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input.MuiInputBase-input[placeholder='YY-MM-DD']")
            )
        )
        driver.execute_script("arguments[0].click();", date_input)
        log("  ✓ 날짜 필드 클릭 (달력 오픈)", account_id)
        time.sleep(1.5)
    except Exception as e:
        log(f"  ✗ 날짜 필드 클릭 실패: {e}", account_id)
        return False

    # ── 2. 목표 월로 이동 ───────────────────────────────────────────────────
    if not navigate_calendar_month(driver, account_id, year, month):
        return False

    # ── 3. 날짜 버튼 더블클릭 ───────────────────────────────────────────────
    try:
        day_buttons = driver.find_elements(
            By.CSS_SELECTOR,
            ".MuiPickersDay-root"
        )

        clicked = False
        for btn in day_buttons:
            if btn.text.strip() != str(day):
                continue

            # data-timestamp 로 연/월/일 정확히 검증
            ts = btn.get_attribute("data-timestamp")
            if ts:
                btn_dt = datetime.fromtimestamp(int(ts) / 1000)
                if not (btn_dt.year == year and btn_dt.month == month and btn_dt.day == day):
                    continue

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.3)

            # 방법 1: ActionChains 더블클릭
            try:
                ActionChains(driver).double_click(btn).perform()
                log(f"  ✓ {day}일 더블클릭 (ActionChains)", account_id)
                clicked = True
            except Exception:
                pass

            # 방법 2: JS 클릭 2회 (폴백)
            if not clicked:
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", btn)
                log(f"  ✓ {day}일 JS 2회 클릭 (폴백)", account_id)
                clicked = True

            break

        if not clicked:
            log(f"  ✗ {day}일 버튼을 찾을 수 없음", account_id)
            return False

    except Exception as e:
        log(f"  ✗ 날짜 클릭 실패: {e}", account_id)
        return False

    # ── 4. 달력 닫힘 대기 ───────────────────────────────────────────────────
    time.sleep(1.0)
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        log("  ✓ ESC로 달력 닫기", account_id)
    except Exception:
        pass

    for _ in range(6):
        try:
            cal = driver.find_element(By.CSS_SELECTOR, ".MuiPickersDay-root")
            if cal.is_displayed():
                time.sleep(0.5)
            else:
                break
        except Exception:
            break

    log(f"  ✅ 날짜 선택 완료: {target_date}", account_id)
    time.sleep(0.5)
    return True


# ============================================================================
# 조회 버튼 클릭
# ============================================================================

def click_search_button(driver, account_id: str) -> bool:
    """
    돋보기(조회) 버튼 클릭 후 데이터 로딩 대기.
    선택자: div.css-i9gxme > button.css-slx3eq (돋보기 SVG 포함)
    """
    log("  조회 버튼 클릭", account_id)

    selectors = [
        ("CSS", "div.css-i9gxme > button",   "css-i9gxme 내 버튼"),
        ("CSS", "button.css-slx3eq",          "css-slx3eq 버튼"),
        ("XPATH", "//div[contains(@class,'css-i9gxme')]/button", "XPath css-i9gxme"),
    ]

    wait = WebDriverWait(driver, 10)
    search_btn = None

    for method, selector, desc in selectors:
        try:
            if method == "CSS":
                el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            else:
                el = wait.until(EC.presence_of_element_located((By.XPATH, selector)))

            if el.is_displayed():
                log(f"  ✓ 조회 버튼 발견: {desc}", account_id)
                search_btn = el
                break
        except Exception:
            continue

    if not search_btn:
        log("  ✗ 조회 버튼을 찾을 수 없음", account_id)
        return False

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", search_btn)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", search_btn)
    log("  ✓ 조회 버튼 클릭 완료", account_id)

    # 데이터 로딩 대기
    delay = random.uniform(3.0, 5.0)
    log(f"  ⏳ 데이터 로딩 대기 ({delay:.1f}초)...", account_id)
    time.sleep(delay)
    log("  ✅ 조회 완료", account_id)
    return True


# ============================================================================
# 다운로드
# ============================================================================

def click_download_and_wait(driver, account_id: str, target_date: str) -> Optional[str]:
    """
    '전체 토픽 내보내기' 버튼 클릭 후,
    버튼이 비활성화 → 재활성화되면 다운로드 완료로 판단.
    완성된 파일 경로 반환.
    """
    log("  다운로드 시작", account_id)

    # 다운로드 전 파일 스냅샷
    existing_files = set(DOWNLOAD_DIR.glob("*"))

    # 1. 버튼 찾기 (SaveAlt 아이콘 + '전체 토픽 내보내기' 텍스트)
    try:
        wait = WebDriverWait(driver, 10)
        export_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(., '전체 토픽 내보내기')]")
            )
        )
        log("  ✓ '전체 토픽 내보내기' 버튼 발견", account_id)
    except TimeoutException:
        log("  ✗ 내보내기 버튼을 찾을 수 없음", account_id)
        return None

    # 2. 버튼 클릭
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", export_btn)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", export_btn)
    log("  ✓ 버튼 클릭 완료", account_id)

    # 3. 버튼이 disabled 상태가 될 때까지 잠깐 대기
    time.sleep(1.5)

    # 4. 버튼이 재활성화될 때까지 폴링 (= 다운로드 완료 신호)
    log(f"  ⏳ 버튼 재활성화 대기 (최대 {TIMING['btn_max_wait']}초)...", account_id)
    elapsed = 0
    download_done = False

    while elapsed < TIMING["btn_max_wait"]:
        time.sleep(TIMING["btn_poll_interval"])
        elapsed += TIMING["btn_poll_interval"]

        try:
            # 버튼을 매번 새로 찾아 stale element 방지
            btns = driver.find_elements(By.XPATH, "//button[contains(., '전체 토픽 내보내기')]")
            if btns:
                is_disabled = btns[0].get_attribute("disabled")
                if is_disabled is None:  # disabled 속성 없음 = 활성화 상태
                    log(f"  ✅ 버튼 재활성화 감지 ({elapsed:.0f}초 소요)", account_id)
                    download_done = True
                    break
            else:
                # 버튼이 DOM에서 사라졌다가 다시 나타나는 경우 대비
                pass
        except Exception:
            pass

    if not download_done:
        log("  ⚠️ 버튼 재활성화 미감지 (타임아웃, 파일 직접 확인)", account_id)

    # 5. 다운로드된 파일 탐지
    time.sleep(2.0)  # 파일 쓰기 완료 여유
    current_files = set(DOWNLOAD_DIR.glob("*"))
    new_files = [
        f for f in (current_files - existing_files)
        if not f.name.endswith(".crdownload") and f.is_file()
    ]

    if not new_files:
        log("  ✗ 다운로드된 파일 없음", account_id)
        return None

    downloaded = new_files[0]
    log(f"  ✓ 다운로드 파일: {downloaded.name}", account_id)

    downloaded = new_files[0]
    log(f"  ✓ 다운로드 파일: {downloaded.name}", account_id)

    return str(downloaded)


# ============================================================================
# 단일 계정 처리
# ============================================================================

def create_result(account_id: str, target_date: str) -> Dict[str, Any]:
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


def process_single_account(account: Dict, date_list: List[str]) -> List[Dict[str, Any]]:
    account_id = account["id"]
    password   = account["pw"]
    results    = []
    driver     = None

    try:
        log(f"처리 시작 (총 {len(date_list)}일)", account_id)
        driver = launch_browser(account_id)

        if not do_login(driver, account_id, password):
            for d in date_list:
                r = create_result(account_id, d)
                r["error"] = "로그인 실패"
                results.append(r)
            return results

        if not navigate_to_voc_page(driver, account_id):
            for d in date_list:
                r = create_result(account_id, d)
                r["error"] = "VOC 페이지 이동 실패"
                results.append(r)
            return results

        for idx, target_date in enumerate(date_list):
            log(f"\n[{idx+1}/{len(date_list)}] 날짜 처리: {target_date}", account_id)
            result = create_result(account_id, target_date)

            try:
                # 날짜 입력
                if not set_date_input(driver, account_id, target_date):
                    result["error"] = "날짜 입력 실패"
                    results.append(result)
                    continue

                # 조회 버튼 클릭 → 로딩 대기
                if not click_search_button(driver, account_id):
                    result["error"] = "조회 실패"
                    results.append(result)
                    continue

                # 다운로드
                downloaded_file = click_download_and_wait(driver, account_id, target_date)

                if not downloaded_file:
                    result["error"] = "다운로드 실패"
                    results.append(result)
                    continue

                result["success"]       = True
                result["downloaded_file"] = downloaded_file
                result["file_size_mb"]  = round(Path(downloaded_file).stat().st_size / (1024 * 1024), 2)
                result["collected_at"]  = datetime.now().isoformat()

                log(f"✅ {target_date} 완료 ({result['file_size_mb']}MB)", account_id)
                results.append(result)

                # 날짜 간 딜레이
                if idx < len(date_list) - 1:
                    delay = random.uniform(*TIMING["date_change_wait"])
                    time.sleep(delay)

            except Exception as e:
                result["error"] = f"처리 오류: {e}"
                results.append(result)
                log(f"❌ {target_date} 실패: {e}", account_id)

        success_cnt = sum(1 for r in results if r["success"])
        log(f"✅ 계정 처리 완료 (성공: {success_cnt}/{len(date_list)})", account_id)

    except Exception as e:
        log(f"❌ 계정 처리 중단: {e}", account_id)
        processed = {r["target_date"] for r in results}
        for d in date_list:
            if d not in processed:
                r = create_result(account_id, d)
                r["error"] = f"계정 처리 중단: {e}"
                results.append(r)

    finally:
        if driver:
            try:
                driver.quit()
                log("브라우저 종료", account_id)
            except Exception:
                pass

    return results


# ============================================================================
# 배치 처리
# ============================================================================

def _process_with_delay(account, date_list, start_delay):
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    return process_single_account(account, date_list)


def process_batch(batch: List[Dict], date_list: List[str]) -> List[Dict]:
    results = []
    with ThreadPoolExecutor(max_workers=len(batch)) as executor:
        futures = []
        cum_delay = 0.0
        for idx, account in enumerate(batch):
            if idx > 0:
                cum_delay += random.uniform(*TIMING["account_stagger"])
            futures.append(executor.submit(_process_with_delay, account, date_list, cum_delay))
        for f in as_completed(futures):
            results.extend(f.result())
    return results


# ============================================================================
# 메인 함수
# ============================================================================

def run_toorder_voc_crawling(
    account_df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    투오더 VOC 분석 크롤링 메인 함수

    Parameters
    ----------
    account_df : DataFrame (columns: channel, id, pw)
    start_date : str, optional  예) "2025-12-01"  (생략 시 전일)
    end_date   : str, optional  예) "2025-12-31"  (생략 시 전일)

    Returns
    -------
    result_df : DataFrame
    """
    if start_date is None or end_date is None:
        yesterday  = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = start_date or yesterday
        end_date   = end_date   or yesterday

    log("=" * 60)
    log(f"투오더 VOC 크롤링 시작 (HEADLESS={HEADLESS_MODE})")
    log("=" * 60)

    account_list = convert_account_df_to_list(account_df)
    date_list    = generate_date_range(start_date, end_date)

    log(f"  날짜 범위: {start_date} ~ {end_date} ({len(date_list)}일)")
    log(f"  계정 수: {len(account_list)}개")

    if not account_list:
        log("toorder 계정이 없습니다.")
        return pd.DataFrame()

    batches     = split_into_random_batches(account_list, BATCH_SIZE_RANGE)
    all_results = []

    for idx, batch in enumerate(batches):
        log(f"\n{'='*40}")
        log(f"배치 {idx+1}/{len(batches)} 시작 ({len(batch)}개 계정)")
        log(f"{'='*40}")

        all_results.extend(process_batch(batch, date_list))

        if idx < len(batches) - 1:
            delay = random.uniform(*TIMING["batch_rest"])
            log(f"다음 배치까지 {delay:.1f}초 대기...")
            time.sleep(delay)

    result_df = pd.DataFrame(all_results)
    total_ok  = sum(1 for r in all_results if r["success"])

    log("\n" + "=" * 60)
    log(f"투오더 VOC 크롤링 완료 — 성공 {total_ok} / 전체 {len(all_results)}")
    log("=" * 60)

    return result_df


# ============================================================================
# 로컬 테스트
# ============================================================================

if __name__ == "__main__":
    test_df = pd.DataFrame([{
        "channel": "toorder",
        "id": "doridang15",
        "pw": "ehfl5233!",
    }])

    result = run_toorder_voc_crawling(
        test_df,
        start_date="2026-01-31",
        end_date="2026-02-01",
    )
    print(result[["account_id", "target_date", "success", "file_size_mb", "error"]])
