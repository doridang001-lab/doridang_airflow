"""
투오더 종합보고서 크롤링 모듈

채널별(일) 매출보고서를 날짜별로 자동 다운로드한다.
단일 날짜와 날짜 범위 두 가지 공개 인터페이스를 제공한다.

공개 함수:
    run_crawling_single_date  - 단일 날짜 다운로드 (브라우저 열고 닫음)
    run_crawling_date_range   - 날짜 리스트 다운로드 (세션 유지)
    generate_date_range       - 날짜 범위 → 날짜 리스트 변환
"""

import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.selenium_uc import configure_uc_data_path

logger = logging.getLogger(__name__)


# ============================================================
# 상수 - 환경
# ============================================================

HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None


# ============================================================
# 상수 - URL
# ============================================================

LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"
SALES_REPORT_DATE_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"
LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/dashboard"]


# ============================================================
# 상수 - 파일명
# ============================================================

ORIG_FILENAME = "종합보고서_채널별(일)매출보고서.xlsx"
ORIG_DATE_FILENAME = "종합보고서_일별매출보고서.xlsx"


# ============================================================
# 상수 - 타이밍
# ============================================================

TIMING = {
    "typing_char":    (0.03, 0.08),
    "date_interval":  (2.0, 4.0),
}


# ============================================================
# 내부 유틸리티
# ============================================================

def _random_delay(min_sec: float, max_sec: float) -> None:
    """지정된 범위 내에서 랜덤 딜레이."""
    time.sleep(random.uniform(min_sec, max_sec))


def _human_type(element, text: str) -> None:
    """사람처럼 타이핑 (React input 호환: click → CTRL+A → DELETE → send_keys)."""
    element.click()
    time.sleep(0.2)
    element.send_keys(Keys.CONTROL + "a")
    time.sleep(0.1)
    element.send_keys(Keys.DELETE)
    time.sleep(0.1)
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(*TIMING["typing_char"]))
    time.sleep(0.3)


def _react_set_value(driver, element, value: str) -> None:
    """React 컨트롤드 인풋에 값을 주입한다 (native setter + input 이벤트 dispatch)."""
    driver.execute_script(
        """
        const el = arguments[0];
        const value = arguments[1];
        const setter = Object.getOwnPropertyDescriptor(
            window.HTMLInputElement.prototype, 'value'
        ).set;
        setter.call(el, value);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        element,
        value,
    )


def _fill_react_input(driver, element, value: str, account_id: str, field_name: str) -> bool:
    """
    React 컨트롤드 인풋에 값을 입력한다.
    1) ActionChains (W3C Actions API) - 가장 실제에 가까운 키보드 시뮬레이션
    2) JS nativeInputValueSetter + input 이벤트 폴백
    """
    # ── 1차: ActionChains (OS 수준 키보드 이벤트, React synthetic event 트리거)
    try:
        ActionChains(driver).click(element).perform()
        time.sleep(0.2)
        ActionChains(driver).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).perform()
        time.sleep(0.1)
        ActionChains(driver).send_keys(Keys.DELETE).perform()
        time.sleep(0.1)
        ActionChains(driver).send_keys(value).perform()
        time.sleep(0.3)
    except Exception as exc:
        logger.warning("[%s] %s ActionChains 예외: %s", account_id, field_name, exc)

    actual = driver.execute_script("return arguments[0].value;", element) or ""
    if actual == value:
        logger.debug("[%s] %s ActionChains OK", account_id, field_name)
        return True

    logger.warning(
        "[%s] %s ActionChains 실패(len=%d), JS setter 폴백",
        account_id, field_name, len(actual),
    )

    # ── 2차: JS nativeInputValueSetter 폴백
    try:
        _react_set_value(driver, element, value)
        time.sleep(0.3)
    except Exception as exc:
        logger.error("[%s] %s JS setter 예외: %s", account_id, field_name, exc)
        return False

    actual2 = driver.execute_script("return arguments[0].value;", element) or ""
    logger.info("[%s] %s 최종 DOM value: len=%d", account_id, field_name, len(actual2))
    return actual2 == value


def _save_login_debug(driver, account_id: str, tag: str) -> None:
    """로그인 실패 디버그용 스크린샷/HTML 저장."""
    try:
        from modules.transform.utility.paths import ANALYTICS_DB
        from datetime import datetime as _dt
        debug_dir = ANALYTICS_DB / "ai_daily_collection" / "_debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        png = debug_dir / f"{tag}_{ts}.png"
        html = debug_dir / f"{tag}_{ts}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
        logger.error("[%s] 디버그 저장: %s", account_id, png.name)
    except Exception as exc:
        logger.warning("[%s] 디버그 저장 실패: %s", account_id, exc)


# ============================================================
# 브라우저 제어
# ============================================================

def _launch_browser(account_id: str, download_dir: Path) -> uc.Chrome:
    """
    undetected_chromedriver 브라우저 인스턴스를 생성한다.

    Parameters:
        account_id:   로그용 계정 ID
        download_dir: 다운로드 기본 경로 (Path 객체)

    Returns:
        초기화된 Chrome 드라이버
    """
    logger.info("[%s] 브라우저 실행 시작 (headless=%s)", account_id, HEADLESS_MODE)

    options = uc.ChromeOptions()

    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    chrome_path = Path(chrome_bin)
    if chrome_path.exists() and isinstance(chrome_bin, str) and chrome_bin.strip():
        options.binary_location = chrome_bin

    if HEADLESS_MODE:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": str(download_dir.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    # 설치된 Chrome 메이저 버전을 직접 감지해서 ChromeDriver 버전을 맞춘다.
    # version_main=None 이면 undetected_chromedriver가 최신 드라이버를 자동 다운로드해
    # 설치된 Chrome 버전과 불일치할 수 있으므로, 명시적으로 지정한다.
    version_main: int | None = None
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True, text=True, timeout=5
        )
        version_str = result.stdout.strip()  # e.g. "Google Chrome 145.0.7632.109"
        version_main = int(version_str.split()[-1].split(".")[0])
        logger.info("[%s] 감지된 Chrome 메이저 버전: %s", account_id, version_main)
    except Exception as e:
        logger.warning("[%s] Chrome 버전 감지 실패, version_main=None 사용: %s", account_id, e)

    try:
        configure_uc_data_path()
        kwargs: Dict[str, Any] = {"options": options}
        if version_main:
            kwargs["version_main"] = version_main
        driver = uc.Chrome(**kwargs)
    except Exception as exc:
        match = re.search(r"Current browser version is (\d+)", str(exc))
        if not match:
            raise
        detected_version = int(match.group(1))
        logger.warning(
            "[%s] ChromeDriver 버전 불일치 감지, version_main=%s 로 재시도",
            account_id,
            detected_version,
        )
        configure_uc_data_path()
        driver = uc.Chrome(options=options, version_main=detected_version)

    driver.set_window_size(1920, 1080)
    logger.info("[%s] 브라우저 실행 완료", account_id)
    return driver


def _wait_for_react_load(driver, timeout: int = 10) -> bool:
    """
    투오더 React 앱의 로그인 입력 필드가 렌더링될 때까지 대기한다.

    Returns:
        True: 필드 발견, False: 타임아웃
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        ready = driver.execute_script(
            "return document.querySelector('input[name=\"id\"]') !== null;"
        )
        if ready:
            return True
        time.sleep(0.5)
    return False


def _do_login(driver, account_id: str, password: str) -> bool:
    """
    투오더 CEO 사이트에 로그인한다.

    Parameters:
        driver:     Chrome 드라이버
        account_id: 투오더 로그인 ID
        password:   투오더 비밀번호

    Returns:
        True: 로그인 성공, False: 실패
    """
    logger.info("[%s] 로그인 시도", account_id)

    try:
        driver.get(LOGIN_URL)
        if not _wait_for_react_load(driver, timeout=15):
            logger.error("[%s] React 앱 로드 타임아웃", account_id)
            return False
        time.sleep(1.0)
    except Exception as exc:
        logger.error("[%s] 페이지 이동 실패: %s", account_id, exc)
        return False

    try:
        wait = WebDriverWait(driver, 10)
        id_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']"))
        )
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='id']")))
    except TimeoutException:
        logger.error("[%s] ID 필드 타임아웃", account_id)
        return False

    try:
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
    except NoSuchElementException:
        logger.error("[%s] 비밀번호 필드 없음", account_id)
        return False

    if not _fill_react_input(driver, id_input, account_id, account_id, "ID"):
        logger.error("[%s] ID 입력 실패", account_id)
        _save_login_debug(driver, account_id, "login_id_fail")
        return False

    if not _fill_react_input(driver, pw_input, password, account_id, "PW"):
        logger.error("[%s] PW 입력 실패", account_id)
        _save_login_debug(driver, account_id, "login_pw_fail")
        return False

    logger.info(
        "[%s] 입력 검증 OK: id=%r, pw_len=%d",
        account_id,
        driver.execute_script("return arguments[0].value;", id_input),
        len(driver.execute_script("return arguments[0].value;", pw_input) or ""),
    )

    time.sleep(0.3)
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        if not checkbox.is_selected():
            driver.execute_script("arguments[0].click();", checkbox)
            logger.info("[%s] 기업회원 체크박스 체크", account_id)
        else:
            logger.info("[%s] 기업회원 체크박스 이미 체크됨 (유지)", account_id)
    except Exception:
        pass

    time.sleep(0.5)

    submit_btn = None
    try:
        submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    except Exception:
        pass
    if not submit_btn:
        try:
            submit_btn = driver.find_element(
                By.XPATH, "//button[contains(text(), '로그인')]"
            )
        except Exception:
            pass
    if not submit_btn:
        logger.error("[%s] 로그인 버튼 없음", account_id)
        return False

    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center'});", submit_btn
    )
    time.sleep(0.3)

    # submit 직전 DOM 값 최종 확인
    pre_id = driver.execute_script("return document.querySelector(\"input[name='id']\").value;") or ""
    pre_pw = driver.execute_script("return document.querySelector(\"input[name='password']\").value;") or ""
    pre_company = driver.execute_script("return document.querySelector(\"input[name='isCompany']\").checked;")
    logger.info(
        "[%s] submit 직전: id=%r, pw_len=%d, isCompany=%s",
        account_id, pre_id, len(pre_pw), pre_company,
    )

    driver.execute_script("arguments[0].click();", submit_btn)

    # 고정 sleep 대신 dashboard URL로 전환될 때까지 대기 (최대 15초)
    try:
        WebDriverWait(driver, 15).until(EC.url_contains("/dashboard"))
        time.sleep(1.0)
    except TimeoutException:
        pass  # 아래 URL 체크에서 최종 판정

    current_url = driver.current_url
    success = "/dashboard" in current_url and not any(
        p in current_url for p in LOGIN_FAIL_URL_PATTERNS
    )
    if success:
        logger.info("[%s] 로그인 성공: %s", account_id, current_url)
    else:
        logger.error("[%s] 로그인 실패 (url=%s)", account_id, current_url)
        _save_login_debug(driver, account_id, "login_fail")
    return success


# ============================================================
# 보고서 페이지 제어
# ============================================================

def _navigate_to_sales_report(driver, account_id: str) -> bool:
    """
    채널별(일) 매출보고서 페이지로 이동한다.

    Returns:
        True: 이동 성공, False: 실패
    """
    logger.info("[%s] 보고서 페이지 이동 중", account_id)
    try:
        driver.get(SALES_REPORT_URL)
        time.sleep(4.0)
        if "sales-report/orderkinds" in driver.current_url:
            logger.info("[%s] 보고서 페이지 도착", account_id)
            return True
        logger.error("[%s] 잘못된 페이지: %s", account_id, driver.current_url)
        return False
    except Exception as exc:
        logger.error("[%s] 페이지 이동 실패: %s", account_id, exc)
        return False


def _set_date_range(driver, account_id: str, target_date: str) -> bool:
    """
    보고서 날짜 범위 입력 필드에 시작일과 종료일을 동일하게 설정한다.

    Parameters:
        target_date: "YYYY-MM-DD" 형식 날짜

    Returns:
        True: 설정 성공, False: 실패
    """
    logger.info("[%s] 날짜 설정: %s", account_id, target_date)
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%y-%m-%d")  # "26-03-25"

        date_inputs = driver.find_elements(
            By.CSS_SELECTOR,
            "input.MuiInputBase-input.MuiOutlinedInput-input.MuiInputBase-inputAdornedStart",
        )
        if len(date_inputs) < 2:
            date_inputs = driver.find_elements(
                By.CSS_SELECTOR, ".MuiMultiInputDateRangeField-root input"
            )
        if len(date_inputs) < 2:
            logger.error("[%s] 날짜 입력 필드 부족 (%d개)", account_id, len(date_inputs))
            return False

        start_input = date_inputs[0]
        end_input = date_inputs[1]

        # 시작일
        start_input.click()
        time.sleep(0.3)
        start_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        start_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        start_input.send_keys(formatted_date)
        time.sleep(0.5)

        # 종료일
        end_input.click()
        time.sleep(0.3)
        end_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        end_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        end_input.send_keys(formatted_date)
        time.sleep(0.3)

        driver.find_element(By.TAG_NAME, "body").click()
        time.sleep(1.0)

        logger.info("[%s] 날짜 설정 완료: %s", account_id, formatted_date)
        return True

    except Exception as exc:
        logger.error("[%s] 날짜 설정 실패: %s", account_id, exc)
        return False


def _download_report_for_date(
    driver, account_id: str, target_date: str, download_dir: Path
) -> Dict[str, Any]:
    """
    세션이 유지된 브라우저에서 단일 날짜 보고서를 다운로드하고 파일명을 변경한다.

    다운로드 완료 파일명 규칙:
        종합보고서_채널별(일)매출보고서_YYYYMMDD.xlsx

    Parameters:
        driver:       Chrome 드라이버 (로그인 및 보고서 페이지 진입 완료 상태)
        account_id:   로그용 계정 ID
        target_date:  "YYYY-MM-DD" 형식 날짜
        download_dir: 다운로드 경로 (Path 객체)

    Returns:
        {"success": bool, "file": str|None, "date": str, "error": str|None}
    """
    result: Dict[str, Any] = {
        "success": False,
        "file": None,
        "date": target_date,
        "error": None,
    }

    try:
        if not _set_date_range(driver, account_id, target_date):
            result["error"] = "날짜 설정 실패"
            return result

        existing_files = set(download_dir.glob("*"))

        wait = WebDriverWait(driver, 15)
        report_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), '보고서 생성')]")
            )
        )
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", report_btn
        )
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        logger.info("[%s] 보고서 생성 버튼 클릭: %s", account_id, target_date)

        # 다운로드 완료 대기 (최대 20초)
        downloaded_file = None
        for _ in range(20):
            time.sleep(1)
            current_files = set(download_dir.glob("*"))
            new_files = current_files - existing_files
            completed = [
                f
                for f in new_files
                if not f.name.endswith(".crdownload") and f.is_file()
            ]
            if completed:
                time.sleep(1)  # 쓰기 완료 보장
                downloaded_file = completed[0]
                break

        if not downloaded_file:
            result["error"] = "다운로드 파일 없음"
            return result

        # 파일명 변경: 원본 → 종합보고서_채널별(일)매출보고서_YYYYMMDD.xlsx
        date_str = target_date.replace("-", "")
        stem = Path(ORIG_FILENAME).stem
        suffix = Path(ORIG_FILENAME).suffix
        new_name = f"{stem}_{date_str}{suffix}"
        new_path = download_dir / new_name

        if new_path.exists():
            bak_name = new_path.with_name(
                f"{new_path.stem}_bak_{datetime.now().strftime('%H%M%S')}{new_path.suffix}"
            )
            new_path.rename(bak_name)
            logger.warning("[%s] 기존 파일 백업: %s", account_id, bak_name.name)

        downloaded_file.rename(new_path)
        result["success"] = True
        result["file"] = str(new_path)
        logger.info("[%s] 다운로드 완료: %s", account_id, new_path.name)

    except TimeoutException:
        result["error"] = "보고서 생성 버튼 없음 (타임아웃)"
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("[%s] 다운로드 중 오류: %s", account_id, exc)

    return result


# ============================================================
# 공개 인터페이스
# ============================================================

def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """
    시작일부터 종료일까지의 날짜 문자열 리스트를 생성한다.

    Parameters:
        start_date: "YYYY-MM-DD" 형식 시작일
        end_date:   "YYYY-MM-DD" 형식 종료일

    Returns:
        날짜 문자열 리스트 (예: ["2026-03-01", "2026-03-02", ...])

    Raises:
        ValueError: start_date가 end_date보다 늦을 때
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError(
            f"시작일({start_date})이 종료일({end_date})보다 늦습니다."
        )

    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return date_list


def run_crawling_single_date(
    toorder_id: str,
    toorder_pw: str,
    target_date: str,
    download_dir: Path,
) -> Dict[str, Any]:
    """
    단일 날짜의 투오더 종합보고서를 다운로드한다.

    브라우저를 열고, 로그인하고, 보고서를 다운로드한 뒤 브라우저를 닫는다.

    Parameters:
        toorder_id:   투오더 로그인 ID
        toorder_pw:   투오더 비밀번호
        target_date:  대상 날짜 ("YYYY-MM-DD")
        download_dir: 다운로드 저장 경로 (Path 객체)

    Returns:
        {"success": bool, "file": str|None, "date": str, "error": str|None}
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    driver = None
    result: Dict[str, Any] = {
        "success": False,
        "file": None,
        "date": target_date,
        "error": None,
    }

    try:
        driver = _launch_browser(toorder_id, download_dir)

        if not _do_login(driver, toorder_id, toorder_pw):
            result["error"] = "로그인 실패"
            return result

        if not _navigate_to_sales_report(driver, toorder_id):
            result["error"] = "보고서 페이지 이동 실패"
            return result

        result = _download_report_for_date(driver, toorder_id, target_date, download_dir)

    except Exception as exc:
        result["error"] = str(exc)
        logger.error("[%s] run_crawling_single_date 오류: %s", toorder_id, exc)

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[%s] 브라우저 종료", toorder_id)
            except Exception:
                pass

    return result


def run_crawling_daily_date_page(
    toorder_id: str,
    toorder_pw: str,
    target_date: str,
    download_dir: Path,
) -> Dict[str, Any]:
    """
    단일 날짜의 '일별매출보고서' 엑셀을 다운로드한다 (/date 페이지).

    Returns:
        {"success": bool, "file": str|None, "date": str, "error": str|None}
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    driver = None
    result: Dict[str, Any] = {
        "success": False,
        "file": None,
        "date": target_date,
        "error": None,
    }

    try:
        driver = _launch_browser(toorder_id, download_dir)

        if not _do_login(driver, toorder_id, toorder_pw):
            result["error"] = "로그인 실패"
            return result

        logger.info("[%s] 일별매출보고서 페이지 이동", toorder_id)
        driver.get(SALES_REPORT_DATE_URL)
        time.sleep(4.0)
        if "sales-report/orderkinds" not in driver.current_url:
            result["error"] = f"페이지 이동 실패: {driver.current_url}"
            return result

        if not _set_date_range(driver, toorder_id, target_date):
            result["error"] = "날짜 설정 실패"
            return result

        existing_files = set(download_dir.glob("*"))

        wait = WebDriverWait(driver, 15)
        report_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), '보고서 생성')]")
            )
        )
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", report_btn
        )
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", report_btn)
        logger.info("[%s] 보고서 생성 클릭: %s", toorder_id, target_date)

        downloaded_file = None
        for _ in range(30):
            time.sleep(1)
            new_files = set(download_dir.glob("*")) - existing_files
            completed = [
                f for f in new_files
                if not f.name.endswith(".crdownload") and f.is_file()
            ]
            if completed:
                time.sleep(1)
                downloaded_file = completed[0]
                break

        if not downloaded_file:
            result["error"] = "다운로드 파일 없음"
            return result

        yymmdd = datetime.strptime(target_date, "%Y-%m-%d").strftime("%y%m%d")
        new_name = f"{Path(ORIG_DATE_FILENAME).stem}_{yymmdd}{Path(ORIG_DATE_FILENAME).suffix}"
        new_path = download_dir / new_name

        if new_path.exists():
            bak = new_path.with_name(
                f"{new_path.stem}_bak_{datetime.now().strftime('%H%M%S')}{new_path.suffix}"
            )
            new_path.rename(bak)

        downloaded_file.rename(new_path)
        result["success"] = True
        result["file"] = str(new_path)
        logger.info("[%s] 다운로드 완료: %s", toorder_id, new_path.name)

    except TimeoutException:
        result["error"] = "보고서 생성 버튼 없음 (타임아웃)"
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("[%s] run_crawling_daily_date_page 오류: %s", toorder_id, exc)

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[%s] 브라우저 종료", toorder_id)
            except Exception:
                pass

    return result


def run_crawling_date_range(
    toorder_id: str,
    toorder_pw: str,
    date_list: List[str],
    download_dir: Path,
) -> List[Dict[str, Any]]:
    """
    날짜 리스트에 해당하는 투오더 종합보고서를 세션을 유지하며 순차 다운로드한다.

    하나의 브라우저 세션으로 모든 날짜를 처리하므로 로그인은 최초 1회만 수행된다.

    Parameters:
        toorder_id:   투오더 로그인 ID
        toorder_pw:   투오더 비밀번호
        date_list:    대상 날짜 리스트 (["YYYY-MM-DD", ...])
        download_dir: 다운로드 저장 경로 (Path 객체)

    Returns:
        날짜별 결과 리스트
        [{"success": bool, "file": str|None, "date": str, "error": str|None}, ...]
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    all_results: List[Dict[str, Any]] = []
    driver = None

    logger.info(
        "[%s] run_crawling_date_range 시작 - 총 %d일",
        toorder_id,
        len(date_list),
    )

    try:
        driver = _launch_browser(toorder_id, download_dir)

        if not _do_login(driver, toorder_id, toorder_pw):
            logger.error("[%s] 로그인 실패 - 전체 날짜 스킵", toorder_id)
            for d in date_list:
                all_results.append(
                    {"success": False, "file": None, "date": d, "error": "로그인 실패"}
                )
            return all_results

        if not _navigate_to_sales_report(driver, toorder_id):
            logger.error("[%s] 보고서 페이지 이동 실패 - 전체 날짜 스킵", toorder_id)
            for d in date_list:
                all_results.append(
                    {
                        "success": False,
                        "file": None,
                        "date": d,
                        "error": "보고서 페이지 이동 실패",
                    }
                )
            return all_results

        for idx, target_date in enumerate(date_list):
            logger.info(
                "[%s] [%d/%d] %s 다운로드 중",
                toorder_id,
                idx + 1,
                len(date_list),
                target_date,
            )
            result = _download_report_for_date(
                driver, toorder_id, target_date, download_dir
            )
            all_results.append(result)

            # 마지막 날짜가 아니면 인터벌 대기
            if idx < len(date_list) - 1:
                _random_delay(*TIMING["date_interval"])

    except Exception as exc:
        logger.error("[%s] run_crawling_date_range 오류: %s", toorder_id, exc)

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[%s] 브라우저 종료", toorder_id)
            except Exception:
                pass

    success_count = sum(1 for r in all_results if r["success"])
    logger.info(
        "[%s] 완료: %d/%d 성공",
        toorder_id,
        success_count,
        len(date_list),
    )
    return all_results
