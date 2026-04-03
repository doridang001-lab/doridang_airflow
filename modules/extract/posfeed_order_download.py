"""
포스피드 주문 목록 엑셀 자동 다운로드

처리 흐름:
1. 브라우저 실행 (다운로드 설정 포함)
2. Posfeed 로그인
3. 주문 목록 페이지 이동
4. 검색 버튼 클릭
5. 엑셀 다운로드 버튼 클릭
6. 확인 팝업 처리
7. 다운로드 완료 대기
"""

import logging
import os
import re
import subprocess
import time
from pathlib import Path

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from modules.transform.utility.paths import DOWN_DIR

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
POSFEED_ID = "siw2222@naver.com"
POSFEED_PW = "ehfl8877!!"
LOGIN_URL = "https://admin.posfeed.co.kr/#/login?redirect=%2Fdashboard"
ORDER_LIST_URL = "https://admin.posfeed.co.kr/#/order/list"
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None

# 타임아웃 설정
WAIT_TIMEOUT = 30  # 요소 대기 타임아웃 (초)
DOWNLOAD_TIMEOUT = 120  # 다운로드 완료 대기 타임아웃 (초)


# ============================================================
# 내부 유틸 - Chrome 버전 감지
# ============================================================
def _get_chrome_version() -> int | None:
    """설치된 Chrome 버전 감지 (Docker 환경 고려)"""
    candidates = [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["chromium-browser", "--version"],
        ["chromium", "--version"],
    ]
    for cmd in candidates:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            match = re.search(r"(\d+)\.", result.stdout.strip())
            if match:
                return int(match.group(1))
        except Exception:
            continue
    return None


# ============================================================
# 내부 유틸 - 브라우저 (다운로드 설정 포함)
# ============================================================
def _launch_browser_with_download(download_dir: Path) -> uc.Chrome:
    """다운로드 설정이 포함된 Chrome 브라우저 실행"""
    logger.info("브라우저 실행 (headless=%s, download_dir=%s)", HEADLESS_MODE, download_dir)

    # 다운로드 디렉토리 생성
    download_dir.mkdir(parents=True, exist_ok=True)

    def _make_options() -> uc.ChromeOptions:
        options = uc.ChromeOptions()
        
        # Chrome 바이너리 경로 (환경변수)
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin and Path(chrome_bin).exists():
            options.binary_location = chrome_bin
        
        # Headless 설정
        if HEADLESS_MODE:
            options.add_argument("--headless=new")
        
        # Docker 환경 대응
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # 다운로드 설정
        prefs = {
            "download.default_directory": str(download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        
        return options

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        if chrome_version:
            kwargs["version_main"] = chrome_version
        driver = uc.Chrome(**kwargs)
        driver.set_window_size(1920, 1080)
        logger.info("브라우저 실행 성공 (Chrome version: %s)", chrome_version or "auto")
        return driver
    except Exception as e:
        # 버전 불일치 시 재시도
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning("버전 불일치 감지 → %d 버전으로 재시도", detected)
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            driver.set_window_size(1920, 1080)
            logger.info("브라우저 실행 성공 (Chrome version: %d)", detected)
            return driver
        raise


# ============================================================
# 내부 유틸 - 로그인
# ============================================================
def _login(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """Posfeed 로그인"""
    logger.info("로그인 페이지 이동")
    driver.get(LOGIN_URL)
    time.sleep(2)  # Vue.js 초기 렌더링 대기
    
    # ID 입력
    id_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
    id_input.clear()
    id_input.send_keys(POSFEED_ID)
    
    # PW 입력
    pw_input = driver.find_element(By.NAME, "password")
    pw_input.clear()
    pw_input.send_keys(POSFEED_PW)
    
    # 로그인 실행 (Enter)
    pw_input.send_keys(Keys.RETURN)
    
    # 로그인 완료 대기 (URL 변경 확인)
    wait.until(lambda d: "#/login" not in d.current_url)
    time.sleep(2)  # 대시보드 렌더링 대기
    
    logger.info("로그인 완료 | 현재 URL: %s", driver.current_url)


# ============================================================
# 내부 유틸 - 다운로드 대기
# ============================================================
def _get_existing_files(directory: Path) -> set[Path]:
    """다운로드 디렉토리의 기존 Excel 파일 목록 수집"""
    files = set(directory.glob("*.xlsx")) | set(directory.glob("*.xls"))
    return files


def _wait_for_download(
    directory: Path,
    existing_files: set[Path],
    timeout: int = DOWNLOAD_TIMEOUT,
) -> Path | None:
    """새로 다운로드된 파일 대기 (기존 파일 제외)"""
    logger.info("다운로드 완료 대기 (timeout=%d초, 기존파일=%d개)", timeout, len(existing_files))
    end_time = time.time() + timeout

    while time.time() < end_time:
        current = set(directory.glob("*.xlsx")) | set(directory.glob("*.xls"))
        new_files = current - existing_files
        crdownload = list(directory.glob("*.crdownload"))

        if new_files and not crdownload:
            latest = sorted(new_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
            logger.info("다운로드 완료 | 파일: %s", latest.name)
            return latest

        time.sleep(1)

    logger.warning("다운로드 타임아웃 (제한시간 초과)")
    return None


# ============================================================
# 메인 함수
# ============================================================
def download_posfeed_orders() -> Path | None:
    """
    포스피드 주문 목록 엑셀 다운로드
    
    Returns:
        Path | None: 다운로드된 파일 경로 (실패 시 None)
    """
    driver = None
    
    try:
        # 1. 브라우저 실행
        driver = _launch_browser_with_download(DOWN_DIR)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        
        # 2. 로그인
        _login(driver, wait)
        
        # 3. 주문 목록 페이지 이동
        logger.info("주문 목록 페이지 이동")
        driver.get(ORDER_LIST_URL)
        time.sleep(3)  # Vue.js 페이지 렌더링 대기
        
        # 4. 검색 버튼 클릭 (el-button--success, el-icon-search)
        logger.info("검색 버튼 클릭")
        search_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.el-button--success"))
        )
        search_btn.click()
        time.sleep(2)  # 검색 결과 로딩 대기
        
        # 5. 엑셀 다운로드 버튼 클릭 (el-icon-download 아이콘 기반)
        logger.info("엑셀 다운로드 버튼 클릭")
        existing_files = _get_existing_files(DOWN_DIR)
        download_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class,'el-button--primary') and .//i[contains(@class,'el-icon-download')]]")
            )
        )
        download_btn.click()
        time.sleep(1)  # 확인 팝업 렌더링 대기
        
        # 6. 확인 팝업 처리 (Element UI MessageBox)
        logger.info("확인 팝업 처리")
        try:
            # 방법 1: MessageBox 내부 확인 버튼 (class 기반)
            confirm_btn = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-message-box__btns button.el-button--primary"))
            )
            confirm_btn.click()
            logger.info("확인 버튼 클릭 완료")
        except TimeoutException:
            # 방법 2: XPath로 "확인" 텍스트 기반 검색
            try:
                confirm_btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//span[text()='확인']/parent::button"))
                )
                confirm_btn.click()
                logger.info("확인 버튼 클릭 완료 (XPath)")
            except TimeoutException:
                logger.warning("확인 팝업을 찾을 수 없음 (다운로드는 시작되었을 수 있음)")
        
        # 7. 다운로드 완료 대기 (기존 파일 제외)
        downloaded_file = _wait_for_download(DOWN_DIR, existing_files)
        
        if downloaded_file:
            logger.info("다운로드 성공 | 파일: %s", downloaded_file)
            return downloaded_file
        else:
            logger.error("다운로드 실패 (파일을 찾을 수 없음)")
            return None
    
    except TimeoutException as e:
        logger.error("타임아웃 에러: %s", e)
        return None
    except NoSuchElementException as e:
        logger.error("요소를 찾을 수 없음: %s", e)
        return None
    except Exception as e:
        logger.exception("예상치 못한 에러 발생: %s", e)
        return None
    
    finally:
        # 8. 브라우저 종료
        if driver:
            try:
                driver.quit()
                logger.info("브라우저 정상 종료")
            except Exception as e:
                logger.warning("브라우저 종료 중 에러: %s", e)


# ============================================================
# 테스트용 실행
# ============================================================
if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    
    result = download_posfeed_orders()
    if result:
        print(f"✅ 다운로드 성공: {result}")
    else:
        print("❌ 다운로드 실패")
