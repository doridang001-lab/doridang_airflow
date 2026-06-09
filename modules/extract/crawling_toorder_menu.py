"""
투오더 메뉴별 판매량 분석 크롤링 모듈

product-sales-date 페이지에서 메뉴별 판매량(aggregate)과
매장별 메뉴 판매량(per-store detail)을 자동 다운로드한다.

공개 함수:
    run_toorder_menu_crawl - 날짜 범위 기준 수집
"""

import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.error

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
# 상수
# ============================================================

HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
MENU_ANALYSIS_URL   = "https://ceo.toorder.co.kr/dashboard/product-analysis/product-sales-date"
OPTION_ANALYSIS_URL = "https://ceo.toorder.co.kr/dashboard/product-analysis/option-sales-date"
LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]

DATAGRID_ROW_HEIGHT = 52   # MUI DataGrid 행 높이 (px)
SCROLL_STEP = 260          # 5행 단위 스크롤


# ============================================================
# 내부 유틸리티 (crawling_toorder_sales_report.py 패턴 재사용)
# ============================================================

def _react_set_value(driver, element, value: str) -> None:
    """React 컨트롤드 인풋에 native setter로 값을 주입한다."""
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
    """React 컨트롤드 인풋에 값을 입력한다. ActionChains 우선, JS setter 폴백."""
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
        return True

    try:
        _react_set_value(driver, element, value)
        time.sleep(0.3)
    except Exception as exc:
        logger.error("[%s] %s JS setter 예외: %s", account_id, field_name, exc)
        return False

    actual2 = driver.execute_script("return arguments[0].value;", element) or ""
    return actual2 == value


def _launch_browser(account_id: str, download_dir: Path) -> uc.Chrome:
    """undetected_chromedriver 브라우저 인스턴스를 생성한다."""
    options = uc.ChromeOptions()

    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists() and chrome_bin.strip():
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

    version_main: Optional[int] = None
    try:
        result = subprocess.run(
            [chrome_bin, "--version"], capture_output=True, text=True, timeout=5
        )
        version_main = int(result.stdout.strip().split()[-1].split(".")[0])
        logger.info("[%s] Chrome 버전: %s", account_id, version_main)
    except Exception as exc:
        logger.warning("[%s] Chrome 버전 감지 실패: %s", account_id, exc)

    _UC_INIT_RETRIES = 3
    _UC_INIT_RETRY_DELAY = 10

    driver = None
    for attempt in range(1, _UC_INIT_RETRIES + 1):
        try:
            configure_uc_data_path()
            kwargs: Dict[str, Any] = {"options": options}
            if version_main:
                kwargs["version_main"] = version_main
            driver = uc.Chrome(**kwargs)
            break
        except urllib.error.URLError as exc:
            if attempt < _UC_INIT_RETRIES:
                logger.warning(
                    "[%s] ChromeDriver 초기화 네트워크 오류 (시도 %d/%d), %ds 후 재시도: %s",
                    account_id, attempt, _UC_INIT_RETRIES, _UC_INIT_RETRY_DELAY, exc,
                )
                time.sleep(_UC_INIT_RETRY_DELAY)
            else:
                raise
        except Exception as exc:
            match = re.search(r"Current browser version is (\d+)", str(exc))
            if not match:
                raise
            configure_uc_data_path()
            driver = uc.Chrome(options=options, version_main=int(match.group(1)))
            break

    driver.set_window_size(1920, 1080)
    logger.info("[%s] 브라우저 실행 완료 (headless=%s)", account_id, HEADLESS_MODE)
    return driver


def _do_login(driver, account_id: str, password: str) -> bool:
    """투오더 CEO 사이트에 로그인한다."""
    logger.info("[%s] 로그인 시도", account_id)
    try:
        driver.get(LOGIN_URL)
        end_time = time.time() + 15
        while time.time() < end_time:
            if driver.execute_script(
                "return document.querySelector('input[name=\"id\"]') !== null;"
            ):
                break
            time.sleep(0.5)
        else:
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
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
    except (TimeoutException, NoSuchElementException) as exc:
        logger.error("[%s] 입력 필드 없음: %s", account_id, exc)
        return False

    if not _fill_react_input(driver, id_input, account_id, account_id, "ID"):
        logger.error("[%s] ID 입력 실패", account_id)
        return False
    if not _fill_react_input(driver, pw_input, password, account_id, "PW"):
        logger.error("[%s] PW 입력 실패", account_id)
        return False

    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        if not checkbox.is_selected():
            driver.execute_script("arguments[0].click();", checkbox)
    except Exception:
        pass

    time.sleep(0.5)
    submit_btn = None
    for sel in ["button[type='submit']"]:
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except Exception:
            pass
    if not submit_btn:
        try:
            submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), '로그인')]")
        except Exception:
            pass
    if not submit_btn:
        logger.error("[%s] 로그인 버튼 없음", account_id)
        return False

    driver.execute_script("arguments[0].click();", submit_btn)

    try:
        WebDriverWait(driver, 15).until(EC.url_contains("/dashboard"))
        time.sleep(1.0)
    except TimeoutException:
        pass

    success = "/dashboard" in driver.current_url and not any(
        p in driver.current_url for p in LOGIN_FAIL_URL_PATTERNS
    )
    if success:
        logger.info("[%s] 로그인 성공", account_id)
    else:
        logger.error("[%s] 로그인 실패 (url=%s)", account_id, driver.current_url)
    return success


NETWORK_ERRORS = (
    "ERR_NAME_NOT_RESOLVED",
    "ERR_INTERNET_DISCONNECTED",
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_NETWORK_CHANGED",
)


def _navigate_to_analysis_page(
    driver, account_id: str, url: str, retries: int = 3, retry_delay: int = 10
) -> bool:
    """분석 페이지로 이동한다. 일시적 네트워크 오류는 재시도한다."""
    url_fragment = url.rsplit("/", 1)[-1]  # e.g. "product-sales-date" or "option-sales-date"
    for attempt in range(1, retries + 1):
        logger.info("[%s] 페이지 이동 (시도 %d/%d): %s", account_id, attempt, retries, url_fragment)
        try:
            driver.get(url)
            WebDriverWait(driver, 20).until(lambda d: url_fragment in d.current_url)
            time.sleep(3.0)
            return True
        except Exception as exc:
            err_str = str(exc)
            is_network_err = any(e in err_str for e in NETWORK_ERRORS)
            if is_network_err and attempt < retries:
                logger.warning(
                    "[%s] 네트워크 오류 (시도 %d/%d), %ds 후 재시도: %s",
                    account_id, attempt, retries, retry_delay, err_str[:80],
                )
                time.sleep(retry_delay)
                continue
            logger.error("[%s] 페이지 이동 실패 (%s): %s", account_id, url_fragment, exc)
            return False
    return False


def _set_date_range(driver, account_id: str, start_date: str, end_date: str) -> bool:
    """날짜 범위 입력 필드에 시작일과 종료일을 각각 설정한다."""
    logger.info("[%s] 날짜 설정: %s ~ %s", account_id, start_date, end_date)
    try:
        start_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y-%m-%d")
        end_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%y-%m-%d")

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

        for inp, val in [(date_inputs[0], start_fmt), (date_inputs[1], end_fmt)]:
            inp.click()
            time.sleep(0.3)
            inp.send_keys(Keys.CONTROL + "a")
            time.sleep(0.1)
            inp.send_keys(Keys.DELETE)
            time.sleep(0.1)
            inp.send_keys(val)
            time.sleep(0.5)

        driver.find_element(By.TAG_NAME, "body").click()
        time.sleep(1.5)
        logger.info("[%s] 날짜 설정 완료: %s ~ %s", account_id, start_fmt, end_fmt)
        return True
    except Exception as exc:
        logger.error("[%s] 날짜 설정 실패: %s", account_id, exc)
        return False


def _wait_for_export_btn(driver, account_id: str, timeout: int = 20) -> bool:
    """내보내기 버튼이 클릭 가능할 때까지 대기한다 (페이지 로드 확인용)."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='내보내기']"))
        )
        time.sleep(0.5)
        return True
    except TimeoutException:
        logger.error("[%s] 내보내기 버튼 로딩 타임아웃", account_id)
        return False


def _click_search(driver, account_id: str) -> bool:
    """돋보기(조회) 버튼을 클릭한다. DataGrid에 데이터를 로드시키는 트리거."""
    try:
        wait = WebDriverWait(driver, 10)
        btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.MuiLoadingButton-root"))
        )
        driver.execute_script("arguments[0].click();", btn)
        logger.info("[%s] 조회 버튼 클릭", account_id)
        return True
    except TimeoutException:
        pass
    # 폴백: iconify--eva SVG를 포함한 버튼
    try:
        btn = driver.find_element(
            By.XPATH, "//button[.//svg[contains(@class,'iconify--eva')]]"
        )
        driver.execute_script("arguments[0].click();", btn)
        logger.info("[%s] 조회 버튼 클릭 (폴백 선택자)", account_id)
        return True
    except Exception as exc:
        logger.error("[%s] 조회 버튼 없음: %s", account_id, exc)
        return False


def _download_export_excel(
    driver,
    account_id: str,
    download_dir: Path,
    file_prefix: str,
    timeout: int = 60,
) -> Optional[Path]:
    """내보내기 → Excel로 내보내기 클릭 후 다운로드 완료를 기다린다."""
    existing = set(download_dir.glob("*"))
    try:
        wait = WebDriverWait(driver, 10)
        export_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='내보내기']"))
        )
        driver.execute_script("arguments[0].click();", export_btn)
        time.sleep(1.0)

        excel_item = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//li[@role='menuitem'][contains(text(),'Excel로 내보내기')]")
            )
        )
        driver.execute_script("arguments[0].click();", excel_item)
        logger.info("[%s] Excel 내보내기 클릭: %s", account_id, file_prefix)
    except Exception as exc:
        logger.error("[%s] 내보내기 버튼 오류 (%s): %s", account_id, file_prefix, exc)
        return None

    for _ in range(timeout):
        time.sleep(1)
        current = set(download_dir.glob("*"))
        new_files = current - existing
        completed = [f for f in new_files if not f.name.endswith(".crdownload") and f.is_file()]
        if completed:
            time.sleep(0.5)
            downloaded = completed[0]
            new_name = download_dir / f"{file_prefix}{downloaded.suffix}"
            if new_name.exists():
                new_name.unlink()
            downloaded.rename(new_name)
            logger.info("[%s] 다운로드 완료: %s", account_id, new_name.name)
            return new_name

    logger.warning("[%s] 다운로드 타임아웃: %s", account_id, file_prefix)
    return None


def _collect_all_menu_names(driver, account_id: str) -> List[str]:
    """DataGrid를 끝까지 스크롤하며 모든 메뉴명(title 속성)을 순서대로 수집한다."""
    seen: List[str] = []
    cell_sel = "[data-field='productName'] .MuiDataGrid-cellContent"
    grid_sel = ".MuiDataGrid-virtualScroller"
    scroll_pos = 0

    # DataGrid 맨 위에서 시작
    driver.execute_script(
        f"var g = document.querySelector('{grid_sel}'); if(g) g.scrollTop = 0;"
    )
    time.sleep(0.5)

    while True:
        cells = driver.find_elements(By.CSS_SELECTOR, cell_sel)
        for cell in cells:
            title = cell.get_attribute("title") or cell.text
            if title and title not in seen:
                seen.append(title)

        # 스크롤 바닥 도달 여부 확인
        at_bottom = driver.execute_script(
            "var g = document.querySelector(arguments[0]);"
            "return g ? (g.scrollTop + g.clientHeight >= g.scrollHeight - 5) : true;",
            grid_sel,
        )
        if at_bottom:
            break

        scroll_pos += SCROLL_STEP
        driver.execute_script(
            f"var g = document.querySelector('{grid_sel}'); if(g) g.scrollTop = {scroll_pos};"
        )
        time.sleep(0.8)

    logger.info("[%s] 총 %d개 메뉴 발견", account_id, len(seen))
    return seen


def _scroll_and_find_menu_btn(
    driver, account_id: str, menu_name: str, row_index: int
) -> Optional[Any]:
    """특정 메뉴 행으로 스크롤하고 OpenInNew 버튼 요소를 반환한다."""
    # 행이 화면 중앙에 오도록 스크롤
    scroll_top = max(0, (row_index - 2) * DATAGRID_ROW_HEIGHT)
    driver.execute_script(
        "var g = document.querySelector('.MuiDataGrid-virtualScroller');"
        "if(g) g.scrollTop = arguments[0];",
        scroll_top,
    )
    time.sleep(0.8)

    cells = driver.find_elements(By.CSS_SELECTOR, "[data-field='productName'] .MuiDataGrid-cellContent")
    for cell in cells:
        title = cell.get_attribute("title") or cell.text
        if title == menu_name:
            try:
                row_el = driver.execute_script(
                    "return arguments[0].closest('[role=\"row\"]');", cell
                )
                return row_el.find_element(By.CSS_SELECTOR, "button[aria-label='More']")
            except Exception as exc:
                logger.warning("[%s] %s 버튼 탐색 실패: %s", account_id, menu_name, exc)
    return None


def _download_store_detail(
    driver,
    account_id: str,
    menu_name: str,
    row_index: int,
    download_dir: Path,
    start_date: str,
    end_date: str,
    page_url: str = MENU_ANALYSIS_URL,
) -> Optional[Path]:
    """
    메뉴/옵션 행의 OpenInNew를 클릭하고 매장별 상세 Excel을 다운로드한다.

    새 탭 또는 SPA 페이지 이동 양쪽 모두 처리한다.
    SPA 이동 후 복귀 시 날짜 범위를 재설정한다.
    """
    btn = _scroll_and_find_menu_btn(driver, account_id, menu_name, row_index)
    if not btn:
        logger.warning("[%s] 버튼 없음 스킵: %s", account_id, menu_name)
        return None

    original_handle = driver.current_window_handle
    original_url = driver.current_url
    existing_handles = set(driver.window_handles)

    try:
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(2.5)
    except Exception as exc:
        logger.error("[%s] OpenInNew 클릭 실패 (%s): %s", account_id, menu_name, exc)
        return None

    new_handles = set(driver.window_handles) - existing_handles
    new_tab = bool(new_handles)

    if new_tab:
        driver.switch_to.window(new_handles.pop())
    else:
        try:
            WebDriverWait(driver, 10).until(lambda d: d.current_url != original_url)
        except TimeoutException:
            pass

    # 페이지 로드 확인 → 조회 버튼 클릭 → DataGrid 로딩 완료 대기
    _wait_for_export_btn(driver, account_id, timeout=20)
    _click_search(driver, account_id)
    time.sleep(1.0)
    _wait_for_export_btn(driver, account_id, timeout=20)

    safe_name = re.sub(r'[\\/:*?"<>|]', "_", menu_name)
    yymmdd = datetime.now().strftime("%y%m%d")
    file_path = _download_export_excel(
        driver, account_id, download_dir, f"store_{safe_name}_{yymmdd}"
    )

    if new_tab:
        driver.close()
        driver.switch_to.window(original_handle)
    else:
        driver.back()
        time.sleep(2.0)
        # SPA 복귀 후 페이지 상태 확인 및 날짜 재설정
        url_fragment = page_url.rsplit("/", 1)[-1]
        if url_fragment not in driver.current_url:
            if not _navigate_to_analysis_page(driver, account_id, page_url):
                return None
        _set_date_range(driver, account_id, start_date, end_date)
        _wait_for_export_btn(driver, account_id)

    return file_path


# ============================================================
# 공개 인터페이스
# ============================================================

def run_toorder_menu_crawl(
    toorder_id: str,
    toorder_pw: str,
    start_date: str,
    end_date: str,
    download_dir: Path,
    target_menus: Optional[List[str]] = None,
    page_url: str = MENU_ANALYSIS_URL,
) -> Dict[str, Any]:
    """
    투오더 메뉴별 판매량 분석 데이터를 수집한다.

    Parameters:
        toorder_id:    투오더 로그인 ID
        toorder_pw:    투오더 비밀번호
        start_date:    수집 시작일 ("YYYY-MM-DD")
        end_date:      수집 종료일 ("YYYY-MM-DD")
        download_dir:  임시 다운로드 경로
        target_menus:  None=전체 수집, 리스트=지정 메뉴만 재시도 (aggregate 스킵)

    Returns:
        {
            "store_files":  [(menu_name, Path), ...], # 매장별 상세 Excel 목록
            "menu_names":   [str, ...],               # DataGrid 전체 메뉴 목록 (retry_missing 비교용)
            "error":        str | None,
        }
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {"store_files": [], "menu_names": [], "error": None}
    driver = None

    try:
        driver = _launch_browser(toorder_id, download_dir)

        if not _do_login(driver, toorder_id, toorder_pw):
            result["error"] = "로그인 실패"
            return result

        if not _navigate_to_analysis_page(driver, toorder_id, page_url):
            result["error"] = "분석 페이지 이동 실패"
            return result

        if not _set_date_range(driver, toorder_id, start_date, end_date):
            result["error"] = "날짜 설정 실패"
            return result

        if not _wait_for_export_btn(driver, toorder_id):
            result["error"] = "DataGrid 로딩 실패"
            return result

        # ── 1. 전체 메뉴 목록 수집 (DataGrid 스크롤)
        menu_names = _collect_all_menu_names(driver, toorder_id)
        if not menu_names:
            logger.warning("[%s] 메뉴 목록 없음, 매장별 수집 생략", toorder_id)
            return result

        result["menu_names"] = menu_names

        # 원본 DataGrid 인덱스를 보존하며 필터링
        # idx가 enumerate 순서(0,1,2...)가 아닌 실제 DataGrid 행 위치여야 스크롤이 정확함
        if target_menus is not None:
            target_set = set(target_menus)
            menu_with_idx = [
                (orig_idx, name)
                for orig_idx, name in enumerate(menu_names)
                if name in target_set
            ]
            logger.info(
                "[%s] 재시도 모드: %d개 대상 (DataGrid 전체 %d개 중)",
                toorder_id, len(menu_with_idx), len(menu_names),
            )
        else:
            menu_with_idx = list(enumerate(menu_names))

        # DataGrid 스크롤 초기화
        driver.execute_script(
            "var g = document.querySelector('.MuiDataGrid-virtualScroller'); if(g) g.scrollTop = 0;"
        )
        time.sleep(0.5)

        # ── 3. 메뉴별 매장 상세 다운로드
        store_files: List[Tuple[str, Path]] = []
        for iter_idx, (orig_idx, menu_name) in enumerate(menu_with_idx):
            logger.info(
                "[%s] [%d/%d] 매장별 수집: %s",
                toorder_id, iter_idx + 1, len(menu_with_idx), menu_name,
            )
            path = _download_store_detail(
                driver, toorder_id, menu_name, orig_idx, download_dir, start_date, end_date,
                page_url=page_url,
            )
            if path:
                store_files.append((menu_name, path))
            time.sleep(random.uniform(0.8, 1.5))

        result["store_files"] = store_files
        logger.info(
            "[%s] 수집 완료: 매장별 %d/%d건 (전체 메뉴 %d개)",
            toorder_id, len(store_files), len(menu_with_idx), len(result["menu_names"]),
        )

    except Exception as exc:
        result["error"] = str(exc)
        logger.error("[%s] run_toorder_menu_crawl 오류: %s", toorder_id, exc, exc_info=True)

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[%s] 브라우저 종료", toorder_id)
            except Exception:
                pass

    return result
