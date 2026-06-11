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
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.selenium_uc import configure_uc_data_path, launch_uc_chrome

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
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)

    driver = launch_uc_chrome(
        options=options,
        account_id=account_id,
        chrome_bin=os.getenv("CHROME_BIN", "/usr/bin/google-chrome"),
        log_fn=lambda msg: logger.info("[%s] %s", account_id, msg),
    )
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(download_dir.absolute()),
            },
        )
        logger.info("[%s] download behavior configured: %s", account_id, download_dir)
    except Exception as exc:
        logger.warning("[%s] download behavior configure failed: %s", account_id, exc)
    driver.set_window_size(1920, 1080)
    logger.info("[%s] 브라우저 실행 완료 (headless=%s)", account_id, HEADLESS_MODE)
    return driver


def _do_login(driver, account_id: str, password: str) -> bool:
    """투오더 CEO 사이트에 로그인한다."""
    logger.info("[%s] 로그인 시도", account_id)

    retry_errors = ("Connection aborted", "RemoteDisconnected", "Connection refused", "NewConnectionError", "MaxRetryError")
    for attempt in range(1, 4):
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
            break
        except Exception as exc:
            err_str = str(exc)
            if attempt < 3 and any(err in err_str for err in retry_errors):
                logger.warning(
                    "[%s] 페이지 이동 실패 (재시도 %d/3): %s",
                    account_id,
                    attempt,
                    err_str,
                )
                time.sleep(5)
                continue
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
    "Connection aborted",
    "RemoteDisconnected",
)

_BROWSER_DEAD_MARKERS = (
    "Connection refused", "NewConnectionError", "MaxRetryError",
    "RemoteDisconnected", "Connection aborted",
)


def _is_browser_dead(exc: Exception) -> bool:
    s = str(exc)
    return any(m in s for m in _BROWSER_DEAD_MARKERS)


def _check_browser_alive(driver) -> bool:
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def _recover_browser(
    driver,
    account_id: str,
    password: str,
    download_dir: Path,
    page_url: str,
    start_date: str,
    end_date: str,
) -> Optional[Any]:
    """브라우저 재시작 후 분석 페이지+날짜 복구. 성공 시 새 driver, 실패 시 None."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    new_driver = _launch_browser(account_id, download_dir)
    if not _do_login(new_driver, account_id, password):
        logger.error("[%s] 브라우저 재시작 후 로그인 실패", account_id)
        return None
    if not _navigate_to_analysis_page(new_driver, account_id, page_url):
        logger.error("[%s] 브라우저 재시작 후 페이지 이동 실패", account_id)
        return None
    if not _set_date_range(new_driver, account_id, start_date, end_date):
        logger.error("[%s] 브라우저 재시작 후 날짜 설정 실패", account_id)
        return None
    _wait_for_export_btn(new_driver, account_id)
    logger.info("[%s] 브라우저 재시작 완료", account_id)
    return new_driver


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


def _pick_completed_download(download_dir: Path, since_ts: float, known_names: set[str]) -> Optional[Path]:
    candidates: List[Path] = []
    allowed_suffixes = {".xlsx", ".xls", ".csv"}
    for path in download_dir.glob("*"):
        if not path.is_file():
            continue
        if path.name.endswith(".crdownload"):
            continue
        if path.suffix.lower() not in allowed_suffixes:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if path.name not in known_names or stat.st_mtime >= since_ts:
            candidates.append(path)

    if not candidates:
        return None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _wait_for_downloaded_file(
    download_dir: Path,
    file_prefix: str,
    timeout: int,
    logger_account_id: str,
    log_prefix: str,
) -> Optional[Path]:
    start_ts = time.time()
    known_names = {p.name for p in download_dir.glob("*") if p.is_file()}
    stable_path: Optional[Path] = None
    stable_size: Optional[int] = None
    stable_hits = 0

    for _ in range(timeout):
        time.sleep(1)
        picked = _pick_completed_download(download_dir, start_ts, known_names)
        if picked is None:
            continue

        try:
            size = picked.stat().st_size
        except OSError:
            continue

        if stable_path == picked and stable_size == size:
            stable_hits += 1
        else:
            stable_path = picked
            stable_size = size
            stable_hits = 1

        if stable_hits < 2:
            continue

        new_name = download_dir / f"{file_prefix}{picked.suffix}"
        if new_name.exists():
            new_name.unlink()
        picked.rename(new_name)
        logger.info("[%s] %s 다운로드 완료: %s", logger_account_id, log_prefix, new_name.name)
        return new_name

    logger.warning("[%s] %s 다운로드 타임아웃: %s", logger_account_id, log_prefix, file_prefix)
    return None


def _wait_for_store_dialog(driver, account_id: str, timeout: int = 15):
    """메뉴 행 액션 클릭 후 열리는 매장별 상세 dialog를 기다린다."""
    try:
        wait = WebDriverWait(driver, timeout)
        dialog = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[role='dialog']"))
        )
        wait.until(lambda d: "매장별" in dialog.text and "매장 명" in dialog.text)
        return dialog
    except TimeoutException:
        logger.error("[%s] 매장별 상세 dialog 로딩 타임아웃", account_id)
        return None


def _download_export_excel_from_dialog(
    driver,
    dialog,
    account_id: str,
    download_dir: Path,
    file_prefix: str,
    timeout: int = 60,
) -> Optional[Path]:
    """매장별 상세 dialog 내부 내보내기 버튼으로 Excel 다운로드를 수행한다."""
    wait = WebDriverWait(driver, 10)
    for attempt in range(1, 3):
        try:
            export_btn = wait.until(
                lambda d: dialog.find_element(By.CSS_SELECTOR, "button[aria-label='내보내기']")
            )
            driver.execute_script("arguments[0].click();", export_btn)
            time.sleep(1.0)

            excel_item = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[@role='menuitem'][contains(text(),'Excel로 내보내기')]")
                )
            )
            driver.execute_script("arguments[0].click();", excel_item)
            logger.info("[%s] dialog Excel 내보내기 클릭: %s (attempt=%d)", account_id, file_prefix, attempt)
        except Exception as exc:
            logger.error("[%s] dialog 내보내기 오류 (%s): %s", account_id, file_prefix, exc)
            return None

        downloaded = _wait_for_downloaded_file(
            download_dir=download_dir,
            file_prefix=file_prefix,
            timeout=timeout,
            logger_account_id=account_id,
            log_prefix="dialog",
        )
        if downloaded is not None:
            return downloaded

        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.0)
        except Exception:
            pass

    return None


def _wait_for_downloaded_file_v2(
    download_dir: Path,
    file_prefix: str,
    timeout: int,
    logger_account_id: str,
    log_prefix: str,
) -> Optional[Path]:
    start_ts = time.time()
    known_names = {p.name for p in download_dir.glob("*") if p.is_file()}
    stable_path: Optional[Path] = None
    stable_size: Optional[int] = None
    stable_hits = 0

    for _ in range(timeout):
        time.sleep(1)
        picked = _pick_completed_download(download_dir, start_ts, known_names)
        if picked is None:
            continue

        try:
            stat = picked.stat()
        except OSError:
            continue

        size = stat.st_size
        if stable_path == picked and stable_size == size:
            stable_hits += 1
        else:
            stable_path = picked
            stable_size = size
            stable_hits = 1

        if stable_hits < 2:
            continue

        new_name = download_dir / f"{file_prefix}{picked.suffix}"
        if new_name.exists():
            new_name.unlink()
        picked.rename(new_name)
        logger.info("[%s] %s download complete: %s", logger_account_id, log_prefix, new_name.name)
        return new_name

    recent_files = []
    for path in sorted(
        download_dir.glob("*"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )[:5]:
        try:
            stat = path.stat()
            recent_files.append(f"{path.name}|size={int(stat.st_size)}|mtime={int(stat.st_mtime)}")
        except OSError:
            continue
    if recent_files:
        logger.info("[%s] %s recent download candidates: %s", logger_account_id, log_prefix, recent_files)
    logger.warning("[%s] %s download timeout: %s", logger_account_id, log_prefix, file_prefix)
    return None


def _find_active_detail_dialog_v2(driver, page_url: str, existing_dialog_count: int = 0):
    dialogs = driver.find_elements(By.CSS_SELECTOR, "[role='dialog']")
    if len(dialogs) <= existing_dialog_count:
        return None

    page_fragment = page_url.rsplit("/", 1)[-1]
    title_tokens = ["매장별"] if page_fragment == "product-sales-date" else ["하위 메뉴", "옵션별", "매장별"]
    header_tokens = ["매장 명", "매장명"] if page_fragment == "product-sales-date" else ["메뉴 명", "메뉴명", "하위 옵션", "옵션 명", "옵션명", "판매량 (개)"]

    for dialog in reversed(dialogs):
        try:
            if not dialog.is_displayed():
                continue
            text = dialog.text or ""
            has_export = bool(dialog.find_elements(By.CSS_SELECTOR, "button[aria-label='내보내기']"))
            has_rows = len(dialog.find_elements(By.CSS_SELECTOR, "[role='row']")) > 1
            if not has_export or not has_rows:
                continue
            if page_fragment == "option-sales-date":
                return dialog
            if any(token in text for token in title_tokens) or any(token in text for token in header_tokens):
                return dialog
        except Exception:
            continue
    return None


def _wait_for_store_dialog_v2(
    driver,
    account_id: str,
    page_url: str,
    existing_dialog_count: int = 0,
    timeout: int = 15,
):
    try:
        wait = WebDriverWait(driver, timeout)
        dialog = wait.until(lambda d: _find_active_detail_dialog_v2(d, page_url, existing_dialog_count))
        return dialog
    except TimeoutException:
        logger.error("[%s] detail dialog load timeout (%s)", account_id, page_url.rsplit("/", 1)[-1])
        return None


def _download_export_excel_from_dialog_v2(
    driver,
    dialog,
    account_id: str,
    download_dir: Path,
    file_prefix: str,
    timeout: int = 60,
) -> Optional[Path]:
    wait = WebDriverWait(driver, 10)
    export_buttons = dialog.find_elements(By.CSS_SELECTOR, "button[aria-label='내보내기']")
    if not export_buttons:
        logger.error("[%s] dialog export button missing: %s", account_id, file_prefix)
        return None

    export_btn = export_buttons[0]
    for attempt in range(1, 3):
        try:
            driver.execute_script("arguments[0].click();", export_btn)
            time.sleep(1.0)
            excel_item = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[@role='menuitem'][contains(text(),'Excel로 내보내기')]")
                )
            )
            driver.execute_script("arguments[0].click();", excel_item)
            logger.info("[%s] dialog Excel export click: %s (attempt=%d)", account_id, file_prefix, attempt)
        except Exception as exc:
            logger.error("[%s] dialog export error (%s): %s", account_id, file_prefix, exc)
            return None

        downloaded = _wait_for_downloaded_file_v2(
            download_dir=download_dir,
            file_prefix=file_prefix,
            timeout=timeout,
            logger_account_id=account_id,
            log_prefix="dialog",
        )
        if downloaded is not None:
            return downloaded

        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.0)
        except Exception:
            pass

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


def _normalize_menu_name(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _scroll_and_find_menu_btn(
    driver, account_id: str, menu_name: str, row_index: int, page_url: str = MENU_ANALYSIS_URL
) -> Optional[Any]:
    """특정 메뉴 행으로 스크롤하고 OpenInNew 버튼 요소를 반환한다."""
    target_name = _normalize_menu_name(menu_name)
    base_scroll_top = max(0, (row_index - 2) * DATAGRID_ROW_HEIGHT)
    scroll_candidates = [base_scroll_top]
    if page_url.rsplit("/", 1)[-1] == "option-sales-date":
        scroll_candidates.extend(
            [
                max(0, base_scroll_top - DATAGRID_ROW_HEIGHT),
                base_scroll_top + DATAGRID_ROW_HEIGHT,
                max(0, base_scroll_top - (DATAGRID_ROW_HEIGHT * 2)),
            ]
        )

    seen_scrolls = set()
    for scroll_top in scroll_candidates:
        if scroll_top in seen_scrolls:
            continue
        seen_scrolls.add(scroll_top)
        driver.execute_script(
            "var g = document.querySelector('.MuiDataGrid-virtualScroller');"
            "if(g) g.scrollTop = arguments[0];",
            scroll_top,
        )
        time.sleep(1.0 if page_url.rsplit("/", 1)[-1] == "option-sales-date" else 0.8)

        cells = driver.find_elements(By.CSS_SELECTOR, "[data-field='productName'] .MuiDataGrid-cellContent")
        for cell in cells:
            title = cell.get_attribute("title") or cell.text
            if _normalize_menu_name(title) != target_name:
                continue
            try:
                row_el = driver.execute_script(
                    "return arguments[0].closest('[role=\"row\"]');", cell
                )
                return row_el.find_element(By.CSS_SELECTOR, "button[aria-label='More']")
            except Exception as exc:
                logger.warning("[%s] %s 버튼 탐색 실패: %s", account_id, menu_name, exc)
    return None


def _close_detail_dialog(driver, dialog) -> None:
    try:
        close_btn = dialog.find_element(
            By.XPATH, ".//button[.//*[contains(@data-testid,'CloseIcon')]]"
        )
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(1.5 if HEADLESS_MODE else 1.0)
        return
    except Exception:
        pass

    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(1.0)
    except Exception:
        pass


def _reset_detail_grid(driver, account_id: str) -> None:
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.5)
    except Exception:
        pass
    _click_search(driver, account_id)
    time.sleep(1.0)


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


def _download_store_detail_from_dialog(
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
    메뉴/옵션 행의 OpenInNew 버튼으로 매장별 상세 dialog를 열고
    dialog 내부의 내보내기 버튼으로 Excel을 다운로드한다.
    """
    btn = _scroll_and_find_menu_btn(driver, account_id, menu_name, row_index)
    if not btn:
        logger.warning("[%s] 버튼 없음 스킵: %s", account_id, menu_name)
        return None

    try:
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(1.5)
    except Exception as exc:
        logger.error("[%s] OpenInNew 클릭 실패 (%s): %s", account_id, menu_name, exc)
        return None

    dialog = _wait_for_store_dialog(driver, account_id, timeout=15)
    if dialog is None:
        return None

    safe_name = re.sub(r'[\\/:*?"<>|]', "_", menu_name)
    yymmdd = datetime.now().strftime("%y%m%d")
    file_path = _download_export_excel_from_dialog(
        driver, dialog, account_id, download_dir, f"store_{safe_name}_{yymmdd}"
    )

    try:
        close_btn = dialog.find_element(
            By.XPATH, ".//button[.//*[contains(@data-testid,'CloseIcon')]]"
        )
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(1.0)
    except Exception:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.0)
        except Exception:
            pass

    return file_path


def _download_store_detail_from_dialog_v2(
    driver,
    account_id: str,
    menu_name: str,
    row_index: int,
    download_dir: Path,
    start_date: str,
    end_date: str,
    page_url: str = MENU_ANALYSIS_URL,
) -> Optional[Path]:
    file_path, _, _ = _download_store_detail_attempt_v2(
        driver=driver,
        account_id=account_id,
        menu_name=menu_name,
        row_index=row_index,
        download_dir=download_dir,
        start_date=start_date,
        end_date=end_date,
        page_url=page_url,
    )
    return file_path


def _download_store_detail_attempt_v2(
    driver,
    account_id: str,
    menu_name: str,
    row_index: int,
    download_dir: Path,
    start_date: str,
    end_date: str,
    page_url: str = MENU_ANALYSIS_URL,
) -> Tuple[Optional[Path], Optional[str], Optional[str]]:
    safe_name = re.sub(r'[\\/:*?"<>|]', "_", menu_name)
    yymmdd = datetime.now().strftime("%y%m%d")
    file_prefix = f"store_{safe_name}_{yymmdd}"
    download_timeout = 120 if HEADLESS_MODE else 60
    page_fragment = page_url.rsplit("/", 1)[-1]

    for open_attempt in range(1, 3):
        btn = _scroll_and_find_menu_btn(
            driver,
            account_id,
            menu_name,
            row_index,
            page_url=page_url,
        )
        if not btn:
            if page_fragment == "option-sales-date" and open_attempt == 1:
                _reset_detail_grid(driver, account_id)
                continue
            logger.warning("[%s] button not found: %s", account_id, menu_name)
            return None, "button_not_found", "find_button"

        existing_dialog_count = len(driver.find_elements(By.CSS_SELECTOR, "[role='dialog']"))
        try:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(2.0 if HEADLESS_MODE else 1.2)
        except Exception as exc:
            logger.error("[%s] OpenInNew click failed (%s): %s", account_id, menu_name, exc)
            return None, "click_failed", "open_detail"

        dialog = _wait_for_store_dialog_v2(
            driver,
            account_id,
            page_url=page_url,
            existing_dialog_count=existing_dialog_count,
            timeout=20 if HEADLESS_MODE else 12,
        )
        if dialog is None:
            if open_attempt == 1:
                _reset_detail_grid(driver, account_id)
                continue
            return None, "dialog_timeout", "wait_dialog"

        file_path = _download_export_excel_from_dialog_v2(
            driver,
            dialog,
            account_id,
            download_dir,
            file_prefix,
            timeout=download_timeout,
        )

        _close_detail_dialog(driver, dialog)

        if file_path is not None:
            return file_path, None, None

        if open_attempt == 1:
            _reset_detail_grid(driver, account_id)

    return None, "download_timeout", "download_export"


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
    result: Dict[str, Any] = {
        "store_files": [],
        "menu_names": [],
        "skipped_details": [],
        "error": None,
    }
    driver = None

    try:
        _MAX_BROWSER_RETRIES = 2
        for _br_attempt in range(1, _MAX_BROWSER_RETRIES + 1):
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
            driver = _launch_browser(toorder_id, download_dir)
            if _do_login(driver, toorder_id, toorder_pw):
                break
            if _br_attempt < _MAX_BROWSER_RETRIES:
                logger.warning(
                    "[%s] 로그인 실패, 브라우저 재시작 (시도 %d/%d)",
                    toorder_id, _br_attempt, _MAX_BROWSER_RETRIES,
                )
        else:
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
        skipped_details: List[Dict[str, str]] = []
        for iter_idx, (orig_idx, menu_name) in enumerate(menu_with_idx):
            logger.info(
                "[%s] [%d/%d] 매장별 수집: %s",
                toorder_id, iter_idx + 1, len(menu_with_idx), menu_name,
            )
            try:
                path, reason, stage = _download_store_detail_attempt_v2(
                    driver, toorder_id, menu_name, orig_idx, download_dir, start_date, end_date,
                    page_url=page_url,
                )
            except Exception as exc:
                if _is_browser_dead(exc):
                    logger.warning(
                        "[%s] 브라우저 크래시 감지 (item %d), 재시작: %s",
                        toorder_id, iter_idx + 1, exc,
                    )
                    driver = _recover_browser(
                        driver, toorder_id, toorder_pw, download_dir,
                        page_url, start_date, end_date,
                    )
                    if driver is None:
                        result["error"] = "브라우저 재시작 실패"
                        return result
                    try:
                        path, reason, stage = _download_store_detail_attempt_v2(
                            driver, toorder_id, menu_name, orig_idx, download_dir, start_date, end_date,
                            page_url=page_url,
                        )
                    except Exception as retry_exc:
                        logger.warning("[%s] 재시도 실패 (skip): %s", toorder_id, retry_exc)
                        path = None
                else:
                    logger.warning(
                        "[%s] [%d/%d] 수집 스킵: %s",
                        toorder_id, iter_idx + 1, len(menu_with_idx), exc,
                    )
                    path = None
            else:
                # driver.back() 후 navigate 실패로 None 반환됐지만 driver 자체가 죽은 경우
                if path is None and not _check_browser_alive(driver):
                    logger.warning(
                        "[%s] 브라우저 무응답 감지 (item %d 복귀 중), 재시작",
                        toorder_id, iter_idx + 1,
                    )
                    driver = _recover_browser(
                        driver, toorder_id, toorder_pw, download_dir,
                        page_url, start_date, end_date,
                    )
                    if driver is None:
                        result["error"] = "브라우저 재시작 실패"
                        return result

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


def download_first_menu_detail_for_debug(
    toorder_id: str,
    toorder_pw: str,
    target_date: str,
    download_dir: Path,
    page_url: str = MENU_ANALYSIS_URL,
) -> Dict[str, Any]:
    """
    디버깅용: 지정 날짜의 첫 번째 메뉴 1개만 상세 Excel로 내려받는다.

    Returns:
        {
            "menu_name": str | None,
            "file_path": Path | None,
            "menu_count": int,
            "error": str | None,
        }
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {
        "menu_name": None,
        "file_path": None,
        "menu_count": 0,
        "error": None,
    }
    driver = None

    try:
        driver = _launch_browser(toorder_id, download_dir)
        if not _do_login(driver, toorder_id, toorder_pw):
            result["error"] = "로그인 실패"
            return result

        if not _navigate_to_analysis_page(driver, toorder_id, page_url):
            result["error"] = "분석 페이지 이동 실패"
            return result

        if not _set_date_range(driver, toorder_id, target_date, target_date):
            result["error"] = "날짜 설정 실패"
            return result

        if not _wait_for_export_btn(driver, toorder_id):
            result["error"] = "DataGrid 로딩 실패"
            return result

        menu_names = _collect_all_menu_names(driver, toorder_id)
        result["menu_count"] = len(menu_names)
        if not menu_names:
            result["error"] = "메뉴 목록 없음"
            return result

        menu_name = menu_names[0]
        file_path = _download_store_detail_from_dialog_v2(
            driver,
            toorder_id,
            menu_name,
            0,
            download_dir,
            target_date,
            target_date,
            page_url=page_url,
        )
        result["menu_name"] = menu_name
        result["file_path"] = file_path
        if file_path is None:
            result["error"] = "첫 메뉴 상세 다운로드 실패"
        return result

    except Exception as exc:
        result["error"] = str(exc)
        logger.error("[%s] download_first_menu_detail_for_debug 오류: %s", toorder_id, exc, exc_info=True)
        return result
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


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
    ToOrder 메뉴/옵션 상세 분석 파일을 수집한다.

    Returns:
        {
            "store_files": [(menu_name, Path), ...],
            "menu_names": [str, ...],
            "skipped_details": [{"menu_name": str, "reason": str, "stage": str}],
            "error": str | None,
        }
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {
        "store_files": [],
        "menu_names": [],
        "skipped_details": [],
        "error": None,
    }
    driver = None

    try:
        max_browser_retries = 2
        for browser_attempt in range(1, max_browser_retries + 1):
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            driver = _launch_browser(toorder_id, download_dir)
            if _do_login(driver, toorder_id, toorder_pw):
                break

            if browser_attempt < max_browser_retries:
                logger.warning(
                    "[%s] 로그인 실패, 브라우저 재시작 (%d/%d)",
                    toorder_id,
                    browser_attempt,
                    max_browser_retries,
                )
        else:
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

        menu_names = _collect_all_menu_names(driver, toorder_id)
        if not menu_names:
            logger.warning("[%s] 메뉴 목록 없음, 상세 수집 생략", toorder_id)
            return result

        result["menu_names"] = menu_names

        if target_menus is not None:
            target_set = set(target_menus)
            menu_with_idx = [
                (orig_idx, name)
                for orig_idx, name in enumerate(menu_names)
                if name in target_set
            ]
            logger.info(
                "[%s] 재시도 모드: %d개 대상 (DataGrid 전체 %d개 중)",
                toorder_id,
                len(menu_with_idx),
                len(menu_names),
            )
        else:
            menu_with_idx = list(enumerate(menu_names))

        driver.execute_script(
            "var g = document.querySelector('.MuiDataGrid-virtualScroller'); if(g) g.scrollTop = 0;"
        )
        time.sleep(0.5)

        store_files: List[Tuple[str, Path]] = []
        skipped_details: List[Dict[str, str]] = []

        for iter_idx, (orig_idx, menu_name) in enumerate(menu_with_idx):
            logger.info(
                "[%s] [%d/%d] 매장별 수집: %s",
                toorder_id,
                iter_idx + 1,
                len(menu_with_idx),
                menu_name,
            )
            reason = None
            stage = None

            try:
                path, reason, stage = _download_store_detail_attempt_v2(
                    driver,
                    toorder_id,
                    menu_name,
                    orig_idx,
                    download_dir,
                    start_date,
                    end_date,
                    page_url=page_url,
                )
            except Exception as exc:
                if _is_browser_dead(exc):
                    logger.warning(
                        "[%s] 브라우저 오류 감지 (item %d), 재시작: %s",
                        toorder_id,
                        iter_idx + 1,
                        exc,
                    )
                    driver = _recover_browser(
                        driver,
                        toorder_id,
                        toorder_pw,
                        download_dir,
                        page_url,
                        start_date,
                        end_date,
                    )
                    if driver is None:
                        result["error"] = "브라우저 재시작 실패"
                        return result

                    try:
                        path, reason, stage = _download_store_detail_attempt_v2(
                            driver,
                            toorder_id,
                            menu_name,
                            orig_idx,
                            download_dir,
                            start_date,
                            end_date,
                            page_url=page_url,
                        )
                    except Exception as retry_exc:
                        logger.warning("[%s] 재시도 실패 (skip): %s", toorder_id, retry_exc)
                        path = None
                        reason = "retry_exception"
                        stage = "recover_browser"
                else:
                    logger.warning(
                        "[%s] [%d/%d] 수집 스킵: %s",
                        toorder_id,
                        iter_idx + 1,
                        len(menu_with_idx),
                        exc,
                    )
                    path = None
                    reason = "unexpected_exception"
                    stage = "collect_loop"
            else:
                if path is None and not _check_browser_alive(driver):
                    logger.warning(
                        "[%s] 브라우저 무응답 감지 (item %d 복구 중), 재시작",
                        toorder_id,
                        iter_idx + 1,
                    )
                    driver = _recover_browser(
                        driver,
                        toorder_id,
                        toorder_pw,
                        download_dir,
                        page_url,
                        start_date,
                        end_date,
                    )
                    if driver is None:
                        result["error"] = "브라우저 재시작 실패"
                        return result

            if path:
                store_files.append((menu_name, path))
            elif reason:
                skipped_details.append(
                    {
                        "menu_name": menu_name,
                        "reason": reason,
                        "stage": stage or "",
                    }
                )

            time.sleep(random.uniform(0.8, 1.5))

        result["store_files"] = store_files
        result["skipped_details"] = skipped_details
        logger.info(
            "[%s] 수집 완료: 매장별 %d/%d건 (전체 메뉴 %d개)",
            toorder_id,
            len(store_files),
            len(menu_with_idx),
            len(result["menu_names"]),
        )
        if skipped_details:
            reason_counts: Dict[str, int] = {}
            for item in skipped_details:
                skip_reason = item.get("reason", "unknown")
                reason_counts[skip_reason] = reason_counts.get(skip_reason, 0) + 1
            logger.info("[%s] skipped detail summary: %s", toorder_id, reason_counts)

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
