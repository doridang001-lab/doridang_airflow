"""
Food Guide order history collection pipeline.

The first operating version keeps the downloaded Excel workbook as the source
artifact. Parsing into a mart schema should be added after a real sample is
confirmed.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile, is_zipfile

import pendulum
import pandas as pd
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.paths import FOOD_GUIDE_RAW_DIR, MANUAL_DOWN_DIR, TEMP_DIR
from modules.transform.utility.selenium_uc import configure_chrome_temp_root, launch_uc_chrome

logger = logging.getLogger(__name__)

LOGIN_URL = "https://hffg.dwhf.co.kr/?w2xPath=/cm/main/Login3.xml"

ENV_ID_KEYS = ("FOOD_GUIDE_ID", "HFFG_FOOD_GUIDE_ID")
ENV_PW_KEYS = ("FOOD_GUIDE_PW", "HFFG_FOOD_GUIDE_PW", "FOOD_GUIDE_PASSWORD")
VAR_ID_KEYS = ENV_ID_KEYS
VAR_PW_KEYS = ENV_PW_KEYS

DEFAULT_DOWNLOAD_DIR = TEMP_DIR / "food_guide_orders_download"
DEFAULT_DEST_DIR = FOOD_GUIDE_RAW_DIR
DEBUG_DIR = TEMP_DIR / "food_guide_debug"
DEFAULT_ORDER_XLSX_GLOB = str(MANUAL_DOWN_DIR / "주문 예정 목*")
FOOD_GUIDE_ORDER_COLUMNS = [
    "선택",
    "배송일",
    "사업장",
    "상품코드",
    "상품명",
    "규격",
    "단위",
    "단가",
    "수량",
    "공급가",
    "부가세",
    "합계",
    "비고",
    "구분",
    "대분류",
    "최초저장시간",
    "최종수정시간",
]
NUMERIC_COLUMNS = ["단가", "수량", "공급가", "부가세", "합계"]
DATETIME_COLUMNS = ["배송일", "최초저장시간", "최종수정시간"]

WAIT_TIMEOUT = int(os.getenv("FOOD_GUIDE_WAIT_TIMEOUT_SEC", "40"))
DOWNLOAD_TIMEOUT = int(os.getenv("FOOD_GUIDE_DOWNLOAD_TIMEOUT_SEC", "180"))
GRID_READY_TIMEOUT = int(os.getenv("FOOD_GUIDE_GRID_READY_TIMEOUT_SEC", "600"))
GRID_POLL_SEC = float(os.getenv("FOOD_GUIDE_GRID_POLL_SEC", "2"))
ALLOW_NO_DATA_SUCCESS = os.getenv("FOOD_GUIDE_ALLOW_NO_DATA_SUCCESS", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
LOGIN_STABILIZE_SEC = float(os.getenv("FOOD_GUIDE_LOGIN_STABILIZE_SEC", "4"))
MENU_STABILIZE_SEC = float(os.getenv("FOOD_GUIDE_MENU_STABILIZE_SEC", "2"))
DATE_SET_REQUIRED = os.getenv("FOOD_GUIDE_REQUIRE_DATE_SET", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "n",
}
SKIP_DATE_SET = os.getenv("FOOD_GUIDE_SKIP_DATE_SET", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "n",
}

SELECTORS = {
    "user_id": (By.ID, "mf_ibx_USER_ID"),
    "password": (By.ID, "mf_sct_USER_PASSWORD"),
    "login_button": (By.ID, "mf_btn_login"),
    "order_menu": (By.ID, "mf_wfm_side_generator1_0_menu"),
    "hq_order_history": (By.ID, "mf_wfm_side_trv_menu_label_5"),
    "search_button": (
        By.XPATH,
        "//div[contains(@class,'w2tabcontrol_contents_wrapper_selected')]"
        "//input[contains(@id,'_body_wfm_wframe1_searchButton') and @value='조회']",
    ),
    "export_button": (
        By.XPATH,
        "//div[contains(@class,'w2tabcontrol_contents_wrapper_selected')]"
        "//input[contains(@id,'_body_btn_excel') and @title='엑셀다운로드']",
    ),
}

TEXT_FALLBACK_XPATH = {
    "order_menu": "//*[normalize-space()='주문관리' or contains(normalize-space(), '주문관리')]",
    "hq_order_history": "//*[normalize-space()='주문내역조회(본사)' or contains(normalize-space(), '주문내역조회')]",
    "search_button": "//input[@type='button' and @value='조회'] | //button[normalize-space()='조회']",
    "export_button": (
        "//*[@id='mf_tac_layout_contents_42_body_btn_excel']"
        " | //*[@id='mf_tac_layout_contents_42_body_btn_order_excel']"
        " | //*[contains(@class,'bar_btndiv')]"
        " | //input[@type='button' and (contains(@value,'엑셀') or contains(@value,'다운'))]"
        " | //button[contains(normalize-space(),'엑셀') or contains(normalize-space(),'다운')]"
    ),
}


def _is_displayed_enabled(element: Any) -> bool:
    try:
        return bool(element.is_displayed() and element.is_enabled())
    except WebDriverException:
        return False


def _get_airflow_variable(key: str) -> str:
    try:
        from airflow.models import Variable

        return (Variable.get(key, default_var=None) or "").strip()
    except Exception:
        return ""


def _first_config_value(keys: tuple[str, ...]) -> str:
    for key in keys:
        value = (os.getenv(key) or _get_airflow_variable(key)).strip()
        if value:
            return value
    return ""


def _resolve_credentials(food_guide_id: str | None = None, food_guide_pw: str | None = None) -> tuple[str, str]:
    account_id = (food_guide_id or "").strip() or _first_config_value(ENV_ID_KEYS)
    password = (food_guide_pw or "").strip() or _first_config_value(ENV_PW_KEYS)
    if not account_id or not password:
        raise RuntimeError(
            "Food Guide 계정 정보가 없습니다. "
            "FOOD_GUIDE_ID/FOOD_GUIDE_PW 환경변수 또는 Airflow Variable을 설정하세요."
        )
    return account_id, password


def _headless_enabled() -> bool:
    explicit = os.getenv("FOOD_GUIDE_HEADLESS")
    if explicit is not None:
        return explicit.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return os.getenv("AIRFLOW_HOME") is not None or os.getenv("IS_DOCKER", "").lower() == "true"


def _resolve_profile_dir() -> Path:
    """Keep the Chrome profile on container-local storage, not a Windows 9p mount.

    프로필을 매 실행 임시 생성하면(기본 동작) Docker Desktop의 9p 마운트 위에서
    new session이 120초를 넘겨 tab crashed로 이어진다. 고정 프로필을 써서
    생성 비용을 1회로 줄인다.
    """
    root = configure_chrome_temp_root() or str(TEMP_DIR)
    profile_dir = Path(root) / "food_guide_profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    return profile_dir


def _kill_stale_profile_chrome(profile_dir: Path) -> None:
    """Remove zombie Chrome processes still holding this profile's SingletonLock."""
    try:
        subprocess.run(
            ["pkill", "-f", f"user-data-dir={profile_dir}"],
            capture_output=True,
            timeout=10,
            check=False,
        )
        time.sleep(1.0)
    except Exception as exc:
        logger.warning("Food Guide 잔여 Chrome 정리 실패(무시): %s", exc)


def _build_options(download_dir: Path, profile_dir: Path) -> uc.ChromeOptions:
    options = uc.ChromeOptions()
    chrome_bin = os.getenv("CHROME_BIN", "").strip()
    if chrome_bin and Path(chrome_bin).exists():
        options.binary_location = chrome_bin

    options.add_argument(f"--user-data-dir={profile_dir}")
    if _headless_enabled():
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--no-zygote")  # Chrome 148+ Docker 크래시(tab crashed) 방지
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disk-cache-size=1")  # 캐시 최소화 (OOM 방지)
    options.add_argument("--js-flags=--max-old-space-size=1024")  # 렌더러 행/OOM 방지
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--mute-audio")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-features=Translate,BackForwardCache")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    return options


def _launch_browser(download_dir: Path) -> Chrome:
    download_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = _resolve_profile_dir()
    _kill_stale_profile_chrome(profile_dir)
    driver = launch_uc_chrome(
        _build_options(download_dir, profile_dir),
        account_id="food_guide",
        chrome_bin=os.getenv("CHROME_BIN", "").strip() or None,
        # Food Guide(hffg.dwhf.co.kr)는 봇 차단이 없다. UC patcher 경로는 실패 시
        # 캐시 삭제→재다운로드로 2분 이상을 태우므로 표준 chromedriver를 기본으로 쓴다.
        prefer_standard=os.getenv("FOOD_GUIDE_PREFER_STANDARD_CHROME", "1").strip().lower()
        in {"1", "true", "yes", "y"},
        command_timeout_sec=int(os.getenv("FOOD_GUIDE_WEBDRIVER_COMMAND_TIMEOUT_SEC", "90")),
    )
    driver.set_window_size(1920, 1080)
    try:
        driver.maximize_window()
    except Exception as exc:
        logger.warning("Food Guide browser maximize 실패(무시): %s", exc)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir.resolve())},
        )
    except Exception as exc:
        logger.warning("Food Guide download path CDP 설정 실패(무시): %s", exc)
    return driver


def _close_driver(driver: Chrome | None) -> None:
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass
    try:
        driver.quit = lambda *args, **kwargs: None
    except Exception:
        pass


def _save_debug_artifacts(driver: Chrome, tag: str) -> dict[str, str]:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    ts = pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    safe_tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", tag).strip("_") or "debug"
    png_path = DEBUG_DIR / f"{safe_tag}_{ts}.png"
    html_path = DEBUG_DIR / f"{safe_tag}_{ts}.html"
    out: dict[str, str] = {}
    try:
        driver.save_screenshot(str(png_path))
        out["screenshot"] = str(png_path)
    except Exception as exc:
        logger.warning("Food Guide debug screenshot 저장 실패: %s", exc)
    try:
        html_path.write_text(driver.page_source, encoding="utf-8")
        out["html"] = str(html_path)
    except Exception as exc:
        logger.warning("Food Guide debug html 저장 실패: %s", exc)
    return out


def _wait_document_ready(driver: Chrome, timeout: int = WAIT_TIMEOUT) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in {"interactive", "complete"}
    )


def _wait_ajax_idle(driver: Chrome, timeout: int = WAIT_TIMEOUT) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last_state = driver.execute_script(
                """
                const visibleLoading = Array.from(document.querySelectorAll('*')).filter((el) => {
                  const style = getComputedStyle(el);
                  const text = (el.innerText || '').trim();
                  return (el.offsetWidth || el.offsetHeight || el.getClientRects().length)
                    && style.display !== 'none'
                    && style.visibility !== 'hidden'
                    && /(로딩|조회중|처리중|Loading)/.test(text);
                }).length;
                return {
                  jqueryActive: window.jQuery ? window.jQuery.active : null,
                  visibleLoading,
                  bodyText: (document.body && document.body.innerText || '').slice(0, 500),
                };
                """
            )
        except WebDriverException:
            time.sleep(0.5)
            continue
        jquery_active = last_state.get("jqueryActive")
        visible_loading = int(last_state.get("visibleLoading") or 0)
        if jquery_active in (0, None) and visible_loading == 0:
            return last_state
        time.sleep(0.5)
    return last_state


def _wait_visible(driver: Chrome, locator: tuple[str, str], timeout: int = WAIT_TIMEOUT) -> Any:
    return WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))


def _wait_clickable(driver: Chrome, key: str, timeout: int = WAIT_TIMEOUT) -> Any:
    locators = [SELECTORS[key]]
    fallback = TEXT_FALLBACK_XPATH.get(key)
    if fallback:
        locators.append((By.XPATH, fallback))

    def _condition(current_driver: Chrome) -> Any:
        for locator in locators:
            try:
                elements = current_driver.find_elements(*locator)
            except WebDriverException:
                continue
            for element in elements:
                if _is_displayed_enabled(element):
                    return element
        return False

    return WebDriverWait(driver, timeout).until(_condition)


def _wait_order_history_ready(driver: Chrome, timeout: int = WAIT_TIMEOUT) -> None:
    wait = WebDriverWait(driver, timeout)
    wait.until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                "//*[contains(@class,'w2tabcontrol_contents') "
                "and contains(.,'주문내역조회(본사)') and contains(.,'배송기간')]",
            )
        )
    )
    for key in ("search_button", "export_button"):
        try:
            _wait_clickable(driver, key, timeout=timeout)
        except TimeoutException as exc:
            debug = _save_debug_artifacts(driver, f"order_history_{key}_timeout")
            raise RuntimeError(f"Food Guide 주문내역 화면 버튼 대기 실패: button={key}, debug={debug}") from exc


def _read_order_grid_state(driver: Chrome) -> dict[str, Any]:
    state: dict[str, Any] = {}
    try:
        return driver.execute_script(
            """
            const isVisible = (el) => {
              if (!el) return false;
              const style = getComputedStyle(el);
              return !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)
                && style.display !== 'none'
                && style.visibility !== 'hidden';
            };
            const selectedRoot = document.querySelector('.w2tabcontrol_contents_wrapper_selected')
              || document.querySelector('div.w2tabcontrol_contents');
            const visibleLoading = Array.from(document.querySelectorAll('*')).filter((el) => {
              const text = (el.innerText || '').trim();
              return isVisible(el) && /(로딩|조회중|처리중|Loading)/.test(text);
            }).length;
            const totalCandidates = Array.from(document.querySelectorAll('[id$="_body_totalCnt"], div.total .sum'))
              .filter(isVisible)
              .map((el) => {
                const totalGroup = el.closest('div.total');
                const root = el.closest('.w2tabcontrol_contents_wrapper_selected, div.w2tabcontrol_contents');
                const raw = (el.innerText || el.textContent || '').trim();
                const groupText = (totalGroup?.innerText || totalGroup?.textContent || '').trim();
                return {
                  raw,
                  groupText,
                  inSelected: !!selectedRoot && (selectedRoot === el || selectedRoot.contains(el)),
                  inSelectedWrapper: !!root && root.classList.contains('w2tabcontrol_contents_wrapper_selected'),
                };
              });
            totalCandidates.sort((a, b) =>
              Number(b.inSelectedWrapper) - Number(a.inSelectedWrapper)
              || Number(b.inSelected) - Number(a.inSelected)
            );
            const totalCandidate = totalCandidates[0] || {};
            const gridEl = Array.from(document.querySelectorAll('[id$="_body_scheduledOrderGrid"]'))
              .filter(isVisible)
              .sort((a, b) => {
                const ar = a.closest('.w2tabcontrol_contents_wrapper_selected, div.w2tabcontrol_contents');
                const br = b.closest('.w2tabcontrol_contents_wrapper_selected, div.w2tabcontrol_contents');
                return Number(!!br && br.classList.contains('w2tabcontrol_contents_wrapper_selected'))
                  - Number(!!ar && ar.classList.contains('w2tabcontrol_contents_wrapper_selected'));
              })[0];
            const gridText = (gridEl?.innerText || '').slice(0, 1000);
            const hasDataRows = !!gridEl && Array.from(gridEl.querySelectorAll('tr')).some((row) => {
              const text = (row.innerText || '').trim();
              const cells = row.querySelectorAll('td').length;
              return cells >= 5
                && !/(No\\.|선택|합계|데이터 없음)/.test(text)
                && /\\d{4}[-.]\\d{2}[-.]\\d{2}/.test(text);
            });
            return {
              jqueryActive: window.jQuery ? window.jQuery.active : null,
              visibleLoading,
              total_count: totalCandidate.raw || '',
              total_group_text: totalCandidate.groupText || '',
              total_candidates: totalCandidates.slice(0, 5),
              grid_text: gridText,
              has_data_rows: hasDataRows,
              bodyText: (document.body && document.body.innerText || '').slice(0, 1000),
            };
            """
        ) or {}
    except Exception as exc:
        logger.debug("Food Guide grid 상태 읽기 실패(재시도): %s", exc)
        return state


def _wait_for_order_grid_ready(
    driver: Chrome,
    *,
    timeout: int = GRID_READY_TIMEOUT,
    poll_sec: float = GRID_POLL_SEC,
    monotonic: Any = time.monotonic,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    deadline = monotonic() + timeout
    last_state: dict[str, Any] = {}
    _wait_order_history_ready(driver, timeout=WAIT_TIMEOUT)
    while monotonic() < deadline:
        state = _read_order_grid_state(driver)
        last_state = state
        total_count = _parse_grid_total_count(state.get("total_count"), allow_bare_number=True)
        if total_count is None:
            total_count = _parse_grid_total_count(state.get("total_group_text"), allow_bare_number=False)
        has_data_rows = bool(state.get("has_data_rows"))
        loading = int(state.get("visibleLoading") or 0) > 0
        jquery_active = state.get("jqueryActive")
        busy = loading or (jquery_active not in (0, None))

        if has_data_rows or (total_count is not None and total_count > 0):
            state["grid_ready_reason"] = "data"
            return state

        sleep(poll_sec)

    if ALLOW_NO_DATA_SUCCESS and _grid_state_indicates_no_data(last_state):
        last_state["grid_ready_reason"] = "timeout_no_data"
        return last_state
    debug = _save_debug_artifacts(driver, "grid_ready_timeout")
    raise RuntimeError(
        "Food Guide 주문내역 그리드 로딩 완료를 확인하지 못했습니다. "
        f"timeout={timeout}, last_state={last_state}, debug={debug}"
    )


def _parse_grid_total_count(value: Any, *, allow_bare_number: bool = True) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"총\s*([0-9][0-9,]*)\s*건", text)
    if not match:
        match = re.search(r"([0-9][0-9,]*)\s*건", text)
    if allow_bare_number and not match and re.fullmatch(r"[0-9][0-9,]*", text):
        match = re.match(r"([0-9][0-9,]*)", text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def _grid_state_indicates_no_data(grid_state: dict[str, Any] | None) -> bool:
    if not grid_state:
        return False
    total_count = _parse_grid_total_count(grid_state.get("total_count"), allow_bare_number=True)
    if total_count is not None:
        return total_count == 0
    total_group_count = _parse_grid_total_count(grid_state.get("total_group_text"), allow_bare_number=False)
    if total_group_count is not None:
        return total_group_count == 0
    for item in grid_state.get("total_candidates") or []:
        if not isinstance(item, dict):
            continue
        candidate_count = _parse_grid_total_count(item.get("raw"), allow_bare_number=True)
        if candidate_count is None:
            candidate_count = _parse_grid_total_count(item.get("groupText"), allow_bare_number=False)
        if candidate_count is not None:
            return candidate_count == 0
    for key in ("bodyText", "grid_text"):
        total_count = _parse_grid_total_count(grid_state.get(key), allow_bare_number=False)
        if total_count is not None:
            return total_count == 0

    grid_text = str(grid_state.get("grid_text") or "")
    body_text = str(grid_state.get("bodyText") or "")
    return "데이터 없음" in grid_text or "사업장주문내역(총0건)" in body_text


def _food_guide_no_data_summary(
    *,
    date_from: str,
    date_to: str,
    grid_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "success": True,
        "no_data": True,
        "rows": 0,
        "downloaded_path": "",
        "date_from": date_from,
        "date_to": date_to,
        "parquet_files": [],
        "deleted_sources": [],
        "grid_state": grid_state or {},
    }


def _click(driver: Chrome, key: str, timeout: int = WAIT_TIMEOUT) -> None:
    element = _wait_clickable(driver, key, timeout=timeout)
    driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", element)
    time.sleep(0.2)
    driver.execute_script("arguments[0].click();", element)


def _set_input_value(driver: Chrome, element: Any, value: str) -> None:
    element.click()
    element.send_keys(Keys.CONTROL + "a")
    element.send_keys(Keys.DELETE)
    element.send_keys(value)
    driver.execute_script(
        """
        const el = arguments[0];
        const value = arguments[1];
        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        setter.call(el, value);
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        """,
        element,
        value,
    )


def _login(driver: Chrome, account_id: str, password: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    driver.get(LOGIN_URL)
    _wait_document_ready(driver)
    user_input = wait.until(EC.presence_of_element_located(SELECTORS["user_id"]))
    pw_input = wait.until(EC.presence_of_element_located(SELECTORS["password"]))
    _set_input_value(driver, user_input, account_id)
    _set_input_value(driver, pw_input, password)
    _click(driver, "login_button", timeout=WAIT_TIMEOUT)
    try:
        wait.until(lambda d: not d.find_elements(*SELECTORS["user_id"]) or d.find_elements(*SELECTORS["order_menu"]))
    except TimeoutException as exc:
        debug = _save_debug_artifacts(driver, "login_timeout")
        raise RuntimeError(f"Food Guide 로그인 완료 확인 실패: debug={debug}") from exc
    if driver.find_elements(*SELECTORS["user_id"]) and not driver.find_elements(*SELECTORS["order_menu"]):
        debug = _save_debug_artifacts(driver, "login_failed")
        raise RuntimeError(f"Food Guide 로그인 실패 또는 로그인 화면 유지: debug={debug}")
    _wait_visible(driver, SELECTORS["order_menu"], timeout=WAIT_TIMEOUT)
    _wait_ajax_idle(driver, timeout=WAIT_TIMEOUT)
    time.sleep(LOGIN_STABILIZE_SEC)


def _normalize_yyyymmdd(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y%m%d")


def _date_label(value: str) -> str:
    dt = datetime.strptime(value, "%Y-%m-%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return f"{dt.strftime('%Y-%m-%d')} ({weekdays[dt.weekday()]})"


def _date_selector_pair_from_env() -> tuple[str, str] | None:
    start = os.getenv("FOOD_GUIDE_DATE_FROM_SELECTOR", "").strip()
    end = os.getenv("FOOD_GUIDE_DATE_TO_SELECTOR", "").strip()
    return (start, end) if start and end else None


def _set_dates_with_explicit_selectors(driver: Chrome, date_from: str, date_to: str, selectors: tuple[str, str]) -> bool:
    values = (_date_label(date_from), _date_label(date_to))
    for selector, value in zip(selectors, values):
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if not elements:
            raise RuntimeError(f"Food Guide 날짜 selector를 찾지 못했습니다: {selector}")
        _set_input_value(driver, elements[0], value)
    return True


def _set_order_history_date_inputs(driver: Chrome, date_from: str, date_to: str) -> bool:
    inputs = driver.find_elements(
        By.CSS_SELECTOR,
        "td[id$='_body_wq_uuid_855'] input.w2inputCalendar_divInput, "
        "div.dual_calendar input.w2inputCalendar_divInput",
    )
    if len(inputs) < 2:
        return False
    for element, value in zip(inputs[:2], (_date_label(date_from), _date_label(date_to))):
        _set_input_value(driver, element, value)
        driver.execute_script("arguments[0].blur();", element)
    actual = [
        driver.execute_script("return arguments[0].value;", element) or ""
        for element in inputs[:2]
    ]
    expected = [_date_label(date_from), _date_label(date_to)]
    if actual != expected:
        raise RuntimeError(f"Food Guide 날짜 입력값 반영 실패: actual={actual}, expected={expected}")
    return True


def _set_dates_by_heuristic(driver: Chrome, date_from: str, date_to: str) -> bool:
    values = (_date_label(date_from), _date_label(date_to))
    candidates = driver.execute_script(
        """
        const tokens = /(date|dt|ymd|기간|일자|조회|from|to|start|end|시작|종료|fr|st)/i;
        return Array.from(document.querySelectorAll('div.w2tabcontrol_contents input')).map((el, idx) => {
          const rect = el.getBoundingClientRect();
          const style = getComputedStyle(el);
          const meta = [el.id, el.name, el.title, el.placeholder, el.className].join(' ');
          return {
            idx, id: el.id, name: el.name, type: el.type, value: el.value,
            meta, visible: !!(rect.width || rect.height || el.getClientRects().length)
              && style.display !== 'none' && style.visibility !== 'hidden',
            disabled: el.disabled || el.readOnly,
            score: tokens.test(meta) ? 1 : 0,
          };
        }).filter(x => x.visible && !x.disabled && x.type === 'text' && !x.disabled && !x.readOnly && (x.score || /\\d{4}[-.]?\\d{2}[-.]?\\d{2}/.test(x.value || '')));
        """
    )
    if not isinstance(candidates, list) or len(candidates) < 2:
        return False

    indexes = [int(item["idx"]) for item in candidates[:2]]
    for input_index, value in zip(indexes, values):
        element = driver.find_elements(By.CSS_SELECTOR, "div.w2tabcontrol_contents input")[input_index]
        _set_input_value(driver, element, value)
    logger.info("Food Guide 날짜 input 휴리스틱 적용: candidates=%s", candidates[:2])
    return True


def _set_date_range(driver: Chrome, date_from: str, date_to: str) -> None:
    if SKIP_DATE_SET:
        logger.info("Food Guide 날짜 입력 스킵: 화면 기본 배송기간으로 조회합니다.")
        return
    explicit = _date_selector_pair_from_env()
    if explicit:
        _set_dates_with_explicit_selectors(driver, date_from, date_to, explicit)
        return
    if _set_order_history_date_inputs(driver, date_from, date_to):
        return
    if _set_dates_by_heuristic(driver, date_from, date_to):
        return
    if DATE_SET_REQUIRED:
        debug = _save_debug_artifacts(driver, "date_inputs_not_found")
        raise RuntimeError(
            "Food Guide 날짜 입력 필드를 확정하지 못했습니다. "
            "FOOD_GUIDE_DATE_FROM_SELECTOR/FOOD_GUIDE_DATE_TO_SELECTOR를 설정하세요. "
            f"debug={debug}"
        )
    logger.warning("Food Guide 날짜 입력 필드 미확정: 화면 기본 기간으로 조회합니다.")


def _cleanup_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for path in download_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix.lower() in {".xlsx", ".xls", ".csv", ".crdownload", ".tmp", ".part"}:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.warning("Food Guide 기존 다운로드 파일 삭제 실패(무시): %s", path)


def _wait_for_download(download_dir: Path, existing_files: set[Path], timeout: int = DOWNLOAD_TIMEOUT) -> Path | None:
    deadline = time.monotonic() + timeout
    stable_sizes: dict[Path, tuple[int, int]] = {}
    allowed = {".xlsx", ".xls", ".csv"}
    while time.monotonic() < deadline:
        current = {path for path in download_dir.iterdir() if path.is_file()}
        if any(path.suffix.lower() in {".crdownload", ".part", ".tmp"} for path in current):
            time.sleep(1)
            continue
        candidates = sorted(
            [path for path in current - existing_files if path.suffix.lower() in allowed],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                size = path.stat().st_size
            except OSError:
                continue
            prev_size, stable_count = stable_sizes.get(path, (-1, 0))
            stable_count = stable_count + 1 if size > 0 and size == prev_size else 0
            stable_sizes[path] = (size, stable_count)
            if stable_count >= 2:
                return path
        time.sleep(1)
    return None


def _validate_download(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Food Guide 다운로드 파일이 없습니다: {path}")
    if path.suffix.lower() == ".xlsx":
        if not is_zipfile(path):
            raise ValueError(f"Food Guide 다운로드 xlsx가 유효하지 않습니다: {path}")
        try:
            with ZipFile(path) as zf:
                names = set(zf.namelist())
        except BadZipFile as exc:
            raise ValueError(f"Food Guide 다운로드 xlsx가 깨졌습니다: {path}") from exc
        required = {"[Content_Types].xml", "xl/workbook.xml"}
        missing = sorted(required - names)
        if missing:
            raise ValueError(f"Food Guide 다운로드 xlsx 필수 항목 누락: missing={missing} path={path}")
    if path.stat().st_size <= 0:
        raise ValueError(f"Food Guide 다운로드 파일이 비어 있습니다: {path}")
    return {"path": str(path), "name": path.name, "size": path.stat().st_size, "suffix": path.suffix.lower()}


def _write_summary(dest_dir: Path, summary: dict[str, Any]) -> Path:
    summary_path = dest_dir / "latest_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return summary_path


def _is_summary_row(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool, index=df.index)
    summary_cols = [col for col in ("배송일", "사업장", "상품코드", "상품명", "규격", "단위") if col in df.columns]
    if not summary_cols:
        return pd.Series(False, index=df.index)
    return df[summary_cols].apply(lambda row: any(str(value).strip() == "합계" for value in row), axis=1)


def _normalize_food_guide_orders_df(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    missing = [col for col in FOOD_GUIDE_ORDER_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Food Guide 주문 엑셀 필수 컬럼 누락: missing={missing}, columns={list(df.columns)}")

    out = df[FOOD_GUIDE_ORDER_COLUMNS].copy().fillna("")
    before = len(out)
    out = out.loc[~_is_summary_row(out)].copy()
    removed_summary_rows = before - len(out)

    out = out.replace(r"^\s*$", pd.NA, regex=True)
    out = out.dropna(how="all").copy()
    if "배송일" in out.columns:
        out = out[out["배송일"].notna()].copy()

    for col in DATETIME_COLUMNS:
        out[col] = pd.to_datetime(out[col], errors="coerce")
    out = out[out["배송일"].notna()].copy()

    for col in NUMERIC_COLUMNS:
        out[col] = (
            out[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )
        out[col] = pd.to_numeric(out[col], errors="coerce")

    text_cols = [col for col in FOOD_GUIDE_ORDER_COLUMNS if col not in NUMERIC_COLUMNS + DATETIME_COLUMNS]
    for col in text_cols:
        out[col] = out[col].fillna("").astype(str).str.strip()

    return out.reset_index(drop=True), removed_summary_rows


def _read_food_guide_orders_xlsx(path: str | Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    src = Path(path)
    _validate_download(src)
    raw_df = pd.read_excel(src, dtype=str, engine="openpyxl").fillna("")
    normalized, removed_summary_rows = _normalize_food_guide_orders_df(raw_df)
    meta = {
        "source_path": str(src),
        "raw_rows": int(len(raw_df)),
        "rows": int(len(normalized)),
        "removed_summary_rows": int(removed_summary_rows),
    }
    return normalized, meta


def _resolve_order_xlsx_paths(source_paths: str | Path | list[str | Path] | None) -> list[Path]:
    if source_paths is None:
        source_paths = DEFAULT_ORDER_XLSX_GLOB
    if isinstance(source_paths, (str, Path)):
        token = str(source_paths)
        if any(ch in token for ch in "*?["):
            parent = Path(token).parent
            paths = sorted(parent.glob(Path(token).name))
        else:
            paths = [Path(token)]
    else:
        paths = [Path(path) for path in source_paths]
    existing = [path for path in paths if path.exists() and path.is_file()]
    if not existing:
        raise FileNotFoundError(f"Food Guide 변환 대상 엑셀 파일이 없습니다: {source_paths}")
    return existing


def _write_monthly_parquets(
    df: pd.DataFrame,
    *,
    dest_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    if df.empty:
        raise ValueError("Food Guide 주문 변환 결과가 비어 있습니다.")
    resolved_dest = Path(dest_dir) if dest_dir else DEFAULT_DEST_DIR
    resolved_dest.mkdir(parents=True, exist_ok=True)

    work = df.copy()
    work["_ym"] = work["배송일"].dt.strftime("%Y%m")
    if work["_ym"].isna().any():
        raise ValueError("Food Guide 주문 배송일에서 저장 월을 계산하지 못했습니다.")

    results: list[dict[str, Any]] = []
    for ym, month_df in sorted(work.groupby("_ym", dropna=False), key=lambda item: str(item[0])):
        month_out = month_df.drop(columns=["_ym"]).reset_index(drop=True)
        parquet_path = resolved_dest / f"food_guide_orders_{ym}.parquet"
        month_out.to_parquet(parquet_path, index=False, engine="pyarrow")
        check_df = pd.read_parquet(parquet_path)
        if len(check_df) != len(month_out):
            raise RuntimeError(
                f"Food Guide parquet 검증 실패: path={parquet_path}, "
                f"written={len(month_out)}, read={len(check_df)}"
            )
        if "배송일" not in check_df.columns:
            raise RuntimeError(f"Food Guide parquet 검증 실패: 배송일 컬럼 없음 path={parquet_path}")
        results.append({"ym": str(ym), "path": str(parquet_path), "rows": int(len(check_df))})
    return results


def convert_food_guide_order_files_to_parquet(
    *,
    source_paths: str | Path | list[str | Path] | None = None,
    dest_dir: str | Path | None = None,
    cleanup_source: bool = True,
) -> dict[str, Any]:
    paths = _resolve_order_xlsx_paths(source_paths)
    frames: list[pd.DataFrame] = []
    source_meta: list[dict[str, Any]] = []
    for path in paths:
        df, meta = _read_food_guide_orders_xlsx(path)
        frames.append(df)
        source_meta.append(meta)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=FOOD_GUIDE_ORDER_COLUMNS)
    parquet_results = _write_monthly_parquets(combined, dest_dir=dest_dir)

    deleted_sources: list[str] = []
    cleanup_errors: list[str] = []
    if cleanup_source:
        for path in paths:
            try:
                path.unlink()
                deleted_sources.append(str(path))
            except Exception as exc:
                cleanup_errors.append(f"{path}: {type(exc).__name__}: {exc}")
                logger.warning("Food Guide 원본 cleanup 실패: %s (%s)", path, exc)

    summary = {
        "source_files": [str(path) for path in paths],
        "source_meta": source_meta,
        "parquet_files": parquet_results,
        "rows": int(len(combined)),
        "deleted_sources": deleted_sources,
        "cleanup_errors": cleanup_errors,
        "converted_at": pendulum.now("Asia/Seoul").to_iso8601_string(),
    }
    dest = Path(dest_dir) if dest_dir else DEFAULT_DEST_DIR
    _write_summary(dest, summary)
    logger.info("Food Guide 월별 parquet 변환 완료: %s", summary)
    return summary


def download_food_guide_orders(
    *,
    date_from: str,
    date_to: str,
    download_dir: str | Path | None = None,
    food_guide_id: str | None = None,
    food_guide_pw: str | None = None,
    **_: object,
) -> dict[str, Any]:
    account_id, password = _resolve_credentials(food_guide_id, food_guide_pw)
    resolved_download_dir = Path(download_dir) if download_dir else DEFAULT_DOWNLOAD_DIR
    _cleanup_download_dir(resolved_download_dir)

    driver: Chrome | None = None
    try:
        driver = _launch_browser(resolved_download_dir)
        _login(driver, account_id, password)
        _click(driver, "order_menu")
        time.sleep(MENU_STABILIZE_SEC)
        _wait_ajax_idle(driver, timeout=WAIT_TIMEOUT)
        _wait_visible(driver, SELECTORS["hq_order_history"], timeout=WAIT_TIMEOUT)
        _click(driver, "hq_order_history")
        time.sleep(MENU_STABILIZE_SEC)
        _wait_ajax_idle(driver, timeout=WAIT_TIMEOUT)
        _wait_order_history_ready(driver)
        _set_date_range(driver, date_from, date_to)
        _click(driver, "search_button")
        grid_state = _wait_for_order_grid_ready(driver)
        if _grid_state_indicates_no_data(grid_state) and ALLOW_NO_DATA_SUCCESS:
            logger.info(
                "Food Guide 주문내역 데이터 없음: date_from=%s date_to=%s grid_state=%s",
                date_from,
                date_to,
                grid_state,
            )
            return _food_guide_no_data_summary(date_from=date_from, date_to=date_to, grid_state=grid_state)
        existing_files = {path for path in resolved_download_dir.iterdir() if path.is_file()}
        _click(driver, "export_button")
        downloaded = _wait_for_download(resolved_download_dir, existing_files)
        if downloaded is None:
            if _grid_state_indicates_no_data(grid_state) and ALLOW_NO_DATA_SUCCESS:
                logger.info(
                    "Food Guide 주문내역 데이터 없음(다운로드 없음): date_from=%s date_to=%s grid_state=%s",
                    date_from,
                    date_to,
                    grid_state,
                )
                return _food_guide_no_data_summary(date_from=date_from, date_to=date_to, grid_state=grid_state)
            debug = _save_debug_artifacts(driver, "download_not_found")
            no_data_note = " no_data_success_disabled=True" if _grid_state_indicates_no_data(grid_state) else ""
            raise RuntimeError(
                "Food Guide 주문내역 다운로드 파일을 찾지 못했습니다. "
                f"download_dir={resolved_download_dir},{no_data_note} debug={debug}, grid_state={grid_state}"
            )
        validation = _validate_download(downloaded)
        logger.info("Food Guide 주문 다운로드 완료: %s", validation)
        return {"success": True, "downloaded_path": str(downloaded), "validation": validation}
    finally:
        _close_driver(driver)


def save_food_guide_orders(
    *,
    downloaded_path: str,
    date_from: str,
    date_to: str,
    dest_dir: str | Path | None = None,
    **_: object,
) -> dict[str, Any]:
    if not str(downloaded_path or "").strip():
        logger.info("Food Guide 저장 스킵: 다운로드 파일 없음(데이터 없음) date_from=%s date_to=%s", date_from, date_to)
        return _food_guide_no_data_summary(date_from=date_from, date_to=date_to)
    summary = convert_food_guide_order_files_to_parquet(
        source_paths=downloaded_path,
        dest_dir=dest_dir,
        cleanup_source=True,
    )
    summary["date_from"] = date_from
    summary["date_to"] = date_to
    return summary


def run_food_guide_orders_collect(
    *,
    date_from: str,
    date_to: str,
    download_dir: str | Path | None = None,
    dest_dir: str | Path | None = None,
    food_guide_id: str | None = None,
    food_guide_pw: str | None = None,
    **kwargs: object,
) -> dict[str, Any]:
    downloaded = download_food_guide_orders(
        date_from=date_from,
        date_to=date_to,
        download_dir=download_dir,
        food_guide_id=food_guide_id,
        food_guide_pw=food_guide_pw,
        **kwargs,
    )
    if downloaded.get("no_data"):
        return downloaded
    return save_food_guide_orders(
        downloaded_path=str(downloaded["downloaded_path"]),
        date_from=date_from,
        date_to=date_to,
        dest_dir=dest_dir,
        **kwargs,
    )
