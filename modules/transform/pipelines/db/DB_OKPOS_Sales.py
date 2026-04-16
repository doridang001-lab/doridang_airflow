"""
OKPOS 매출 원본파일 자동 다운로드 파이프라인

처리 흐름:
1. 실행 날짜 범위 결정 (conf 또는 yesterday)
2. today 페이지 매장별 엑셀 다운로드
3. receipt_details 페이지 매장별 엑셀 다운로드
4. 파일 이동 및 파일명 표준화 저장 (RAW_OKPOS_SALES)
5. log.parquet 실행 이력 기록
"""

import hashlib
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from modules.transform.utility.paths import ANALYTICS_DB, RAW_OKPOS_SALES, TEMP_DIR

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
OKPOS_LOGIN_URL = "https://my.okpos.co.kr/asp/login"
OKPOS_ID        = "ue60"
OKPOS_PW        = "93832"

STORES = [
    {"name": "도리당 동두천지행점", "shopCd": "RL2725"},
    {"name": "도리당 삼송점",       "shopCd": "UE6850"},
    {"name": "도리당 평택비전점",    "shopCd": "UW4935"},
    {"name": "도리당 광주상무점",    "shopCd": "XK8828"},
]

PAGE_TYPES = {
    "today": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/today",
        "date_input_id": "datepicker-input",
        "excel_js": "exportDetailSheet()",
    },
    "receipt": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/receipt/details",
        "date_input_id": "saleDate",
        "excel_js": "exportReceiptDetailExcel()",
    },
}

DOWNLOAD_TIMEOUT = 120
WAIT_TIMEOUT     = 30
HEADLESS_MODE    = os.getenv("AIRFLOW_HOME") is not None


# ============================================================
# 내부 유틸
# ============================================================

def _get_chrome_version() -> int | None:
    import subprocess
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


def _launch_browser(download_dir: Path) -> uc.Chrome:
    """다운로드 경로가 설정된 Chrome 브라우저 실행"""
    download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"브라우저 실행 (headless={HEADLESS_MODE}, download_dir={download_dir})")

    def _make_options() -> uc.ChromeOptions:
        options = uc.ChromeOptions()
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        if Path(chrome_bin).exists():
            options.binary_location = chrome_bin
        if HEADLESS_MODE:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        return options

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        if chrome_version:
            kwargs["version_main"] = chrome_version
        driver = uc.Chrome(**kwargs)
        driver.set_window_size(1920, 1080)
        if HEADLESS_MODE:
            driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(download_dir),
                "eventsEnabled": True,
            })
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning(f"버전 불일치 → {detected} 으로 재시도")
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            driver.set_window_size(1920, 1080)
            return driver
        raise


def _login(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """OKPOS 로그인"""
    driver.get(OKPOS_LOGIN_URL)
    time.sleep(3)

    # ID 입력 필드 (여러 셀렉터 시도)
    _ID_SELECTORS = [
        (By.ID, "userId"),
        (By.NAME, "userId"),
        (By.CSS_SELECTOR, "input[type='text']"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'아이디')]"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'ID')]"),
    ]
    id_input = None
    for sel in _ID_SELECTORS:
        try:
            id_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            logger.info(f"ID 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if id_input is None:
        inputs = [(el.get_attribute("id"), el.get_attribute("name"), el.get_attribute("type"))
                  for el in driver.find_elements(By.TAG_NAME, "input")]
        logger.error(f"ID 입력 필드를 찾을 수 없음 | 현재 URL: {driver.current_url} | inputs: {inputs}")
        raise TimeoutException("OKPOS 로그인 ID 입력 필드를 찾을 수 없습니다.")

    id_input.clear()
    id_input.send_keys(OKPOS_ID)

    # PW 입력 필드 (여러 셀렉터 시도, wait 사용)
    _PW_SELECTORS = [
        (By.ID, "userPw"),
        (By.NAME, "userPw"),
        (By.ID, "password"),
        (By.NAME, "password"),
        (By.CSS_SELECTOR, "input[type='password']"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'비밀번호')]"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'PW')]"),
    ]
    pw_input = None
    for sel in _PW_SELECTORS:
        try:
            pw_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            logger.info(f"PW 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if pw_input is None:
        inputs = [(el.get_attribute("id"), el.get_attribute("name"), el.get_attribute("type"))
                  for el in driver.find_elements(By.TAG_NAME, "input")]
        logger.error(f"PW 입력 필드를 찾을 수 없음 | inputs: {inputs}")
        raise TimeoutException("OKPOS 로그인 PW 입력 필드를 찾을 수 없습니다.")

    pw_input.clear()
    pw_input.send_keys(OKPOS_PW)
    pw_input.send_keys(Keys.RETURN)

    wait.until(lambda d: "/login" not in d.current_url)
    time.sleep(2)
    logger.info(f"OKPOS 로그인 완료 | URL: {driver.current_url}")


def _date_to_kst_ms(sale_date: str) -> int:
    """YYYY-MM-DD → KST 00:00:00 기준 Unix timestamp (ms)"""
    dt = datetime.strptime(sale_date, "%Y-%m-%d")
    kst_offset = 9 * 3600
    epoch = datetime(1970, 1, 1)
    ts_utc = (dt - epoch).total_seconds() - kst_offset
    return int(ts_utc * 1000)


def _select_date_tui(driver: uc.Chrome, wait: WebDriverWait, sale_date: str, date_input_id: str) -> None:
    """OKPOS 날짜 입력 필드에 날짜 설정 (다양한 방식 순서대로 시도)"""
    # 날짜 형식 준비
    year, month, day = sale_date.split("-")
    date_slash = f"{year}/{month}/{day}"    # YYYY/MM/DD
    date_dot   = f"{year}.{month}.{day}"    # YYYY.MM.DD
    date_short = f"{year}-{month}-{day}"    # YYYY-MM-DD

    # 입력 필드 찾기 (ID 또는 NAME)
    input_el = None
    for sel in [
        (By.ID, date_input_id),
        (By.NAME, date_input_id),
        (By.CSS_SELECTOR, f"[id='{date_input_id}']"),
    ]:
        try:
            input_el = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if input_el is None:
        raise TimeoutException(f"날짜 입력 필드를 찾을 수 없습니다: id={date_input_id}")

    # 현재 값 및 타입 로깅
    el_type  = input_el.get_attribute("type") or ""
    el_value = input_el.get_attribute("value") or ""
    logger.info(f"날짜 입력 필드 발견: id={date_input_id}, type={el_type}, current_value={el_value}")

    # ── 전략 1: JS로 직접 value 설정 + change/input 이벤트 발생 ──────────
    for fmt in [date_slash, date_dot, date_short]:
        try:
            driver.execute_script(
                "arguments[0].value = arguments[1];"
                "arguments[0].dispatchEvent(new Event('input',  {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                input_el, fmt
            )
            time.sleep(0.3)
            new_val = input_el.get_attribute("value") or ""
            if new_val and new_val != el_value:
                logger.info(f"날짜 설정 완료 (JS setValue): {fmt} → value={new_val}")
                return
        except Exception:
            continue

    # ── 전략 2: 클릭 → 키보드 입력 ──────────────────────────────────────
    try:
        driver.execute_script("arguments[0].click();", input_el)
        time.sleep(0.5)
        _debug_screenshot(driver, f"after_click_{date_input_id}")

        # 달력 팝업 여부 확인
        calendar_visible = False
        for cal_sel in [
            ".tui-datepicker",
            ".datepicker",
            "[class*='calendar']",
            "[class*='datepicker']",
        ]:
            els = driver.find_elements(By.CSS_SELECTOR, cal_sel)
            if any(e.is_displayed() for e in els):
                calendar_visible = True
                logger.info(f"달력 팝업 감지: {cal_sel}")
                break

        if calendar_visible:
            # data-timestamp 방식 (KST 기준, UTC 기준 두 가지 모두 시도)
            for ts_ms in [_date_to_kst_ms(sale_date), int(datetime.strptime(sale_date, "%Y-%m-%d").timestamp() * 1000)]:
                cells = driver.find_elements(By.XPATH, f"//td[@data-timestamp='{ts_ms}']")
                visible = [c for c in cells if c.is_displayed()]
                if visible:
                    driver.execute_script("arguments[0].click();", visible[0])
                    time.sleep(0.5)
                    logger.info(f"날짜 선택 완료 (data-timestamp={ts_ms}): {sale_date}")
                    return

            # 텍스트 기반 일(day) 셀 클릭
            day_str = str(int(day))
            for td_sel in ["td.tui-calendar-day", "td.day", "td[class*='day']", "table td"]:
                tds = driver.find_elements(By.CSS_SELECTOR, td_sel)
                for td in tds:
                    if td.text.strip() == day_str and td.is_displayed():
                        driver.execute_script("arguments[0].click();", td)
                        time.sleep(0.5)
                        logger.info(f"날짜 선택 완료 (텍스트 셀): {sale_date}")
                        return

            # 달력에서 찾는 셀 전체 로깅
            all_tds = [(td.text.strip(), td.get_attribute("data-timestamp"), td.is_displayed())
                       for td in driver.find_elements(By.CSS_SELECTOR, "td") if td.text.strip().isdigit()]
            logger.warning(f"달력 td 목록: {all_tds[:30]}")

        # 달력 없거나 실패 → 직접 키보드 입력
        input_el.click()
        input_el.send_keys(Keys.CONTROL + "a")
        input_el.send_keys(Keys.DELETE)
        for fmt in [date_slash, date_short]:
            input_el.clear()
            input_el.send_keys(fmt)
            input_el.send_keys(Keys.TAB)
            time.sleep(0.3)
            new_val = input_el.get_attribute("value") or ""
            if new_val and new_val != el_value:
                logger.info(f"날짜 설정 완료 (keyboard): {fmt} → value={new_val}")
                return
    except Exception as e:
        logger.warning(f"날짜 선택 전략 2 실패: {e}")

    raise TimeoutException(f"날짜 셀을 찾을 수 없습니다: {sale_date}")


def _dismiss_alert(driver: uc.Chrome) -> str | None:
    """열려있는 alert를 로깅 후 dismiss. 없으면 None 반환."""
    from selenium.common.exceptions import NoAlertPresentException, UnexpectedAlertPresentException
    try:
        alert = driver.switch_to.alert
        text = alert.text
        logger.warning(f"Alert 감지 → dismiss: {text!r}")
        alert.accept()
        time.sleep(0.3)
        return text
    except NoAlertPresentException:
        return None
    except UnexpectedAlertPresentException:
        try:
            alert = driver.switch_to.alert
            text = alert.text
            logger.warning(f"UnexpectedAlert → dismiss: {text!r}")
            alert.accept()
            time.sleep(0.3)
            return text
        except Exception:
            return None


def _select_store(driver: uc.Chrome, wait: WebDriverWait, shopCd: str) -> bool:
    """IBSheet 팝업에서 매장 선택. 성공 시 True."""
    from selenium.webdriver.common.action_chains import ActionChains
    _dismiss_alert(driver)

    # ── 팝업 열기 ─────────────────────────────────────────────────────────
    shop_btn = None
    for sel in [
        (By.ID, "shopNms"),
        (By.NAME, "shopNms"),
        (By.CSS_SELECTOR, "input[id*='shop']"),
        (By.CSS_SELECTOR, "button[id*='shop']"),
        (By.CSS_SELECTOR, "a[id*='shop']"),
        # receipt/details 페이지: 별도 "매장선택" 버튼
        (By.XPATH, "//button[normalize-space(text())='매장선택']"),
        (By.CSS_SELECTOR, "button[onclick*='Popup']"),
    ]:
        try:
            shop_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(sel))
            logger.info(f"매장 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if shop_btn is None:
        logger.error(f"매장 선택 버튼을 찾을 수 없음: shopCd={shopCd}")
        return False

    ActionChains(driver).move_to_element(shop_btn).click().perform()
    time.sleep(2)
    _dismiss_alert(driver)

    # ── 팝업 내 행 찾기 ───────────────────────────────────────────────────
    # 우선순위: shopCd를 포함한 TD 자체 → 그 TD가 속한 TR의 마지막 TD(매장명)
    target_el = None
    target_desc = ""

    # 1순위: shopCd TD 안에 <a> 링크가 있으면 그것 클릭
    for xpath in [
        f"//td[normalize-space(text())='{shopCd}']/a",
        f"//td[normalize-space(text())='{shopCd}']",
        f"//td[contains(text(),'{shopCd}')]/a",
        f"//td[contains(text(),'{shopCd}')]",
        # TR의 마지막 TD (매장명 컬럼) 클릭
        f"//tr[td[normalize-space(text())='{shopCd}']]/td[last()]",
        f"//tr[td[contains(text(),'{shopCd}')]]/td[last()]",
    ]:
        try:
            el = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, xpath)))
            if el.is_displayed():
                target_el = el
                target_desc = xpath
                logger.info(f"클릭 대상 발견: {xpath}")
                break
        except TimeoutException:
            continue

    if target_el is None:
        all_tds = [td.text.strip() for td in driver.find_elements(By.CSS_SELECTOR, "td") if td.text.strip()]
        logger.error(f"shopCd={shopCd} 셀 없음 | td 목록: {all_tds[:30]}")
        return False

    # ── 팝업 닫힘 감지용: "매장 검색" 텍스트를 포함한 컨테이너 ─────────────
    popup_text_el = None
    for xpath in [
        "//*[normalize-space(text())='매장 검색']",
        "//*[contains(text(),'매장 검색')]",
        "//*[contains(text(),'매장검색')]",
    ]:
        found = driver.find_elements(By.XPATH, xpath)
        visible = [e for e in found if e.is_displayed()]
        if visible:
            popup_text_el = visible[0]
            break

    # ── 클릭 전략: ActionChains → 더블클릭 → JS 순으로 시도 ─────────────
    def _try_click(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.3)
        try:
            ActionChains(driver).move_to_element(el).click().perform()
            return "ActionChains.click"
        except Exception:
            pass
        try:
            ActionChains(driver).move_to_element(el).double_click().perform()
            return "ActionChains.double_click"
        except Exception:
            pass
        driver.execute_script("arguments[0].click();", el)
        return "JS.click"

    method = _try_click(target_el)
    logger.info(f"클릭 실행: {method} | 대상={target_desc}")
    time.sleep(1)
    _dismiss_alert(driver)

    # ── 팝업 닫힘 확인 ───────────────────────────────────────────────────
    closed = False
    if popup_text_el is not None:
        try:
            WebDriverWait(driver, 6).until(EC.staleness_of(popup_text_el))
            closed = True
            logger.info(f"팝업 닫힘 확인 (staleness): shopCd={shopCd}")
        except TimeoutException:
            pass

    if not closed:
        # 매장선택 input에 값이 채워졌는지 확인
        try:
            for sel in [(By.ID, "shopNms"), (By.NAME, "shopNms")]:
                els = driver.find_elements(*sel)
                for e in els:
                    val = e.get_attribute("value") or ""
                    if val.strip():
                        closed = True
                        logger.info(f"매장 선택 input 확인: value={val!r}")
                        break
        except Exception:
            pass

    if not closed:
        logger.warning(f"팝업 닫힘 미확인 — 현재 URL: {driver.current_url}")

    logger.info(f"매장 선택: shopCd={shopCd} | closed={closed} | method={method}")
    return True  # closed 여부와 무관하게 True (조회 후 alert로 확인)


def _wait_for_download(directory: Path, existing_files: set, timeout: int = DOWNLOAD_TIMEOUT) -> Path | None:
    """새로 다운로드된 파일 대기 (기존 파일 제외)"""
    end_time = time.time() + timeout
    while time.time() < end_time:
        current = set(directory.glob("*.xlsx")) | set(directory.glob("*.xls"))
        new_files = current - existing_files
        crdownload = list(directory.glob("*.crdownload"))
        if new_files and not crdownload:
            latest = sorted(new_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
            return latest
        time.sleep(1)
    return None


def _click_search_btn(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """조회 버튼 클릭 (여러 셀렉터 순서대로 시도)"""
    _SEARCH_SELECTORS = [
        (By.ID, "searchBtn"),
        (By.CSS_SELECTOR, "button.btn-search"),
        (By.CSS_SELECTOR, "a.btn-search"),
        (By.XPATH, "//button[contains(@class,'btn') and (contains(text(),'조회') or contains(text(),'검색'))]"),
        (By.XPATH, "//a[contains(@class,'btn') and (contains(text(),'조회') or contains(text(),'검색'))]"),
        (By.XPATH, "//input[@type='button' and (contains(@value,'조회') or contains(@value,'검색'))]"),
        (By.XPATH, "//button[contains(@onclick,'search') or contains(@onclick,'Search')]"),
    ]
    for sel in _SEARCH_SELECTORS:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(sel))
            logger.info(f"조회 버튼 발견: {sel}")
            driver.execute_script("arguments[0].click();", btn)
            return
        except (TimeoutException, Exception):
            continue

    # 찾지 못한 경우 현재 버튼 목록 로깅
    btns = [(b.tag_name, b.get_attribute("id"), b.get_attribute("class"), b.text.strip())
            for b in driver.find_elements(By.XPATH, "//button|//a[contains(@class,'btn')]|//input[@type='button']")]
    logger.error(f"조회 버튼을 찾을 수 없음 | 버튼 목록: {btns[:20]}")
    raise TimeoutException("조회 버튼을 찾을 수 없습니다.")


def _download_excel_for_store(
    driver: uc.Chrome,
    wait: WebDriverWait,
    page_type_key: str,
    page_cfg: dict,
    sale_date: str,
    store: dict,
    download_dir: Path,
) -> Path | None:
    """단일 매장 엑셀 다운로드 시퀀스 (매장마다 페이지 재로드)"""
    store_name = store["name"]
    shop_cd    = store["shopCd"]
    store_slug = store_name.replace(" ", "_")
    tag        = f"[{page_type_key}|{store_name}|{sale_date}]"

    try:
        # ── 0. 매장마다 페이지 재로드 (IBSheet 중복 방지) ──────────────────
        logger.info(f"{tag} 페이지 재로드: {page_cfg['url']}")
        driver.get(page_cfg["url"])
        time.sleep(2)
        _dismiss_alert(driver)

        # ── 1. 날짜 선택 ────────────────────────────────────────────────────
        logger.info(f"{tag} 날짜 선택 시작")
        _select_date_tui(driver, wait, sale_date, page_cfg["date_input_id"])
        _dismiss_alert(driver)
        logger.info(f"{tag} 날짜 선택 완료")

        # ── 2. 매장 선택 ────────────────────────────────────────────────────
        logger.info(f"{tag} 매장 선택 시작: shopCd={shop_cd}")
        ok = _select_store(driver, wait, shop_cd)
        if not ok:
            logger.error(f"{tag} 매장 선택 실패 → skip")
            return None
        logger.info(f"{tag} 매장 선택 완료")

        # ── 3. 조회 버튼 클릭 ───────────────────────────────────────────────
        logger.info(f"{tag} 조회 버튼 클릭")
        _dismiss_alert(driver)
        _click_search_btn(driver, wait)
        time.sleep(2)
        alert = _dismiss_alert(driver)
        if alert:
            logger.error(f"{tag} 조회 후 alert 발생: {alert!r}")
            # "매장을 선택" alert면 팝업이 열려있으므로 닫기 시도
            if "매장" in alert:
                logger.info(f"{tag} 팝업 닫기 후 매장 선택 재시도")
                for close_xpath in [
                    "//button[@class='modal-close' or contains(@class,'close')]",
                    "//*[contains(@class,'modal')]//button[contains(@class,'close')]",
                    "//div[contains(text(),'매장 검색')]/following-sibling::button",
                    "//*[@aria-label='close' or @title='close' or @title='닫기']",
                ]:
                    try:
                        btn = driver.find_element(By.XPATH, close_xpath)
                        if btn.is_displayed():
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(0.5)
                            break
                    except Exception:
                        continue
                from selenium.webdriver.common.keys import Keys as _Keys
                driver.find_element(By.TAG_NAME, "body").send_keys(_Keys.ESCAPE)
                time.sleep(0.5)
            return None
        logger.info(f"{tag} 조회 완료")

        # ── 4. 엑셀 다운로드 ────────────────────────────────────────────────
        existing = set(download_dir.glob("*.xlsx")) | set(download_dir.glob("*.xls"))
        logger.info(f"{tag} 엑셀 다운로드 JS 실행: {page_cfg['excel_js']}")
        _dismiss_alert(driver)
        driver.execute_script(page_cfg["excel_js"])
        time.sleep(1)
        _dismiss_alert(driver)

        # ── 5. 다운로드 완료 대기 ───────────────────────────────────────────
        downloaded = _wait_for_download(download_dir, existing)
        if downloaded is None:
            logger.warning(f"{tag} 다운로드 타임아웃")
            return None

        # 즉시 rename — 다음 매장 다운로드 시 동일 파일명 충돌 방지
        # (Chrome은 같은 파일명으로 덮어쓰므로 existing_files에서 새 파일로 감지 불가)
        renamed = download_dir / f"{sale_date}__{page_type_key}__{store_slug}.xlsx"
        shutil.move(str(downloaded), str(renamed))
        logger.info(f"{tag} 다운로드 완료: {renamed.name}")
        return renamed

    except Exception:
        logger.exception(f"{tag} 다운로드 실패 (상세 traceback)")
        _dismiss_alert(driver)
        return None


# ============================================================
# Task 함수
# ============================================================

def resolve_sale_dates(**context) -> str:
    """실행 날짜 범위 결정 (conf 또는 yesterday)"""
    conf = context.get("dag_run").conf or {}
    date_from = conf.get("sale_date_from")
    date_to   = conf.get("sale_date_to")

    if date_from or date_to:
        if not date_from or not date_to:
            raise ValueError("sale_date_from 과 sale_date_to 는 함께 지정해야 합니다.")
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            dt_to   = datetime.strptime(date_to,   "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"날짜 형식 오류 (YYYY-MM-DD 필요): {e}")
        if dt_from > dt_to:
            raise ValueError(f"sale_date_from({date_from}) > sale_date_to({date_to})")
        sale_dates = []
        current = dt_from
        while current <= dt_to:
            sale_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        sale_dates = [yesterday]

    context["ti"].xcom_push(key="sale_dates", value=sale_dates)
    logger.info(f"실행 날짜 범위: {sale_dates}")
    return f"날짜 범위 결정 완료: {sale_dates[0]} ~ {sale_dates[-1]} ({len(sale_dates)}일)"


def download_today_stores(**context) -> str:
    """today 페이지 매장별 엑셀 다운로드"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    download_dir = TEMP_DIR / "okpos_download"
    download_dir.mkdir(parents=True, exist_ok=True)
    page_cfg = PAGE_TYPES["today"]
    results: dict = {}

    driver = _launch_browser(download_dir)
    wait   = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        _login(driver, wait)
        driver.get(page_cfg["url"])
        time.sleep(2)

        for sale_date in sale_dates:
            for store in STORES:
                key = f"{sale_date}__{store['name']}"
                downloaded = _download_excel_for_store(
                    driver, wait, "today", page_cfg, sale_date, store, download_dir
                )
                if downloaded:
                    results[key] = str(downloaded)
                else:
                    results[key] = ""
    finally:
        driver.quit()
        logger.info("WebDriver 종료 (today)")

    success = sum(1 for v in results.values() if v)
    context["ti"].xcom_push(key="today_files", value=results)
    logger.info(f"today 다운로드 완료: {success}/{len(results)}건")
    return f"today 다운로드: {success}/{len(results)}건 성공"


def download_receipt_stores(**context) -> str:
    """receipt/details 페이지 매장별 엑셀 다운로드 (영수증별매출상세현황)"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    download_dir = TEMP_DIR / "okpos_download"
    download_dir.mkdir(parents=True, exist_ok=True)
    page_cfg = PAGE_TYPES["receipt"]
    results: dict = {}

    driver = _launch_browser(download_dir)
    wait   = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        _login(driver, wait)
        driver.get(page_cfg["url"])
        time.sleep(2)

        for sale_date in sale_dates:
            for store in STORES:
                key = f"{sale_date}__{store['name']}"
                downloaded = _download_excel_for_store(
                    driver, wait, "receipt", page_cfg, sale_date, store, download_dir
                )
                if downloaded:
                    results[key] = str(downloaded)
                else:
                    results[key] = ""
    finally:
        driver.quit()
        logger.info("WebDriver 종료 (receipt)")

    success = sum(1 for v in results.values() if v)
    context["ti"].xcom_push(key="receipt_files", value=results)
    logger.info(f"receipt 다운로드 완료: {success}/{len(results)}건")
    return f"receipt 다운로드: {success}/{len(results)}건 성공"


def _read_okpos_excel(path: str) -> pd.DataFrame:
    """OKPOS xlsx 읽기: 상단 메타 행을 건너뛰고 실제 컬럼 헤더 행 자동 감지.

    - 상위 12행을 스캔하여 업무 컬럼명 매칭 점수가 가장 높은 행을 header로 선택
    - #NAME? 수식 오류 셀 → 빈 문자열로 대체
    """
    preview = pd.read_excel(path, header=None, nrows=12, dtype=str)

    header_candidates = {
        "NO", "포스번호", "영수증번호", "구분", "테이블명", "최초주문", "결제시간",
        "상품코드", "바코드", "상품명", "수량", "총매출액", "할인액", "할인구분",
        "실매출액", "가액", "부가세",
    }

    header_row = 0
    best_score = -1
    for i in range(len(preview)):
        row_vals = [str(v).strip() for v in preview.iloc[i].tolist() if pd.notna(v)]
        if not row_vals:
            continue
        score = sum(1 for v in row_vals if v in header_candidates)
        if score > best_score:
            best_score = score
            header_row = i

    # 점수가 너무 낮으면 기존 규칙 fallback
    if best_score < 2:
        for i in range(min(4, len(preview))):
            val = str(preview.iloc[i, 0]).strip().upper()
            if val == "NO":
                header_row = i

    df = pd.read_excel(path, header=header_row, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.replace("#NAME?", "", regex=False)
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    logger.info(f"xlsx 읽기: {path} | header_row={header_row} | shape={df.shape} | columns={list(df.columns[:6])}")
    return df


def _transform_okpos_df(df: pd.DataFrame, store_name: str, sale_date: str):
    """xlsx DataFrame 정제 + 파생 컬럼 추가. (cleaned_df, ym) 반환."""
    # 엑셀 메타 행/빈 열로 생긴 Unnamed 컬럼 제거
    df = df[[c for c in df.columns if not str(c).startswith("Unnamed")]].copy()

    # 합계 행 제거: 첫 번째 컬럼(NO)이 숫자가 아닌 행 제거
    no_col = df.columns[0]
    df = df[df[no_col].astype(str).str.strip().ne(str(no_col).strip())].copy()
    df = df[pd.to_numeric(df[no_col], errors="coerce").notna()].copy()
    df.reset_index(drop=True, inplace=True)

    ym = sale_date[:7]  # YYYY-MM
    n_orig = len(df.columns)

    df["매장명"]       = store_name
    df["sale_date"]    = sale_date
    df["ym"]           = ym
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # PK: 원본 컬럼 값 결정론적 hash (파생 컬럼 제외) — hashlib.md5 사용
    # Python 내장 hash()는 PYTHONHASHSEED 랜덤화로 프로세스마다 값이 달라져 dedup 불가
    df["_pk"] = df.iloc[:, :n_orig].apply(
        lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
        axis=1,
    )
    return df, ym


def save_to_raw(**context) -> str:
    """xlsx 다운로드 파일 → 합계 제거 + 매장명 추가 → 파티션 CSV 저장"""
    from modules.load.load_onedrive import onedrive_csv_save

    today_files   = context["ti"].xcom_pull(task_ids="download_today",   key="today_files")   or {}
    receipt_files = context["ti"].xcom_pull(task_ids="download_receipt", key="receipt_files") or {}

    saved: list = []
    # page_type → (xcom_files, csv_filename)
    page_map = {
        "today":   (today_files,   "okpos_order"),        # 당일매출 → okpos_order.csv
        "receipt": (receipt_files, "okpos_order_item"),   # 영수증별매출 → okpos_order_item.csv
    }

    for page_type, (files, csv_name) in page_map.items():
        for key, src_str in files.items():
            if not src_str:
                logger.warning(f"파일 없음 (다운로드 실패): {page_type} | {key}")
                continue
            src = Path(src_str)
            if not src.exists():
                logger.warning(f"파일 경로 없음: {src}")
                continue

            sale_date, store_name = key.split("__", 1)
            # "도리당 동두천지행점" → "동두천지행점"
            store_short = store_name.replace("도리당 ", "", 1)

            try:
                df = _read_okpos_excel(str(src))

                df, ym = _transform_okpos_df(df, store_name, sale_date)
                if df.empty:
                    logger.warning(f"변환 후 빈 DataFrame: {page_type} | {key}")
                    src.unlink(missing_ok=True)
                    continue

                # 파티션 경로: brand=도리당/store=동두천지행점/ym=2026-04/{okpos_order|okpos_order_item}.csv
                dest = (
                    RAW_OKPOS_SALES
                    / "brand=도리당"
                    / f"store={store_short}"
                    / f"ym={ym}"
                    / f"{csv_name}.csv"
                )
                dest.parent.mkdir(parents=True, exist_ok=True)

                # 기존 CSV의 _pk가 hash()로 생성된 경우 → MD5로 재계산 후 중복 제거
                # (hash()는 PYTHONHASHSEED 랜덤화로 프로세스마다 값이 달라 dedup 무력화)
                if dest.exists():
                    try:
                        existing_df = pd.read_csv(dest)
                        if "_pk" in existing_df.columns:
                            n_orig_ex = len(existing_df.columns) - 5  # 파생 컬럼 5개 제외
                            existing_df["_pk"] = existing_df.iloc[:, :n_orig_ex].apply(
                                lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
                                axis=1,
                            )
                            before = len(existing_df)
                            existing_df.drop_duplicates(subset=["_pk"], keep="last", inplace=True)
                            existing_df.to_csv(dest, index=False, encoding="utf-8-sig")
                            logger.info(
                                f"기존 CSV _pk 재계산: {dest.name} | "
                                f"중복제거={before - len(existing_df)}건"
                            )
                    except Exception:
                        logger.warning(f"기존 CSV _pk 재계산 실패 (무시): {dest}")

                result = onedrive_csv_save(
                    df=df,
                    file_path=str(dest),
                    pk_col="_pk",
                    timestamp_col="collected_at",
                    if_exists="append",
                )
                src.unlink(missing_ok=True)
                saved.append(str(dest))
                logger.info(
                    f"저장 완료: {dest.relative_to(RAW_OKPOS_SALES)} | "
                    f"신규={result.get('inserted',0)} 중복={result.get('duplicated',0)}"
                )
            except Exception:
                logger.exception(f"변환/저장 실패: {page_type} | {key}")

    context["ti"].xcom_push(key="saved_files", value=saved)
    logger.info(f"save_to_raw 완료: {len(saved)}개 저장")
    return f"변환 저장 완료: {len(saved)}개"


def write_log(**context) -> str:
    """log.parquet 실행 이력 기록"""
    saved_files = context["ti"].xcom_pull(task_ids="save_to_raw", key="saved_files") or []
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    log_path = RAW_OKPOS_SALES / "log.parquet"

    try:
        RAW_OKPOS_SALES.mkdir(parents=True, exist_ok=True)

        now_ts = pd.Timestamp.now(tz="Asia/Seoul")
        rows = []
        for file_str in saved_files:
            try:
                p = Path(file_str)
                parts = p.parts
                # 경로: brand=도리당 / store=동두천지행점 / ym=2026-04 / {order|order_item}.csv
                store_short = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("store=")), "unknown")
                ym = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("ym=")), "unknown")
                csv_name = p.stem  # okpos_order or okpos_order_item
                page_type = "today" if csv_name in ("order", "okpos_order") else "receipt" if csv_name in ("order_item", "okpos_order_item") else csv_name
                sale_date = sale_dates[0] if sale_dates else "unknown"

                rows.append({
                    "run_at": now_ts,
                    "sale_date": sale_date,
                    "page_type": page_type,
                    "store": store_short,
                    "ym": ym,
                    "result": "success",
                    "file_path": str(p),
                })
            except Exception as e:
                logger.warning(f"로그 행 파싱 실패: {file_str} | {e}")

        new_df = pd.DataFrame(
            rows,
            columns=["run_at", "sale_date", "page_type", "store", "ym", "result", "file_path"],
        )

        if log_path.exists() and log_path.stat().st_size > 0:
            prev_df = pd.read_parquet(log_path)
            out_df = pd.concat([prev_df, new_df], ignore_index=True)
        else:
            out_df = new_df

        if not out_df.empty and "run_at" in out_df.columns:
            out_df["run_at"] = pd.to_datetime(out_df["run_at"], errors="coerce")
            out_df = out_df.sort_values("run_at", ascending=False).reset_index(drop=True)

        out_df.to_parquet(log_path, index=False)

        logger.info(f"log.parquet 기록 완료: {log_path} | {len(new_df)}건")
        return f"log.parquet 기록 완료: {len(new_df)}건"

    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
