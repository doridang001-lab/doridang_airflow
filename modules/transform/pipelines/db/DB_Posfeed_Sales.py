"""
Posfeed 관리자 사이트 엑셀 다운로드 파이프라인

처리 흐름:
1. undetected_chromedriver로 Posfeed 로그인
2. 주문 목록 검색 후 엑셀 다운로드
3. 다운로드 파일을 DOWN_DIR/업로드_temp/ 로 이동
"""

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
from selenium.common.exceptions import TimeoutException, WebDriverException

from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import DOWN_DIR, ANALYTICS_DB
from modules.transform.utility.selenium_uc import configure_uc_data_path
from modules.transform.utility.store_normalize import normalize as _normalize_store_series
from modules.load.load_onedrive import onedrive_csv_save

logger = logging.getLogger(__name__)

# ============================================================
# 상수 - 계정 / URL
# ============================================================
POSFEED_ID  = "siw2222@naver.com"
POSFEED_PW  = "ehfl8877!!"
LOGIN_URL   = "https://admin.posfeed.co.kr/#/login?redirect=%2Fdashboard"
ORDER_URL   = "https://admin.posfeed.co.kr/#/order/list"

# ============================================================
# 상수 - 브라우저 설정
# ============================================================
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None


# ============================================================
# 내부 유틸 - 브라우저 안정화
# ============================================================
def _kill_chrome_processes() -> None:
    """좀비 Chrome/ChromeDriver 프로세스를 정리해 다음 브라우저 실행을 안정화"""
    import subprocess

    # 컨테이너(리눅스) 기준. pkill이 없어도 무시한다.
    targets = ["google-chrome", "chrome", "chromium", "chromium-browser", "undetected_chromedriver", "chromedriver"]
    for name in targets:
        try:
            subprocess.run(["pkill", "-9", "-f", name], capture_output=True, timeout=5)
        except Exception:
            pass
    time.sleep(1)


# ============================================================
# Reset (옵션)
# ============================================================
def reset_posfeed_partitions(**context) -> str:
    """
    posfeed analytics 파티션을 삭제해 '처음부터' 재수집할 수 있게 한다.

    트리거(둘 다 필요):
      - dag_run.conf.reset = true
      - dag_run.conf.reset_confirm = "DELETE"

    옵션:
      - dag_run.conf.reset_scope: "all"(default) | "sales" | "detail"
    """
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    if not conf.get("reset"):
        logger.info("reset=false (no-op)")
        return "reset=false (no-op)"

    if str(conf.get("reset_confirm", "")).upper() != "DELETE":
        raise ValueError('Reset requested but reset_confirm != "DELETE"')

    scope = str(conf.get("reset_scope", "all")).lower()
    targets: list[Path] = []
    if scope in ("all", "sales"):
        targets.append(ANALYTICS_DB / "posfeed_orders.csv")  # legacy (단일 파일로 저장하던 시기)
        targets.append(ANALYTICS_DB / "posfeed_sales")  # legacy
    if scope in ("all", "detail"):
        targets.append(ANALYTICS_DB / "posfeed_order_item.csv")  # legacy (단일 파일로 저장하던 시기)
        targets.append(ANALYTICS_DB / "posfeed_sales_detail")  # legacy

    deleted = 0
    for target in targets:
        if not target.exists():
            logger.info("reset: not found (skip): %s", target)
            continue
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=False)
        else:
            target.unlink(missing_ok=False)
        logger.warning("reset: deleted: %s", target)
        deleted += 1

    return f"reset 완료 | scope={scope} | deleted={deleted}"


# ============================================================
# 내부 유틸
# ============================================================
def _get_chrome_version() -> int | None:
    """설치된 Chrome 메이저 버전 반환"""
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
    _kill_chrome_processes()
    logger.info(f"브라우저 실행 (headless={HEADLESS_MODE})")
    configure_uc_data_path()

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

        # 다운로드 경로 설정
        options.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        })
        return options

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        if chrome_version:
            kwargs["version_main"] = chrome_version
            logger.info(f"ChromeDriver 버전: {chrome_version}")
        driver = uc.Chrome(**kwargs)
        driver.set_window_size(1920, 1080)
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning(f"버전 불일치 → {detected} 으로 재시도")
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            driver.set_window_size(1920, 1080)
            logger.info("브라우저 실행 성공 (재시도)")
            return driver
        # DNS/네트워크 오류: 기존에 패치된 바이너리를 직접 지정하여 오프라인 재시도
        # (is_binary_patched()가 True이면 fetch_release_number() 네트워크 호출 생략)
        if "No address associated with hostname" in str(e) or "URLError" in type(e).__name__:
            _known_paths = [
                "/tmp/undetected_chromedriver/undetected_chromedriver",
                "/root/.local/share/undetected_chromedriver/undetected_chromedriver",
            ]
            for _p in _known_paths:
                if Path(_p).exists():
                    logger.warning("DNS 오류 → 캐시 드라이버 재사용: %s", _p)
                    driver = uc.Chrome(
                        options=_make_options(),
                        version_main=chrome_version,
                        driver_executable_path=_p,
                    )
                    driver.set_window_size(1920, 1080)
                    logger.info("브라우저 실행 성공 (오프라인 재시도)")
                    return driver
        raise


def _restart_driver_with_download(download_dir: Path) -> tuple[uc.Chrome, WebDriverWait]:
    """브라우저를 재생성하고 CDP 다운로드 경로까지 설정한 새 세션 반환"""
    driver = _launch_browser(download_dir)
    wait = WebDriverWait(driver, 20)
    try:
        driver.execute_cdp_cmd(
            "Browser.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir), "eventsEnabled": True},
        )
    except Exception as err:
        logger.warning("CDP 다운로드 경로 설정 실패(무시): %s", err)
    return driver, wait


def _login(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """Posfeed 로그인"""
    driver.get(LOGIN_URL)
    time.sleep(2)

    id_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
    id_input.clear()
    id_input.send_keys(POSFEED_ID)

    pw_input = driver.find_element(By.NAME, "password")
    pw_input.clear()
    pw_input.send_keys(POSFEED_PW)
    pw_input.send_keys(Keys.RETURN)  # Vue SPA: submit() 대신 Enter

    # 로그인 성공 = URL 해시가 /login 에서 벗어날 때까지 대기
    wait.until(lambda d: "#/login" not in d.current_url)
    time.sleep(2)
    logger.info(f"Posfeed 로그인 완료 | URL: {driver.current_url}")


def _read_panel_ym(panel) -> tuple[int, int] | tuple[None, None]:
    """달력 패널 헤더에서 (year, month) 읽기. 파싱 실패 시 (None, None)."""
    labels = panel.find_elements(By.CSS_SELECTOR, ".el-date-picker__header-label")
    header_text = " ".join(lbl.text for lbl in labels)
    year_m = re.search(r"(\d{4})", header_text)
    month_m = re.search(r"(\d{1,2})\s*[月월]", header_text) or re.search(r"[月월]\s*(\d{1,2})", header_text)
    if not year_m or not month_m:
        return None, None
    return int(year_m.group(1)), int(month_m.group(1))


def _navigate_calendar_to_month(driver: uc.Chrome, panel, target_year: int, target_month: int) -> bool:
    """El-UI 달력 패널을 target_year/month로 이동 (최대 24개월). 성공 여부 반환.

    EL-UI 달력에는 지난해/다음해(d-arrow) 와 지난달/다음달(arrow) 버튼이 별개로 존재.
    단일 화살표 클래스(el-icon-arrow-left/right)로 월 이동 버튼만 정확히 선택한다.
    """
    # 단일 화살표 = 월 이동, 이중 화살표(d-arrow) = 연도 이동
    _PREV_CSS = "button.el-icon-arrow-left"
    _NEXT_CSS = "button.el-icon-arrow-right"

    def _find_nav_btn(go_prev: bool):
        css = _PREV_CSS if go_prev else _NEXT_CSS
        btn = driver.execute_script("return arguments[0].querySelector(arguments[1]);", panel, css)
        return btn

    for _ in range(24):
        cur_year, cur_month = _read_panel_ym(panel)
        if cur_year is None:
            logger.warning("_read_panel_ym 실패 — 패널 헤더 파싱 불가")
            return False
        if cur_year == target_year and cur_month == target_month:
            return True
        go_prev = (cur_year, cur_month) > (target_year, target_month)
        btn = _find_nav_btn(go_prev)
        if btn is None:
            logger.warning("달력 %s 버튼 없음 (현재 %d-%02d)", "지난달" if go_prev else "다음달", cur_year, cur_month)
            return False
        prev_ym = (cur_year, cur_month)
        driver.execute_script("arguments[0].click();", btn)
        for _ in range(20):
            time.sleep(0.1)
            new_year, new_month = _read_panel_ym(panel)
            if (new_year, new_month) != prev_ym:
                break
    return False


def _select_date(driver: uc.Chrome, editor_el, target_date: datetime) -> None:
    """날짜 피커 에디터 클릭 → 달력 팝업 → 월 이동 → 날짜 셀 클릭."""
    from selenium.webdriver.common.action_chains import ActionChains
    try:
        icon = editor_el.find_element(By.CSS_SELECTOR, ".el-icon-date")
        ActionChains(driver).move_to_element(icon).click().perform()
    except Exception:
        ActionChains(driver).move_to_element(editor_el).click().perform()
    time.sleep(0.5)

    try:
        WebDriverWait(driver, 8).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".el-picker-panel"))
        )
    except TimeoutException:
        time.sleep(2)

    panels = [p for p in driver.find_elements(By.CSS_SELECTOR, ".el-picker-panel") if p.is_displayed()]
    if not panels:
        raise TimeoutException("보이는 달력 패널이 없습니다.")
    panel = panels[-1]

    nav_ok = _navigate_calendar_to_month(driver, panel, target_date.year, target_date.month)
    if not nav_ok:
        cur_y, cur_m = _read_panel_ym(panel)
        raise TimeoutException(
            f"달력 이동 실패: 목표 {target_date.year}-{target_date.month:02d} | 현재 패널 {cur_y}-{cur_m}"
        )

    target_day = str(target_date.day)
    _CELL_CSS = "td.available:not(.prev-month):not(.next-month)"
    day_cell = None
    for _attempt in range(10):
        for td in panel.find_elements(By.CSS_SELECTOR, _CELL_CSS):
            try:
                span = td.find_element(By.TAG_NAME, "span")
            except Exception:
                continue
            if span.text.strip() == target_day:
                day_cell = td
                break
        if day_cell is not None:
            break
        time.sleep(0.5)

    if day_cell is None:
        found_days = []
        for td in panel.find_elements(By.CSS_SELECTOR, _CELL_CSS):
            try:
                found_days.append(td.find_element(By.TAG_NAME, "span").text.strip())
            except Exception:
                found_days.append("(no span)")
        logger.error("달력 available 날짜 목록: %s", found_days)
        raise TimeoutException(f"달력에서 {target_date.strftime('%Y-%m-%d')} 셀을 찾을 수 없습니다.")

    ActionChains(driver).move_to_element(day_cell).click().perform()
    time.sleep(0.5)
    logger.info("날짜 선택: %s", target_date.strftime("%Y-%m-%d"))


def _click_download(driver: uc.Chrome, wait: WebDriverWait, target_date: datetime = None) -> None:
    """주문 목록 검색 → 엑셀 다운로드 → 확인

    target_date: 조회 날짜 (None이면 어제)
    """
    from datetime import timedelta as _td
    if target_date is None:
        target_date = datetime.now() - _td(days=1)

    def _ensure_order_page() -> None:
        """주문 목록 페이지 진입 보장 - 로그인 리다이렉트 시 재로그인"""
        driver.get(ORDER_URL)
        time.sleep(3)

        # 로그인 페이지로 리다이렉트됐으면 재로그인
        if "#/login" in driver.current_url:
            logger.warning(f"세션 만료 감지 - 재로그인 시도 | URL: {driver.current_url}")
            _login(driver, wait)
            driver.get(ORDER_URL)
            time.sleep(3)

        wait.until(lambda d: "#/login" not in d.current_url)

        # 강제 비밀번호 변경 페이지 감지 → "다음에 (30일간 보지않기)" 버튼 클릭 후 주문 페이지 재진입
        if "#/admin/change-password" in driver.current_url:
            logger.warning(f"비밀번호 변경 페이지 리다이렉트 감지 - 스킵 버튼 클릭 시도 | URL: {driver.current_url}")
            _SKIP_SELECTORS = [
                (By.XPATH, "//button[.//span[contains(normalize-space(),'다음에')]]"),
                (By.XPATH, "//span[contains(normalize-space(),'다음에')]/parent::button"),
                (By.XPATH, "//button[contains(normalize-space(),'다음에')]"),
            ]
            skip_clicked = False
            for sel in _SKIP_SELECTORS:
                try:
                    skip_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(sel))
                    driver.execute_script("arguments[0].click();", skip_btn)
                    logger.info("'다음에 (30일간 보지않기)' 버튼 클릭 완료")
                    skip_clicked = True
                    time.sleep(2)
                    break
                except Exception:
                    continue
            if not skip_clicked:
                logger.warning("스킵 버튼을 찾지 못했습니다 - ORDER_URL 직접 진입 시도")
            driver.get(ORDER_URL)
            time.sleep(4)

        time.sleep(2)
        logger.info(f"주문 목록 페이지 로드 완료 | URL: {driver.current_url}")

    _ensure_order_page()

    # ── 날짜 피커: 시작일 / 종료일 모두 target_date로 설정 ───────────────────
    logger.info("날짜 피커 설정: %s", target_date.strftime("%Y-%m-%d"))

    try:
        date_editors = WebDriverWait(driver, 8).until(
            lambda d: [e for e in d.find_elements(By.CSS_SELECTOR, ".el-date-editor--date") if e.is_displayed()]
        )
    except TimeoutException:
        date_editors = []

    if len(date_editors) >= 2:
        _select_date(driver, date_editors[0], target_date)  # 시작일
        _select_date(driver, date_editors[1], target_date)  # 종료일
    elif len(date_editors) == 1:
        _select_date(driver, date_editors[0], target_date)
    else:
        logger.warning("날짜 입력 필드를 찾지 못했습니다 - 기본값(오늘) 사용")

    # 검색 버튼: el-icon-search 아이콘으로 특정 (el-button--success 단독보다 명확)
    search_btn = wait.until(EC.presence_of_element_located(
        (By.XPATH, "//button[contains(@class,'el-button--success')][.//i[contains(@class,'el-icon-search')]]")
    ))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", search_btn)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", search_btn)
    logger.info("검색 버튼 클릭")

    # 검색 클릭 후: 로그인 리다이렉트 OR 다운로드 버튼 출현 대기 (최대 20초)
    _DL_XPATH = "//button[contains(@class,'el-button--primary')][.//i[contains(@class,'el-icon-download')]]"

    def _search_result_or_redirect(d):
        return "#/login" in d.current_url or len(d.find_elements(By.XPATH, _DL_XPATH)) > 0

    try:
        WebDriverWait(driver, 20).until(_search_result_or_redirect)
    except TimeoutException:
        logger.warning("검색 결과 대기 타임아웃 - 현재 URL 확인 후 계속")

    logger.info(f"검색 후 URL: {driver.current_url}")

    # 세션 만료로 로그인 페이지 리다이렉트됐으면 재로그인 후 처음부터 재시도 (1회)
    if "#/login" in driver.current_url:
        logger.warning("검색 후 세션 만료 감지 - 재로그인 후 전체 재시도")
        _login(driver, wait)
        _ensure_order_page()
        search_btn2 = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[contains(@class,'el-button--success')][.//i[contains(@class,'el-icon-search')]]")
        ))
        driver.execute_script("arguments[0].click();", search_btn2)
        logger.info("검색 버튼 재클릭")
        try:
            WebDriverWait(driver, 20).until(_search_result_or_redirect)
        except TimeoutException:
            pass
        if "#/login" in driver.current_url:
            raise TimeoutException(f"재로그인 후에도 세션 유지 실패 | URL: {driver.current_url}")

    # 다운로드 버튼 탐색 (이미 대기 중 발견됐거나, 아직 로딩 중이면 추가 대기)
    download_btn = None
    _DL_SELECTORS = [
        (By.XPATH, _DL_XPATH),
        (By.XPATH, "//button[.//span[contains(normalize-space(),'엑셀 다운로드')]]"),
    ]
    for sel in _DL_SELECTORS:
        try:
            download_btn = WebDriverWait(driver, 10).until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if download_btn is None:
        btns = [b.text.strip() for b in driver.find_elements(By.TAG_NAME, "button") if b.text.strip()]
        logger.error(f"다운로드 버튼 못 찾음 | URL={driver.current_url} | 버튼목록={btns[:15]}")
        raise TimeoutException("엑셀 다운로드 버튼을 찾을 수 없습니다.")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", download_btn)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", download_btn)
    logger.info("엑셀 다운로드 버튼 클릭")
    time.sleep(3)

    # 확인 다이얼로그 - 4가지 셀렉터 순서대로 시도
    _CONFIRM_SELECTORS = [
        (By.CSS_SELECTOR, ".el-dialog__footer .el-button--primary"),
        (By.CSS_SELECTOR, ".el-dialog .el-button--primary"),
        (By.XPATH, "//div[contains(@class,'el-dialog')]//button[contains(@class,'el-button--primary')]"),
        (By.XPATH, "//span[normalize-space(text())='확인']/parent::button"),
        (By.XPATH, "//span[normalize-space(text())='확인']"),
    ]
    confirm_btn = None
    for selector in _CONFIRM_SELECTORS:
        try:
            confirm_btn = WebDriverWait(driver, 5).until(EC.presence_of_element_located(selector))
            logger.info(f"확인 버튼 발견: {selector}")
            break
        except Exception:
            continue
    if confirm_btn is None:
        raise TimeoutException("확인 다이얼로그 버튼을 찾을 수 없습니다. debug_dialog.png 확인 필요")
    driver.execute_script("arguments[0].click();", confirm_btn)
    logger.info("확인 클릭 완료 - 다운로드 시작")
    time.sleep(5)  # 다운로드 완료까지 고정 대기


def _is_manual_file(p: Path) -> bool:
    """수동 today 파일 여부: order-info-list_매장명.xlsx (날짜형 8자리 제외)"""
    if not p.stem.startswith("order-info-list_"):
        return False
    suffix = p.stem[len("order-info-list_"):]
    return not re.fullmatch(r"\d{8}", suffix)


def _find_downloaded_file(download_dir: Path, min_mtime: float | None = None) -> Path:
    """다운로드된 엑셀 파일을 후보 경로에서 찾아 반환.
    min_mtime: 이 시각(epoch) 이후에 생성/수정된 파일만 허용 — 기존 파일 오인식 방지.
    수동 today 파일(order-info-list_매장명.xlsx)은 제외.
    """
    candidate_dirs = [
        download_dir,
        Path("/root/Downloads"),
        Path("/home/airflow/Downloads"),
        Path.home() / "Downloads",
        Path("/tmp"),
    ]
    found = []
    for d in candidate_dirs:
        if not d.exists():
            continue
        files = [f for f in (list(d.glob("*.xlsx")) + list(d.glob("*.xls")))
                 if f.suffix not in (".crdownload", ".tmp") and not _is_manual_file(f)]
        if min_mtime is not None:
            files = [f for f in files if f.stat().st_mtime >= min_mtime]
        found.extend(files)
        if files:
            logger.info(f"파일 발견 경로: {d} | {[f.name for f in files]}")
    if not found:
        raise FileNotFoundError(f"다운로드된 엑셀 파일을 찾을 수 없습니다. 확인 경로: {[str(d) for d in candidate_dirs if d.exists()]}")
    return max(found, key=lambda f: f.stat().st_mtime)


def _wait_for_downloaded_file(
    download_dir: Path,
    min_mtime: float | None = None,
    timeout_sec: int = 60,
    poll_interval: float = 1.0,
) -> Path:
    """Wait until a downloaded Excel file is finalized."""
    candidate_dirs = [
        download_dir,
        Path("/root/Downloads"),
        Path("/home/airflow/Downloads"),
        Path.home() / "Downloads",
        Path("/tmp"),
    ]
    deadline = time.time() + timeout_sec
    last_partial_files: list[str] = []

    while time.time() <= deadline:
        try:
            downloaded = _find_downloaded_file(download_dir, min_mtime=min_mtime)
            logger.info("download completed | file=%s", downloaded)
            return downloaded
        except FileNotFoundError:
            partial_files: list[str] = []
            for d in candidate_dirs:
                if not d.exists():
                    continue
                temp_files = list(d.glob("*.crdownload")) + list(d.glob("*.tmp"))
                if min_mtime is not None:
                    temp_files = [f for f in temp_files if f.stat().st_mtime >= min_mtime]
                partial_files.extend([f"{d}:{f.name}" for f in temp_files])

            if partial_files and partial_files != last_partial_files:
                logger.info("download in progress | temp_files=%s", partial_files)
                last_partial_files = partial_files

            time.sleep(poll_interval)

    partial_msg = f" | progress={last_partial_files}" if last_partial_files else ""
    raise FileNotFoundError(
        f"다운로드된 엑셀 파일을 찾을 수 없습니다. 확인 경로: {[str(d) for d in candidate_dirs if d.exists()]}{partial_msg}"
    )


# ============================================================
# Task 함수
# ============================================================
def download_posfeed_excel(collect_mode: str = None, **context) -> str:
    """Posfeed 관리자 사이트에서 주문 엑셀 다운로드"""
    _mode = str(collect_mode or "").strip()
    if _mode and _mode not in ("yesterday", "backfill_missing"):
        raise AirflowSkipException(
            f"date mode ({collect_mode}): main download skipped — collect_missing_dates handles this"
        )
    download_dir = DOWN_DIR
    download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"다운로드 경로: {download_dir}")

    driver, wait = _restart_driver_with_download(download_dir)
    logger.info("CDP 다운로드 경로 설정: %s", download_dir)

    try:
        _login(driver, wait)
        download_start = time.time()
        _click_download(driver, wait)
        downloaded_file = _wait_for_downloaded_file(download_dir, min_mtime=download_start)
        file_path = str(downloaded_file)
        from datetime import timedelta
        data_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("✅ 다운로드 완료 | 파일: %s | 데이터 날짜: %s", downloaded_file.name, data_date)
        context['ti'].xcom_push(key='downloaded_file_path', value=file_path)
        return file_path
    finally:
        driver.quit()
        logger.info("WebDriver 종료")


def move_to_storage(**context) -> str:
    """다운로드된 엑셀 → UTF-8 CSV 변환 후 DOWN_DIR/ 에 저장 (OneDrive 업로드 전 처리용)"""
    from datetime import timedelta
    file_path = context['ti'].xcom_pull(
        task_ids='download_excel',
        key='downloaded_file_path',
    )
    if not file_path:
        raise ValueError("다운로드 파일 경로 XCom 값이 없습니다.")

    src = Path(file_path)
    if not src.exists():
        raise FileNotFoundError(f"다운로드 파일 없음: {src}")

    dest_dir = DOWN_DIR  # E:\down — OneDrive 업로드 후 업로드_temp로 이동됨
    dest_dir.mkdir(parents=True, exist_ok=True)

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y%m%d")

    # 엑셀 읽기 → 등록날짜 컬럼 추가 → UTF-8 CSV 저장
    # 파일 매직 바이트로 실제 포맷 판별 후 적절한 엔진 선택
    with open(str(src), "rb") as _f:
        _magic = _f.read(8)

    if _magic[:4] == b'\xd0\xcf\x11\xe0':
        # OLE2 복합 문서 형식 (xls)
        df = pd.read_excel(str(src), dtype=str, engine="xlrd")
        logger.info("엑셀 파일 엔진: xlrd (xls)")
    elif _magic[:4] == b'PK\x03\x04':
        # ZIP 기반 (xlsx)
        df = pd.read_excel(str(src), dtype=str, engine="openpyxl")
        logger.info("엑셀 파일 엔진: openpyxl (xlsx)")
    elif _magic[:5] in (b'<?xml', b'<html', b'<HTML') or _magic.lstrip()[:1] == b'<':
        # HTML 테이블로 내려온 경우
        logger.warning("HTML 형식 파일 감지 - HTML 테이블로 파싱 시도")
        dfs = pd.read_html(str(src), encoding="utf-8")
        df = dfs[0].astype(str)
    else:
        # 최후 시도: openpyxl → xlrd 순으로 fallback
        logger.warning(f"파일 매직 바이트 미확인({_magic[:8].hex()}) - openpyxl로 시도")
        try:
            df = pd.read_excel(str(src), dtype=str, engine="openpyxl")
        except Exception:
            df = pd.read_excel(str(src), dtype=str, engine="xlrd")
    df["등록날짜"] = yesterday
    dest = dest_dir / f"{src.stem}_{today}.csv"
    df.to_csv(str(dest), index=False, encoding="utf-8-sig")  # BOM 포함 → Excel 한글 깨짐 방지

    src.unlink()

    result = f"✅ 저장 완료: {dest.name} | 행수: {len(df):,} | 등록날짜: {yesterday}"
    logger.info(result)
    context['ti'].xcom_push(key='saved_file_path', value=str(dest))
    return result


# ============================================================
# OneDrive 파티션 저장
# ============================================================

_BRAND_PATTERNS = {
    "도리당": ["닭도리탕전문도리당"],
    "나홀로": ["나홀로"],
}

def _classify_brand(store_name: str):
    """가맹점명 → 브랜드명 반환. 대상 외 매장은 None."""
    for brand, patterns in _BRAND_PATTERNS.items():
        if any(p in store_name for p in patterns):
            return brand
    return None


def _normalize_store(store_name: str, brand: str) -> str:
    """가맹점명 → '브랜드 지점명' 형태로 표준화.

    예) 닭도리탕전문도리당_서울역삼점 → 도리당 역삼점
        나홀로1인곱도리탕_역삼점       → 나홀로 역삼점
        [음식배달] 나홀로 1인 곱도리탕 삼송점 → 나홀로 삼송점
    """
    # 괄호 태그 제거: "[음식배달] " 등
    name = re.sub(r"\[.*?\]\s*", "", store_name).strip()
    # '_' 기준으로 분리 → 마지막 토큰이 지점명
    if "_" in name:
        branch = name.split("_")[-1].strip()
    else:
        # 공백 기준 나홀로 변형 처리
        # "나홀로 1인 곱도리탕 삼송점" → 마지막 단어가 지점명
        tokens = name.split()
        branch = tokens[-1] if tokens else name
    normalized = f"{brand} {branch}"
    # alias 치환은 store_normalize.STORE_NAME_MAP(SSOT)에 위임
    return _normalize_store_series(pd.Series([normalized])).iloc[0]


def partition_to_onedrive(**context) -> str:
    """포스피드 주문 CSV → 브랜드/지점/월 파티션 CSV로 누적 저장

    - 도리당 / 나홀로 대상만 필터링
    - 원본 컬럼 유지 + 끝에 파생 컬럼 추가 (브랜드, 지점명, ym, collected_at, source_file)
    - PK: (주문 번호, 가맹점코드) 기준 dedup
    - 저장 경로: ANALYTICS_DB/posfeed_sales/brand=X/store=Y/ym=Z/posfeed_orders.csv
    """
    csv_path = context['ti'].xcom_pull(
        task_ids='move_to_storage',
        key='saved_file_path',
    )
    if not csv_path:
        raise ValueError("move_to_storage XCom 'saved_file_path' 값이 없습니다.")

    src = Path(csv_path)
    if not src.exists():
        raise FileNotFoundError(f"CSV 파일 없음: {src}")

    df = pd.read_csv(str(src), dtype=str, encoding="utf-8-sig")
    total = len(df)
    logger.info("읽은 건수: %d | 파일: %s", total, src.name)

    required_cols = {"가맹점명", "주문 번호", "가맹점코드", "등록날짜"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"필수 컬럼 누락 — 잘못된 파일이 처리됐을 가능성: {sorted(missing_cols)} | "
            f"전체 컬럼: {list(df.columns)[:15]} | 파일: {src.name}"
        )

    # 브랜드 분류 및 필터
    df["브랜드"] = df["가맹점명"].apply(_classify_brand)
    excluded = df["브랜드"].isna().sum()
    df = df[df["브랜드"].notna()].copy()
    target = len(df)
    logger.info("적재 대상: %d | 제외: %d", target, excluded)

    if target == 0:
        logger.warning("적재 대상 없음 — 파이프라인 종료")
        return f"✅ 적재 대상 없음 (전체 {total}건 모두 제외)"

    # 파생 컬럼 추가
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["지점명"] = df.apply(lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1)
    # downstream(상세 수집) 호환: 기존 파티션 컬럼명(brand/store)도 함께 유지
    df["brand"] = df["브랜드"]
    df["store"] = df["지점명"]
    df["ym"] = pd.to_datetime(df["등록날짜"], errors="coerce").dt.strftime("%Y-%m")
    df["collected_at"] = collected_at
    df["source_file"] = src.name

    # 브랜드별 건수 로그
    for brand, cnt in df["브랜드"].value_counts().items():
        logger.info("브랜드 [%s]: %d건", brand, cnt)

    # 복합 PK 컬럼 생성 (onedrive_csv_save는 단일 문자열 pk_col만 지원)
    df["_pk"] = df["주문 번호"].astype(str) + "_" + df["가맹점코드"].astype(str)

    # 파티션별 저장 (파일명만 변경)
    base_path = ANALYTICS_DB / "posfeed_sales"
    saved_partitions = 0
    inserted_sum = 0
    duplicated_sum = 0

    for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
        partition_path = base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        save_result = onedrive_csv_save(
            df=part_df.reset_index(drop=True),
            file_path=partition_path,
            pk_col="_pk",
            timestamp_col="collected_at",
            add_timestamp=False,
            if_exists="append",
        )
        inserted_sum += int(save_result.get("inserted", 0) or 0)
        duplicated_sum += int(save_result.get("duplicated", 0) or 0)
        logger.info(
            "저장: %s | inserted=%s duplicated=%s",
            partition_path,
            save_result.get("inserted"),
            save_result.get("duplicated"),
        )
        saved_partitions += 1

    # OneDrive 업로드 완료 후 DOWN_DIR → DOWN_DIR/업로드_temp/ 로 이동
    archive_dir = DOWN_DIR / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived = archive_dir / src.name
    shutil.move(str(src), str(archived))
    logger.info("파일 이동 완료: %s → %s", src, archived)

    result = (
        f"✅ OneDrive 저장 완료 | "
        f"총 {total}건 → 적재 {target}건 / 제외 {excluded}건 | "
        f"파티션 {saved_partitions}개 | inserted {inserted_sum} / duplicated {duplicated_sum} | 이동: {archived.name}"
    )
    logger.info(result)
    return result


# ============================================================
# 수동 today 파일 인제스트
# ============================================================

def ingest_manual_csvs(down_dir: Path = DOWN_DIR, **context) -> str:
    """DOWN_DIR의 수동 today 파일(order-info-list_매장명.csv/xlsx)을 파티션에 저장.

    처리 대상: order-info-list_*.{csv,xlsx}
    제외 대상: order-info-list_20XXXXXX.* (날짜형 — 자동 다운로드 파일)
    파일이 없으면 0건으로 정상 종료.
    """
    candidates = [
        *down_dir.glob("order-info-list_*.csv"),
        *down_dir.glob("order-info-list_*.xlsx"),
    ]

    def _is_date_file(p: Path) -> bool:
        suffix = p.stem[len("order-info-list_"):]
        return bool(re.fullmatch(r"\d{8}", suffix))

    manual_files = [f for f in candidates if not _is_date_file(f) and not f.name.startswith("~$")]

    if not manual_files:
        logger.info("수동 today 파일 없음 — 건너뜀")
        return "수동 today 파일 없음"

    archive_dir = down_dir / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)
    base_path = ANALYTICS_DB / "posfeed_sales"

    total_inserted = 0
    total_partitions = 0
    processed_files = []
    # detail 데이터가 없는 매장의 backfill 정보 수집
    backfill_stores: list[dict] = []

    for file_path in manual_files:
        logger.info("수동 파일 처리: %s", file_path.name)
        try:
            if file_path.suffix.lower() in (".xlsx", ".xls"):
                df = pd.read_excel(str(file_path), dtype=str)
            else:
                df = pd.read_csv(str(file_path), dtype=str, encoding="utf-8-sig", errors="replace")

            if df.empty:
                logger.warning("빈 파일: %s", file_path.name)
                shutil.move(str(file_path), str(archive_dir / file_path.name))
                continue

            # 등록날짜 생성
            if "등록날짜" not in df.columns:
                if "주문등록 시각" in df.columns:
                    df["등록날짜"] = pd.to_datetime(
                        df["주문등록 시각"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                else:
                    df["등록날짜"] = datetime.now().strftime("%Y-%m-%d")
                    logger.warning("등록날짜/주문등록 시각 컬럼 없음 → 오늘 날짜 사용: %s", file_path.name)

            required_cols = {"가맹점명", "주문 번호", "가맹점코드"}
            missing_cols = required_cols - set(df.columns)
            if missing_cols:
                logger.warning("필수 컬럼 누락 (%s): %s — 건너뜀", file_path.name, missing_cols)
                continue

            df["브랜드"] = df["가맹점명"].apply(_classify_brand)
            excluded = df["브랜드"].isna().sum()
            df = df[df["브랜드"].notna()].copy()

            if df.empty:
                logger.warning("적재 대상 없음 (전체 %d건 제외): %s", excluded, file_path.name)
                shutil.move(str(file_path), str(archive_dir / file_path.name))
                continue

            collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df["지점명"] = df.apply(lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1)
            df["brand"] = df["브랜드"]
            df["store"] = df["지점명"]
            df["ym"] = pd.to_datetime(df["등록날짜"], errors="coerce").dt.strftime("%Y-%m")
            df["collected_at"] = collected_at
            df["source_file"] = file_path.name
            df["_pk"] = df["주문 번호"].astype(str) + "_" + df["가맹점코드"].astype(str)

            for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
                partition_path = (
                    base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
                )
                partition_path.parent.mkdir(parents=True, exist_ok=True)
                save_result = onedrive_csv_save(
                    df=part_df.reset_index(drop=True),
                    file_path=partition_path,
                    pk_col="_pk",
                    timestamp_col="collected_at",
                    add_timestamp=False,
                    if_exists="append",
                )
                inserted = int(save_result.get("inserted", 0) or 0)
                total_inserted += inserted
                total_partitions += 1
                logger.info(
                    "저장: %s | inserted=%s duplicated=%s",
                    partition_path,
                    save_result.get("inserted"),
                    save_result.get("duplicated"),
                )

            # detail 데이터 없는 매장 감지 → backfill 대상 등록
            for (brand, store), store_df in df.groupby(["브랜드", "지점명"]):
                detail_path = ANALYTICS_DB / "posfeed_sales_detail" / f"brand={brand}" / f"store={store}"
                has_detail = detail_path.exists() and any(detail_path.rglob("*.csv"))
                if not has_detail:
                    valid_dates = store_df["등록날짜"].dropna()
                    if not valid_dates.empty:
                        backfill_stores.append({
                            "brand": brand,
                            "store": store,
                            "from": valid_dates.min(),
                            "to": valid_dates.max(),
                        })
                        logger.info(
                            "backfill 대상 등록: %s/%s | %s ~ %s",
                            brand, store, valid_dates.min(), valid_dates.max(),
                        )

            shutil.move(str(file_path), str(archive_dir / file_path.name))
            processed_files.append(file_path.name)
            logger.info("파일 이동: %s → 업로드_temp/", file_path.name)

        except Exception as exc:
            logger.error("수동 파일 처리 실패 (%s): %s", file_path.name, exc, exc_info=True)

    # backfill 정보 XCom push (extract_order_codes에서 참조)
    if backfill_stores and context.get("ti"):
        context["ti"].xcom_push(key="manual_backfill", value={"backfill": backfill_stores})
        logger.info("XCom push manual_backfill | %d개 매장", len(backfill_stores))

    if not processed_files:
        return "수동 today 파일: 처리된 파일 없음"

    result = (
        f"수동 today 파일 처리 완료 | "
        f"{len(processed_files)}개 파일 | inserted {total_inserted}건 | 파티션 {total_partitions}개 | "
        f"파일: {', '.join(processed_files)}"
    )
    logger.info(result)
    return result


# ============================================================
# 월별 현황 점검 + 갭 감지 + 재수집 + 무결성 검사 (파이프라인 맨앞)
# ============================================================



def check_monthly_collection(**context) -> str:
    """월별 수집 현황 점검, 갭 감지·재수집, 금액 무결성 검사.

    1. posfeed_sales 파티션 → 월별 현황 표 로그
    2. 갭 감지: 파티션 최초 날짜 ~ yesterday 전체 범위에서 누락 날짜 탐지
       (step 1에서 1월 데이터가 보이면 1월부터 전부 검사)
    3. 누락 날짜 중 최근 _MONTHLY_CHECK_REDOWNLOAD_DAYS일만 Selenium 재다운로드
       (주말은 no-data 처리, 실패는 경고만, 태스크 미실패)
    4. 완성된 ym(당월 제외): sum(총 주문금액) vs sum(합계) 비교 → WARNING 경고만
       (아이템은 주문코드별 개별 스크래핑이므로 날짜 재다운로드로 자동 수정 불가)
    """
    sales_base  = ANALYTICS_DB / "posfeed_sales"
    detail_base = ANALYTICS_DB / "posfeed_sales_detail"

    # ── STEP 1: 월별 현황 ────────────────────────────────────────
    summary_rows: list[dict] = []
    for csv_path in sorted(sales_base.glob("brand=*/store=*/ym=*/posfeed_orders.csv")):
        try:
            df_s = pd.read_csv(csv_path, usecols=["brand", "store", "ym"], dtype=str)
            if df_s.empty:
                continue
            b = df_s["brand"].iloc[0]
            s = df_s["store"].iloc[0]
            y = df_s["ym"].iloc[0]
            summary_rows.append({"ym": y, "brand": b, "store": s, "orders": len(df_s)})
        except Exception as exc:
            logger.warning("[월별현황] 파티션 읽기 실패: %s | %s", csv_path, exc)

    if summary_rows:
        summary_rows.sort(key=lambda r: (r["ym"], r["brand"], r["store"]))
        header = f"{'ym':<10} | {'brand':<10} | {'store':<18} | {'orders':>8}"
        sep    = "-" * len(header)
        lines  = ["\n[월별 현황]", sep, header, sep]
        for r in summary_rows:
            lines.append(f"{r['ym']:<10} | {r['brand']:<10} | {r['store']:<18} | {r['orders']:>8,}")
        lines.append(sep)
        logger.info("\n".join(lines))
    else:
        logger.info("[월별 현황] 수집된 파티션 없음")

    # ── STEP 2: 갭 감지 ──────────────────────────────────────────
    # 파티션에 데이터가 있으면 가장 이른 ym 첫날부터 검사 (step 1과 범위 일치)
    # 데이터가 없으면 어제 하루만 검사
    today     = datetime.now().date()
    yesterday = today - timedelta(days=1)

    if summary_rows:
        earliest_ym = summary_rows[0]["ym"]  # 이미 정렬됨
        try:
            start_dt = datetime.strptime(earliest_ym + "-01", "%Y-%m-%d").date()
        except Exception:
            start_dt = yesterday
    else:
        start_dt = yesterday

    expected_dates: set[str] = set()
    cur = start_dt
    while cur <= yesterday:
        expected_dates.add(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    existing_dates: set[str] = set()
    for csv_path in sales_base.glob("brand=*/store=*/ym=*/posfeed_orders.csv"):
        try:
            df_check = pd.read_csv(csv_path, usecols=["등록날짜"], dtype=str)
            existing_dates.update(df_check["등록날짜"].dropna().tolist())
        except Exception:
            continue

    missing_dates = sorted(expected_dates - existing_dates)
    original_missing_count = len(missing_dates)

    if not missing_dates:
        logger.info("[갭 감지] %s ~ %s 누락 없음", start_dt, yesterday)
    else:
        logger.info(
            "[갭 감지] %s ~ %s 중 누락 %d일 (전체 재다운로드): %s",
            start_dt, yesterday, original_missing_count, missing_dates,
        )

    # ── STEP 3: 누락 날짜 재다운로드 ────────────────────────────
    success_dates: list[str] = []
    nodata_dates:  list[str] = []
    fail_dates:    list[str] = []

    if missing_dates:
        download_dir = DOWN_DIR
        download_dir.mkdir(parents=True, exist_ok=True)
        archive_dir  = download_dir / "업로드_temp"
        archive_dir.mkdir(parents=True, exist_ok=True)

        driver, wait = _restart_driver_with_download(download_dir)
        try:
            for attempt in range(2):
                try:
                    _login(driver, wait)
                    break
                except Exception as err:
                    logger.warning("[재수집] 로그인 실패(재시작 %d): %s", attempt, err)
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    if attempt >= 1:
                        logger.error("[재수집] 로그인 2회 실패 → 재수집 전체 건너뜀")
                        fail_dates.extend(missing_dates)
                        missing_dates = []
                        break
                    driver, wait = _restart_driver_with_download(download_dir)

            for date_str in missing_dates:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")

                # 주말 → no-data 처리 (Saturday=5, Sunday=6)
                if target_date.weekday() >= 5:
                    logger.info("[재수집] %s 주말 — no-data 처리", date_str)
                    nodata_dates.append(date_str)
                    continue

                try:
                    download_start = time.time()
                    _click_download(driver, wait, target_date=target_date)
                    downloaded_file = _wait_for_downloaded_file(download_dir, min_mtime=download_start)

                    df_new = pd.read_excel(str(downloaded_file), dtype=str, engine="openpyxl")

                    # 날짜 정합성 검증
                    if "주문등록 시각" in df_new.columns and len(df_new) > 0:
                        actual_months = (
                            pd.to_datetime(df_new["주문등록 시각"], errors="coerce")
                            .dt.month.dropna()
                        )
                        if len(actual_months) > 0:
                            actual_month = actual_months.mode()[0]
                            if actual_month != target_date.month:
                                logger.warning(
                                    "[재수집] %s 날짜 피커 오류: 실제월=%d 대상월=%d — 건너뜀",
                                    date_str, int(actual_month), target_date.month,
                                )
                                downloaded_file.unlink(missing_ok=True)
                                fail_dates.append(date_str)
                                continue

                    df_new["브랜드"] = df_new["가맹점명"].apply(_classify_brand)
                    df_filtered = df_new[df_new["브랜드"].notna()].copy()

                    if len(df_filtered) == 0:
                        logger.info("[재수집] %s 주문 없음 (no-data)", date_str)
                        downloaded_file.unlink(missing_ok=True)
                        nodata_dates.append(date_str)
                        continue

                    df_filtered["등록날짜"]     = date_str
                    df_filtered["지점명"]       = df_filtered.apply(
                        lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1
                    )
                    df_filtered["brand"]        = df_filtered["브랜드"]
                    df_filtered["store"]        = df_filtered["지점명"]
                    df_filtered["ym"]           = date_str[:7]
                    df_filtered["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df_filtered["source_file"]  = downloaded_file.name
                    df_filtered["_pk"] = (
                        df_filtered["주문 번호"].astype(str) + "_" + df_filtered["가맹점코드"].astype(str)
                    )

                    saved_partitions = 0
                    for (brand, store, ym), part_df in df_filtered.groupby(["브랜드", "지점명", "ym"]):
                        partition_path = (
                            sales_base / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
                        )
                        partition_path.parent.mkdir(parents=True, exist_ok=True)
                        onedrive_csv_save(
                            df=part_df.reset_index(drop=True),
                            file_path=partition_path,
                            pk_col="_pk",
                            timestamp_col="collected_at",
                            add_timestamp=False,
                            if_exists="append",
                        )
                        saved_partitions += 1

                    logger.info("[재수집] %s 완료 | %d건 | 파티션 %d개", date_str, len(df_filtered), saved_partitions)
                    shutil.move(str(downloaded_file), str(archive_dir / downloaded_file.name))
                    success_dates.append(date_str)

                except Exception as exc:
                    logger.exception("[재수집] %s 실패: %s", date_str, exc)
                    fail_dates.append(date_str)
                    try:
                        if "#/login" in driver.current_url:
                            _login(driver, wait)
                    except Exception:
                        pass

        finally:
            try:
                driver.quit()
            except Exception:
                pass

        logger.info(
            "[재수집] 완료 | 성공=%d 노데이터=%d 실패=%d",
            len(success_dates), len(nodata_dates), len(fail_dates),
        )
        if fail_dates:
            logger.warning("[재수집] 실패 날짜: %s", fail_dates)

    # ── STEP 4: 금액 무결성 검사 (read-only) ────────────────────
    current_ym = datetime.now().strftime("%Y-%m")

    all_sales_yms: set[str] = set()
    for csv_path in sales_base.glob("brand=*/store=*/ym=*/posfeed_orders.csv"):
        for part in csv_path.parts:
            if part.startswith("ym="):
                all_sales_yms.add(part[3:])
                break

    completed_yms = sorted(ym for ym in all_sales_yms if ym < current_ym)

    if not completed_yms:
        logger.info("[무결성] 검사 대상 완성 ym 없음")

    integrity_warnings = 0
    for ym in completed_yms:
        try:
            orders_total = 0.0
            for csv_path in sales_base.glob(f"brand=*/store=*/ym={ym}/posfeed_orders.csv"):
                try:
                    df_o = pd.read_csv(csv_path, usecols=["총 주문금액"], dtype=str)
                    orders_total += pd.to_numeric(
                        df_o["총 주문금액"].str.replace(",", "", regex=False), errors="coerce"
                    ).fillna(0).sum()
                except Exception as exc:
                    logger.warning("[무결성] 주문 파티션 읽기 실패: %s | %s", csv_path, exc)

            items_total = 0.0
            for csv_path in detail_base.glob(f"brand=*/store=*/ym={ym}/posfeed_order_item.csv"):
                try:
                    df_i = pd.read_csv(csv_path, usecols=["합계"], dtype=str)
                    items_total += pd.to_numeric(
                        df_i["합계"].str.replace(",", "", regex=False), errors="coerce"
                    ).fillna(0).sum()
                except Exception as exc:
                    logger.warning("[무결성] 아이템 파티션 읽기 실패: %s | %s", csv_path, exc)

            diff = abs(orders_total - items_total)
            if diff > 0:
                logger.warning(
                    "[무결성] ym=%s | 주문금액=%s ≠ 아이템합계=%s | 차이=%s → detail 재수집 필요",
                    ym, f"{orders_total:,.0f}", f"{items_total:,.0f}", f"{diff:,.0f}",
                )
                integrity_warnings += 1
            else:
                logger.info("[무결성] ym=%s OK | 주문금액=%s / 아이템합계=%s", ym, f"{orders_total:,.0f}", f"{items_total:,.0f}")

        except Exception as exc:
            logger.warning("[무결성] ym=%s 검사 오류: %s", ym, exc)

    result = (
        f"[check_monthly] 파티션={len(summary_rows)}개 | "
        f"갭감지={original_missing_count}일 | "
        f"재수집: 성공={len(success_dates)} 노데이터={len(nodata_dates)} 실패={len(fail_dates)} | "
        f"무결성경고={integrity_warnings}건(ym {len(completed_yms)}개 검사)"
    )
    logger.info(result)
    return result


# ============================================================
# 누락 날짜 재수집: partition_to_onedrive 완료 후 자동 실행
# ============================================================

_MISSING_LOOKBACK_DAYS = 30  # 최근 N일 검사


def _resolve_missing_scan_range(collect_mode: str, conf: dict) -> tuple[datetime, datetime, str]:
    """누락 재수집 대상 날짜 범위를 결정한다.

    우선순위:
    1) dag_run.conf.start_date/end_date
    2) collect_mode 날짜 지정(단일/범위)
    3) 최근 _MISSING_LOOKBACK_DAYS 자동 모드
    """
    start_str = conf.get("start_date")
    end_str = conf.get("end_date")
    if start_str and end_str:
        return (
            datetime.strptime(start_str, "%Y-%m-%d"),
            datetime.strptime(end_str, "%Y-%m-%d"),
            f"conf 기간 지정 모드: {start_str} ~ {end_str}",
        )

    cm = str(collect_mode or "").strip()
    if cm and cm not in ("yesterday", "backfill_missing"):
        if "~" in cm:
            from_str, to_str = [s.strip() for s in cm.split("~", 1)]
            return (
                datetime.strptime(from_str, "%Y-%m-%d"),
                datetime.strptime(to_str, "%Y-%m-%d"),
                f"collect_mode 기간 모드: {from_str} ~ {to_str}",
            )
        single = datetime.strptime(cm, "%Y-%m-%d")
        return (single, single, f"collect_mode 단일일 모드: {cm}")

    yesterday = datetime.now() - timedelta(days=1)
    end_dt = yesterday
    start_dt = yesterday - timedelta(days=_MISSING_LOOKBACK_DAYS - 1)
    return (start_dt, end_dt, f"자동 모드: 최근 {_MISSING_LOOKBACK_DAYS}일 검사")


def collect_missing_dates(
    enable_missing_backfill: bool = True,
    collect_mode: str = "yesterday",
    **context,
) -> str:
    """OneDrive posfeed_sales 파티션에서 누락 날짜를 감지해 Posfeed에서 재수집.

    - 최근 LOOKBACK_DAYS 일 중 등록날짜 컬럼에 데이터가 전혀 없는 날짜를 누락으로 판정
    - 누락 날짜마다 Posfeed에서 해당 날짜 데이터 다운로드 → 파티션 저장
    - trigger_rule=ALL_DONE 으로 메인 수집 성공/실패 여부와 무관하게 실행
    """
    from datetime import timedelta

    if not enable_missing_backfill:
        raise AirflowSkipException("collect_missing_dates disabled (enable_missing_backfill=False)")

    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    if conf.get("reset") or conf.get("disable_missing_backfill"):
        raise AirflowSkipException("collect_missing_dates disabled by conf (reset/disable_missing_backfill)")

    start_dt, end_dt, mode_msg = _resolve_missing_scan_range(collect_mode, conf)
    if start_dt > end_dt:
        raise ValueError(
            f"collect_missing_dates 기간 오류: start_date({start_dt:%Y-%m-%d}) > end_date({end_dt:%Y-%m-%d})"
        )
    logger.info(mode_msg)

    cur, expected_dates = start_dt, set()
    while cur <= end_dt:
        expected_dates.add(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    base_path = ANALYTICS_DB / "posfeed_sales"
    existing_dates: set[str] = set()
    for csv_path in base_path.glob("brand=*/store=*/ym=*/posfeed_orders.csv"):
        try:
            df_check = pd.read_csv(csv_path, usecols=["등록날짜"], dtype=str)
            existing_dates.update(df_check["등록날짜"].dropna().tolist())
        except Exception:
            continue

    missing_dates = sorted(expected_dates - existing_dates)
    if not missing_dates:
        logger.info("누락 날짜 없음 — 최근 %d일 수집 완료", _MISSING_LOOKBACK_DAYS)
        return f"✅ 누락 날짜 없음 (최근 {_MISSING_LOOKBACK_DAYS}일 기준)"

    logger.info("누락 날짜 %d개 감지: %s", len(missing_dates), missing_dates)

    download_dir = DOWN_DIR
    download_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = download_dir / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)

    driver, wait = _restart_driver_with_download(download_dir)

    success_dates: list[str] = []
    fail_dates: list[str] = []
    total_loaded = 0

    try:
        # 크롬/드라이버가 중간에 죽으면(localhost:port connection refused) 재시작 후 재로그인
        for attempt in range(2):
            try:
                _login(driver, wait)
                break
            except (WebDriverException, Exception) as err:
                logger.warning("로그인 실패(재시작): %s", err)
                try:
                    driver.quit()
                except Exception:
                    pass
                if attempt >= 1:
                    raise
                driver, wait = _restart_driver_with_download(download_dir)

        for date_str in missing_dates:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            try:
                download_start_time = time.time()
                _click_download(driver, wait, target_date=target_date)
                downloaded_file = _wait_for_downloaded_file(download_dir, min_mtime=download_start_time)

                df = pd.read_excel(str(downloaded_file), dtype=str, engine="openpyxl")

                # 날짜 정합성 검증: 주문등록 시각이 target_date와 같은 달인지 확인
                if "주문등록 시각" in df.columns and len(df) > 0:
                    actual_months = pd.to_datetime(df["주문등록 시각"], errors="coerce").dt.month.dropna()
                    if len(actual_months) > 0:
                        actual_month = actual_months.mode()[0]
                        if actual_month != target_date.month:
                            logger.warning(
                                "[%s] 날짜 피커 오류 감지 — 다운로드 데이터 월(%d월)이 대상 월(%d월)과 불일치. 건너뜀.",
                                date_str, int(actual_month), target_date.month,
                            )
                            downloaded_file.unlink(missing_ok=True)
                            fail_dates.append(date_str)
                            continue

                df["등록날짜"] = date_str

                df["브랜드"] = df["가맹점명"].apply(_classify_brand)
                df = df[df["브랜드"].notna()].copy()

                if len(df) == 0:
                    logger.info("[%s] 적재 대상 없음 (해당일 주문 없거나 대상 브랜드 없음)", date_str)
                    downloaded_file.unlink(missing_ok=True)
                    success_dates.append(date_str)
                    continue

                df["지점명"] = df.apply(lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1)
                df["brand"] = df["브랜드"]
                df["store"] = df["지점명"]
                df["ym"] = date_str[:7]
                df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df["source_file"] = downloaded_file.name
                df["_pk"] = df["주문 번호"].astype(str) + "_" + df["가맹점코드"].astype(str)

                saved_partitions = 0
                for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
                    partition_path = (
                        base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
                    )
                    partition_path.parent.mkdir(parents=True, exist_ok=True)
                    onedrive_csv_save(
                        df=part_df.reset_index(drop=True),
                        file_path=partition_path,
                        pk_col="_pk",
                        timestamp_col="collected_at",
                        add_timestamp=False,
                        if_exists="append",
                    )
                    saved_partitions += 1
                logger.info("[%s] 재수집 완료 | %d건 | 파티션 %d개", date_str, len(df), saved_partitions)

                total_loaded += len(df)
                shutil.move(str(downloaded_file), str(archive_dir / downloaded_file.name))
                success_dates.append(date_str)

            except Exception as exc:
                logger.exception("[%s] 재수집 실패: %s", date_str, exc)
                fail_dates.append(date_str)
                try:
                    if "#/login" in driver.current_url:
                        _login(driver, wait)
                except Exception:
                    pass

    finally:
        driver.quit()

    summary = (
        f"누락 재수집 완료 | "
        f"성공 {len(success_dates)}일 / 실패 {len(fail_dates)}일 | "
        f"총 {total_loaded}건 적재"
    )
    if fail_dates:
        summary += f" | 실패 날짜: {fail_dates}"
    logger.info(summary)
    return summary


# ============================================================
# 백필: DOWN_DIR에 쌓인 xlsx 일괄 처리
# ============================================================

def backfill_xlsx_to_onedrive(**context) -> str:
    """DOWN_DIR 내 order-info-list_*.xlsx 파일을 일괄로 OneDrive 파티션 CSV에 적재.

    - 기존 download_posfeed_excel + move_to_storage + partition_to_onedrive 흐름을
      거치지 않고 DOWN_DIR에 수동으로 쌓아둔 xlsx 파일을 대상으로 백필 처리.
    - 처리 완료된 파일은 DOWN_DIR/업로드_temp/ 로 이동.
    - 반환: 요약 문자열 (XCom용)
    """
    base_path = ANALYTICS_DB / "posfeed_sales"
    archive_dir = DOWN_DIR / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)

    xlsx_files = sorted(
        f for f in DOWN_DIR.glob("*.xlsx")
        if f.parent == DOWN_DIR and not f.name.startswith("~$")
    )
    if not xlsx_files:
        logger.warning("[backfill] DOWN_DIR에 처리할 xlsx 파일이 없습니다: %s", DOWN_DIR)
        return "[backfill] 처리할 파일 없음"

    logger.info("[backfill] 발견된 파일 수: %d", len(xlsx_files))

    total_success = 0
    total_fail = 0
    total_loaded = 0
    total_partitions = 0

    for xlsx_path in xlsx_files:
        try:
            # (a) 엑셀 읽기
            df = pd.read_excel(str(xlsx_path), dtype=str, engine="openpyxl")
            total_rows = len(df)

            # (b) 등록날짜 파생
            df["등록날짜"] = pd.to_datetime(
                df["주문등록 시각"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

            # (c) 브랜드 분류 + 필터
            df["브랜드"] = df["가맹점명"].apply(_classify_brand)
            excluded = int(df["브랜드"].isna().sum())
            df = df[df["브랜드"].notna()].copy()
            target = len(df)

            if target == 0:
                logger.warning(
                    "[backfill] 적재 대상 없음 | 파일: %s | 전체 %d건 모두 제외",
                    xlsx_path.name, total_rows,
                )
                # 이동만 수행하고 다음 파일로
                shutil.move(str(xlsx_path), str(archive_dir / xlsx_path.name))
                total_success += 1
                continue

            # (d) 지점명 표준화
            df["지점명"] = df.apply(
                lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1
            )
            df["brand"] = df["브랜드"]
            df["store"] = df["지점명"]

            # (e) ym 파생
            df["ym"] = pd.to_datetime(
                df["등록날짜"], errors="coerce"
            ).dt.strftime("%Y-%m")

            # (f) collected_at
            df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # (g) source_file
            df["source_file"] = xlsx_path.name

            # (h) PK 컬럼
            df["_pk"] = df["주문 번호"].astype(str) + "_" + df["가맹점코드"].astype(str)

            # (i) 파티션별 저장 (파일명만 변경)
            saved_partitions = 0
            for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
                partition_path = (
                    base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
                )
                partition_path.parent.mkdir(parents=True, exist_ok=True)
                onedrive_csv_save(
                    df=part_df.reset_index(drop=True),
                    file_path=partition_path,
                    pk_col="_pk",
                    timestamp_col="collected_at",
                    add_timestamp=False,
                    if_exists="append",
                )
                saved_partitions += 1

            logger.info(
                "[backfill] 완료: %s | 읽은 %d건 → 적재 %d건 / 제외 %d건 | 파티션 %d개",
                xlsx_path.name,
                total_rows,
                target,
                excluded,
                saved_partitions,
            )

            # (j) 처리 완료 파일 이동
            shutil.move(str(xlsx_path), str(archive_dir / xlsx_path.name))

            total_success += 1
            total_loaded += target
            total_partitions += saved_partitions

        except Exception as exc:
            logger.exception("[backfill] 실패: %s | 오류: %s", xlsx_path.name, exc)
            total_fail += 1
            continue

    summary = (
        f"[backfill] ===== 완료 | "
        f"성공 {total_success}파일 / 실패 {total_fail}파일 | "
        f"총 적재 {total_loaded}건 | "
        f"파티션 {total_partitions}개 ====="
    )
    logger.info(summary)
    return summary
