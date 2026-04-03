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
from datetime import datetime
from pathlib import Path

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from modules.transform.utility.paths import DOWN_DIR, ANALYTICS_DB
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
    logger.info(f"브라우저 실행 (headless={HEADLESS_MODE})")

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
        raise


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


def _click_download(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """주문 목록 검색 → 엑셀 다운로드 → 확인"""

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
        time.sleep(2)
        logger.info(f"주문 목록 페이지 로드 완료 | URL: {driver.current_url}")

    _ensure_order_page()

    # ── 날짜 피커: 시작일 / 종료일 모두 어제로 설정 ──────────────────────────
    from datetime import timedelta as _td
    from selenium.webdriver.common.action_chains import ActionChains
    _yesterday = datetime.now() - _td(days=1)
    _yesterday_day = str(_yesterday.day)  # '23' 같은 숫자 문자열

    def _select_yesterday(editor_el) -> None:
        """날짜 피커 에디터 클릭 → 달력 팝업 대기 → 어제 날짜 셀 클릭"""
        # 달력 아이콘 클릭 (input 자체보다 아이콘이 더 확실하게 팝업을 열음)
        try:
            icon = editor_el.find_element(By.CSS_SELECTOR, ".el-icon-date")
            ActionChains(driver).move_to_element(icon).click().perform()
        except Exception:
            ActionChains(driver).move_to_element(editor_el).click().perform()
        time.sleep(0.5)

        # 달력 팝업 패널이 나타날 때까지 대기 (최대 8초)
        try:
            WebDriverWait(driver, 8).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".el-picker-panel"))
            )
        except TimeoutException:
            time.sleep(2)  # fallback

        # 보이는(visible) 패널만 골라서, 그 안에서 어제 날짜 셀 검색
        panels = [p for p in driver.find_elements(By.CSS_SELECTOR, ".el-picker-panel") if p.is_displayed()]
        if not panels:
            raise TimeoutException("보이는 달력 패널이 없습니다.")
        panel = panels[-1]  # 가장 마지막(최근 열린) 패널

        _CELL_CSS = "td.available:not(.prev-month):not(.next-month)"
        # span 렌더링 대기: 최대 5초간 셀 내부 span이 나타날 때까지 재시도
        day_cell = None
        for _attempt in range(10):
            for td in panel.find_elements(By.CSS_SELECTOR, _CELL_CSS):
                try:
                    span = td.find_element(By.TAG_NAME, "span")
                except Exception:
                    continue
                if span.text.strip() == _yesterday_day:
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
            logger.error(f"달력 available 날짜 목록: {found_days}")
            raise TimeoutException(f"달력에서 어제 날짜({_yesterday_day}일) 셀을 찾을 수 없습니다.")

        ActionChains(driver).move_to_element(day_cell).click().perform()
        time.sleep(0.5)
        logger.info(f"날짜 선택: {_yesterday.strftime('%Y-%m-%d')}")

    # 날짜 피커 에디터 div 2개 (시작일, 종료일) 순서대로 처리
    try:
        date_editors = WebDriverWait(driver, 8).until(
            lambda d: [e for e in d.find_elements(By.CSS_SELECTOR, ".el-date-editor--date") if e.is_displayed()]
        )
    except TimeoutException:
        date_editors = []

    if len(date_editors) >= 2:
        _select_yesterday(date_editors[0])  # 시작일
        _select_yesterday(date_editors[1])  # 종료일
    elif len(date_editors) == 1:
        _select_yesterday(date_editors[0])
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


def _find_downloaded_file(download_dir: Path) -> Path:
    """다운로드된 엑셀 파일을 후보 경로에서 찾아 반환"""
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
                 if not f.suffix in (".crdownload", ".tmp")]
        found.extend(files)
        if files:
            logger.info(f"파일 발견 경로: {d} | {[f.name for f in files]}")
    if not found:
        raise FileNotFoundError(f"다운로드된 엑셀 파일을 찾을 수 없습니다. 확인 경로: {[str(d) for d in candidate_dirs if d.exists()]}")
    return max(found, key=lambda f: f.stat().st_mtime)


# ============================================================
# Task 함수
# ============================================================
def download_posfeed_excel(**context) -> str:
    """Posfeed 관리자 사이트에서 주문 엑셀 다운로드"""
    download_dir = DOWN_DIR
    download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"다운로드 경로: {download_dir}")

    driver = _launch_browser(download_dir)
    wait = WebDriverWait(driver, 20)

    # headless Chrome은 prefs 무시 → Browser.setDownloadBehavior CDP로 강제 설정
    driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": str(download_dir),
        "eventsEnabled": True,
    })
    logger.info(f"CDP 다운로드 경로 설정: {download_dir}")

    try:
        _login(driver, wait)
        _click_download(driver, wait)
        downloaded_file = _find_downloaded_file(download_dir)
        file_path = str(downloaded_file)
        from datetime import timedelta
        data_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"✅ 다운로드 완료 | 파일: {downloaded_file.name} | 데이터 날짜: {data_date}")
        context['ti'].xcom_push(key='downloaded_file_path', value=file_path)
        logger.info(f"XCom push: downloaded_file_path = {file_path}")
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
    df = pd.read_excel(str(src), dtype=str)
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

# 정규화 후 alias 치환: 잘못된 지점명 → 올바른 지점명
_STORE_ALIASES: dict = {
    "도리당 서울역삼점": "도리당 역삼점",
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
    return _STORE_ALIASES.get(normalized, normalized)


def partition_to_onedrive(**context) -> str:
    """포스피드 주문 CSV → 브랜드/지점/월 파티션으로 OneDrive 저장

    - 도리당 / 나홀로 대상만 필터링
    - 원본 컬럼 유지 + 끝에 파생 컬럼 추가 (브랜드, 지점명, ym, collected_at, source_file)
    - PK: (주문 번호, 가맹점코드) 기준 dedup
    - 저장 경로: ANALYTICS_DB/posfeed_sales/brand=X/store=Y/ym=Z/data.csv
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
    df["ym"] = pd.to_datetime(df["등록날짜"], errors="coerce").dt.strftime("%Y-%m")
    df["collected_at"] = collected_at
    df["source_file"] = src.name

    # 브랜드별 건수 로그
    for brand, cnt in df["브랜드"].value_counts().items():
        logger.info("브랜드 [%s]: %d건", brand, cnt)

    # 복합 PK 컬럼 생성 (onedrive_csv_save는 단일 문자열 pk_col만 지원)
    df["_pk"] = df["주문 번호"].astype(str) + "_" + df["가맹점코드"].astype(str)

    # 파티션별 저장
    base_path = ANALYTICS_DB / "posfeed_sales"
    saved_partitions = 0

    for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
        partition_path = base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "data.csv"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        onedrive_csv_save(
            df=part_df.reset_index(drop=True),
            file_path=partition_path,
            pk_col="_pk",
            timestamp_col="collected_at",
            add_timestamp=False,
            if_exists="append",
        )
        logger.info("저장: %s (%d건)", partition_path, len(part_df))
        saved_partitions += 1

    # OneDrive 업로드 완료 후 DOWN_DIR → DOWN_DIR/업로드_temp/ 로 이동
    archive_dir = DOWN_DIR / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived = archive_dir / src.name
    shutil.move(str(src), str(archived))
    logger.info("파일 이동 완료: %s → %s", src, archived)

    result = (
        f"✅ OneDrive 파티션 저장 완료 | "
        f"총 {total}건 → 적재 {target}건 / 제외 {excluded}건 | "
        f"파티션 {saved_partitions}개 | 이동: {archived.name}"
    )
    logger.info(result)
    return result


# ============================================================
# 백필: DOWN_DIR에 쌓인 xlsx 일괄 처리
# ============================================================

def backfill_xlsx_to_onedrive(**context) -> str:
    """DOWN_DIR 내 order-info-list_*.xlsx 파일을 일괄로 OneDrive 파티션에 적재.

    - 기존 download_posfeed_excel + move_to_storage + partition_to_onedrive 흐름을
      거치지 않고 DOWN_DIR에 수동으로 쌓아둔 xlsx 파일을 대상으로 백필 처리.
    - 처리 완료된 파일은 DOWN_DIR/업로드_temp/ 로 이동.
    - 반환: 요약 문자열 (XCom용)
    """
    base_path = ANALYTICS_DB / "posfeed_sales"
    archive_dir = DOWN_DIR / "업로드_temp"
    archive_dir.mkdir(parents=True, exist_ok=True)

    xlsx_files = sorted(DOWN_DIR.glob("order-info-list_*.xlsx"))
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

            # (i) 파티션별 저장
            saved_partitions = 0
            for (brand, store, ym), part_df in df.groupby(["브랜드", "지점명", "ym"]):
                partition_path = (
                    base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "data.csv"
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
                xlsx_path.name, total_rows, target, excluded, saved_partitions,
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
