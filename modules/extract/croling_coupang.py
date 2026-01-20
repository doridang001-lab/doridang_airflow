"""
쿠팡이츠 크롤링 - Airflow 연동용

============================================================================
사용법
============================================================================

from modules.extract.croling_coupang import run_coupang_crawling

# 1. 통계만 수집
stats_df = run_coupang_crawling(account_df, mode="stats")

# 2. 파일만 다운로드
run_coupang_crawling(account_df, mode="download", target_date="2025-07")

# 3. 둘 다 (기본값)
stats_df = run_coupang_crawling(account_df, mode="all", target_date="2025-07")

============================================================================
모드 설명
============================================================================

| mode       | 동작              | target_date |
|------------|-------------------|-------------|
| "stats"    | 통계만 수집        | 불필요       |
| "download" | 파일만 다운로드    | 필수         |
| "all"      | 둘 다             | 필수         |

============================================================================
"""

import time
import random
import re
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException

try:
    from curl_cffi import requests
    IS_CURL_CFFI_AVAILABLE = True
except ImportError:
    import requests
    IS_CURL_CFFI_AVAILABLE = False


# ============================================================================
# 상수 - 작업 모드
# ============================================================================
MODE_STATS = "stats"           # 통계만 수집
MODE_DOWNLOAD = "download"     # 파일만 다운로드
MODE_ALL = "all"               # 둘 다


# ============================================================================
# 상수 - URL
# ============================================================================
COUPANG_LOGIN_URL = "https://store.coupangeats.com/merchant/management/login"
COUPANG_ORDERS_URL_TEMPLATE = "https://store.coupangeats.com/merchant/management/orders/{store_id}"
COUPANG_STATS_API_URL = "https://store.coupangeats.com/api/v1/merchant/web/order/condition"
COUPANG_DOWNLOAD_API_URL_TEMPLATE = "https://store.coupangeats.com/api/v1/merchant/web/emails?type=salesOrder&action=download&downloadRequestDate={target_date}"


# ============================================================================
# 상수 - HTTP
# ============================================================================
HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404


# ============================================================================
# 상수 - 타이밍 (밴 방지)
# ============================================================================
TIMING = {
    "login_wait": (8.0, 12.0),
    "page_load": (4.0, 6.0),
    "typing_char": (0.05, 0.15),
    "typing_pause": (0.3, 0.6),
    "before_click": (0.5, 1.0),
    "api_call": (1.0, 2.0),
    "file_download": (2.0, 3.5),
    "store_switch": (3.0, 6.0),
    "account_stagger": (3.0, 7.0),
    "batch_rest": (45.0, 90.0),
}


# ============================================================================
# 상수 - 배치 설정
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)


# ============================================================================
# 상수 - 경로 (Docker 환경 고려)
# ============================================================================
import os

BASE_DIR = os.getenv("AIRFLOW_HOME", Path.cwd())
CHROME_PROFILE_DIR = Path(os.getenv("CHROME_PROFILE_DIR", f"{BASE_DIR}/chrome_profiles"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", f"{BASE_DIR}/download"))

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 상수 - 기타
# ============================================================================
MIN_VALID_FILE_SIZE = 100

LOGIN_FAIL_URL_PATTERNS = ["/login", "/signin", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/orders", "/management", "/dashboard", "/home", "/main"]

DEFAULT_WIN_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ============================================================================
# 유틸리티
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{account_id}] {message}")


def random_delay(timing_key: str):
    min_sec, max_sec = TIMING[timing_key]
    time.sleep(random.uniform(min_sec, max_sec))


def human_type(element, text: str):
    element.clear()
    random_delay("typing_pause")
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(*TIMING["typing_char"]))
    
    random_delay("typing_pause")


def clean_chrome_profile(account_id: str):
    profile_path = CHROME_PROFILE_DIR / account_id
    
    if not profile_path.exists():
        return
    
    try:
        shutil.rmtree(profile_path)
        log(f"프로필 삭제 완료", account_id)
    except OSError as e:
        raise Exception(f"프로필 삭제 실패 - 브라우저를 수동으로 닫아주세요: {e}")


def convert_account_df_to_list(account_df: pd.DataFrame) -> List[Dict]:
    """account_df를 ACCOUNT_LIST 형태로 변환 (coupangeats 필터)"""
    filtered_df = account_df[account_df["channel"] == "coupangeats"].copy()
    
    account_list = []
    for _, row in filtered_df.iterrows():
        store_ids_raw = str(row["store_ids"])
        store_ids = [s.strip() for s in store_ids_raw.split(",") if s.strip()]
        
        account_list.append({
            "id": row["id"],
            "pw": row["pw"],
            "store_ids": store_ids,
        })
    
    return account_list


def split_into_random_batches(items: List, size_range: tuple) -> List[List]:
    min_size, max_size = size_range
    batches = []
    remaining = list(items)
    
    while remaining:
        batch_size = min(random.randint(min_size, max_size), len(remaining))
        batches.append(remaining[:batch_size])
        remaining = remaining[batch_size:]
    
    return batches


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser(account_id: str):
    log(f"브라우저 실행 시도 (headless)", account_id)
    
    options = uc.ChromeOptions()
    
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists():
        options.binary_location = chrome_bin
        log(f"Chrome 바이너리: {chrome_bin}", account_id)
    else:
        log(f"경고: Chrome 바이너리를 찾을 수 없음 ({chrome_bin})", account_id)
    
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=ko-KR')
    options.add_argument(f'--user-agent={DEFAULT_WIN_CHROME_UA}')
    
    profile_path = CHROME_PROFILE_DIR / account_id
    profile_path.mkdir(parents=True, exist_ok=True)
    options.add_argument(f'--user-data-dir={profile_path.absolute()}')
    
    try:
        driver = uc.Chrome(options=options, version_main=None)
        log(f"브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"브라우저 실행 실패: {e}", account_id)
        raise


def prefetch_akamai_cookies(account_id: str) -> List[Dict[str, str]]:
    """curl_cffi로 로그인 페이지를 미리 로드하여 Akamai 쿠키를 획득"""
    headers = {
        "User-Agent": DEFAULT_WIN_CHROME_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    cookies: List[Dict[str, str]] = []
    try:
        resp = requests.get(COUPANG_LOGIN_URL, headers=headers, impersonate='chrome120') if IS_CURL_CFFI_AVAILABLE else requests.get(COUPANG_LOGIN_URL, headers=headers)
        text_preview = resp.text[:200] if hasattr(resp, 'text') else ''
        log(f"Akamai 프리페치 응답코드: {resp.status_code}, 미리보기: {text_preview.replace(chr(10),' ')[:120]}", account_id)
        
        for c in resp.cookies:
            cookies.append({
                "name": c.name,
                "value": c.value,
                "path": c.path or "/",
            })
        if cookies:
            names = ", ".join([c['name'] for c in cookies])
            log(f"Akamai 쿠키 획득: {names}", account_id)
    except Exception as e:
        log(f"Akamai 프리페치 실패: {e}", account_id)
    return cookies


def inject_cookies_to_driver(driver, cookies: List[Dict[str, str]], account_id: str):
    if not cookies:
        return
    try:
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "path": c.get("path", "/")})
        log(f"브라우저에 Akamai 쿠키 주입 완료 ({len(cookies)}개)", account_id)
    except Exception as e:
        log(f"브라우저 쿠키 주입 실패: {e}", account_id)


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def login_coupang(driver, account_id: str, password: str) -> bool:
    log(f"로그인 시도", account_id)
    wait = WebDriverWait(driver, 30)
    
    try:
        cookies = prefetch_akamai_cookies(account_id)
        driver.get("https://store.coupangeats.com/")
        random_delay("page_load")
        inject_cookies_to_driver(driver, cookies, account_id)
        driver.get(COUPANG_LOGIN_URL)
        random_delay("page_load")
    except WebDriverException as e:
        log(f"  [실패] 로그인 페이지 이동 실패: {e}", account_id)
        return False
    
    def _find_login_input():
        try:
            return wait.until(EC.presence_of_element_located((By.ID, "loginId")))
        except TimeoutException:
            return None

    try:
        id_input = _find_login_input()
        if id_input is None:
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            for idx, frame in enumerate(frames):
                driver.switch_to.frame(frame)
                id_input = _find_login_input()
                if id_input:
                    log(f"  iframe[{idx}]에서 loginId 찾음", account_id)
                    break
                driver.switch_to.default_content()

        if id_input is None:
            page_title = driver.title
            snippet = driver.page_source[:1000]
            log(f"  [실패] loginId 필드 찾기 실패 (URL={driver.current_url}, title={page_title})", account_id)
            log(f"  [페이지 일부]\n{snippet}", account_id)
            return False

        human_type(id_input, account_id)
        log(f"  ID 입력 완료", account_id)
        driver.switch_to.default_content()
    except Exception as e:
        log(f"  [실패] ID 입력 중 오류: {e}", account_id)
        return False
    
    try:
        pw_input = driver.find_element(By.ID, "password")
        human_type(pw_input, password)
        log(f"  비밀번호 입력 완료", account_id)
    except NoSuchElementException:
        log(f"  [실패] password 필드 찾기 실패 (URL={driver.current_url})", account_id)
        return False
    except Exception as e:
        log(f"  [실패] 비밀번호 입력 중 오류: {e}", account_id)
        return False
    
    try:
        random_delay("before_click")
        login_btn = driver.find_element(By.CSS_SELECTOR, ".merchant-submit-btn")
        login_btn.click()
        log(f"  로그인 버튼 클릭", account_id)
    except Exception as e:
        log(f"  [실패] 로그인 버튼 클릭 오류: {e}", account_id)
        return False
    
    random_delay("login_wait")
    current_url = driver.current_url
    
    if is_on_success_page(current_url):
        log(f"  로그인 성공 (URL 매칭)", account_id)
        return True
    
    if not is_on_login_page(current_url):
        log(f"  로그인 성공 (URL 변경 확인)", account_id)
        return True
    
    random_delay("page_load")
    current_url = driver.current_url
    page_title = driver.title
    log(f"  로그인 재확인 URL={current_url}, title={page_title}", account_id)
    
    if is_on_login_page(current_url):
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text[:500]
            log(f"  [실패] 로그인 실패 메시지 스니펫: {body_text}", account_id)
        except Exception:
            pass
        log(f"  [실패] 로그인 실패 - ID/PW 오류 또는 캡차", account_id)
        return False
    
    log(f"  로그인 성공", account_id)
    return True


# ============================================================================
# 인증 정보 추출
# ============================================================================

def extract_auth_data(driver, account_id: str) -> Dict[str, str]:
    log(f"인증 정보 추출", account_id)
    
    user_agent = driver.execute_script("return navigator.userAgent")
    
    all_cookies = driver.get_cookies()
    if not all_cookies:
        raise Exception("쿠키가 비어있음 - 로그인 상태 확인 필요")
    
    cookie_string = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
    
    chrome_match = re.search(r'Chrome/(\d+)\.', user_agent)
    chrome_version = chrome_match.group(1) if chrome_match else "120"
    sec_ch_ua = f'"Google Chrome";v="{chrome_version}", "Chromium";v="{chrome_version}", "Not A(Brand";v="24"'
    
    log(f"  쿠키 {len(all_cookies)}개, Chrome v{chrome_version}", account_id)
    
    return {
        "user_agent": user_agent,
        "cookie_string": cookie_string,
        "sec_ch_ua": sec_ch_ua,
    }


def build_api_headers(auth_data: Dict, referer_url: str) -> Dict[str, str]:
    return {
        "Cookie": auth_data["cookie_string"],
        "User-Agent": auth_data["user_agent"],
        "Referer": referer_url,
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ko-KR",
        "sec-ch-ua": auth_data["sec_ch_ua"],
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "priority": "u=1, i",
    }


# ============================================================================
# API 호출
# ============================================================================

def call_api(url: str, headers: Dict) -> requests.Response:
    if IS_CURL_CFFI_AVAILABLE:
        return requests.get(url, headers=headers, impersonate='chrome120')
    return requests.get(url, headers=headers)


def validate_api_response(response, store_id: str):
    if response.status_code == HTTP_FORBIDDEN:
        raise Exception(f"API 403 Forbidden - Akamai 차단 (매장: {store_id})")
    
    if response.status_code == HTTP_UNAUTHORIZED:
        raise Exception(f"API 401 Unauthorized - 인증 실패 (매장: {store_id})")
    
    if response.status_code == HTTP_NOT_FOUND:
        raise Exception(f"API 404 Not Found (매장: {store_id})")
    
    if response.status_code != HTTP_OK:
        raise Exception(f"API 오류 (상태코드: {response.status_code}, 매장: {store_id})")


def fetch_store_stats(auth_data: Dict, account_id: str, store_id: str) -> Dict[str, Any]:
    """통계 데이터 수집"""
    log(f"  통계 API 호출 (매장: {store_id})", account_id)
    
    referer_url = COUPANG_ORDERS_URL_TEMPLATE.format(store_id=store_id)
    headers = build_api_headers(auth_data, referer_url)
    
    random_delay("api_call")
    response = call_api(COUPANG_STATS_API_URL, headers)
    validate_api_response(response, store_id)
    
    json_data = response.json()
    condition_data = json_data.get('condition', {})
    
    stats = {
        'account_id': account_id,
        'store_id': store_id,
        'platform': 'coupangeats',
        'collected_at': datetime.now().isoformat(),
        'avg_order_amount': condition_data.get('avgOrderAmount', 0.0),
        'total_order_count': condition_data.get('totalOrderCount', 0),
    }
    
    log(f"    평균 주문 금액: {stats['avg_order_amount']:,.0f}원", account_id)
    return stats


def download_sales_file(auth_data: Dict, account_id: str, store_id: str, target_date: str) -> Path:
    """파일 다운로드"""
    log(f"  파일 다운로드 (매장: {store_id})", account_id)
    
    referer_url = COUPANG_ORDERS_URL_TEMPLATE.format(store_id=store_id)
    headers = build_api_headers(auth_data, referer_url)
    api_url = COUPANG_DOWNLOAD_API_URL_TEMPLATE.format(target_date=target_date)
    
    random_delay("file_download")
    response = call_api(api_url, headers)
    validate_api_response(response, store_id)
    
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    filename = DOWNLOAD_DIR / f"coupang_{account_id}_{store_id}_{target_date}.xlsx"
    
    with open(filename, "wb") as f:
        f.write(response.content)
    
    file_size = filename.stat().st_size
    if file_size < MIN_VALID_FILE_SIZE:
        raise Exception(f"파일 크기 이상 ({file_size} bytes, 매장: {store_id})")
    
    log(f"    저장 완료: {filename.name} ({file_size:,} bytes)", account_id)
    return filename


# ============================================================================
# 단일 매장 / 단일 계정 처리
# ============================================================================

def create_empty_store_result(store_id: str) -> Dict[str, Any]:
    return {
        "store_id": store_id,
        "success": False,
        "filename": None,
        "stats": None,
        "error": None,
    }


def process_single_store(
    auth_data: Dict,
    account_id: str,
    store_id: str,
    mode: str = MODE_ALL,
    target_date: str = None,
) -> Dict[str, Any]:
    """단일 매장 처리 (mode에 따라 동작)"""
    result = create_empty_store_result(store_id)
    
    try:
        # 통계 수집 (stats 또는 all)
        if mode in [MODE_STATS, MODE_ALL]:
            stats = fetch_store_stats(auth_data, account_id, store_id)
            result["stats"] = stats
            log(f"  매장 {store_id} 통계 수집 완료", account_id)
        
        # 파일 다운로드 (download 또는 all)
        if mode in [MODE_DOWNLOAD, MODE_ALL]:
            if not target_date:
                raise ValueError("download 모드에는 target_date 필요")
            filename = download_sales_file(auth_data, account_id, store_id, target_date)
            result["filename"] = str(filename)
            log(f"  매장 {store_id} 파일 다운로드 완료", account_id)
        
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        log(f"  매장 {store_id} 처리 실패: {e}", account_id)
    
    return result


def create_account_result(account_id: str, store_ids: List[str]) -> Dict[str, Any]:
    return {
        "success": False,
        "account_id": account_id,
        "total_stores": len(store_ids),
        "success_stores": 0,
        "failed_stores": len(store_ids),
        "stores": [],
        "stats_list": [],
        "error": None,
    }


def process_single_account(
    account: Dict,
    mode: str = MODE_ALL,
    target_date: str = None,
) -> Dict[str, Any]:
    """단일 계정 처리 (mode에 따라 동작)"""
    account_id = account["id"]
    password = account["pw"]
    store_ids = account.get("store_ids", [])
    
    result = create_account_result(account_id, store_ids)
    
    if not store_ids:
        result["error"] = "매장 ID가 없습니다"
        return result
    
    driver = None
    
    try:
        log(f"처리 시작 (매장 {len(store_ids)}개: {store_ids})", account_id)
        
        clean_chrome_profile(account_id)
        driver = launch_browser(account_id)
        
        if not login_coupang(driver, account_id, password):
            result["error"] = "로그인 실패"
            return result
        
        auth_data = extract_auth_data(driver, account_id)
        
        for idx, store_id in enumerate(store_ids):
            log(f"{'─' * 30}", account_id)
            log(f"매장 {idx + 1}/{len(store_ids)} 처리 (ID: {store_id})", account_id)
            
            if idx > 0:
                random_delay("store_switch")
            
            store_result = process_single_store(
                auth_data=auth_data,
                account_id=account_id,
                store_id=store_id,
                mode=mode,
                target_date=target_date,
            )
            result["stores"].append(store_result)
            
            if store_result.get("stats"):
                result["stats_list"].append(store_result["stats"])
        
        success_count = sum(1 for s in result["stores"] if s["success"])
        result["success_stores"] = success_count
        result["failed_stores"] = len(store_ids) - success_count
        result["success"] = (result["failed_stores"] == 0)
        
        log(f"{'─' * 30}", account_id)
        log(f"계정 처리 완료: 성공 {success_count}/{len(store_ids)} 매장", account_id)
        
    except Exception as e:
        result["error"] = str(e)
        log(f"처리 중단: {e}", account_id)
        
    finally:
        if driver:
            try:
                driver.quit()
            finally:
                if hasattr(driver, "xvfb_proc"):
                    driver.xvfb_proc.terminate()
                log(f"브라우저 종료", account_id)
    
    return result


# ============================================================================
# 배치 처리
# ============================================================================

def process_account_with_delay(
    account: Dict,
    mode: str,
    target_date: str,
    start_delay: float,
) -> Dict:
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    
    return process_single_account(account, mode, target_date)


def process_batch(
    batch: List[Dict],
    mode: str,
    target_date: str,
) -> List[Dict]:
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
                mode,
                target_date,
                cumulative_delay,
            )
            futures.append(future)
        
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


# ============================================================================
# 메인 함수 (Airflow에서 호출)
# ============================================================================

def run_coupang_crawling(
    account_df: pd.DataFrame,
    mode: str = MODE_ALL,
    target_date: str = None,
) -> pd.DataFrame:
    """
    쿠팡이츠 크롤링 메인 함수
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw, store_ids)
        mode: 작업 모드
            - "stats": 통계만 수집
            - "download": 파일만 다운로드
            - "all": 둘 다 (기본값)
        target_date: 다운로드 대상 월 (예: "2025-07") - download/all 모드 필수
    
    Returns:
        stats_df: 수집된 통계 DataFrame (stats/all 모드)
    """
    # 유효성 검사
    if mode in [MODE_DOWNLOAD, MODE_ALL] and not target_date:
        raise ValueError(f"'{mode}' 모드에서는 target_date가 필요합니다")
    
    log("=" * 60)
    log(f"쿠팡이츠 크롤링 시작 (mode: {mode})")
    log("=" * 60)
    
    account_list = convert_account_df_to_list(account_df)
    total_stores = sum(len(acc["store_ids"]) for acc in account_list)
    
    log(f"  작업 모드: {mode}")
    if target_date:
        log(f"  대상 월: {target_date}")
    log(f"  계정 수: {len(account_list)}개")
    log(f"  매장 수: {total_stores}개")
    log(f"  TLS 우회: {'curl_cffi 사용' if IS_CURL_CFFI_AVAILABLE else '미사용 (403 가능)'}")
    
    if not account_list:
        log("coupangeats 계정이 없습니다.")
        return pd.DataFrame()
    
    batches = split_into_random_batches(account_list, BATCH_SIZE_RANGE)
    log(f"  배치 분할: {[len(b) for b in batches]}")
    
    all_results = []
    all_stats = []
    
    for batch_idx, batch in enumerate(batches):
        log(f"\n{'=' * 40}")
        log(f"배치 {batch_idx + 1}/{len(batches)} 시작 ({len(batch)}개 계정)")
        log(f"{'=' * 40}")
        
        batch_results = process_batch(batch, mode, target_date)
        all_results.extend(batch_results)
        
        for result in batch_results:
            all_stats.extend(result.get("stats_list", []))
        
        is_not_last_batch = batch_idx < len(batches) - 1
        if is_not_last_batch:
            delay = random.uniform(*TIMING["batch_rest"])
            log(f"다음 배치까지 {delay:.1f}초 대기...")
            time.sleep(delay)
    
    stats_df = pd.DataFrame(all_stats) if all_stats else pd.DataFrame()
    
    total_success = sum(r["success_stores"] for r in all_results)
    total_failed = sum(r["failed_stores"] for r in all_results)
    
    log("\n" + "=" * 60)
    log("쿠팡이츠 크롤링 완료")
    log(f"  매장: 성공 {total_success}개, 실패 {total_failed}개")
    if mode in [MODE_STATS, MODE_ALL]:
        log(f"  통계: {len(stats_df)}행")
    log("=" * 60)
    
    return stats_df


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    
    test_account_df = pd.DataFrame([
        {
            "channel": "coupangeats",
            "id": "doridang04",
            "pw": "ehfl3652!",
            "store_ids": "814147, 814150",
        },
    ])
    
    # 테스트 1: 통계만
    print("\n[테스트 1] 통계만 수집")
    stats_df = run_coupang_crawling(test_account_df, mode="stats")
    print(stats_df)
    
    # 테스트 2: 다운로드만
    # print("\n[테스트 2] 파일만 다운로드")
    # run_coupang_crawling(test_account_df, mode="download", target_date="2025-07")
    
    # 테스트 3: 둘 다
    # print("\n[테스트 3] 둘 다")
    # stats_df = run_coupang_crawling(test_account_df, mode="all", target_date="2025-07")