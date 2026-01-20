"""
[MODIFY] 사이트명 크롤링 - Airflow 연동용
예: 배민, 요기요, 네이버 등
두 템플릿 차이점

Airflow용: Docker 환경 변수 지원, headless 고정, 배치 처리/병렬 실행, account_df DataFrame 입력
Local 테스트용: HEADLESS_MODE = False로 브라우저 확인 가능, 디버깅 출력 강화, test_login_only(), test_api_only() 분리 테스트 함수 제공

추천 작업 순서

Local 버전에서 HEADLESS_MODE = False로 셀렉터 찾기
브라우저 개발자도구(F12)로 ID/PW/버튼 셀렉터 확인
test_login_only()로 로그인만 먼저 테스트
성공하면 API URL 확인 후 test_api_only()로 테스트
완성되면 Airflow 버전에 복사

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
# 상수 - [MODIFY] 사이트별 수정 필요
# ============================================================================

# [MODIFY] 플랫폼 식별자 (파일명, 로그 등에 사용)
PLATFORM_NAME = "your_platform"  # 예: "baemin", "yogiyo", "naver"

# [MODIFY] URL - 사이트에 맞게 수정
LOGIN_URL = "https://example.com/login"
MAIN_PAGE_URL = "https://example.com/main"  # 로그인 후 이동할 페이지
ORDERS_URL_TEMPLATE = "https://example.com/orders/{store_id}"  # store_id가 필요 없으면 고정 URL
STATS_API_URL = "https://example.com/api/stats"  # 통계 API (없으면 None)
DOWNLOAD_API_URL_TEMPLATE = "https://example.com/api/download?date={target_date}"  # 다운로드 API

# HTTP 상태코드 (공통)
HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404

# 타이밍 (밴 방지) - 사이트 특성에 따라 조절
TIMING = {
    "login_wait": (8.0, 12.0),      # 로그인 후 대기
    "page_load": (4.0, 6.0),        # 페이지 로드 대기
    "typing_char": (0.05, 0.15),    # 타이핑 간격
    "typing_pause": (0.3, 0.6),     # 타이핑 전후 대기
    "before_click": (0.5, 1.0),     # 클릭 전 대기
    "api_call": (1.0, 2.0),         # API 호출 간격
    "file_download": (2.0, 3.5),    # 파일 다운로드 대기
    "store_switch": (3.0, 6.0),     # 매장 전환 대기
    "account_stagger": (3.0, 7.0),  # 계정간 시작 간격
    "batch_rest": (45.0, 90.0),     # 배치간 휴식
}

# 배치 설정
BATCH_SIZE_RANGE = (1, 2)

# 경로 (Docker 환경 고려)
import os

BASE_DIR = os.getenv("AIRFLOW_HOME", Path.cwd())
CHROME_PROFILE_DIR = Path(os.getenv("CHROME_PROFILE_DIR", f"{BASE_DIR}/chrome_profiles"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", f"{BASE_DIR}/download"))


DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# 파일 최소 크기 (bytes)
MIN_VALID_FILE_SIZE = 100

# [MODIFY] 로그인 판별 URL 패턴 - 사이트에 맞게 수정
LOGIN_FAIL_URL_PATTERNS = ["/login", "/signin", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/orders", "/management", "/dashboard", "/home", "/main"]

# 브라우저 UA
DEFAULT_WIN_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ============================================================================
# 유틸리티 (공통 - 수정 불필요)
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] [{account_id}] {message}"
    print(log_msg)
    


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


def convert_account_df_to_list(account_df: pd.DataFrame, channel_name: str) -> List[Dict]:
    """
    account_df를 ACCOUNT_LIST 형태로 변환
    
    [MODIFY] channel_name을 플랫폼에 맞게 전달
    account_df 컬럼: channel, id, pw, store_ids
    """
    filtered_df = account_df[account_df["channel"] == channel_name].copy()
    
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


def prefetch_cookies(account_id: str) -> List[Dict[str, str]]:
    """
    [MODIFY] 필요시 프리페치 URL 변경
    curl_cffi로 로그인 페이지를 미리 로드하여 쿠키를 획득
    """
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
        resp = requests.get(LOGIN_URL, headers=headers, impersonate='chrome120') if IS_CURL_CFFI_AVAILABLE else requests.get(LOGIN_URL, headers=headers)
        text_preview = resp.text[:200] if hasattr(resp, 'text') else ''
        log(f"프리페치 응답코드: {resp.status_code}, 미리보기: {text_preview.replace(chr(10),' ')[:120]}", account_id)
        
        for c in resp.cookies:
            cookies.append({
                "name": c.name,
                "value": c.value,
                "path": c.path or "/",
            })
        if cookies:
            names = ", ".join([c['name'] for c in cookies])
            log(f"쿠키 획득: {names}", account_id)
    except Exception as e:
        log(f"프리페치 실패: {e}", account_id)
    return cookies


def inject_cookies_to_driver(driver, cookies: List[Dict[str, str]], account_id: str):
    if not cookies:
        return
    try:
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "path": c.get("path", "/")})
        log(f"브라우저에 쿠키 주입 완료 ({len(cookies)}개)", account_id)
    except Exception as e:
        log(f"브라우저 쿠키 주입 실패: {e}", account_id)


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def do_login(driver, account_id: str, password: str) -> bool:
    """
    [MODIFY] 로그인 로직 - 사이트별 셀렉터 수정 필요
    """
    log(f"로그인 시도", account_id)
    wait = WebDriverWait(driver, 30)
    
    try:
        # 1) 쿠키 프리페치 후 주입
        cookies = prefetch_cookies(account_id)
        
        # [MODIFY] 메인 도메인으로 먼저 이동 (쿠키 주입을 위해)
        driver.get(MAIN_PAGE_URL.rsplit('/', 1)[0] + "/")  # 도메인 루트
        random_delay("page_load")
        inject_cookies_to_driver(driver, cookies, account_id)
        
        # 2) 로그인 페이지 진입
        driver.get(LOGIN_URL)
        random_delay("page_load")
    except WebDriverException as e:
        log(f"  [실패] 로그인 페이지 이동 실패: {e}", account_id)
        return False
    
    # [MODIFY] ID 입력 필드 셀렉터 - 사이트에 맞게 수정
    # 예: By.ID, "loginId" / By.NAME, "username" / By.CSS_SELECTOR, "input[type='email']"
    ID_FIELD_LOCATOR = (By.ID, "loginId")  # [MODIFY]
    
    def _find_login_input():
        try:
            return wait.until(EC.presence_of_element_located(ID_FIELD_LOCATOR))
        except TimeoutException:
            return None

    try:
        id_input = _find_login_input()
        if id_input is None:
            # iframe 내부 확인
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            for idx, frame in enumerate(frames):
                driver.switch_to.frame(frame)
                id_input = _find_login_input()
                if id_input:
                    log(f"  iframe[{idx}]에서 ID필드 찾음", account_id)
                    break
                driver.switch_to.default_content()

        if id_input is None:
            page_title = driver.title
            snippet = driver.page_source[:1000]
            log(f"  [실패] ID 필드 찾기 실패 (URL={driver.current_url}, title={page_title})", account_id)
            log(f"  [페이지 일부]\n{snippet}", account_id)
            return False

        human_type(id_input, account_id)
        log(f"  ID 입력 완료", account_id)
        driver.switch_to.default_content()
    except Exception as e:
        log(f"  [실패] ID 입력 중 오류: {e}", account_id)
        return False
    
    # [MODIFY] 비밀번호 입력 필드 셀렉터
    PW_FIELD_LOCATOR = (By.ID, "password")  # [MODIFY]
    
    try:
        pw_input = driver.find_element(*PW_FIELD_LOCATOR)
        human_type(pw_input, password)
        log(f"  비밀번호 입력 완료", account_id)
    except NoSuchElementException:
        log(f"  [실패] password 필드 찾기 실패 (URL={driver.current_url})", account_id)
        return False
    except Exception as e:
        log(f"  [실패] 비밀번호 입력 중 오류: {e}", account_id)
        return False
    
    # [MODIFY] 로그인 버튼 셀렉터
    LOGIN_BTN_LOCATOR = (By.CSS_SELECTOR, ".merchant-submit-btn")  # [MODIFY]
    
    try:
        random_delay("before_click")
        login_btn = driver.find_element(*LOGIN_BTN_LOCATOR)
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
# 인증 정보 추출 (공통)
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
    """
    [MODIFY] 사이트에서 요구하는 추가 헤더가 있으면 여기에 추가
    """
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
        # [MODIFY] 사이트별 추가 헤더 (예: X-Requested-With, Authorization 등)
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
        raise Exception(f"API 403 Forbidden - 차단됨 (매장: {store_id})")
    
    if response.status_code == HTTP_UNAUTHORIZED:
        raise Exception(f"API 401 Unauthorized - 인증 실패 (매장: {store_id})")
    
    if response.status_code == HTTP_NOT_FOUND:
        raise Exception(f"API 404 Not Found (매장: {store_id})")
    
    if response.status_code != HTTP_OK:
        raise Exception(f"API 오류 (상태코드: {response.status_code}, 매장: {store_id})")


def fetch_store_stats(auth_data: Dict, account_id: str, store_id: str) -> Dict[str, Any]:
    """
    [MODIFY] 통계 API 호출 및 파싱 - 사이트별 수정 필요
    통계 API가 없는 경우 이 함수를 비워두거나 삭제
    """
    log(f"  통계 API 호출 (매장: {store_id})", account_id)
    
    # [MODIFY] Referer URL 형식
    referer_url = ORDERS_URL_TEMPLATE.format(store_id=store_id)
    headers = build_api_headers(auth_data, referer_url)
    
    random_delay("api_call")
    
    # [MODIFY] 통계 API가 없으면 아래 주석 처리하고 빈 dict 반환
    if STATS_API_URL is None:
        return {}
    
    response = call_api(STATS_API_URL, headers)
    validate_api_response(response, store_id)
    
    json_data = response.json()
    
    # [MODIFY] 응답 JSON 구조에 맞게 파싱
    # 예: condition_data = json_data.get('condition', {})
    stats = {
        'account_id': account_id,
        'store_id': store_id,
        'platform': PLATFORM_NAME,
        'collected_at': datetime.now().isoformat(),
        # [MODIFY] 필요한 필드 추가
        # 'avg_order_amount': json_data.get('avgOrderAmount', 0.0),
        # 'total_order_count': json_data.get('totalOrderCount', 0),
    }
    
    log(f"    통계 수집 완료", account_id)
    return stats


def download_sales_file(auth_data: Dict, account_id: str, store_id: str, target_date: str) -> Path:
    """
    [MODIFY] 다운로드 API 호출 - 사이트별 수정 필요
    """
    log(f"  파일 다운로드 (매장: {store_id})", account_id)
    
    # [MODIFY] Referer URL 형식
    referer_url = ORDERS_URL_TEMPLATE.format(store_id=store_id)
    headers = build_api_headers(auth_data, referer_url)
    
    # [MODIFY] 다운로드 API URL 생성 - 사이트에 맞게 파라미터 수정
    api_url = DOWNLOAD_API_URL_TEMPLATE.format(target_date=target_date, store_id=store_id)
    
    random_delay("file_download")
    response = call_api(api_url, headers)
    validate_api_response(response, store_id)
    
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    
    # [MODIFY] 파일 확장자 (xlsx, csv, pdf 등)
    file_extension = "xlsx"  # [MODIFY]
    filename = DOWNLOAD_DIR / f"{PLATFORM_NAME}_{account_id}_{store_id}_{target_date}.{file_extension}"
    
    with open(filename, "wb") as f:
        f.write(response.content)
    
    file_size = filename.stat().st_size
    if file_size < MIN_VALID_FILE_SIZE:
        raise Exception(f"파일 크기 이상 ({file_size} bytes, 매장: {store_id})")
    
    log(f"    저장 완료: {filename.name} ({file_size:,} bytes)", account_id)
    return filename


# ============================================================================
# 단일 매장 / 단일 계정 처리 (공통)
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
    target_date: str,
) -> Dict[str, Any]:
    result = create_empty_store_result(store_id)
    
    try:
        stats = fetch_store_stats(auth_data, account_id, store_id)
        result["stats"] = stats
        
        filename = download_sales_file(auth_data, account_id, store_id, target_date)
        result["filename"] = str(filename)
        result["success"] = True
        
        log(f"  매장 {store_id} 처리 완료", account_id)
        
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


def process_single_account(account: Dict, target_date: str) -> Dict[str, Any]:
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
        
        if not do_login(driver, account_id, password):
            result["error"] = "로그인 실패"
            return result
        
        auth_data = extract_auth_data(driver, account_id)
        
        for idx, store_id in enumerate(store_ids):
            log(f"{'─' * 30}", account_id)
            log(f"매장 {idx + 1}/{len(store_ids)} 처리 (ID: {store_id})", account_id)
            
            is_not_first_store = idx > 0
            if is_not_first_store:
                random_delay("store_switch")
            
            store_result = process_single_store(auth_data, account_id, store_id, target_date)
            result["stores"].append(store_result)
            
            if store_result["stats"]:
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
# 배치 처리 (공통)
# ============================================================================

def process_account_with_delay(account: Dict, target_date: str, start_delay: float) -> Dict:
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    
    return process_single_account(account, target_date)


def process_batch(batch: List[Dict], target_date: str) -> List[Dict]:
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

def run_crawling(account_df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """
    [MODIFY] 함수명을 플랫폼에 맞게 변경 가능
    예: run_baemin_crawling, run_yogiyo_crawling
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw, store_ids)
        target_date: 다운로드 대상 월 (예: "2025-07")
    
    Returns:
        stats_df: 수집된 통계 DataFrame
    """
    log("=" * 60)
    log(f"{PLATFORM_NAME} 크롤링 시작")
    log("=" * 60)
    
    account_list = convert_account_df_to_list(account_df, PLATFORM_NAME)
    
    total_stores = sum(len(acc["store_ids"]) for acc in account_list)
    
    log(f"  대상 월: {target_date}")
    log(f"  계정 수: {len(account_list)}개")
    log(f"  매장 수: {total_stores}개")
    log(f"  TLS 우회: {'curl_cffi 사용' if IS_CURL_CFFI_AVAILABLE else '미사용 (403 가능)'}")
    
    if not account_list:
        log(f"{PLATFORM_NAME} 계정이 없습니다.")
        return pd.DataFrame()
    
    batches = split_into_random_batches(account_list, BATCH_SIZE_RANGE)
    log(f"  배치 분할: {[len(b) for b in batches]}")
    
    all_results = []
    all_stats = []
    
    for batch_idx, batch in enumerate(batches):
        log(f"\n{'=' * 40}")
        log(f"배치 {batch_idx + 1}/{len(batches)} 시작 ({len(batch)}개 계정)")
        log(f"{'=' * 40}")
        
        batch_results = process_batch(batch, target_date)
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
    log(f"{PLATFORM_NAME} 크롤링 완료")
    log(f"  매장: 성공 {total_success}개, 실패 {total_failed}개")
    log(f"  통계: {len(stats_df)}행")
    log("=" * 60)
    
    return stats_df


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    
    # [MODIFY] 테스트용 계정 정보
    test_account_df = pd.DataFrame([
        {
            "channel": PLATFORM_NAME,  # [MODIFY] 채널명
            "id": "test_account",      # [MODIFY] 계정 ID
            "pw": "test_password",     # [MODIFY] 비밀번호
            "store_ids": "store1, store2",  # [MODIFY] 매장 ID들
        },
    ])
    
    # [MODIFY] 대상 날짜
    TARGET_DATE = "2025-07"
    
    print("\n" + "=" * 70)
    print("테스트용 account_df:")
    print("=" * 70)
    print(test_account_df.to_string(index=False))
    print()
    
    stats_df = run_crawling(test_account_df, TARGET_DATE)
    
    print("\n" + "=" * 70)
    print("수집 결과 (stats_df):")
    print("=" * 70)
    
    if stats_df.empty:
        print("수집된 데이터가 없습니다.")
    else:
        print(f"총 {len(stats_df)}개 매장 수집 완료\n")
        print(stats_df.to_string(index=False))
        
        print("\n[컬럼 목록]")
        for col in stats_df.columns:
            print(f"  - {col}")


# ============================================================================
# 사용법 (Airflow DAG에서)
# ============================================================================
# from crawl_template_airflow import run_crawling
# stats_df = run_crawling(account_df, target_date="2025-07")