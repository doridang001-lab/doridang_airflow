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

import subprocess
import time
import random
import re
import shutil
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
from datetime import timezone
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from modules.transform.utility.selenium_uc import launch_uc_chrome

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
COUPANG_LOGIN_URL = "https://store.coupangeats.com/merchant/login"
COUPANG_ORDERS_URL_TEMPLATE = "https://store.coupangeats.com/merchant/management/orders/{store_id}"
COUPANG_STATS_API_URL = "https://store.coupangeats.com/api/v1/merchant/web/order/condition"
COUPANG_DOWNLOAD_API_URL_TEMPLATE = "https://store.coupangeats.com/api/v1/merchant/web/emails?type=salesOrder&action=download&downloadRequestDate={target_date}"
ORDERS_BASE_URL = "https://store.coupangeats.com/merchant/management/orders"
CMG_REVAMP_URL = "https://store.coupangeats.com/merchant/management/cmg/?cmg_revamp=%2F"


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

_CRASH_KEYWORDS = (
    "Remote end closed",
    "Connection aborted",
    "RemoteDisconnected",
    "Connection refused",
    "Max retries exceeded",
    "NewConnectionError",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "Timed out receiving message from renderer",
)


def is_driver_crash_error(exc: Exception) -> bool:
    return any(keyword in str(exc) for keyword in _CRASH_KEYWORDS)


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
_PROFILE_FALLBACK_ACCOUNTS: set[str] = set()


# ============================================================================
# 상수 - 기타
# ============================================================================
MIN_VALID_FILE_SIZE = 100

_ORDER_COLUMNS = [
    "collected_at",
    "store_id",
    "store_name",
    "order_date",
    "order_id",
    "delivery_type",
    "order_status",
    "order_summary",
    "total_price",
    "is_cancelled",
    "menu_name",
    "menu_qty",
    "menu_price",
    "menu_options",
    "매출액",
    "상점부담_쿠폰",
    "중개_이용료",
    "결제대행사_수수료",
    "배달비",
    "광고비",
    "부가세",
    "즉시할인금액",
    "정산_예정_금액",
    "취소금액",
]

_SETTLEMENT_KEYS = [
    "매출액", "상점부담_쿠폰", "중개_이용료", "결제대행사_수수료", "배달비",
    "광고비", "부가세", "즉시할인금액", "정산_예정_금액", "취소금액",
]

_SETTLEMENT_LABEL_MAP = {
    "매출액":       ["매출액", "주문금액"],
    "상점부담_쿠폰": ["상점부담 쿠폰", "가맹점부담쿠폰", "상점부담쿠폰"],
    "중개_이용료":   ["중개 이용료", "중개이용료", "플랫폼이용료"],
    "결제대행사_수수료": ["결제대행사 수수료", "결제대행사수수료", "PG수수료"],
    "배달비":       ["배달비"],
    "광고비":       ["광고비"],
    "부가세":       ["부가세"],
    "즉시할인금액":  ["즉시할인금액", "즉시할인"],
    "정산_예정_금액": ["정산 예정 금액", "정산예정금액", "정산금액"],
    "취소금액":     ["취소금액"],
}

LOGIN_FAIL_URL_PATTERNS = ["/login", "/signin", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/orders", "/management", "/dashboard", "/home", "/main"]

# 서버측 차단(봇탐지/레이트리밋) 문구 — 자격증명 오류와 구분.
# 발견 시 즉시 중단하고 재시도하지 않는다(재시도는 차단을 깊게 만듦).
BLOCK_PHRASES = [
    "권한이 존재하지 않습니다",   # Akamai/WAF 권한 거부 (실관측)
    "비정상적인",                  # 비정상 접근 차단
    "잠시 후 다시",                # 일시 차단
    "이용이 제한",
    "access denied",
    "forbidden",
]

DEFAULT_WIN_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 실제 실행 환경(Linux 컨테이너)과 일치하는 UA.
# Akamai 봇탐지는 UA-platform 불일치를 강하게 점수화하므로,
# prefetch/API 헤더는 브라우저와 동일한 Linux 플랫폼으로 통일한다.
DEFAULT_LINUX_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
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


def is_attach_mode() -> bool:
    """실제(호스트) 크롬에 attach하는 모드인지."""
    return bool(os.getenv("COUPANG_CHROME_DEBUGGER", "").strip())


def release_driver(driver) -> None:
    """드라이버 해제. attach 모드면 실제 크롬을 닫지 않고 세션만 정리."""
    if driver is None:
        return
    if is_attach_mode():
        # 실제 크롬은 다음 실행에서 재사용해야 하므로 닫지 않는다.
        # chromedriver 서비스만 종료(브라우저는 유지).
        try:
            driver.service.stop()
        except Exception:
            pass
        return
    try:
        driver.quit()
    except Exception:
        pass


def _clean_cache_only(account_id: str) -> None:
    """Chrome 캐시만 삭제 (쿠키 유지 → 세션 재사용).

    attach 모드에서는 사용자의 실제 크롬을 건드리면 안 되므로 즉시 반환.

    Cache, GPUCache, Code Cache 폴더와 SingletonLock 잠금 파일 제거.
    OOM 유발 캐시 누적 해소 + 비정상 종료 후 재실행 크래시 방지.
    """
    if is_attach_mode():
        return  # 실제 호스트 크롬을 pkill/캐시삭제하면 안 됨
    profile_root = CHROME_PROFILE_DIR / account_id
    profile_path_str = str(profile_root.absolute())

    # 이 프로파일을 사용 중인 잔존 Chrome 프로세스 먼저 종료 (크래시 방지)
    try:
        subprocess.run(
            ["pkill", "-f", f"user-data-dir={profile_path_str}"],
            capture_output=True,
            timeout=5,
        )
        time.sleep(1.5)
    except Exception:
        pass
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        lock_file = profile_root / lock_name
        try:
            if lock_file.exists():
                lock_file.unlink()
        except PermissionError as exc:
            _PROFILE_FALLBACK_ACCOUNTS.add(account_id)
            log(f"프로필 잠금 파일 권한 오류, runtime 프로필로 우회: {lock_name} ({exc})", account_id)
            return
        except Exception:
            pass

    profile_default = profile_root / "Default"
    for cache_dir in ("Cache", "GPUCache", "Code Cache"):
        target = profile_default / cache_dir
        try:
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
                log(f"캐시 정리: {cache_dir}", account_id)
        except PermissionError as exc:
            _PROFILE_FALLBACK_ACCOUNTS.add(account_id)
            log(f"캐시 권한 오류, runtime 프로필로 우회: {cache_dir} ({exc})", account_id)
            return


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

def _ensure_xvfb_display() -> str:
    """Xvfb 가상 디스플레이를 시작하고 DISPLAY 환경변수를 반환한다.

    이미 실행 중이면 재사용. 실패 시 빈 문자열 반환.
    Akamai가 --headless=new 를 감지해 React 폼 렌더링을 차단하므로
    Xvfb로 실제 브라우저처럼 실행한다.
    """
    display = ":99"
    try:
        subprocess.Popen(
            ["Xvfb", display, "-screen", "0", "1920x1080x24", "-ac"],
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        time.sleep(1.0)
    except FileNotFoundError:
        return ""
    except Exception:
        pass
    return display


def _attach_to_real_chrome(account_id: str):
    """이미 실행 중인 '진짜 크롬'(호스트)에 CDP debuggerAddress로 attach.

    환경변수 COUPANG_CHROME_DEBUGGER (예: "127.0.0.1:9222") 가 설정되면 사용.
    Akamai Bot Manager가 Docker/Xvfb/SwiftShader 환경을 봇으로 판정하므로,
    수동 로그인이 통과하는 실제 크롬을 그대로 조종해 핑거프린트 문제를 회피한다.
    이 모드에서는 Xvfb/UC/stealth/프로필 설정이 전부 불필요(진짜 브라우저).

    전제(호스트에서 1회 준비):
      chrome.exe --remote-debugging-port=9222 --user-data-dir=<전용프로필>
      → 띄운 크롬에서 쿠팡 1회 로그인 (세션 유지). 최소화해도 됨.
    """
    addr = os.getenv("COUPANG_CHROME_DEBUGGER", "").strip()
    if not addr:
        return None

    from selenium import webdriver as _wd

    log(f"실제 크롬에 attach 시도 (debuggerAddress={addr})", account_id)
    opts = _wd.ChromeOptions()
    opts.add_experimental_option("debuggerAddress", addr)
    # chromedriver는 Selenium Manager가 자동 해결(호스트 크롬 버전과 일치 필요)
    driver = _wd.Chrome(options=opts)
    log("실제 크롬 attach 성공", account_id)
    return driver


def launch_browser(account_id: str):
    # ── attach 모드: 환경변수가 있으면 실제(호스트) 크롬에 붙어서 사용 ──
    attached = _attach_to_real_chrome(account_id)
    if attached is not None:
        return attached

    display = _ensure_xvfb_display()
    if display:
        os.environ["DISPLAY"] = display
        log(f"브라우저 실행 시도 (Xvfb {display})", account_id)
    else:
        log(f"브라우저 실행 시도 (headless fallback)", account_id)

    options = uc.ChromeOptions()

    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists():
        options.binary_location = chrome_bin
        log(f"Chrome 바이너리: {chrome_bin}", account_id)
    else:
        log(f"경고: Chrome 바이너리를 찾을 수 없음 ({chrome_bin})", account_id)

    if not display:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--no-zygote')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-sync')
    options.add_argument('--disk-cache-size=1')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=ko-KR')
    # NOTE: --user-agent 로 Windows UA 를 강제하지 않는다.
    #   Linux 컨테이너에서 도는 Chrome 에 "Windows NT 10.0" UA 를 씌우면
    #   navigator.platform(Linux) 과 UA(Windows) 가 불일치 → Akamai 가 즉시 봇 판정.
    #   네이티브 UA(실제 Chrome 버전 + Linux) 를 그대로 써서 일관성을 유지한다.

    profile_dir_name = f"{account_id}_runtime" if account_id in _PROFILE_FALLBACK_ACCOUNTS else account_id
    profile_path = CHROME_PROFILE_DIR / profile_dir_name
    profile_path.mkdir(parents=True, exist_ok=True)
    options.add_argument(f'--user-data-dir={profile_path.absolute()}')

    try:
        driver = launch_uc_chrome(
            options,
            account_id=account_id,
            chrome_bin=chrome_bin,
            log_fn=lambda message: log(message, account_id),
        )
        _apply_stealth(driver, account_id)
        log(f"브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"브라우저 실행 실패: {e}", account_id)
        raise


def _apply_stealth(driver, account_id: str) -> None:
    """봇탐지 핑거프린트 누수 차단.

    UC 가 navigator.webdriver 는 처리하지만, 컨테이너 환경의 잔여 흔적을 가린다:
      - WebGL vendor/renderer = "Google SwiftShader"(소프트웨어 렌더링) → headless/VM 의 대표 tell.
        실제 GPU 처럼 위장한다.
      - navigator.languages / plugins 비어있음 → 정상 값 주입.
    모든 신규 문서에 navigation 이전 주입(addScriptToEvaluateOnNewDocument).
    """
    stealth_js = r"""
    (() => {
      try { Object.defineProperty(navigator, 'webdriver', {get: () => undefined}); } catch (e) {}
      try { Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']}); } catch (e) {}
      try {
        if (!navigator.plugins || navigator.plugins.length === 0) {
          Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        }
      } catch (e) {}
      // WebGL vendor/renderer 위장 (SwiftShader 노출 차단)
      try {
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (p) {
          if (p === 37445) return 'Intel Inc.';                 // UNMASKED_VENDOR_WEBGL
          if (p === 37446) return 'Intel Iris OpenGL Engine';   // UNMASKED_RENDERER_WEBGL
          return getParameter.call(this, p);
        };
      } catch (e) {}
      try {
        const orig = navigator.permissions && navigator.permissions.query;
        if (orig) {
          navigator.permissions.query = (params) =>
            params && params.name === 'notifications'
              ? Promise.resolve({state: Notification.permission})
              : orig(params);
        }
      } catch (e) {}
    })();
    """
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_js})
        log("stealth 패치 적용 (webgl/languages/plugins)", account_id)
    except Exception as exc:
        log(f"stealth 패치 실패(무시): {exc}", account_id)


def prefetch_akamai_cookies(account_id: str) -> List[Dict[str, str]]:
    """[DEPRECATED — login_coupang에서 더 이상 호출하지 않음]

    배민 로그인 방식으로 전환하면서 제거됨: 외부(curl_cffi)에서 받은 쿠키를
    브라우저에 주입하는 것은 봇탐지 신호이자 핑거프린트 불일치 원인.
    브라우저가 직접 로그인 페이지를 방문해 네이티브 _abck 센서 쿠키를
    생성하도록 두는 것이 더 정상적이다. 재도입 금지.

    UA 는 브라우저와 동일한 Linux 플랫폼으로 통일 (impersonate chrome120 의 JA3 와도 일치).
    Windows UA 로 쿠키를 받아 Linux 브라우저에 주입하면 핑거프린트 불일치를 만든다.
    """
    headers = {
        "User-Agent": DEFAULT_LINUX_CHROME_UA,
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
            if isinstance(c, dict):
                name = c.get("name")
                value = c.get("value")
                path = c.get("path") or "/"
            else:
                name = getattr(c, "name", None)
                value = getattr(c, "value", None)
                path = getattr(c, "path", None) or "/"

            if not name or value is None:
                continue

            cookies.append({
                "name": name,
                "value": value,
                "path": path,
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


def detect_block_text(text: str) -> bool:
    """페이지 텍스트에 쿠팡 서버측 차단 문구가 있으면 True."""
    if not text:
        return False
    lowered = text.lower()
    for phrase in BLOCK_PHRASES:
        if phrase.isascii():
            if phrase in lowered:
                return True
        elif phrase in text:
            return True
    return False


def is_login_blocked(driver) -> bool:
    """현재 driver가 서버측 차단 상태인지 판정 (login_coupang이 설정한 플래그 우선)."""
    if getattr(driver, "_coupang_blocked", False):
        return True
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text[:800]
        return detect_block_text(body_text)
    except Exception:
        return False


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    lowered = url.lower()
    return (not is_on_login_page(lowered)) and any(pattern in lowered for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def _has_authenticated_management_shell(driver) -> bool:
    selectors = [
        ".management-scroll",
        ".management-page",
        ".sales-search-row",
        ".dropdown-btn.highlight",
    ]
    for selector in selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                if element.is_displayed():
                    return True
        except Exception:
            continue

    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        return "매출 관리" in body_text and "홈으로 이동" in body_text
    except Exception:
        return False


def _find_first_visible_in_page(driver, locator_list: List[tuple], account_id: str, timeout: int = 10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        driver.switch_to.default_content()
        for by, value in locator_list:
            try:
                element = driver.find_element(by, value)
                if element.is_displayed():
                    return element
            except Exception:
                continue

        frames = driver.find_elements(By.TAG_NAME, "iframe")
        for idx, frame in enumerate(frames):
            try:
                driver.switch_to.default_content()
                driver.switch_to.frame(frame)
                for by, value in locator_list:
                    try:
                        element = driver.find_element(by, value)
                        if element.is_displayed():
                            log(f"  iframe[{idx}]에서 로그인 필드 찾음", account_id)
                            return element
                    except Exception:
                        continue
            except Exception:
                continue

        time.sleep(0.5)

    driver.switch_to.default_content()
    return None


def _warm_up_akamai_sensor(driver, account_id: str, timeout: float = 10.0) -> None:
    """Akamai _abck 센서 쿠키가 브라우저에서 자연 생성/성숙되도록 대기.

    curl 주입 대신, 실제 사용자처럼 약간의 상호작용(스크롤/마우스 이동)을 발생시켜
    센서 JS가 유효한 _abck 를 만들도록 유도한다. _abck 가 미성숙(`~-1~` 접미)이면
    로그인 POST 가 권한거부될 수 있다.
    """
    deadline = time.time() + timeout
    last_abck = ""
    try:
        action = ActionChains(driver)
        while time.time() < deadline:
            # 가벼운 사용자 상호작용으로 센서 트리거
            try:
                driver.execute_script("window.scrollTo(0, 120); window.scrollTo(0, 0);")
                action.move_by_offset(5, 7).move_by_offset(-3, -4).perform()
            except Exception:
                pass

            cookies = {c["name"]: c.get("value", "") for c in driver.get_cookies()}
            abck = cookies.get("_abck", "")
            bm_sz = cookies.get("bm_sz", "")
            last_abck = abck
            # _abck 가 존재하고 미해결 표식(~-1~)이 없으면 성숙된 것으로 간주
            matured = bool(abck) and "~-1~" not in abck
            if bm_sz and matured:
                log("  Akamai 센서 warm-up 완료 (_abck 성숙)", account_id)
                return
            time.sleep(0.8)
        log(
            f"  Akamai 센서 warm-up 타임아웃 (_abck={'있음' if last_abck else '없음'}"
            f"{', 미성숙' if last_abck and '~-1~' in last_abck else ''})",
            account_id,
        )
    except Exception as exc:
        log(f"  Akamai 센서 warm-up 오류(무시): {exc}", account_id)


def login_coupang(driver, account_id: str, password: str) -> bool:
    log(f"로그인 시도", account_id)

    def _try_authenticated_shell_recovery(stage: str) -> bool:
        if not _has_authenticated_management_shell(driver):
            return False

        log(f"  인증된 관리 셸 감지 ({stage})", account_id)
        try:
            driver.get(ORDERS_BASE_URL)
            random_delay("page_load")
        except Exception as exc:
            log(f"  인증 셸 복구 중 orders 진입 실패 ({stage}): {exc}", account_id)
            return False

        recovered_url = driver.current_url
        if is_on_success_page(recovered_url) or not is_on_login_page(recovered_url):
            log(f"  로그인 성공 (인증 셸 복구, URL={recovered_url})", account_id)
            return True

        log(f"  인증 셸 복구 후에도 로그인 페이지 유지 (URL={recovered_url})", account_id)
        return False

    # 배민 로그인 방식(croling_beamin.login_baemin)과 동일한 접근:
    #   curl_cffi 로 Akamai 쿠키를 미리 받아 브라우저에 주입하는 방식은
    #   (1) 외부에서 받은 쿠키 주입 자체가 봇탐지 신호이고
    #   (2) 브라우저 핑거프린트와 불일치를 만든다.
    # 브라우저가 로그인 페이지를 직접 방문해 네이티브 JS 센서로 _abck 쿠키를
    # 자연스럽게 생성하도록 두는 것이 더 정상적이고 차단이 적다.
    try:
        driver.get(COUPANG_LOGIN_URL)
        random_delay("page_load")
        log("  로그인 페이지 로드 완료", account_id)
    except WebDriverException as e:
        log(f"  [실패] 로그인 페이지 이동 실패: {e}", account_id)
        return False

    # 세션 재사용: 프로필 쿠키가 유효하면 로그인 URL이 인증 페이지로 리다이렉트됨
    if is_on_success_page(driver.current_url):
        log("  이미 로그인된 상태 (세션 재사용)", account_id)
        return True

    # Akamai 센서 warm-up: 브라우저가 직접 _abck/bm_sz 쿠키를 성숙시킬 시간을 준다.
    # 폼 제출 전에 센서 JS가 유효한 _abck 를 생성하지 못하면 로그인 POST 가
    # "권한이 존재하지 않습니다"로 거부될 수 있다. (curl 주입이 아닌 정상 경로)
    _warm_up_akamai_sensor(driver, account_id)

    id_locators = [
        (By.ID, "loginId"),
        (By.CSS_SELECTOR, "input#loginId"),
        (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
        (By.CSS_SELECTOR, "input[type='text']"),
    ]
    password_locators = [
        (By.ID, "password"),
        (By.CSS_SELECTOR, "input#password"),
        (By.CSS_SELECTOR, "input[type='password']"),
    ]
    button_locators = [
        (By.CSS_SELECTOR, "button.merchant-submit-btn"),
        (By.CSS_SELECTOR, "button[type='submit']"),
        (By.XPATH, "//button[contains(normalize-space(.), '로그인')]"),
    ]

    try:
        id_input = _find_first_visible_in_page(driver, id_locators, account_id, timeout=20)
        if False and id_input is None:
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
            input_count = len(driver.find_elements(By.CSS_SELECTOR, "input"))
            button_count = len(driver.find_elements(By.CSS_SELECTOR, "button"))
            try:
                shadow_inputs = driver.execute_script("""
                    function findInputs(root) {
                        var c = root.querySelectorAll('input').length;
                        root.querySelectorAll('*').forEach(function(el) {
                            if (el.shadowRoot) c += findInputs(el.shadowRoot);
                        });
                        return c;
                    }
                    return findInputs(document);
                """)
                button_texts = driver.execute_script("""
                    return Array.from(document.querySelectorAll('button'))
                        .map(function(b){ return (b.innerText||b.textContent||'').trim().substring(0,30); })
                        .filter(function(t){ return t; }).slice(0, 10);
                """)
                body_text = (driver.execute_script("return document.body.innerText") or "")[:300]
                body_html_len = driver.execute_script("return document.body.innerHTML.length") or 0
                log(f"  [DEBUG] DOM summary: inputs={input_count} shadow_inputs={shadow_inputs} buttons={button_count}", account_id)
                log(f"  [DEBUG] button_texts={button_texts}", account_id)
                log(f"  [DEBUG] body_html_len={body_html_len} body_text_preview={body_text[:200]!r}", account_id)
            except Exception as dbg_exc:
                log(f"  [DEBUG] DOM summary: inputs={input_count}, buttons={button_count}", account_id)
                log(f"  [DEBUG] JS inspection failed: {dbg_exc}", account_id)
            log(f"  [실패] loginId 필드 찾기 실패 (URL={driver.current_url}, title={page_title})", account_id)
            return False

        human_type(id_input, account_id)
        log(f"  ID 입력 완료", account_id)
        driver.switch_to.default_content()
    except Exception as e:
        log(f"  [실패] ID 입력 중 오류: {e}", account_id)
        return False
    
    try:
        pw_input = _find_first_visible_in_page(driver, password_locators, account_id, timeout=10)
        if pw_input is None:
            raise NoSuchElementException("password input not found")
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
        login_btn = _find_first_visible_in_page(driver, button_locators, account_id, timeout=10)
        if login_btn is None:
            raise NoSuchElementException("login button not found")
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

    if _try_authenticated_shell_recovery("post-click"):
        return True
    
    random_delay("page_load")
    current_url = driver.current_url
    page_title = driver.title
    log(f"  로그인 재확인 URL={current_url}, title={page_title}", account_id)
    
    if is_on_login_page(current_url):
        if _try_authenticated_shell_recovery("final-check"):
            return True
        body_text = ""
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text[:500]
            log(f"  [실패] 로그인 실패 메시지 스니펫: {body_text}", account_id)
        except Exception:
            pass
        # 서버측 차단(봇탐지/레이트리밋) vs 자격증명 오류 구분
        if detect_block_text(body_text):
            driver._coupang_blocked = True  # 호출측에서 재시도 차단 판단에 사용
            log(
                "  [실패] 로그인 차단 의심 (쿠팡 봇탐지/레이트리밋 — 코드 문제 아님). "
                "수 시간 쿨다운 후 1회만 재시도 권장",
                account_id,
            )
        else:
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

    # sec-ch-ua-platform 은 실제 UA 플랫폼과 반드시 일치해야 한다 (불일치 = 봇 tell)
    ua_lower = user_agent.lower()
    if "windows" in ua_lower:
        platform = '"Windows"'
    elif "mac os" in ua_lower or "macintosh" in ua_lower:
        platform = '"macOS"'
    else:
        platform = '"Linux"'

    log(f"  쿠키 {len(all_cookies)}개, Chrome v{chrome_version}, platform={platform}", account_id)

    return {
        "user_agent": user_agent,
        "cookie_string": cookie_string,
        "sec_ch_ua": sec_ch_ua,
        "sec_ch_ua_platform": platform,
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
        "sec-ch-ua-platform": auth_data.get("sec_ch_ua_platform", '"Linux"'),
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

def _target_single_day_label(target_date: str) -> str:
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return f"{dt.year}.{dt.month:02d}.{dt.day:02d} - {dt.year}.{dt.month:02d}.{dt.day:02d}"


def _extract_store_id_from_orders_url(url: str) -> str | None:
    match = re.search(r"/orders/(\d+)", urlparse(url).path)
    return match.group(1) if match else None


def get_store_id_from_url(driver, account_id: str) -> str | None:
    for attempt in range(1, 4):
        try:
            driver.get(ORDERS_BASE_URL)
            time.sleep(4)
            current_url = driver.current_url
            store_id = _extract_store_id_from_orders_url(current_url)
            if store_id:
                log(f"routed orders store_id extracted from current url: {store_id} ({current_url})", account_id)
                # orders shell 로드 대기 → navigate_to_orders_with_date에서 재이동 불필요
                _wait_orders_shell_ready(driver, timeout=30)
                return store_id
            log(f"routed orders store_id extraction pending ({attempt}/3): {current_url}", account_id)
        except Exception as exc:
            log(f"routed orders store_id extraction error ({attempt}/3): {exc}", account_id)
            if is_driver_crash_error(exc):
                raise
        time.sleep(2)
    return None


def close_error_popups(driver) -> None:
    selectors = [
        "button[aria-label='close']",
        "button[aria-label='Close']",
        "button[class*='close']",
        "div[role='dialog'] button",
    ]
    for selector in selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                if element.is_displayed():
                    driver.execute_script("arguments[0].click();", element)
                    time.sleep(0.2)
        except Exception:
            continue


def _wait_orders_shell_ready(driver, timeout: int = 20) -> bool:
    # 주문 카드 (실제 데이터가 렌더된 상태)
    _ORDER_CARD_SELECTORS = [
        ".order-search-result-content",           # 구 CSS (캐시 버전)
        "li.col-12 .order-item",                  # 주문카드 내부 구조 (클래스 무관)
        "[class*='orderList'] li",                 # 새 CSS 패턴
        "[class*='OrderList'] li",
        "[class*='order-list'] li",
    ]
    # 주문페이지 chrome (검색행/날짜피커/주문건수 헤더) — 주문 0건이거나
    # 리스트가 아직 비어도 "페이지는 떴다"를 의미. 카드만 기다리면 0건 매장이나
    # 소프트블록 상태에서 영원히 미준비로 오판 → 6분 reload 낭비를 유발하므로
    # chrome 존재만으로도 ready로 인정한다.
    _ORDER_CHROME_SELECTORS = [
        ".sales-search-row",
        "[class*='e4pgcj']",                       # 날짜 range 피커
        "div[data-testid='input']",
        ".h1-txt .order-unit-price",               # 주문건수 헤더 ("N 건")
    ]
    deadline = time.time() + timeout
    chrome_ready_since = None
    while time.time() < deadline:
        try:
            for sel in _ORDER_CARD_SELECTORS:
                if driver.find_elements(By.CSS_SELECTOR, sel):
                    return True
            # 카드가 없으면 chrome 확인 — chrome이 1.5초 이상 안정적으로
            # 유지되면(리스트 fetch 완료 후 0건 확정) ready로 간주
            chrome_present = any(
                driver.find_elements(By.CSS_SELECTOR, sel)
                for sel in _ORDER_CHROME_SELECTORS
            )
            if chrome_present:
                now = time.time()
                if chrome_ready_since is None:
                    chrome_ready_since = now
                elif now - chrome_ready_since >= 1.5:
                    return True
            else:
                chrome_ready_since = None
        except Exception:
            pass
        time.sleep(0.5)
    # 타임아웃 시 DOM 디버그 정보 기록
    try:
        debug = driver.execute_script(
            "return Array.from(document.querySelectorAll('ul,ol')).slice(0,8)"
            ".map(e=>e.className.substring(0,80)+'|'+e.children.length);"
        )
        log(f"orders shell debug: {debug}", "debug")
    except Exception:
        pass
    return False


def _get_order_date_label(driver) -> str:
    selectors = [
        "[class*='e4pgcj09']",
        "span.css-1595qa6.e4pgcj00",
        "[data-testid='input']",
    ]
    for selector in selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                text = (element.text or "").strip()
                if text:
                    return text
        except Exception:
            continue
    return ""


def _has_calendar_popup(driver) -> bool:
    popup_selectors = [
        "div.DayPicker",
        "div.DayPicker-Month",
    ]
    for selector in popup_selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                if element.is_displayed():
                    return True
        except Exception:
            continue
    return False


def _get_day_selection_state(driver, target_aria: str) -> str:
    day_selectors = [
        f"div.DayPicker-Day[role='gridcell'][aria-label='{target_aria}']",
        f"div.DayPicker-Day[aria-label='{target_aria}']",
    ]
    for selector in day_selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                classes = (element.get_attribute("class") or "").strip()
                aria_selected = (element.get_attribute("aria-selected") or "").strip()
                if classes or aria_selected:
                    return f"aria-selected={aria_selected or '<empty>'} class={classes or '<empty>'}"
        except Exception:
            continue
    return "<missing>"


def _is_day_selected(driver, target_aria: str) -> bool:
    state = _get_day_selection_state(driver, target_aria)
    return "aria-selected=true" in state or "selected" in state.lower()


def _find_order_cards(driver) -> list:
    for sel in (".order-search-result-content > li.col-12", "li.col-12"):
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            return els
    return []


def _get_first_order_id(driver) -> str:
    try:
        cards = _find_order_cards(driver)
    except Exception:
        return ""

    for card in cards:
        text = (card.text or "").strip()
        if not text:
            continue
        match = re.search(r"(\d{6,})", text)
        if match:
            return match.group(1)
    return ""


def _get_order_card_count(driver) -> int:
    try:
        for sel in (".order-search-result-content > li.col-12", "li.col-12"):
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return len(els)
        return 0
    except Exception:
        return 0


def _force_set_order_date_range(driver, target_date: str, account_id: str) -> bool:
    try:
        start_hidden = driver.find_element(By.CSS_SELECTOR, "input[name='startDate']")
        end_hidden = driver.find_element(By.CSS_SELECTOR, "input[name='endDate']")
        current_value = start_hidden.get_attribute("value") or end_hidden.get_attribute("value") or "0"
        # Coupang은 KST 기준 날짜 → KST midnight epoch 사용 (UTC 사용 시 날짜 오차 발생)
        from datetime import timedelta as _td
        KST = timezone(_td(hours=9))
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=KST)
        target_ms = int(target_dt.timestamp() * 1000)
        visible_label = target_dt.strftime("%Y.%-m.%-d")
        full_label = target_dt.strftime("%Y.%m.%d")

        driver.execute_script(
            """
            const startHidden = arguments[0];
            const endHidden = arguments[1];
            const targetValue = String(arguments[2]);
            const visibleLabel = arguments[3];
            const fullLabel = arguments[4];

            const setValue = (el, value) => {
              el.value = value;
              el.setAttribute('value', value);
              el.dispatchEvent(new Event('input', { bubbles: true }));
              el.dispatchEvent(new Event('change', { bubbles: true }));
            };

            setValue(startHidden, targetValue);
            setValue(endHidden, targetValue);

            const visibleInputs = Array.from(document.querySelectorAll("div[data-testid='input']"));
            if (visibleInputs[0]) visibleInputs[0].textContent = visibleLabel;
            if (visibleInputs[1]) visibleInputs[1].textContent = visibleLabel;

            const rangeLabel = document.querySelector("span.css-1595qa6.e4pgcj00 > span")
              || document.querySelector("[class*='e4pgcj00'] span");
            if (rangeLabel) {
              rangeLabel.textContent = `${fullLabel} - ${fullLabel}`;
            }
            """,
            start_hidden,
            end_hidden,
            target_ms,
            visible_label,
            full_label,
        )
        log(f"force set hidden order date range to {target_date} (epoch_ms={target_ms})", account_id)
        return True
    except Exception as exc:
        log(f"force set hidden order date range failed: {exc}", account_id)
        return False


def navigate_to_orders_with_date(driver, store_id: str, target_date: str, account_id: str) -> dict:
    result = {
        "applied": False,
        "failure_reason": None,
        "observed_date_label": "",
        "observed_url": "",
    }

    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        log(f"target_date format error: {target_date}", account_id)
        result["failure_reason"] = "final date label mismatch"
        return result

    target_label = _target_single_day_label(target_date)
    target_aria = target_dt.strftime("%a %b %d %Y")

    def _update_result(**kwargs):
        result.update(kwargs)
        result["observed_date_label"] = _get_order_date_label(driver)
        result["observed_url"] = getattr(driver, "current_url", "")

    def _fail(reason: str, **kwargs) -> dict:
        _update_result(applied=False, failure_reason=reason, **kwargs)
        details = " ".join(f"{key}={value}" for key, value in kwargs.items())
        log(
            f"{reason}: current_url={result['observed_url'] or '<empty>'} "
            f"order_date_label={result['observed_date_label'] or '<empty>'}"
            f"{(' ' + details) if details else ''}",
            account_id,
        )
        return result

    def _recover_orders_page() -> str | None:
        # 이미 orders shell이 준비된 경우 불필요한 재이동 생략 (get_store_id_from_url에서 대기 완료)
        if _wait_orders_shell_ready(driver, timeout=30):
            actual_store_id = _extract_store_id_from_orders_url(driver.current_url)
            if actual_store_id:
                log(f"orders page already ready (store_id={actual_store_id}), skip recovery", account_id)
                return None

        saw_orders_navigation = False
        for attempt in range(1, 6):
            try:
                log(
                    f"orders direct open attempt={attempt} store_id={store_id} "
                    f"base_url={ORDERS_BASE_URL} (verify auto-routing from base orders URL)",
                    account_id,
                )
                driver.get(ORDERS_BASE_URL)
                time.sleep(4)
                saw_orders_navigation = True
                if _wait_orders_shell_ready(driver, timeout=25):
                    actual_store_id = _extract_store_id_from_orders_url(driver.current_url)
                    if actual_store_id:
                        log(
                            f"current_url resolved actual_store_id={actual_store_id} "
                            f"current_url={driver.current_url}",
                            account_id,
                        )
                    else:
                        log(f"orders base route opened but final store url was not detected: {driver.current_url}", account_id)
                    return None

                log(f"blank orders shell after base orders open ({attempt}/5)", account_id)
                driver.get(CMG_REVAMP_URL)
                time.sleep(4)
                driver.get(ORDERS_BASE_URL)
                time.sleep(4)
                saw_orders_navigation = True
                if _wait_orders_shell_ready(driver, timeout=25):
                    actual_store_id = _extract_store_id_from_orders_url(driver.current_url)
                    if actual_store_id:
                        log(
                            f"current_url resolved actual_store_id={actual_store_id} "
                            f"after shell recovery current_url={driver.current_url}",
                            account_id,
                        )
                    else:
                        log(f"shell recovered but final store url was not detected: {driver.current_url}", account_id)
                    return None
            except Exception as exc:
                log(f"orders page open failed during shell recovery ({attempt}/5): {exc}", account_id)
        return "orders shell recovery failed" if saw_orders_navigation else "orders page open failed"

    def _find_visible(selectors: list[str]):
        for selector in selectors:
            try:
                for element in driver.find_elements(By.CSS_SELECTOR, selector):
                    if element.is_displayed():
                        return element
            except Exception:
                continue
        return None

    def _click(element, label: str) -> bool:
        for attempt in range(1, 4):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", element)
                log(label, account_id)
                return True
            except Exception as exc:
                log(f"{label} failed (attempt {attempt}): {exc}", account_id)
                time.sleep(0.5)
        return False

    recover_failure = _recover_orders_page()
    if recover_failure:
        return _fail(recover_failure)

    close_error_popups(driver)
    before_label = _get_order_date_label(driver)
    before_first_order_id = _get_first_order_id(driver)
    before_order_count = _get_order_card_count(driver)
    log(
        f"order-date before click: {before_label or '<empty>'} "
        f"current_url={driver.current_url} first_order_id={before_first_order_id or '<empty>'} "
        f"order_count={before_order_count}",
        account_id,
    )

    order_date_container = _find_visible([
        "span.css-1595qa6.e4pgcj00 > span",
        "[class*='e4pgcj00'] span",
        "[class*='e4pgcj09']",
    ])
    if order_date_container is None:
        return _fail("date picker open failed")
    if not _click(order_date_container, "order-date click"):
        return _fail("date picker open failed")
    time.sleep(1.0)

    input_selectors = [
        "div[data-testid='input'].css-ibjb8.e1mdtx7j1",
        "div[data-testid='input']",
    ]
    first_input = _find_visible(input_selectors)
    if first_input is None:
        return _fail("first input missing")
    if not _click(first_input, "first input click"):
        return _fail("first input missing")
    time.sleep(0.7)
    calendar_popup_detected = _has_calendar_popup(driver)
    log(f"calendar popup detected after first input click={calendar_popup_detected}", account_id)
    if not calendar_popup_detected:
        return _fail("date picker open failed", calendar_popup_detected=calendar_popup_detected)

    # 현재 달력이 target_aria 날짜를 보여주지 않으면 이전달로 이동
    target_day_css = f"div.DayPicker-Day[aria-label='{target_aria}']"

    def _nav_to_target_month(label: str) -> None:
        for _ in range(3):
            els = driver.find_elements(By.CSS_SELECTOR, target_day_css)
            if any(el.is_displayed() for el in els):
                break
            prev_btns = driver.find_elements(By.CSS_SELECTOR, ".DayPicker-NavButton--prev")
            if not prev_btns or not prev_btns[0].is_displayed():
                break
            driver.execute_script("arguments[0].click();", prev_btns[0])
            log(f"calendar prev-month click ({label}, target={target_aria} not visible)", account_id)
            time.sleep(1.0)  # React 달력 re-render 충분히 대기

    def _action_click_day(element, label: str) -> bool:
        """ActionChains로 실제 마우스 이벤트를 발생시켜 React synthetic event 처리."""
        for attempt in range(1, 3):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
                time.sleep(0.3)
                ActionChains(driver).move_to_element(element).click().perform()
                log(label, account_id)
                return True
            except Exception as exc:
                log(f"{label} failed (attempt {attempt}): {exc}", account_id)
                time.sleep(0.5)
        return False

    _nav_to_target_month("first")

    day_selectors = [
        f"div.DayPicker-Day[role='gridcell'][aria-label='{target_aria}']",
        f"div.DayPicker-Day[aria-label='{target_aria}']",
    ]
    first_day = _find_visible(day_selectors)
    if first_day is None:
        return _fail("target day missing", target_aria=target_aria)
    if not _action_click_day(first_day, "first day click"):
        return _fail("target day missing", target_aria=target_aria)
    time.sleep(0.7)
    selected_state = _get_day_selection_state(driver, target_aria)
    log(f"selected day state after first click: {selected_state}", account_id)

    second_input = _find_visible(input_selectors)
    if second_input is None:
        return _fail("second input missing")
    if not _click(second_input, "second input click"):
        return _fail("second input missing")
    time.sleep(0.7)

    # first_day 클릭 후 달력이 다음 달로 자동 이동하므로 재이동
    _nav_to_target_month("second")

    second_day = _find_visible(day_selectors)
    if second_day is None:
        return _fail("target day missing", target_aria=target_aria)
    if not _action_click_day(second_day, "second day click"):
        return _fail("target day missing", target_aria=target_aria)
    time.sleep(0.7)
    selected_state = _get_day_selection_state(driver, target_aria)
    log(f"selected day state after second click: {selected_state}", account_id)
    # day click이 React state 미갱신 시(이전달 셀 disabled 등) hidden input fallback 적용
    current_label_after_day_clicks = _get_order_date_label(driver)
    if current_label_after_day_clicks != f"주문일\n{target_label}":
        log(
            f"date label after day clicks mismatch; applying hidden input fallback "
            f"(current={current_label_after_day_clicks or '<empty>'}, expected=주문일\\n{target_label})",
            account_id,
        )
        _force_set_order_date_range(driver, target_date, account_id)

    search_button = None
    try:
        for button in driver.find_elements(By.CSS_SELECTOR, "button"):
            if button.is_displayed() and "조회" in (button.text or "").strip():
                search_button = button
                break
    except Exception:
        search_button = None

    if search_button is None:
        search_button = _find_visible([
            "button.css-casqo8.e4pgcj01",
            "button.button--defaultOutlined.css-casqo8.e4pgcj01",
            ".sales-search-row > button.button--defaultOutlined",
        ])

    if search_button is None:
        return _fail("search refresh not observed")
    if not _click(search_button, "search click"):
        return _fail("search refresh not observed")
    time.sleep(2.0)

    after_label = _get_order_date_label(driver)
    normalized_after_label = after_label.replace("주문일", "").strip()
    after_first_order_id = _get_first_order_id(driver)
    after_order_count = _get_order_card_count(driver)
    refresh_observed = (
        before_label != after_label
        or before_first_order_id != after_first_order_id
        or before_order_count != after_order_count
    )
    log(
        f"order-date after search: {after_label or '<empty>'} current_url={driver.current_url} "
        f"first_order_id_before={before_first_order_id or '<empty>'} "
        f"first_order_id_after={after_first_order_id or '<empty>'} "
        f"order_count_before={before_order_count} order_count_after={after_order_count} "
        f"search_refresh_observed={refresh_observed}",
        account_id,
    )
    if normalized_after_label != target_label:
        return _fail("final date label mismatch", expected_date_label=target_label, actual_date_label=after_label or "<empty>")
    if not refresh_observed:
        return _fail(
            "search refresh not observed",
            first_order_id_before=before_first_order_id or "<empty>",
            first_order_id_after=after_first_order_id or "<empty>",
            order_count_before=before_order_count,
            order_count_after=after_order_count,
        )

    _update_result(applied=True, failure_reason=None)
    return result


def _get_expected_order_count(driver, account_id: str) -> int:
    try:
        for el in driver.find_elements(By.CSS_SELECTOR, ".h1-txt .order-unit-price"):
            if el.text.strip() == "건":
                count_el = driver.execute_script("return arguments[0].previousElementSibling;", el)
                if count_el:
                    num_str = (count_el.text or "").replace(",", "").strip()
                    if num_str.isdigit():
                        num = int(num_str)
                        log(f"expected order count: {num}", account_id)
                        return num
    except Exception as exc:
        log(f"expected order count read failed: {exc}", account_id)
    return 0


def _get_first_card_order_id(driver) -> str:
    try:
        cards = _find_order_cards(driver)
        if not cards:
            return ""
        cols = cards[0].find_elements(By.CSS_SELECTOR, ".order-item [class*='col-4']")
        if not cols:
            return ""
        return driver.execute_script(
            "for(var n of arguments[0].childNodes){if(n.nodeType===3&&n.textContent.trim())return n.textContent.trim();}return '';",
            cols[0],
        )
    except Exception:
        return ""


def _click_next_page(driver) -> bool:
    try:
        for btn in driver.find_elements(By.CSS_SELECTOR, "button[data-at='next-btn']"):
            classes = btn.get_attribute("class") or ""
            if btn.is_displayed() and "hide-btn" not in classes:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", btn)
                return True
    except Exception:
        pass
    return False


def _wait_for_page_change(driver, prev_first_order_id: str, account_id: str, timeout: int = 35) -> bool:
    deadline = time.time() + timeout
    consecutive = 0
    while time.time() < deadline:
        current_id = _get_first_card_order_id(driver)
        if current_id and current_id != prev_first_order_id:
            consecutive += 1
            if consecutive >= 1:
                return True
            time.sleep(0.2)
        else:
            consecutive = 0
            time.sleep(0.3)
    log(f"page change wait timed out (prev_id={prev_first_order_id})", account_id)
    return False


def _wait_for_stable_orders(driver, account_id: str, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    last_count = -1
    stable_streak = 0
    while time.time() < deadline:
        try:
            count = len(_find_order_cards(driver))
        except Exception:
            count = 0
        if count > 0 and count == last_count:
            stable_streak += 1
            if stable_streak >= 3:
                return True
        else:
            stable_streak = 0
            last_count = count
        time.sleep(0.3)
    return last_count > 0


def _parse_settlement(detail_section) -> dict:
    """order-details 내 정산 row에서 레이블:금액 매핑 추출. 없으면 빈 값."""
    result = {k: "" for k in _SETTLEMENT_KEYS}
    if detail_section is None:
        return result
    try:
        rows = detail_section.find_elements(
            By.CSS_SELECTOR,
            ".settlement-row, .order-settlement li, .fee-detail li, .order-fee li",
        )
        for row in rows:
            spans = row.find_elements(By.CSS_SELECTOR, "span")
            if len(spans) < 2:
                continue
            label_text = spans[0].text.strip()
            value_text = re.sub(r"[^\d\-]", "", spans[-1].text)
            for key, variants in _SETTLEMENT_LABEL_MAP.items():
                if any(v in label_text for v in variants):
                    result[key] = value_text
                    break
    except Exception:
        pass
    return result


def _parse_order_card(driver, card, store_id: str, store_name: str, collected_at: str) -> list[dict]:
    rows: list[dict] = []
    try:
        order_section_els = card.find_elements(By.CSS_SELECTOR, ".order-item")
        if not order_section_els:
            return rows
        order_section = order_section_els[0]
        detail_els = card.find_elements(By.CSS_SELECTOR, ".order-details")
        detail_section = detail_els[0] if detail_els else None

        date_el = order_section.find_elements(By.CSS_SELECTOR, ".order-date:not(.d-md-none) span:first-child")
        time_el = order_section.find_elements(By.CSS_SELECTOR, ".order-date .gray-txt")
        order_date = (
            (date_el[0].text.strip().replace(" ", "") if date_el else "")
            + " "
            + (time_el[0].text.strip() if time_el else "")
        ).strip()

        cols = order_section.find_elements(By.CSS_SELECTOR, "[class*='col-4']")
        order_id = ""
        if cols:
            order_id = driver.execute_script(
                "for(var n of arguments[0].childNodes){if(n.nodeType===3&&n.textContent.trim())return n.textContent.trim();}return '';",
                cols[0],
            )
        if not order_id and cols:
            m = re.search(r"\d{6,}", cols[0].text or "")
            order_id = m.group(0) if m else ""

        dt_els = order_section.find_elements(By.CSS_SELECTOR, ".delivery-type")
        delivery_type = dt_els[0].text.strip() if dt_els else ""

        price_els = order_section.find_elements(By.CSS_SELECTOR, ".order-price > span:first-child")
        total_price = re.sub(r"[^\d]", "", price_els[0].text) if price_els else ""

        status_els = order_section.find_elements(By.CSS_SELECTOR, ".label-order-item")
        order_status = status_els[0].text.strip() if status_els else ""

        is_cancelled = (
            "Y"
            if detail_section and detail_section.find_elements(By.CSS_SELECTOR, ".total-cancelled-order")
            else "N"
        )

        menu_items_els = detail_section.find_elements(By.CSS_SELECTOR, ".order-detail-item") if detail_section else []
        all_menus = []
        for mi in menu_items_els:
            name_els = mi.find_elements(By.CSS_SELECTOR, ".col-7")
            menu_name = ""
            if name_els:
                menu_name = driver.execute_script(
                    "for(var n of arguments[0].childNodes){if(n.nodeType===3&&n.textContent.trim())return n.textContent.trim();}return '';",
                    name_els[0],
                )
            qty_els = mi.find_elements(By.CSS_SELECTOR, ".col-2")
            menu_qty = re.sub(r"[^\d]", "", qty_els[0].text) if qty_els else "1"
            menu_qty = menu_qty or "1"
            mp_els = mi.find_elements(By.CSS_SELECTOR, ".order-item-price")
            menu_price = re.sub(r"[^\d]", "", mp_els[0].text) if mp_els else ""
            opt_els = mi.find_elements(By.CSS_SELECTOR, ".order-detail-option-name")
            options = [el.text.strip() for el in opt_els if el.text.strip()] or ["기본"]
            if menu_name:
                all_menus.append({"name": menu_name, "qty": menu_qty, "price": menu_price, "options": options})

        order_summary = ""
        if all_menus:
            order_summary = all_menus[0]["name"]
            if len(all_menus) > 1:
                order_summary += f" 외 {len(all_menus) - 1}건"

        settlement = _parse_settlement(detail_section)
        empty_settlement = {k: "" for k in _SETTLEMENT_KEYS}

        base = {
            "collected_at": collected_at,
            "store_id": store_id,
            "store_name": store_name,
            "order_date": order_date,
            "order_id": order_id,
            "delivery_type": delivery_type,
            "order_status": order_status,
            "order_summary": order_summary,
            "total_price": total_price,
            "is_cancelled": is_cancelled,
        }

        if all_menus:
            first_row = True
            for menu in all_menus:
                first_opt = True
                for opt in menu["options"]:
                    rows.append({
                        **base,
                        "menu_name": menu["name"],
                        "menu_qty": menu["qty"],
                        "menu_price": menu["price"] if first_opt else "",
                        "menu_options": opt,
                        **(settlement if first_row else empty_settlement),
                    })
                    first_opt = False
                    first_row = False
        else:
            rows.append({**base, "menu_name": "", "menu_qty": "", "menu_price": "", "menu_options": "", **settlement})

    except Exception as exc:
        log(f"_parse_order_card error: {exc}", "")
    return rows


def collect_orders_all_pages(driver, store_id: str, store_name: str, account_id: str) -> dict:
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expected = _get_expected_order_count(driver, account_id)
    log(f"expected order count={expected}", account_id)

    all_rows: list[dict] = []
    page_count = 0
    empty_streak = 0
    session_lost = False
    incomplete_reason = None

    while True:
        _wait_for_stable_orders(driver, account_id)
        close_error_popups(driver)

        try:
            cards = _find_order_cards(driver)
        except Exception as exc:
            log(f"order list read failed: {exc}", account_id)
            session_lost = True
            break

        if not cards:
            empty_streak += 1
            log(f"empty page (streak={empty_streak})", account_id)
            if empty_streak >= 3:
                incomplete_reason = "consecutive empty pages"
                break
        else:
            empty_streak = 0
            page_count += 1
            log(f"page {page_count}: {len(cards)} cards", account_id)

            for i, card in enumerate(cards):
                try:
                    expand_btns = card.find_elements(By.CSS_SELECTOR, ".order-expand-btn .icon-ce-arrowdown-bold")
                    if expand_btns:
                        driver.execute_script("arguments[0].closest('button').click();", expand_btns[0])
                        for _ in range(5):
                            if card.find_elements(By.CSS_SELECTOR, ".order-details .order-detail-item"):
                                break
                            time.sleep(0.2)
                    card_rows = _parse_order_card(driver, card, store_id, store_name, collected_at)
                    all_rows.extend(card_rows)
                except Exception as exc:
                    log(f"card {i + 1} parse error: {exc}", account_id)

        close_error_popups(driver)
        prev_first_id = _get_first_card_order_id(driver)
        if not _click_next_page(driver):
            log("no next page — collection complete", account_id)
            break

        log(f"waiting for page {page_count + 1}...", account_id)
        if not _wait_for_page_change(driver, prev_first_id, account_id):
            # 약한 SPA stall은 흔하므로 끈질기게 재시도 (스크롤 nudge + 점증 timeout).
            # 한 번의 timeout으로 전체 수집을 폐기하면 부분수집·총합불일치가 발생.
            page_changed = False
            for retry in range(1, 4):
                log(f"page {page_count + 1} load timeout — retry {retry}/3 (scroll+click)", account_id)
                try:
                    # lazy-load SPA를 깨우기 위한 스크롤 nudge
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.4)
                    driver.execute_script("window.scrollTo(0, 0);")
                except Exception:
                    pass
                close_error_popups(driver)
                if not _click_next_page(driver):
                    # next 버튼이 사라졌으면 마지막 페이지 → 정상 완료
                    log("next button gone during retry — treat as last page", account_id)
                    page_changed = None  # 완료 신호
                    break
                if _wait_for_page_change(driver, prev_first_id, account_id, timeout=25):
                    page_changed = True
                    break
                time.sleep(random.uniform(1.5, 2.5))

            if page_changed is None:
                break  # 마지막 페이지로 정상 종료
            if not page_changed:
                log(f"page {page_count + 1} load timeout (3 retries exhausted)", account_id)
                incomplete_reason = "page load timeout"
                break

        time.sleep(random.uniform(0.8, 1.2))

    unique_ids = len({row["order_id"] for row in all_rows if row.get("order_id")})
    matched = unique_ids == expected if expected > 0 else None
    log(
        f"pages={page_count} expected={expected} collected={unique_ids} matched={matched} "
        f"session_lost={session_lost} incomplete={incomplete_reason}",
        account_id,
    )

    return {
        "rows": all_rows,
        "expected": expected,
        "page_count": page_count,
        "session_lost": session_lost,
        "incomplete_reason": incomplete_reason,
        "api_filtered": False,
    }


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
