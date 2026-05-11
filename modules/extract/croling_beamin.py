"""
배민 크롤링 - Airflow 연동용

============================================================================
사용법
============================================================================

from modules.extract.croling_baemin import run_baemin_crawling

# 1. 통계만 수집 (기본값)
stats_df = run_baemin_crawling(account_df, mode="stats")

# 2. 나중에 추가될 기능 (예: 리뷰 수집 등)
# review_df = run_baemin_crawling(account_df, mode="review")

# 3. 둘 다
# result_df = run_baemin_crawling(account_df, mode="all")

============================================================================
모드 설명
============================================================================

| mode       | 동작                    | 비고           |
|------------|------------------------|----------------|
| "stats"    | 우리가게NOW 통계 수집    | 기본값         |
| "all"      | 모든 기능 실행           | 추후 확장용    |

============================================================================
"""

import subprocess
import time
import random
import re
import shutil
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException


# ============================================================================
# 상수 - 작업 모드
# ============================================================================
MODE_STATS = "stats"           # 통계만 수집
MODE_ALL = "all"               # 모든 기능 (추후 확장용)


# ============================================================================
# 상수 - URL
# ============================================================================
LOGIN_URL = "https://biz-member.baemin.com/login?returnUrl=https%3A%2F%2Fself.baemin.com%2F"
MAIN_URL = "https://self.baemin.com/"


# ============================================================================
# 상수 - HTTP
# ============================================================================
HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403


# ============================================================================
# 상수 - 우리가게NOW 통계 항목
# ============================================================================
STAT_LABELS = [
    "조리소요시간",
    "주문접수시간",
    "최근재주문율",
    "조리시간준수율",
    "주문접수율",
    "최근별점",
]


# ============================================================================
# 상수 - 타이밍 (밴 방지)
# ============================================================================
TIMING = {
    "login_wait": (8.0, 12.0),
    "page_load": (4.0, 6.0),
    "typing_char": (0.05, 0.15),
    "typing_pause": (0.3, 0.6),
    "before_click": (0.5, 1.0),
    "store_switch": (4.0, 6.0),
    "stat_extract": (0.2, 0.4),
    "account_stagger": (3.0, 7.0),
    "batch_rest": (45.0, 90.0),
    "logout_wait": (30.0, 90.0),   # 로그아웃 후 계정 간 대기 (봇 탐지 방지)
}

WINDOW_SIZES = [(1366, 768), (1440, 900), (1920, 1080)]


# ============================================================================
# 상수 - 배치 설정
# ============================================================================
BATCH_SIZE_RANGE = (1, 2)


# ============================================================================
# 상수 - 경로 (Docker 환경 고려)
# ============================================================================
BASE_DIR = os.getenv("AIRFLOW_HOME", Path.cwd())
CHROME_PROFILE_DIR = Path(os.getenv("CHROME_PROFILE_DIR", f"{BASE_DIR}/chrome_profiles"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", f"{BASE_DIR}/download"))

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 상수 - 기타
# ============================================================================
LOGIN_FAIL_URL_PATTERNS = ["/login", "/signin", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["self.baemin.com"]

DEFAULT_WIN_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ============================================================================
# 유틸리티 함수
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{account_id}] {message}")


def random_delay(timing_key: str):
    """랜덤 딜레이 (밴 방지)"""
    min_sec, max_sec = TIMING[timing_key]
    time.sleep(random.uniform(min_sec, max_sec))


def human_type(element, text: str):
    """사람처럼 타이핑"""
    element.clear()
    random_delay("typing_pause")
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(*TIMING["typing_char"]))
    
    random_delay("typing_pause")


def clean_chrome_profile(account_id: str):
    """크롬 프로필 폴더 삭제"""
    profile_path = CHROME_PROFILE_DIR / account_id
    
    if not profile_path.exists():
        return
    
    try:
        shutil.rmtree(profile_path)
        log(f"프로필 삭제 완료", account_id)
    except OSError as e:
        raise Exception(f"프로필 삭제 실패 - 브라우저를 수동으로 닫아주세요: {e}")


def convert_account_df_to_list(account_df: pd.DataFrame) -> List[Dict]:
    """account_df를 ACCOUNT_LIST 형태로 변환 (baemin 필터)"""
    filtered_df = account_df[account_df["channel"] == "baemin"].copy()
    
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
    """랜덤 크기의 배치로 분할"""
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

def _detect_chrome_major_version() -> int | None:
    """설치된 Chrome의 major 버전 번호를 반환. 감지 실패 시 None."""
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True, text=True, timeout=5,
        )
        match = re.search(r"(\d+)\.", result.stdout)
        return int(match.group(1)) if match else None
    except Exception:
        return None


def clean_chrome_cache(account_id: str):
    """Chrome 캐시만 삭제 (쿠키는 유지 → 세션 재사용 가능).

    Cache, GPUCache, Code Cache 폴더를 제거해 OOM 유발 캐시 누적 해소.
    SingletonLock 등 잠금 파일도 제거 (비정상 종료 후 재실행 크래시 방지).
    """
    profile_root = CHROME_PROFILE_DIR / account_id
    # 잠금 파일 제거 (이전 비정상 종료 후 남은 파일 → Chrome 즉시 종료 원인)
    for lock_name in ["SingletonLock", "SingletonCookie", "SingletonSocket"]:
        lock_file = profile_root / lock_name
        if lock_file.exists():
            try:
                lock_file.unlink()
                log(f"잠금 해제: {lock_name}", account_id)
            except Exception:
                pass

    profile_default = profile_root / "Default"
    for cache_dir in ["Cache", "GPUCache", "Code Cache"]:
        target = profile_default / cache_dir
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
            log(f"캐시 정리: {cache_dir}", account_id)


def _ensure_xvfb_display() -> str:
    """Xvfb 가상 디스플레이를 시작하고 DISPLAY 환경변수를 반환한다.

    이미 실행 중이면 재사용. 실패 시 빈 문자열 반환.
    """
    display = ":99"
    try:
        # Popen: 백그라운드 실행 (run은 프로세스 종료까지 블로킹 → 사용 불가)
        subprocess.Popen(
            ["Xvfb", display, "-screen", "0", "1920x1080x24", "-ac"],
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        time.sleep(1.0)   # Xvfb 시작 대기
    except FileNotFoundError:
        return ""
    except Exception:
        pass   # 이미 실행 중인 경우 등 무시
    return display


def launch_browser(account_id: str):
    """브라우저 실행 (Xvfb 가상 디스플레이 사용, 비-헤드리스).

    배민 봇 탐지가 headless 모드를 감지해 metrics API를 차단하므로
    Xvfb 가상 디스플레이로 실제 브라우저처럼 실행한다.
    """
    clean_chrome_cache(account_id)   # 세션 유지 + 캐시만 정리 (OOM 방지)

    # Xvfb 가상 디스플레이 설정
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
        options.add_argument('--headless=new')   # Xvfb 없을 때만 헤드리스
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disk-cache-size=1')           # 캐시 최소화 (OOM 방지)
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-default-apps')
    options.add_argument('--no-first-run')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-sync')
    options.add_argument('--mute-audio')
    w, h = random.choice(WINDOW_SIZES)
    options.add_argument(f'--window-size={w},{h}')
    options.add_argument('--lang=ko-KR')
    options.add_argument(f'--user-agent={DEFAULT_WIN_CHROME_UA}')
    
    profile_path = CHROME_PROFILE_DIR / account_id
    profile_path.mkdir(parents=True, exist_ok=True)
    options.add_argument(f'--user-data-dir={profile_path.absolute()}')
    
    try:
        chrome_ver = _detect_chrome_major_version()
        log(f"Chrome 감지 버전: {chrome_ver}", account_id)
        driver = uc.Chrome(options=options, version_main=chrome_ver)
        log(f"브라우저 실행 성공", account_id)
        return driver
    except Exception as e:
        log(f"브라우저 실행 실패: {e}", account_id)
        raise


def is_on_login_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_FAIL_URL_PATTERNS)


def is_on_success_page(url: str) -> bool:
    return any(pattern in url.lower() for pattern in LOGIN_SUCCESS_URL_PATTERNS)


def login_baemin(driver, account_id: str, password: str) -> bool:
    """배민 로그인"""
    log(f"로그인 시도", account_id)
    wait = WebDriverWait(driver, 30)
    
    try:
        driver.get(LOGIN_URL)
        random_delay("page_load")
        log("  로그인 페이지 로드 완료", account_id)
    except WebDriverException as e:
        log(f"  [실패] 로그인 페이지 이동 실패: {e}", account_id)
        return False

    # 세션 재사용으로 이미 로그인된 경우 (프로필 쿠키 유효 → self.baemin.com/ 리다이렉트)
    if is_on_success_page(driver.current_url):
        log("  이미 로그인 상태 (세션 재사용)", account_id)
        return True

    try:
        id_input = wait.until(EC.presence_of_element_located((By.NAME, "id")))
        human_type(id_input, account_id)
        log("  ID 입력 완료", account_id)
    except TimeoutException:
        page_title = driver.title
        snippet = driver.page_source[:1000]
        log(f"  [실패] ID 필드 찾기 실패 (URL={driver.current_url}, title={page_title})", account_id)
        log(f"  [페이지 일부]\n{snippet}", account_id)
        return False
    except Exception as e:
        log(f"  [실패] ID 입력 중 오류: {e}", account_id)
        return False
    
    try:
        pw_input = driver.find_element(By.NAME, "password")
        human_type(pw_input, password)
        log("  비밀번호 입력 완료", account_id)
    except NoSuchElementException:
        log(f"  [실패] password 필드 찾기 실패 (URL={driver.current_url})", account_id)
        return False
    except Exception as e:
        log(f"  [실패] 비밀번호 입력 중 오류: {e}", account_id)
        return False
    
    try:
        random_delay("before_click")
        login_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_btn.click()
        log("  로그인 버튼 클릭", account_id)
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
# 우리가게NOW 통계 수집
# ============================================================================

# 비율로 저장할 항목 (익스텐션과 동일: ÷100 소수 저장)
RATIO_LABELS = {"조리시간준수율", "주문접수율", "최근재주문율"}

_COLLECT_METRICS_JS = r"""
(function() {
    const LABELS = ['조리소요시간','주문접수시간','최근재주문율','조리시간준수율','주문접수율','최근별점'];
    const RATIO  = new Set(['조리시간준수율','주문접수율','최근재주문율']);
    const result = {};

    // 실제 DOM 구조 기반:
    // .WooriShopNowItem-module__TKcC
    //   └── Flex[column]
    //        ├── span[Typography] → label       (spans[0])
    //        ├── Flex[row] → span[Typography]   (spans[1], value)
    //        └── span[Typography] → rank        (spans[2], optional)
    const items = document.querySelectorAll('.WooriShopNowItem-module__TKcC');
    for (const item of items) {
        const spans = item.querySelectorAll('span[data-atelier-component="Typography"]');
        if (spans.length < 2) continue;

        const label = spans[0].textContent.trim();
        if (!LABELS.includes(label)) continue;

        const rawVal  = spans[1].textContent.trim();
        const rawRank = spans.length > 2 ? spans[2].textContent.trim() : '';

        const numMatch = rawVal.match(/[\d.]+/);
        let numStr = numMatch ? numMatch[0] : '';
        if (numStr && RATIO.has(label)) numStr = String(parseFloat(numStr) / 100);

        const rankMatch = rawRank.match(/^(상위|하위)\s*([\d.]+)%$/);
        result[label]               = numStr;
        result[label + '_순위구분'] = rankMatch ? rankMatch[1] : '';
        result[label + '_순위비율'] = rankMatch ? String(parseFloat(rankMatch[2]) / 100) : '';
    }
    return result;
})();
"""


def collect_single_store_stats(driver, store_id: str, account_id: str) -> Dict[str, Any]:
    """단일 매장 우리가게NOW 통계 수집 (익스텐션 _collectMetrics 로직 기반)"""
    stats: Dict[str, Any] = {
        "account_id": account_id,
        "store_id": store_id,
        "platform": "baemin",
        "collected_at": datetime.now().isoformat(),
    }

    try:
        raw = driver.execute_script(_COLLECT_METRICS_JS) or {}
    except Exception as e:
        log(f"JS 실행 오류: {e}", account_id)
        raw = {}

    for label in STAT_LABELS:
        stats[label]               = raw.get(label, "")
        stats[f"{label}_순위구분"] = raw.get(f"{label}_순위구분", "")
        stats[f"{label}_순위비율"] = raw.get(f"{label}_순위비율", "")
        log(
            f"    {label}: {stats[label]} "
            f"({stats[f'{label}_순위구분']} {stats[f'{label}_순위비율']})",
            account_id,
        )

    return stats


# ============================================================================
# 단일 매장 / 단일 계정 처리
# ============================================================================

def create_empty_store_result(store_id: str) -> Dict[str, Any]:
    return {
        "store_id": store_id,
        "success": False,
        "stats": None,
        "error": None,
    }


def process_single_store(
    driver,
    account_id: str,
    store_id: str,
    mode: str = MODE_STATS,
) -> Dict[str, Any]:
    """단일 매장 처리 (mode에 따라 동작)"""
    result = create_empty_store_result(store_id)
    
    try:
        # 1. 매장 선택
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select[class*='ShopSelect']"))
        )
        Select(select_element).select_by_value(str(store_id))
        log(f"  매장 선택 완료", account_id)
        
        # 2. 데이터 로딩 대기
        random_delay("store_switch")
        
        # 3. 통계 수집 (stats 또는 all)
        if mode in [MODE_STATS, MODE_ALL]:
            stats = collect_single_store_stats(driver, store_id, account_id)
            result["stats"] = stats
            log(f"  매장 {store_id} 통계 수집 완료", account_id)
        
        # 4. 추후 추가 기능 (예: 리뷰 수집)
        # if mode in [MODE_REVIEW, MODE_ALL]:
        #     reviews = collect_single_store_reviews(driver, store_id, account_id)
        #     result["reviews"] = reviews
        
        result["success"] = True
        
    except TimeoutException:
        result["error"] = "매장 선택 드롭다운을 찾을 수 없음"
        log(f"  매장 {store_id} 처리 실패: {result['error']}", account_id)
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
    mode: str = MODE_STATS,
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
        
        if not login_baemin(driver, account_id, password):
            result["error"] = "로그인 실패"
            return result
        
        random_delay("page_load")
        
        for idx, store_id in enumerate(store_ids):
            log(f"{'─' * 30}", account_id)
            log(f"매장 {idx + 1}/{len(store_ids)} 처리 (ID: {store_id})", account_id)
            
            if idx > 0:
                driver.get(MAIN_URL)
                random_delay("page_load")
            
            store_result = process_single_store(
                driver=driver,
                account_id=account_id,
                store_id=store_id,
                mode=mode,
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
    start_delay: float,
) -> Dict:
    if start_delay > 0:
        log(f"{start_delay:.1f}초 후 시작", account["id"])
        time.sleep(start_delay)
    
    return process_single_account(account, mode)


def process_batch(
    batch: List[Dict],
    mode: str,
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
                cumulative_delay,
            )
            futures.append(future)
        
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


# ============================================================================
# 메인 함수 (Airflow에서 호출)
# ============================================================================

def run_baemin_crawling(
    account_df: pd.DataFrame,
    mode: str = MODE_STATS,
) -> pd.DataFrame:
    """
    배민 크롤링 메인 함수
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw, store_ids)
        mode: 작업 모드
            - "stats": 우리가게NOW 통계 수집 (기본값)
            - "all": 모든 기능 실행 (추후 확장용)
    
    Returns:
        stats_df: 수집된 통계 DataFrame
    """
    log("=" * 60)
    log(f"배민 크롤링 시작 (mode: {mode})")
    log("=" * 60)
    
    account_list = convert_account_df_to_list(account_df)
    total_stores = sum(len(acc["store_ids"]) for acc in account_list)
    
    log(f"  작업 모드: {mode}")
    log(f"  계정 수: {len(account_list)}개")
    log(f"  매장 수: {total_stores}개")
    
    if not account_list:
        log("baemin 계정이 없습니다.")
        return pd.DataFrame()
    
    batches = split_into_random_batches(account_list, BATCH_SIZE_RANGE)
    log(f"  배치 분할: {[len(b) for b in batches]}")
    
    all_results = []
    all_stats = []
    
    for batch_idx, batch in enumerate(batches):
        log(f"\n{'=' * 40}")
        log(f"배치 {batch_idx + 1}/{len(batches)} 시작 ({len(batch)}개 계정)")
        log(f"{'=' * 40}")
        
        batch_results = process_batch(batch, mode)
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
    log("배민 크롤링 완료")
    log(f"  매장: 성공 {total_success}개, 실패 {total_failed}개")
    log(f"  통계: {len(stats_df)}행")
    log("=" * 60)
    
    return stats_df


# ============================================================================
# 드롭다운 매장 선택 (신규)
# ============================================================================

def get_store_options(driver) -> list:
    """ShopSelect 드롭다운에서 모든 매장 옵션 추출.

    반환: [{"store_id": "14822058", "text": "[음식배달] 나홀로..."}, ...]
    """
    result = driver.execute_script(r"""
        const sel = document.querySelector('select[class*="ShopSelect"]');
        if (!sel) return [];
        return Array.from(sel.options).map(o => ({
            store_id: o.value,
            text: o.textContent.trim()
        }));
    """)
    return result or []


def navigate_to_store(driver, store_id: str) -> bool:
    """store_id URL로 직접 이동 후 메트릭 데이터 로드 대기.

    드롭다운 dispatch 대신 URL 직접 이동으로 React state 문제 및
    세션 복원 URL 의존 문제를 우회한다.
    반환: True=성공, False=실패
    """
    try:
        url = f"https://self.baemin.com/shops/{store_id}/"
        log(f"navigate_to_store: {url}")
        driver.get(url)
        return wait_for_metrics_data(driver, timeout=45)
    except Exception as e:
        log(f"navigate_to_store 실패 ({store_id}): {e}")
        return False


def select_store_by_id(driver, store_id: str) -> bool:
    """드롭다운에서 store_id를 선택하고 WooriShopNowItem 로드를 기다린다.

    React 앱 호환: nativeInputValueSetter + change 이벤트로 선택.
    선택 후 .ShopSelect-module__j4Qm 텍스트로 실제 전환 확인.
    반환: True=성공, False=실패
    """
    try:
        sel_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "select[class*='ShopSelect']")
            )
        )
        # React의 onChange를 트리거하는 방식으로 선택
        driver.execute_script(r"""
            var setter = Object.getOwnPropertyDescriptor(
                window.HTMLSelectElement.prototype, 'value'
            ).set;
            setter.call(arguments[0], arguments[1]);
            arguments[0].dispatchEvent(new Event('change', {bubbles: true}));
        """, sel_elem, str(store_id))

        # 전환 확인: .ShopSelect-module__j4Qm 에 store_id 포함 여부 (빠른 체크만)
        try:
            info_elem = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".ShopSelect-module__j4Qm")
                )
            )
            if str(store_id) not in info_elem.text:
                log(f"매장 전환 미확인 (expected={store_id}, actual={info_elem.text})")
                return False
        except Exception:
            pass   # 확인 실패 시 무시하고 계속

        # 메트릭 데이터 로드는 호출 측(collect_now_stats)에서 time.sleep(20)으로 대기
        return True
    except Exception as e:
        log(f"매장 선택 실패 ({store_id}): {e}")
        return False


def wait_for_metrics_data(driver, timeout: int = 45) -> bool:
    """WooriShopNowItem의 값 span에 실제 데이터가 채워질 때까지 폴링 대기.

    DOM 요소 존재가 아니라 label+value span에 실제 텍스트가 있는지 확인.
    timeout 초 초과 시 새로고침 후 1회 재시도. 반환: True=성공, False=실패.
    """
    def _has_data(d):
        return d.execute_script(r"""
            const LABELS = ['조리소요시간','주문접수시간','최근재주문율',
                            '조리시간준수율','주문접수율','최근별점'];
            const items = document.querySelectorAll('.WooriShopNowItem-module__TKcC');
            if (!items.length) return false;
            const spans = items[0].querySelectorAll(
                'span[data-atelier-component="Typography"]'
            );
            return spans.length >= 2
                && LABELS.includes(spans[0].textContent.trim())
                && spans[1].textContent.trim() !== '';
        """)

    for attempt in range(2):
        try:
            WebDriverWait(driver, timeout).until(_has_data)
            return True
        except TimeoutException:
            if attempt == 0:
                log(f"메트릭 데이터 로드 {timeout}초 초과 → 새로고침 재시도")
                driver.refresh()
                time.sleep(random.uniform(3.0, 5.0))
    return False


# ============================================================================
# 봇 탐지 방지 유틸리티
# ============================================================================

def human_click(driver, element):
    """마우스 이동 후 클릭 (ActionChains 기반, 봇 탐지 방지)"""
    ActionChains(driver).move_to_element(element).pause(
        random.uniform(0.2, 0.5)
    ).click().perform()


def wait_for_page(driver, css_selector: str, timeout: int = 60) -> bool:
    """CSS 셀렉터 요소가 나타날 때까지 대기.

    60초 초과 시 새로고침 후 1회 재시도. 반환: True=성공, False=실패.
    """
    for attempt in range(2):
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            return True
        except TimeoutException:
            if attempt == 0:
                log(f"페이지 로드 {timeout}초 초과 → 새로고침 재시도")
                driver.refresh()
                time.sleep(random.uniform(3.0, 5.0))
    return False


# ============================================================================
# store_id / 로그아웃 (신규)
# ============================================================================

def get_store_id(driver) -> str:
    """로그인 후 현재 선택된 매장의 numeric ID 추출.

    방법 1: URL 패턴 /shops/{id}/
    방법 2: ShopSelect DOM (.ShopSelect-module__j4Qm)
    실패 시 "" 반환.
    """
    url_match = re.search(r'/shops/(\d+)/', driver.current_url)
    if url_match:
        return url_match.group(1)
    try:
        elem = driver.find_element(By.CSS_SELECTOR, ".ShopSelect-module__j4Qm")
        dom_match = re.search(r'(\d+)', elem.text)
        if dom_match:
            return dom_match.group(1)
    except Exception:
        pass
    return ""


def logout_baemin(driver, account_id: str):
    """settings 페이지 이동 → 로그아웃 버튼 클릭 → 대기.

    실패해도 warn만 (driver.quit()은 호출 측에서 항상 실행).
    """
    try:
        driver.get("https://self.baemin.com/settings")
        time.sleep(random.uniform(4.0, 6.0))

        # 로그아웃 버튼: CSS 클래스 우선, 텍스트 기반 fallback
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.LandingPage-module__mLoG")
                )
            )
        except TimeoutException:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(text(), '로그아웃')]")
                )
            )

        human_click(driver, btn)
        time.sleep(random.uniform(2.0, 3.0))
        log("로그아웃 완료", account_id)
    except Exception as e:
        log(f"로그아웃 실패 (warn only): {e}", account_id)


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    
    test_account_df = pd.DataFrame([
        {
            "channel": "baemin",
            "id": "doridang04",
            "pw": "ehfl3652!",
            "store_ids": "14778331, 14535911",
        },
    ])
    
    # 테스트: 통계 수집
    print("\n[테스트] 통계 수집")
    stats_df = run_baemin_crawling(test_account_df, mode="stats")
    print(stats_df)