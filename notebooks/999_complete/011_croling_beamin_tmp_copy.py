"""
배민 크롤링 - Airflow 연동용

수집 기능:
1. [매장별 반복] 우리가게NOW 통계
"""
from datetime import datetime
import time
import random
import re
import shutil
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC


# ============================================================================
# 상수 정의
# ============================================================================

# URL
LOGIN_URL = "https://biz-member.baemin.com/login?returnUrl=https%3A%2F%2Fself.baemin.com%2F"
MAIN_URL = "https://self.baemin.com/"

# 우리가게NOW 통계 항목
STAT_LABELS = [
    "조리소요시간",
    "주문접수시간",
    "최근재주문율",
    "조리시간준수율",
    "주문접수율",
    "최근별점",
]

# 타이밍 (밴 방지)
LOGIN_WAIT_SEC = (8.0, 12.0)
PAGE_LOAD_SEC = (4.0, 6.0)
STORE_SWITCH_SEC = (4.0, 6.0)
TYPING_SEC = (0.05, 0.15)

# 경로
CHROME_PROFILE_DIR = Path("./chrome_profiles")
LOG_FILE = Path("./crawling_log.txt")


# ============================================================================
# 유틸리티 함수
# ============================================================================

def log(message: str, account_id: str = "SYSTEM"):
    """로그 출력 및 파일 저장"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] [{account_id}] {message}"
    print(log_msg)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_msg + "\n")


def human_delay(delay_range: tuple):
    """랜덤 딜레이 (밴 방지)"""
    min_sec, max_sec = delay_range
    time.sleep(random.uniform(min_sec, max_sec))


def human_type(element, text: str):
    """사람처럼 타이핑"""
    element.clear()
    human_delay((0.3, 0.6))
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(*TYPING_SEC))
    
    human_delay((0.3, 0.5))


def clean_profile(account_id: str):
    """크롬 프로필 폴더 삭제"""
    profile_path = CHROME_PROFILE_DIR / account_id
    
    if not profile_path.exists():
        return
    
    try:
        shutil.rmtree(profile_path)
        log(f"프로필 삭제 완료", account_id)
    except OSError as e:
        log(f"프로필 삭제 실패: {e}", account_id)


def convert_account_df_to_list(account_df: pd.DataFrame, channel: str = "baemin") -> List[Dict]:
    """
    account_df를 ACCOUNT_LIST 형태로 변환
    
    account_df 컬럼: channel, id, pw, store_ids
    store_ids는 쉼표로 구분된 문자열 (예: "14778331, 14535911")
    """
    # 채널 필터링
    filtered_df = account_df[account_df["channel"] == channel].copy()
    
    account_list = []
    for _, row in filtered_df.iterrows():
        # store_ids 파싱 (쉼표 구분 문자열 → 리스트)
        store_ids_raw = str(row["store_ids"])
        store_ids = [s.strip() for s in store_ids_raw.split(",") if s.strip()]
        
        account_list.append({
            "id": row["id"],
            "pw": row["pw"],
            "store_ids": store_ids,
        })
    
    return account_list


# ============================================================================
# 브라우저/로그인
# ============================================================================

def launch_browser(account_id: str):
    """브라우저 실행"""
    log("브라우저 실행 시도", account_id)
    
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    
    profile_path = CHROME_PROFILE_DIR / account_id
    profile_path.mkdir(parents=True, exist_ok=True)
    options.add_argument(f'--user-data-dir={profile_path.absolute()}')
    
    driver = uc.Chrome(options=options, version_main=None)
    log("브라우저 실행 성공", account_id)
    
    return driver


def login_baemin(driver, account_id: str, password: str) -> bool:
    """배민 로그인"""
    log("로그인 시도", account_id)
    wait = WebDriverWait(driver, 30)
    
    # 1. 로그인 페이지 이동
    try:
        driver.get(LOGIN_URL)
        human_delay(PAGE_LOAD_SEC)
        log("  로그인 페이지 로드 완료", account_id)
    except Exception as e:
        log(f"  [실패] 로그인 페이지 로드: {e}", account_id)
        return False
    
    # 2. ID 입력
    try:
        id_input = wait.until(EC.presence_of_element_located((By.NAME, "id")))
        human_type(id_input, account_id)
        log("  ID 입력 완료", account_id)
    except Exception as e:
        log(f"  [실패] ID 입력: {e}", account_id)
        return False
    
    # 3. 비밀번호 입력
    try:
        pw_input = driver.find_element(By.NAME, "password")
        human_type(pw_input, password)
        log("  비밀번호 입력 완료", account_id)
    except Exception as e:
        log(f"  [실패] 비밀번호 입력: {e}", account_id)
        return False
    
    # 4. 로그인 버튼 클릭
    try:
        human_delay((0.5, 1.0))
        login_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_btn.click()
        log("  로그인 버튼 클릭", account_id)
    except Exception as e:
        log(f"  [실패] 로그인 버튼 클릭: {e}", account_id)
        return False
    
    # 5. 로그인 결과 확인
    human_delay(LOGIN_WAIT_SEC)
    current_url = driver.current_url
    
    is_login_success = "self.baemin.com" in current_url
    is_still_login_page = "/login" in current_url.lower()
    
    if is_login_success:
        log("  로그인 성공", account_id)
        return True
    
    if is_still_login_page:
        log("  [실패] 로그인 실패 - ID/PW 오류 또는 캡차", account_id)
        return False
    
    log(f"  로그인 결과 불확실 (URL: {current_url})", account_id)
    return True


# ============================================================================
# 우리가게NOW 통계 수집
# ============================================================================

def extract_stat_value(driver, label: str) -> Dict[str, str]:
    """단일 통계 항목 추출"""
    result = {"값": "N/A", "순위": "N/A"}
    
    try:
        xpath = f"//span[contains(text(), '{label}')]/parent::div"
        container = driver.find_element(By.XPATH, xpath)
        raw_text = container.text.replace("\n", " ").strip()
        
        # 숫자 추출 (예: 99.2, 30, 4.9)
        value_match = re.search(r'(\d+\.?\d*)', raw_text)
        if value_match:
            result["값"] = value_match.group(1)
        
        # 순위 추출 (예: 상위 50, 하위 20)
        rank_match = re.search(r'(상위|하위)\s*(\d+)', raw_text)
        if rank_match:
            result["순위"] = f"{rank_match.group(1)} {rank_match.group(2)}"
            
    except Exception:
        pass
    
    return result


def collect_single_store_stats(driver, store_id: str, account_id: str) -> Dict[str, Any]:
    """단일 매장 통계 수집"""
    stats = {
        "account_id": account_id,
        "store_id": store_id,
        "collected_at": datetime.now().isoformat(),
    }
    
    for label in STAT_LABELS:
        extracted = extract_stat_value(driver, label)
        stats[f"{label}_값"] = extracted["값"]
        stats[f"{label}_순위"] = extracted["순위"]
        
        log(f"    {label}: {extracted['값']} ({extracted['순위']})", account_id)
        human_delay((0.2, 0.4))
    
    return stats


def collect_all_stores_stats(driver, account: Dict) -> List[Dict]:
    """[매장별 반복] 모든 매장 통계 수집"""
    account_id = account["id"]
    store_ids = account["store_ids"]
    all_stats = []
    
    total_stores = len(store_ids)
    log(f"매장 통계 수집 시작 ({total_stores}개)", account_id)
    
    for idx, store_id in enumerate(store_ids):
        store_number = idx + 1
        log(f"\n  [{store_number}/{total_stores}] 매장 {store_id}", account_id)
        
        # 1. 매장 선택
        try:
            select_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select[class*='ShopSelect']"))
            )
            Select(select_element).select_by_value(str(store_id))
            log(f"    매장 선택 완료", account_id)
        except Exception as e:
            log(f"    [실패] 매장 선택: {e}", account_id)
            continue
        
        # 2. 데이터 로딩 대기
        human_delay(STORE_SWITCH_SEC)
        
        # 3. 통계 수집
        try:
            stats = collect_single_store_stats(driver, store_id, account_id)
            all_stats.append(stats)
            log(f"    매장 {store_id} 수집 완료", account_id)
        except Exception as e:
            log(f"    [실패] 통계 수집: {e}", account_id)
            continue
        
        # 4. 다음 매장 전 메인 이동
        is_last_store = idx >= total_stores - 1
        if not is_last_store:
            driver.get(MAIN_URL)
            human_delay(PAGE_LOAD_SEC)
    
    log(f"\n매장 통계 수집 완료: {len(all_stats)}개", account_id)
    return all_stats


def process_single_account(account: Dict) -> List[Dict]:
    """단일 계정 처리 (브라우저 실행 → 로그인 → 수집 → 종료)"""
    account_id = account["id"]
    password = account["pw"]
    
    # 프로필 초기화
    clean_profile(account_id)
    
    driver = None
    stats_list = []
    
    try:
        # 1. 브라우저 실행
        driver = launch_browser(account_id)
        
        # 2. 로그인
        if not login_baemin(driver, account_id, password):
            log("로그인 실패로 스킵", account_id)
            return stats_list
        
        # 3. 페이지 로드 대기
        human_delay(PAGE_LOAD_SEC)
        
        # 4. 우리가게NOW 통계 수집
        stats_list = collect_all_stores_stats(driver, account)
        
    except Exception as e:
        log(f"에러 발생: {e}", account_id)
    
    finally:
        if driver:
            driver.quit()
            log("브라우저 종료", account_id)
    
    return stats_list


# ============================================================================
# 메인 함수 (Airflow에서 호출)
# ============================================================================

def run_baemin_crawling(account_df: pd.DataFrame) -> pd.DataFrame:
    """
    배민 크롤링 메인 함수 (Airflow DAG에서 호출)
    
    Parameters:
        account_df: 계정 정보 DataFrame (columns: channel, id, pw, store_ids)
    
    Returns:
        stats_df: 수집된 통계 DataFrame
    """
    log("\n" + "=" * 70)
    log("배민 크롤링 시작")
    log("=" * 70)
    
    # 1. account_df → ACCOUNT_LIST 변환 (baemin 필터)
    account_list = convert_account_df_to_list(account_df, channel="baemin")
    
    log(f"  baemin 계정 수: {len(account_list)}")
    for acc in account_list:
        log(f"    - {acc['id']}: {len(acc['store_ids'])}개 매장")
    
    if not account_list:
        log("baemin 계정이 없습니다.")
        return pd.DataFrame()
    
    # 2. 모든 계정 순회하며 수집
    all_stats = []
    
    for idx, account in enumerate(account_list):
        account_num = idx + 1
        log(f"\n{'='*50}")
        log(f"계정 처리 [{account_num}/{len(account_list)}]: {account['id']}")
        log(f"{'='*50}")
        
        stats_list = process_single_account(account)
        all_stats.extend(stats_list)
        
        # 계정 간 휴식 (마지막 계정 제외)
        if idx < len(account_list) - 1:
            human_delay((3.0, 5.0))
    
    # 3. DataFrame 생성
    stats_df = pd.DataFrame(all_stats) if all_stats else pd.DataFrame()
    
    # 4. 결과 요약
    log("\n" + "=" * 70)
    log("배민 크롤링 완료")
    log(f"  처리 계정: {len(account_list)}개")
    log(f"  수집 매장: {len(stats_df)}개")
    log("=" * 70)
    
    return stats_df


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    
    # 테스트용 account_df 생성
    test_account_df = pd.DataFrame([
        {
            "channel": "coupangeats",
            "id": "doridang04",
            "pw": "ehfl3652!",
            "store_ids": "",  # 쿠팡이츠는 store_ids 없음
        },
        {
            "channel": "baemin",
            "id": "doridang04",
            "pw": "ehfl3652!",
            "store_ids": "14778331, 14535911",
        },
        {
            "channel": "baemin",
            "id": "doridang04",
            "pw": "ehfl3652!",
            "store_ids": "14778331",
        },
        # 계정 추가 예시
        # {
        #     "channel": "baemin",
        #     "id": "another_account",
        #     "pw": "password123",
        #     "store_ids": "12345678",
        # },
    ])
    
    print("\n" + "=" * 70)
    print("테스트용 account_df:")
    print("=" * 70)
    print(test_account_df.to_string(index=False))
    print()
    
    # 크롤링 실행
    stats_df = run_baemin_crawling(test_account_df)
    
    # 결과 출력
    print("\n" + "=" * 70)
    print("수집 결과 (stats_df):")
    print("=" * 70)
    
    if stats_df.empty:
        print("수집된 데이터가 없습니다.")
    else:
        print(f"총 {len(stats_df)}개 매장 수집 완료\n")
        print(stats_df.to_string(index=False))
        
        # 컬럼별 확인
        print("\n[컬럼 목록]")
        for col in stats_df.columns:
            print(f"  - {col}")