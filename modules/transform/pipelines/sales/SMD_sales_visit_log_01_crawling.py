"""
플로우(Flow) 방문일지 크롤링 - Airflow 연동용 (개선 버전)

개선사항:
1. 로딩 확인 기반 속도 개선 - WebDriverWait 활용
2. 수집 누락 방지 - 더보기 전 현재 화면 전체 수집
3. 명확한 수집 로직 - 스크롤 → 게시물 수집 → 더보기 → 반복
"""

import time
import random
import re
import os
import platform
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

from modules.transform.utility.selenium_uc import configure_uc_data_path


# ============================================================================
# 상수 - DAG 설정
# ============================================================================
COLLECTION_MODE = os.getenv("FLOW_COLLECTION_MODE", "recent")
RECENT_DAYS = int(os.getenv("FLOW_RECENT_DAYS", "8"))
ALL_DATA_START_DATE = os.getenv("FLOW_ALL_START_DATE", None)


# ============================================================================
# 상수 - 브라우저 설정
# ============================================================================
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None


# ============================================================================
# 상수 - URL 및 계정
# ============================================================================
PLATFORM_NAME = "flow_visit"
LOGIN_URL = "https://flow.team/signin.act"
FLOW_ID = "a17019@doridang.com"
FLOW_PW = "Falswns3040@"


# ============================================================================
# 상수 - 타이밍
# ============================================================================
TIMING = {
    "page_load": (2.0, 3.0),
    "typing_char": (0.03, 0.08),
    "typing_pause": (0.2, 0.4),
    "before_click": (0.3, 0.5),
    "after_click": (1.5, 2.5),
    "search_loading": (2.0, 3.0),
    "post_loading": (1.5, 2.5),
    "more_button_wait": (1.0, 2.0),
    "scroll_wait": (0.5, 1.0),
    "popup_close_wait": (0.8, 1.2),
}


# ============================================================================
# 상수 - 경로
# ============================================================================
def get_output_dir():
    """실행 환경에 맞는 출력 디렉토리 반환"""
    if os.getenv("AIRFLOW_HOME"):
        # Airflow 환경에서는 LOCAL_DB/업로드_temp 사용
        try:
            import sys
            from pathlib import Path
            # modules 경로 추가
            sys.path.insert(0, str(Path(__file__).parent.parent.parent))
            return Path("/opt/airflow/download/업로드_temp")
        except Exception as e:
            # import 실패 시 기본 경로
            print(f"LOCAL_DB import 실패: {e}, 기본 경로 사용")
            return Path("/opt/airflow/download/업로드_temp")
    
    if platform.system() == "Windows":
        return Path("E:/down")
    
    return Path("/mnt/d/down")

OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 상수 - 기타
# ============================================================================
VISIT_LOG_PATTERN = re.compile(r'방문일지')


# ============================================================================
# 유틸리티 함수
# ============================================================================

def log(message: str, context: str = "SYSTEM"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{context}] {message}")


def random_delay(timing_key: str):
    """랜덤 딜레이"""
    min_sec, max_sec = TIMING[timing_key]
    time.sleep(random.uniform(min_sec, max_sec))


def human_type(element, text: str):
    """사람처럼 타이핑"""
    element.clear()
    time.sleep(0.2)
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    
    time.sleep(0.3)


def parse_post_date(date_str: str) -> Optional[datetime]:
    """게시글 날짜 파싱"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M")
    except:
        try:
            return datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        except:
            return None


def is_valid_visit_log_title(title: str) -> bool:
    """방문일지 제목 패턴 검증"""
    if not title or not title.strip():
        return False
    
    if VISIT_LOG_PATTERN.search(title):
        exclude_keywords = ['가이드', '자동 수집', '월간', '총괄']
        if any(keyword in title for keyword in exclude_keywords):
            return False
        return True
    
    return False


def extract_visit_date_from_title(title: str) -> Optional[datetime]:
    """제목에서 방문일 추출"""
    try:
        # 패턴 1: YY.M.D 또는 YY.MM.DD 형식
        match = re.search(r'(\d{2})\.(\d{1,2})\.(\d{1,2})', title)
        if match:
            year_2digit = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            year = 2000 + year_2digit
            return datetime(year, month, day)
        
        # 패턴 2: YY-M-D 또는 YY-MM-DD 형식
        match = re.search(r'(\d{2})-(\d{1,2})-(\d{1,2})', title)
        if match:
            year_2digit = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            year = 2000 + year_2digit
            return datetime(year, month, day)
        
        # 패턴 3: YYYY.M.D 또는 YYYY.MM.DD 형식
        match = re.search(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', title)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day)
        
        # 패턴 4: YYYY-M-D 또는 YYYY-MM-DD 형식
        match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', title)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day)
        
        # 패턴 5: M/D 형식
        match = re.search(r'(\d{1,2})/(\d{1,2})', title)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = datetime.now().year
            
            try:
                return datetime(year, month, day)
            except ValueError:
                pass
        
        return None
        
    except ValueError:
        return None
    except Exception:
        return None


def get_chrome_version():
    """Chrome 버전 감지"""
    try:
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        result = subprocess.run(
            [chrome_bin, "--version"], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        version_str = result.stdout.strip()
        version_match = re.search(r'(\d+)\.', version_str)
        if version_match:
            major_version = int(version_match.group(1))
            log(f"Chrome 감지 버전: {major_version}")
            return major_version
    except Exception as e:
        log(f"Chrome 버전 감지 실패: {e}")
    return None


# ============================================================================
# 브라우저 / 로그인
# ============================================================================

def launch_browser():
    """브라우저 실행"""
    log(f"브라우저 실행 (headless={HEADLESS_MODE})")
    
    def create_chrome_options():
        """ChromeOptions 생성 함수"""
        options = uc.ChromeOptions()
        
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        chrome_path = Path(chrome_bin)
        
        if chrome_path.exists():
            options.binary_location = str(chrome_bin)
        
        if HEADLESS_MODE:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        return options
    
    try:
        configure_uc_data_path()
        chrome_version = get_chrome_version()
        
        if chrome_version:
            log(f"ChromeDriver 버전: {chrome_version} 사용")
            driver = uc.Chrome(options=create_chrome_options(), version_main=chrome_version)
        else:
            log(f"Chrome 버전 자동 매칭 시도")
            driver = uc.Chrome(options=create_chrome_options())
        
        driver.maximize_window()
        log(f"브라우저 창 최대화")
        log(f"✅ 브라우저 실행 성공")
        return driver
        
    except Exception as e:
        error_msg = str(e)
        log(f"❌ 첫 번째 시도 실패")
        
        version_match = re.search(r'Current browser version is (\d+)', error_msg)
        
        if version_match:
            detected_version = int(version_match.group(1))
            log(f"⚠️ 에러 메시지에서 Chrome 버전 감지: {detected_version}")
            log(f"재시도: version_main={detected_version} 사용")
            
            try:
                configure_uc_data_path()
                driver = uc.Chrome(
                    options=create_chrome_options(),
                    version_main=detected_version
                )
                
                driver.maximize_window()
                log(f"브라우저 창 최대화")
                log(f"✅ 브라우저 실행 성공 (재시도)")
                return driver
                
            except Exception as e2:
                log(f"❌ 재시도 실패: {e2}")
                raise
        else:
            log(f"❌ 에러 메시지에서 Chrome 버전을 찾을 수 없음")
            raise


def do_login(driver) -> bool:
    """플로우 로그인"""
    log(f"로그인 시도")
    
    try:
        driver.get(LOGIN_URL)
        random_delay("page_load")
        
        id_input = driver.find_element(By.CSS_SELECTOR, "input#userId")
        human_type(id_input, FLOW_ID)
        log(f"  ✓ ID 입력")
        
        pw_input = driver.find_element(By.CSS_SELECTOR, "input#password")
        human_type(pw_input, FLOW_PW)
        log(f"  ✓ 비밀번호 입력")
        
        login_btn = driver.find_element(By.CSS_SELECTOR, "a#normalLoginButton")
        driver.execute_script("arguments[0].click();", login_btn)
        log(f"  ✓ 로그인 버튼 클릭")
        
        time.sleep(5.0)
        
        current_url = driver.current_url
        if "signin" not in current_url.lower():
            log(f"  ✅ 로그인 성공!")
            
            wait = WebDriverWait(driver, 15)
            try:
                search_box = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#topSearchKeywordBox"))
                )
                log(f"  ✓ 메인 페이지 로드 완료")
                time.sleep(2.0)
            except:
                log(f"  [경고] 검색 박스 로드 확인 실패")
            
            return True
        else:
            log(f"  ❌ 로그인 실패")
            return False
            
    except Exception as e:
        log(f"  ❌ 로그인 오류: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# 검색 및 게시글 수집
# ============================================================================

def open_search_popup(driver) -> bool:
    """검색 팝업 열기"""
    log("검색 팝업 열기")
    
    try:
        wait = WebDriverWait(driver, 15)
        search_box = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input#topSearchKeywordBox"))
        )
        log(f"  ✓ 검색 박스 발견")
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
        time.sleep(0.5)
        
        clicked = False
        
        try:
            driver.execute_script("arguments[0].click();", search_box)
            log(f"  ✓ 검색 박스 클릭 (JS)")
            clicked = True
        except Exception as e:
            log(f"  ⚠️ JS 클릭 실패: {e}")
        
        if not clicked:
            try:
                search_box.click()
                log(f"  ✓ 검색 박스 클릭 (일반)")
                clicked = True
            except Exception as e:
                log(f"  ⚠️ 일반 클릭 실패: {e}")
        
        if not clicked:
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(driver).move_to_element(search_box).click().perform()
                log(f"  ✓ 검색 박스 클릭 (ActionChains)")
                clicked = True
            except Exception as e:
                log(f"  ⚠️ ActionChains 클릭 실패: {e}")
        
        if not clicked:
            return False
        
        time.sleep(1.5)
        
        try:
            search_popup = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div#topSearchPopupLayer"))
            )
            log(f"  ✓ 검색 팝업 표시 확인")
        except TimeoutException:
            log(f"  ✗ 검색 팝업 표시 확인 실패")
            return False
        
        try:
            search_input = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "input#searchPopupInput"))
            )
            log(f"  ✓ 검색 입력 필드 확인")
        except TimeoutException:
            log(f"  ✗ 검색 입력 필드 확인 실패")
            return False
        
        log("  ✅ 검색 팝업 열림 완료")
        return True
        
    except Exception as e:
        log(f"  ✗ 검색 팝업 열기 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def search_keyword(driver, keyword: str) -> bool:
    """키워드 검색"""
    log(f"키워드 검색: {keyword}")
    
    try:
        search_input = driver.find_element(By.CSS_SELECTOR, "input#searchPopupInput")
        driver.execute_script("arguments[0].focus();", search_input)
        time.sleep(0.3)
        
        human_type(search_input, keyword)
        log(f"  ✓ 검색어 입력")
        
        search_input.send_keys(Keys.RETURN)
        log(f"  ✓ Enter 키 입력")
        
        random_delay("search_loading")
        return True
        
    except Exception as e:
        log(f"  ✗ 검색 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def click_post_tab(driver) -> bool:
    """게시물 탭 클릭"""
    log("게시물 탭 클릭")
    
    try:
        post_tab = driver.find_element(By.CSS_SELECTOR, "button.js-tab-item[data-code='post']")
        driver.execute_script("arguments[0].click();", post_tab)
        random_delay("after_click")
        
        log("  ✓ 게시물 탭 클릭")
        return True
        
    except Exception as e:
        log(f"  ✗ 게시물 탭 클릭 실패: {e}")
        return False


def click_latest_sort(driver) -> bool:
    """최신순 정렬"""
    log("최신순 정렬")
    
    try:
        latest_sort = driver.find_element(By.CSS_SELECTOR, "label[for='sort-02']")
        driver.execute_script("arguments[0].click();", latest_sort)
        random_delay("after_click")
        
        log("  ✓ 최신순 정렬")
        return True
        
    except Exception as e:
        log(f"  ✗ 최신순 정렬 실패: {e}")
        return False


def click_all_period(driver) -> bool:
    """전체 기간 필터 클릭"""
    log("전체 기간 필터 클릭")
    
    try:
        wait = WebDriverWait(driver, 10)
        
        # 전체 기간 라벨 클릭
        all_period = driver.find_element(By.CSS_SELECTOR, "label[for='period-01']")
        driver.execute_script("arguments[0].click();", all_period)
        log("  ✓ 전체 기간 필터 클릭")
        
        # 로딩 대기
        time.sleep(2.0)
        
        # 검색 결과 재로딩 대기
        try:
            # 기존 요소가 사라질 때까지 대기 (stale 상태)
            time.sleep(1.0)
            
            # 새로운 검색 결과 로딩 확인
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "li.elasticSearch-result-item.js-search-item[data-code='post']")
            ))
            log("  ✓ 검색 결과 재로딩 완료")
            
            # 추가 대기 (완전히 로드되도록)
            time.sleep(2.0)
            
        except:
            log("  [경고] 검색 결과 재로딩 확인 실패")
            time.sleep(3.0)
        
        return True
        
    except Exception as e:
        log(f"  ✗ 전체 기간 필터 클릭 실패: {e}")
        return False


def collect_all_posts(driver, start_dt, end_dt):
    """게시글 수집: 1개씩 스크롤 → 제목 확인 → 즉시 본문 수집"""
    log(f"게시글 수집 시작 (기간: {start_dt.date()} ~ {end_dt.date()})")
    
    # 초기 로딩 대기 (요소 확인)
    wait = WebDriverWait(driver, 10)
    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "li.elasticSearch-result-item.js-search-item[data-code='post']")
        ))
        log("✓ 검색 결과 로딩 완료")
    except:
        log("[경고] 검색 결과 로딩 확인 실패")
        time.sleep(2.0)

    loading_iteration = 0
    max_loading_attempts = 100
    all_results = []
    processed_indices = set()  # 처리한 인덱스 추적

    while loading_iteration < max_loading_attempts:
        try:
            log(f"\n[라운드 {loading_iteration + 1}] ===================================")
            
            # 1단계: 현재 화면의 모든 게시글 요소 가져오기
            post_items = driver.find_elements(
                By.CSS_SELECTOR,
                "li.elasticSearch-result-item.js-search-item[data-code='post']"
            )
            log(f"   발견된 게시글 요소: {len(post_items)}개")
            
            round_collected = 0
            
            # 2단계: 각 게시글을 순회하면서 즉시 수집
            for idx, item in enumerate(post_items):
                # 이미 처리한 인덱스는 건너뛰기
                if idx in processed_indices:
                    continue
                
                try:
                    # ⭐ 스크롤하여 요소를 화면에 노출 (제목 로딩)
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'auto'});", item)
                    time.sleep(0.2)  # 로딩 대기
                    
                    # 제목 추출
                    title = ""
                    title_selectors = [
                        "b.result-item-title",
                        ".result-item-title",
                        "strong.result-item-title"
                    ]
                    
                    for selector in title_selectors:
                        try:
                            title_elem = item.find_element(By.CSS_SELECTOR, selector)
                            title = title_elem.text.strip()
                            if title:
                                break
                        except:
                            continue
                    
                    # 제목이 없으면 건너뛰기
                    if not title:
                        processed_indices.add(idx)
                        continue
                    
                    # 방문일지 패턴 확인
                    if not is_valid_visit_log_title(title):
                        processed_indices.add(idx)
                        continue
                    
                    # post_date 추출
                    post_date = None
                    try:
                        date_elems = item.find_elements(By.CSS_SELECTOR, "span")
                        for elem in date_elems:
                            text = elem.text.strip()
                            if re.match(r'\d{4}-\d{2}-\d{2}', text):
                                post_date = parse_post_date(text)
                                break
                    except:
                        pass
                    
                    # visit_date 추출
                    visit_date = extract_visit_date_from_title(title)
                    
                    # 작성자/프로젝트
                    author = ""
                    project = ""
                    try:
                        info_divs = item.find_elements(By.CSS_SELECTOR, ".result-item-info a")
                        if len(info_divs) >= 2:
                            author = info_divs[0].text.strip()
                            project = info_divs[1].text.strip()
                    except:
                        pass
                    
                    # 기간 내 포함 여부 판단
                    include = False
                    if COLLECTION_MODE == "all":
                        include = True
                    elif post_date and start_dt <= post_date <= end_dt:
                        include = True
                    elif visit_date and start_dt <= visit_date <= end_dt:
                        include = True
                    
                    # 처리 완료 표시
                    processed_indices.add(idx)
                    
                    if not include:
                        # 기간 외이면 로그만 남기고 건너뛰기
                        if visit_date:
                            if visit_date.date() < start_dt.date():
                                log(f"   [{idx}] ❌ {title[:40]} (visit: {visit_date.date()}) - 시작일 이전")
                            else:
                                log(f"   [{idx}] ❌ {title[:40]} (visit: {visit_date.date()}) - 기간 외")
                        continue
                    
                    # ⭐ 기간 내 게시글 → 즉시 본문 수집
                    log(f"\n   [{idx}] 📝 {title}")
                    
                    post_info = {
                        'title': title,
                        'post_date': post_date,
                        'author': author,
                        'project': project
                    }
                    
                    result = process_single_post(driver, post_info)
                    if result:
                        all_results.append(result)
                        round_collected += 1
                        log(f"   ✅ 수집 완료 (누적: {len(all_results)}개)")
                    else:
                        log(f"   ❌ 수집 실패")
                    
                except Exception as e:
                    log(f"   [{idx}] ⚠️ 처리 오류: {e}")
                    processed_indices.add(idx)
                    continue
            
            log(f"\n   이번 라운드 수집: {round_collected}개")
            
            # 3단계: 종료 조건 확인
            log("\n3️⃣ 종료 조건 확인...")
            
            # 이번 라운드에 기간 내 게시글이 하나도 없으면 종료
            if COLLECTION_MODE != "all" and loading_iteration > 0 and round_collected == 0:
                log(f"   ✓ 더보기 후 기간 내 게시글 없음 - 수집 종료")
                break
            
            # 시작일 이전 게시글 발견 확인
            if COLLECTION_MODE != "all" and all_results:
                visit_dates = [r['visit_date'] for r in all_results if r.get('visit_date')]
                if visit_dates:
                    min_visit = min(visit_dates)
                    if min_visit.date() < start_dt.date():
                        log(f"   ✓ 시작일({start_dt.date()}) 이전 게시글 발견 ({min_visit.date()}) - 수집 종료")
                        break
            
            # 4단계: 더보기 버튼 확인 및 클릭
            log("\n4️⃣ 더보기 버튼 확인 중...")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            
            more_btn = None
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, "button.result-wrap-accordian-button")
                if btns:
                    btn = btns[0]
                    computed_display = driver.execute_script(
                        "return window.getComputedStyle(arguments[0]).display;", btn
                    )
                    style = btn.get_attribute("style") or ""
                    if computed_display != "none" and "display: none" not in style:
                        more_btn = btn
            except:
                pass
            
            if not more_btn:
                try:
                    btns = driver.find_elements(By.CSS_SELECTOR, "button.result-wrap-accordion-button")
                    if btns:
                        btn = btns[0]
                        computed_display = driver.execute_script(
                            "return window.getComputedStyle(arguments[0]).display;", btn
                        )
                        if computed_display != "none":
                            more_btn = btn
                except:
                    pass
            
            if not more_btn:
                log("   ✓ 더보기 버튼 없음 - 모든 게시글 수집 완료")
                break
            
            # 더보기 클릭
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                    more_btn
                )
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", more_btn)
                loading_iteration += 1
                log(f"   ✓ 더보기 클릭 성공 ({loading_iteration}회)")
                
                # 새 게시글 로딩 대기
                old_count = len(post_items)
                try:
                    wait.until(lambda d: len(d.find_elements(
                        By.CSS_SELECTOR,
                        "li.elasticSearch-result-item.js-search-item[data-code='post']"
                    )) > old_count)
                    log(f"   ✓ 새 게시글 로딩 완료")
                except:
                    log(f"   [경고] 새 게시글 로딩 확인 실패")
                    time.sleep(1.5)
                
            except Exception as e:
                log(f"   [경고] 더보기 클릭 실패: {e}")
                break
            
        except Exception as e:
            log(f"   [오류] 라운드 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
            break
    
    log(f"\n{'='*60}")
    log(f"최종 더보기 클릭 횟수: {loading_iteration}회")
    log(f"✅ 총 {len(all_results)}개 게시글 수집 완료")
    
    if all_results:
        dates_with_value = [r['visit_date'] for r in all_results if r.get('visit_date')]
        if dates_with_value:
            earliest = min(dates_with_value)
            latest = max(dates_with_value)
            log(f"수집 범위: {earliest.date()} ~ {latest.date()}")
    
    log(f"{'='*60}\n")
    return all_results


def find_and_click_post_by_title(driver, title: str) -> bool:
    """제목으로 게시글 찾아서 클릭"""
    try:
        wait = WebDriverWait(driver, 5)
        
        # 검색 결과 로딩 대기
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "li.elasticSearch-result-item.js-search-item[data-code='post']")
        ))
        
        post_items = driver.find_elements(
            By.CSS_SELECTOR,
            "li.elasticSearch-result-item.js-search-item[data-code='post']"
        )
        
        for idx, item in enumerate(post_items):
            try:
                title_elem = item.find_element(By.CSS_SELECTOR, "b.result-item-title")
                item_title = title_elem.text.strip()
                
                if item_title == title:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", item)
                    time.sleep(0.5)
                    
                    clicked = False
                    
                    try:
                        driver.execute_script("arguments[0].click();", item)
                        clicked = True
                    except Exception:
                        pass
                    
                    if not clicked:
                        try:
                            item.click()
                            clicked = True
                        except Exception:
                            pass
                    
                    if not clicked:
                        try:
                            title_elem.click()
                            clicked = True
                        except Exception:
                            pass
                    
                    if not clicked:
                        return False
                    
                    # 팝업 로딩 대기
                    try:
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h4.js-post-title.post-title")))
                    except:
                        time.sleep(1.5)
                    
                    return True
                    
            except Exception:
                continue
        
        log(f"  ✗ 제목과 일치하는 게시글을 찾을 수 없음: {title}")
        return False
        
    except Exception as e:
        log(f"  ✗ 게시글 찾기 실패: {e}")
        return False


def click_more_button_in_post(driver) -> bool:
    """게시글 본문의 '더보기' 버튼 클릭"""
    try:
        wait = WebDriverWait(driver, 3)
        
        more_btns = driver.find_elements(By.CSS_SELECTOR, "button#postMoreButton.js-contents-more-btn")
        
        if not more_btns:
            return False
        
        more_btn = more_btns[0]
        
        style = more_btn.get_attribute("style") or ""
        is_displayed = more_btn.is_displayed()
        
        if is_displayed and "display: none" not in style and "display:none" not in style:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_btn)
            time.sleep(0.3)
            
            driver.execute_script("arguments[0].click();", more_btn)
            
            # 본문 펼쳐짐 대기
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#originalPost.post-card-content")))
                time.sleep(0.5)
            except:
                time.sleep(1.0)
            
            return True
        else:
            return False
            
    except NoSuchElementException:
        return False
    except Exception:
        return False


def extract_post_content(driver) -> Dict[str, Any]:
    """게시글 본문 내용 추출"""
    content_data = {
        "content_text": None,
    }
    
    try:
        click_more_button_in_post(driver)
        
        try:
            content_elem = driver.find_element(By.CSS_SELECTOR, "div#originalPost.post-card-content")
            
            content_html = content_elem.get_attribute("innerHTML")
            
            soup = BeautifulSoup(content_html, 'html.parser')
            
            for img in soup.find_all(['div'], class_=re.compile('image')):
                img.decompose()
            
            content_data["content_text"] = soup.get_text(separator='\n', strip=True)
            
        except Exception as e:
            log(f"  [경고] 본문 추출 실패: {e}")
        
        return content_data
        
    except Exception as e:
        log(f"  ✗ 게시글 내용 추출 실패: {e}")
        return content_data


def close_popup(driver) -> bool:
    """게시글 팝업 닫기"""
    try:
        close_btn = driver.find_element(By.CSS_SELECTOR, "button.btn-close.card-popup-close")
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", close_btn)
        time.sleep(0.2)
        
        clicked = False
        
        try:
            driver.execute_script("arguments[0].click();", close_btn)
            clicked = True
        except Exception:
            pass
        
        if not clicked:
            try:
                close_btn.click()
                clicked = True
            except Exception:
                pass
        
        if not clicked:
            return False
        
        # 팝업 닫힘 대기
        wait = WebDriverWait(driver, 3)
        try:
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "button.btn-close.card-popup-close")))
        except:
            time.sleep(0.5)
        
        return True
        
    except NoSuchElementException:
        return False
    except Exception:
        return False


def process_single_post(driver, post_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """단일 게시글 처리"""
    title = post_info["title"]
    
    max_retries = 2
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                log(f"  재시도 {attempt}/{max_retries-1}")
            
            if not find_and_click_post_by_title(driver, title):
                if attempt < max_retries - 1:
                    time.sleep(1.0)
                    continue
                else:
                    return None
            
            content_data = extract_post_content(driver)
            
            visit_date = extract_visit_date_from_title(title)
            
            result = {
                "visit_date": visit_date,
                "post_date": post_info["post_date"],
                "project": post_info["project"],
                "title": title,
                "author": post_info["author"],
                "content_text": content_data["content_text"],
                "collected_at": datetime.now().isoformat(),
            }
            
            close_popup(driver)
            
            return result
            
        except Exception as e:
            try:
                close_popup(driver)
            except:
                pass
            
            if attempt < max_retries - 1:
                time.sleep(1.0)
            else:
                return None
    
    return None


# ============================================================================
# 메인 함수
# ============================================================================

def run_flow_visit_crawling(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """플로우 방문일지 크롤링 메인 함수"""
    
    # 날짜 설정
    if start_date is None or end_date is None:
        today = datetime.now()
        end_date = end_date or today.strftime("%Y-%m-%d")
        
        if COLLECTION_MODE == "all":
            if ALL_DATA_START_DATE:
                start_date = ALL_DATA_START_DATE
                log(f"전체 수집 모드: {start_date} ~ {end_date}")
            else:
                start_date = "2020-01-01"
                log(f"전체 수집 모드 (시작일 미지정): {start_date} ~ {end_date}")
        else:
            start_date = start_date or (today - timedelta(days=RECENT_DAYS)).strftime("%Y-%m-%d")
            log(f"최근 {RECENT_DAYS}일 수집 모드: {start_date} ~ {end_date}")
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    
    log("=" * 60)
    log(f"플로우 방문일지 크롤링 시작 (HEADLESS_MODE={HEADLESS_MODE})")
    log(f"수집 모드: {COLLECTION_MODE}")
    
    if COLLECTION_MODE == "all":
        if ALL_DATA_START_DATE:
            log(f"수집 범위: {start_date} 이후 전체")
        else:
            log(f"수집 범위: 전체 기간")
    else:
        log(f"수집 기간: {start_date} ~ {end_date}")
    
    log("=" * 60)
    
    driver = None
    
    try:
        driver = launch_browser()
        
        if not do_login(driver):
            log("로그인 실패로 종료")
            return pd.DataFrame()
        
        if not open_search_popup(driver):
            log("검색 팝업 열기 실패")
            return pd.DataFrame()
        
        if not search_keyword(driver, "방문일지"):
            log("검색 실패")
            return pd.DataFrame()
        
        if not click_post_tab(driver):
            log("게시물 탭 클릭 실패")
            return pd.DataFrame()
        
        if not click_latest_sort(driver):
            log("최신순 정렬 실패")
            return pd.DataFrame()
        
        if not click_all_period(driver):
            log("전체 기간 필터 클릭 실패")
            return pd.DataFrame()
        
        all_results = collect_all_posts(driver, start_dt, end_dt)
        
        if not all_results:
            log("수집된 게시글이 없습니다")
            return pd.DataFrame()
        
        # DataFrame 생성
        result_df = pd.DataFrame(all_results)
        
        if not result_df.empty:
            columns_order = [
                'visit_date',
                'post_date',
                'project', 
                'title', 
                'author',
                'content_text', 
                'collected_at'
            ]
            
            available_columns = [col for col in columns_order if col in result_df.columns]
            result_df = result_df[available_columns]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = OUTPUT_DIR / f"{PLATFORM_NAME}_{start_date}_{end_date}_{timestamp}.csv"
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            log(f"\n✅ CSV 저장: {output_file}")
            log(f"   컬럼: {list(result_df.columns)}")
        
        log("\n" + "=" * 60)
        log("플로우 방문일지 크롤링 완료")
        log(f"  수집 성공: {len(all_results)}개")
        log("=" * 60)
        
        return result_df
        
    except Exception as e:
        log(f"❌크롤링 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
        
    finally:
        if driver:
            try:
                driver.quit()
                time.sleep(0.5)
                log("브라우저 종료")
            except:
                pass


# ============================================================================
# 테스트 실행
# ============================================================================

if __name__ == "__main__":
    print("\n[테스트] 수집 시작")
    result_df = run_flow_visit_crawling()
    
    if not result_df.empty:
        print("\n수집 결과:")
        print(result_df[["visit_date", "title", "author", "project"]].head(10))
        print(f"\n총 {len(result_df)}개 수집")
    else:
        print("수집된 데이터가 없습니다")
