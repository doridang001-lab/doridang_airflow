
# modules/extract/crawling_relay_cs_fdam.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time


def setup_chrome_driver(headless: bool = True):
    """Chrome 드라이버 설정"""
    options = Options()

    if headless:
        # headless=new가 더 안정적 (Chrome 109+)
        options.add_argument('--headless=new')

    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # ✅ 화면 최대화 (요청사항)
        try:
            driver.maximize_window()
        except Exception:
            # headless 등에서 maximize 실패시 대체 사이즈
            driver.set_window_size(1920, 1080)

        print("✅ Chrome 드라이버 초기화 성공")
        return driver

    except Exception as e:
        print(f"❌ Chrome 드라이버 초기화 실패: {e}")
        raise


def wait_page_ready(driver, timeout: int = 30):
    """
    공통: 페이지가 완전히 로딩될 때까지 대기
    - document.readyState === 'complete'
    - 로딩 스피너/오버레이가 있다면 사라질 때까지 대기 (선택자 추정)
    """
    wait = WebDriverWait(driver, timeout)
    try:
        # 1) 문서 준비 상태 대기
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

        # 2) 로딩 스피너가 있다면 사라질 때까지 대기 (Ant Design 패턴)
        spinner_selectors = [
            # 일반 스피너
            "//div[contains(@class,'ant-spin') and contains(@class,'ant-spin-spinning')]",
            # 중첩 로딩 컨테이너
            "//div[contains(@class,'ant-spin-nested-loading') and .//div[contains(@class,'ant-spin-spinning')]]",
            # 모달 확인/로딩
            "//div[contains(@class,'ant-modal') and contains(@class,'ant-modal-confirm')]",
            # 전체 화면 오버레이(있다면)
            "//div[contains(@class,'ant-message') and contains(@class,'ant-message-notice')]",
        ]

        for selector in spinner_selectors:
            try:
                wait.until_not(EC.presence_of_element_located((By.XPATH, selector)))
            except Exception:
                # 해당 선택자가 없거나 이미 사라져 있으면 패스
                pass

        # 약간의 안정화 대기
        time.sleep(0.5)
    except Exception as e:
        print(f"⚠️ 페이지 로딩 대기 중 이슈: {e}")


def login_relay_fms(driver, user_id: str, password: str, server_name: str = "도리당", server_number: str = "10625"):
    """
    Relay FMS 로그인 (서버 추가 포함)
    순서:
    1. ID 입력
    2. PW 입력
    3. 서버 목록 버튼 클릭 → 서버 추가 → 선택
    4. 로그인 버튼 클릭
    5. 로그인 후 로딩 대기 (요청사항)
    """
    try:
        driver.get("https://erp.relayfms.com/login")
        wait = WebDriverWait(driver, 20)
        wait_page_ready(driver, timeout=20)

        # 1. ID 입력
        print("📝 ID 입력 중...")
        id_input = wait.until(EC.presence_of_element_located((By.ID, "login_form_id")))
        id_input.clear()
        id_input.send_keys(user_id)
        time.sleep(0.3)

        # 2. PW 입력
        print("🔒 PW 입력 중...")
        pw_input = driver.find_element(By.ID, "login_form_pw")
        pw_input.clear()
        pw_input.send_keys(password)
        time.sleep(0.3)

        # 3. 서버 목록 추가 (로그인 전)
        print("🔧 서버 목록 추가 작업 시작...")
        add_server_to_list(driver, server_name, server_number)

        # 로그인 전 URL 기록
        pre_url = driver.current_url

        # 4. 로그인 버튼 클릭
        print("🚀 로그인 버튼 클릭 중...")
        login_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@type='submit' and contains(@class, 'ant-btn-primary')]")
            )
        )
        driver.execute_script("arguments[0].click();", login_button)

        # ✅ 로그인 후 로딩/전환 대기 (요청사항)
        post_wait = WebDriverWait(driver, 30)
        # 1) URL 변경 대기 (변경되지 않아도 다음 단계 진행)
        try:
            post_wait.until(lambda d: d.current_url != pre_url)
        except Exception:
            pass

        # 2) 페이지 준비 상태
        wait_page_ready(driver, timeout=30)

        # 3) 사이드 네비 등장 → 로그인 성공 판단
        try:
            post_wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'ant-layout')]"))
            )
        except Exception:
            pass

        print("✅ 로그인 성공 및 로딩 완료")

    except Exception as e:
        print(f"❌ 로그인 실패: {e}")
        raise


def add_server_to_list(driver, server_name: str = "도리당", server_number: str = "10625"):
    """
    서버 목록에 서버 추가 (로그인 전에 실행)
    """
    try:
        wait = WebDriverWait(driver, 15)

        # 1. "서버 목록" 버튼 클릭
        print("  ├─ 📋 서버 목록 버튼 클릭")
        server_list_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class, 'ant-btn-link') and .//span[text()='서버 목록']]")
            )
        )
        driver.execute_script("arguments[0].click();", server_list_btn)
        time.sleep(1.0)  # 모달 로딩 대기

        # 2. "추가" 버튼 클릭 (plus-circle 아이콘)
        print("  ├─ ➕ 추가 버튼 클릭")
        add_btn = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[contains(@class, 'ant-btn-primary')]//span[@aria-label='plus-circle']/..")
            )
        )
        driver.execute_script("arguments[0].click();", add_btn)
        time.sleep(0.6)  # 폼 로딩 대기

        # 3. 사용처명 입력
        print(f"  ├─ 📝 사용처명 입력: {server_name}")
        name_input = wait.until(EC.presence_of_element_located((By.ID, "edit_form_name")))
        name_input.clear()
        name_input.send_keys(server_name)
        time.sleep(0.2)

        # 4. 연결서버 입력
        print(f"  ├─ 🔢 연결서버 입력: {server_number}")
        server_input = driver.find_element(By.ID, "edit_form_server_number")
        server_input.clear()
        server_input.send_keys(server_number)
        time.sleep(0.2)

        # 5. "추가" 버튼 클릭 (폼 제출)
        print("  ├─ ✅ 추가 버튼 클릭 (폼 제출)")
        submit_buttons = driver.find_elements(
            By.XPATH,
            "//button[contains(@class, 'ant-btn-primary') and .//span[text()='추가']]"
        )
        submit_btn = submit_buttons[-1] if len(submit_buttons) > 1 else submit_buttons[0]
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(0.8)  # 저장 완료 대기

        # 6. "선택" 버튼 클릭
        print("  └─ ✅ 선택 버튼 클릭")
        select_btn = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[contains(@class, 'ant-btn-primary') and .//span[text()='선택']]")
            )
        )
        driver.execute_script("arguments[0].click();", select_btn)
        time.sleep(0.3)

        print("  ✅ 서버 추가 및 선택 완료")

    except Exception as e:
        print(f"  ⚠️ 서버 추가 실패 (이미 존재하거나 에러): {e}")
        # 에러가 나도 계속 진행
        try:
            print("  └─ 선택 버튼 찾기 시도...")
            select_btns = driver.find_elements(
                By.XPATH,
                "//button[contains(@class, 'ant-btn-primary') and .//span[text()='선택']]"
            )
            if select_btns:
                driver.execute_script("arguments[0].click();", select_btns[0])
                print("  └─ 선택 버튼 클릭 성공")
                time.sleep(0.3)
            else:
                print("  └─ ESC 키로 모달 닫기")
                driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                time.sleep(0.3)
        except Exception:
            print("  └─ 모달 닫기 실패, 계속 진행")


def expand_submenu_and_click_item(
    driver,
    submenu_text: str,
    item_text: str,
    timeout: int = 20
):
    """
    Ant Design 메뉴:
    - 서브메뉴 타이틀(div.role=menuitem)을 클릭해 펼치고
    - aria-controls로 연결된 팝업 컨테이너 안에서 item_text를 클릭
    """
    wait = WebDriverWait(driver, timeout)

    # 1) 서브메뉴 타이틀 찾기 (텍스트: CS관리)
    submenu_title = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            # role=menuitem + ant-menu-submenu-title + 내부 텍스트에 submenu_text 포함
            "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')"
            " and .//span[contains(@class,'ant-menu-title-content')]"
            f"[.//span[contains(normalize-space(.), '{submenu_text}')]]]"
        ))
    )
    # 화면 중앙으로 스크롤 후 클릭
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submenu_title)
    time.sleep(0.2)

    # 2) 펼침 상태 확인/토글
    try:
        expanded = submenu_title.get_attribute("aria-expanded")
    except Exception:
        expanded = None

    if expanded != "true":
        # 클릭으로 펼치기 (hover가 필요한 경우도 대비해 마우스 이동)
        try:
            ActionChains(driver).move_to_element(submenu_title).perform()
            time.sleep(0.1)
        except Exception:
            pass
        driver.execute_script("arguments[0].click();", submenu_title)

        # 펼침 완료 대기
        try:
            wait.until(lambda d: submenu_title.get_attribute("aria-expanded") == "true")
        except Exception:
            # 일부 테마는 aria-expanded 미사용 → 팝업 등장으로 대체
            pass

    # 3) 팝업 컨테이너 ID 파악 (aria-controls)
    popup_id = submenu_title.get_attribute("aria-controls")
    item = None

    if popup_id:
        # 팝업 컨테이너 내부에서 '매장CS' 찾기
        popup = wait.until(EC.presence_of_element_located((By.ID, popup_id)))
        item_xpath = (
            f"//*[@id='{popup_id}']"
            "//li[@role='menuitem' and .//span[contains(@class,'ant-menu-title-content')]"
            f"[contains(normalize-space(.), '{item_text}')]]"
        )
        item = wait.until(EC.element_to_be_clickable((By.XPATH, item_xpath)))
    else:
        # Fallback: 같은 서브메뉴 블록 하위에서 찾기
        item_xpath = (
            "//li[contains(@class,'ant-menu-submenu') and .//div[@role='menuitem']"
            "  [.//span[contains(@class,'ant-menu-title-content')]"
            f"   [.//span[contains(normalize-space(.), '{submenu_text}')]]]]"
            "//li[@role='menuitem' and .//span[contains(@class,'ant-menu-title-content')]"
            f"[contains(normalize-space(.), '{item_text}')]]"
        )
        item = wait.until(EC.element_to_be_clickable((By.XPATH, item_xpath)))

    # 4) 항목 클릭
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    time.sleep(0.1)
    try:
        ActionChains(driver).move_to_element(item).perform()
    except Exception:
        pass
    driver.execute_script("arguments[0].click();", item)

    # 페이지 전환/로딩 안정화
    wait_page_ready(driver, timeout=30)
    time.sleep(0.3)


def navigate_to_cs_market(driver):
    """CS관리 > 매장CS 메뉴로 이동 (서브메뉴 오버레이 대응)"""
    try:
        print("📂 CS관리 메뉴 클릭 중...")
        expand_submenu_and_click_item(driver, submenu_text="CS관리", item_text="매장CS")
        print("✅ 매장CS 페이지 이동 완료")
    except Exception as e:
        print(f"❌ 메뉴 이동 실패: {e}")
        # 디버깅용으로 현재 서브메뉴 요약 출력
        try:
            menus = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]")
            print(f"  🔎 발견된 서브메뉴 타이틀 수: {len(menus)}")
            for m in menus[:10]:
                try:
                    txt_el = m.find_element(By.XPATH, ".//span[contains(@class,'ant-menu-title-content')]")
                    txt = txt_el.text
                    print(f"   - 서브메뉴: {txt} | expanded={m.get_attribute('aria-expanded')} | controls={m.get_attribute('aria-controls')}")
                except Exception:
                    pass
        except Exception:
            pass
        raise


def extract_cs_table(driver) -> pd.DataFrame:
    """CS 테이블 데이터 추출"""
    try:
        wait = WebDriverWait(driver, 25)

        # 테이블 로딩 대기
        print("⏳ 테이블 로딩 대기 중...")
        wait_page_ready(driver, timeout=30)
        table = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ant-table-container')]")))
        time.sleep(1.0)  # 실제 데이터 렌더링 안정화

        # 데이터 추출
        rows = driver.find_elements(
            By.XPATH,
            "//tbody[@class='ant-table-tbody']/tr[not(contains(@class, 'measure-row'))]"
        )

        print(f"📊 발견된 행 수: {len(rows)}")

        data = []

        for idx, row in enumerate(rows, 1):
            try:
                cells = row.find_elements(By.TAG_NAME, "td")

                if len(cells) < 21:
                    print(f"  ⚠️ 행 {idx}: 셀 개수 부족 ({len(cells)}개)")
                    continue

                # 이미지 URL 추출
                try:
                    img_element = cells[8].find_element(By.TAG_NAME, "img")
                    img_url = img_element.get_attribute("src").replace("_thumb.jpg", ".jpg")
                except Exception:
                    img_url = ""

                try:
                    process_img = cells[14].find_element(By.TAG_NAME, "img")
                    process_img_url = process_img.get_attribute("src").replace("_thumb.jpg", ".jpg")
                except Exception:
                    process_img_url = ""

                row_data = {
                    '접수_접수번호': cells[1].text.strip(),
                    '접수_등록일': cells[2].text.strip(),
                    '접수_회사구분': cells[3].text.strip(),
                    '접수_매장명': cells[4].text.strip(),
                    '접수_CS구분': cells[5].text.strip(),
                    '접수_유형': cells[6].text.strip(),
                    '접수_내용': cells[7].text.strip(),
                    '접수_이미지': img_url,
                    '접수_유입경로': cells[9].text.strip(),
                    '접수_접수자': cells[10].text.strip(),
                    '접수_매입처': cells[11].text.strip(),
                    '접수_제조일자': cells[12].text.strip(),
                    '처리_내용': cells[13].text.strip(),
                    '처리_이미지': process_img_url,
                    '처리_FMS전송': cells[15].text.strip(),
                    '처리_처리일자': cells[16].text.strip(),
                    '처리_처리자': cells[17].text.strip(),
                    'SV_이름': cells[18].text.strip(),
                    '진행상태': cells[19].text.strip(),
                    '비공개메모': cells[20].text.strip(),
                }

                data.append(row_data)

            except Exception as e:
                print(f"  ⚠️ 행 {idx} 추출 실패: {e}")
                continue

        df = pd.DataFrame(data)
        print(f"✅ 테이블 추출 완료: {len(df)}건")

        return df

    except Exception as e:
        print(f"❌ 테이블 추출 실패: {e}")
        raise


def run_relay_cs_crawling(
    user_id: str,
    password: str,
    headless: bool = True,
    server_name: str = "도리당",
    server_number: str = "10625"
) -> pd.DataFrame:
    """
    Relay FMS 매장CS 크롤링 메인 함수
    """
    driver = None

    try:
        print("="*60)
        print("🚀 Relay CS 크롤링 시작")
        print("="*60)

        # 1. 드라이버 설정 (최대화 포함)
        driver = setup_chrome_driver(headless=headless)

        # 2. 로그인 (서버 추가 포함 + 로딩 대기)
        login_relay_fms(driver, user_id, password, server_name, server_number)

        # 3. 매장CS 메뉴 이동 (오버레이 대응 + 로딩 대기)
        navigate_to_cs_market(driver)

        # 4. 테이블 데이터 추출
        df = extract_cs_table(driver)

        print("="*60)
        print(f"🎉 크롤링 완료: {len(df)}건")
        print("="*60)
        return df

    except Exception as e:
        print("="*60)
        print(f"❌ 크롤링 실패: {e}")
        print("="*60)
        raise

    finally:
        if driver:
            driver.quit()
            print("✅ 드라이버 종료")

import datetime as dt

if __name__ == "__main__":
    # 테스트
    df = run_relay_cs_crawling(
        user_id="조민준",
        password="1234",
        headless=False,  # UI 확인 시 False 권장
        server_name="도리당",
        server_number="10625"
    )
    print(df)
    now = dt.datetime.now()
    save_path = f"C:\\Local_DB\\문의_DB\\프담_매장CS_수집_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(save_path, index=False, encoding="utf-8-sig")

