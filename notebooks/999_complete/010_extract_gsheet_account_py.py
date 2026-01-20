"""
쿠팡이츠 로그인 자동화 - 방법 3: 수동 세션 저장 후 자동화 (Firefox 버전)

* 최초 1회 수동 로그인으로 쿠키를 저장하고, 이후 자동화 시 그 쿠키를 사용합니다.
"""

import time
import random
from pathlib import Path
from typing import Optional
import pandas as pd
import re

# ⚠️ 필요 라이브러리: pip install selenium webdriver-manager gspread google-auth
try:
    from selenium import webdriver
    # ★ Chrome 대신 Firefox 서비스/옵션 사용 ★
    from selenium.webdriver.firefox.service import Service
    from selenium.webdriver.firefox.options import Options as FirefoxOptions 
    from webdriver_manager.firefox import GeckoDriverManager # Firefox 드라이버 매니저
    import pickle
except ImportError as e:
    print(f"❌ 필수 라이브러리가 부족합니다: {e}")
    print("→ pip install selenium webdriver-manager gspread google-auth")
    
# ==========================================
# 설정 상수 (이전에 정의된 값 유지)
# ==========================================
DEFAULT_CREDENTIALS = r"C:\Users\tjrrj\vscode\doritest\config\glowing-palace-465904-h6-7f82df929812.json"
DEFAULT_SHEET = "시트1"

# ★ 쿠키 파일 위치
COOKIE_FILE = Path("./coupang_firefox_cookies.pkl") # 파일명도 Firefox용으로 변경

# ==========================================
# 구글시트 추출 함수 (이전 코드에서 그대로 사용)
# ==========================================
def extract_gsheet(
    sheet_name: Optional[str] = None,
    *,
    url: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    credentials_path: Optional[str] = None,
) -> pd.DataFrame:
    # 이 함수는 브라우저와 무관하므로 수정 없이 사용
    # (내부 코드는 이전 답변과 동일하다고 가정하고 생략합니다.)
    
    try:
        import gspread
        from google.oauth2 import service_account
    except ImportError as e:
        raise ImportError(
            "필수 라이브러리가 없습니다. pip install gspread google-auth"
        ) from e

    sheet_name = sheet_name or DEFAULT_SHEET
    credentials_path = credentials_path or DEFAULT_CREDENTIALS

    if not Path(credentials_path).exists():
        raise FileNotFoundError(
            f"서비스 계정 JSON 파일이 존재하지 않습니다: {credentials_path}"
        )
    
    # ... (생략된 구글시트 접근 인증 및 데이터프레임 리턴 로직) ...
    # 실제 실행을 위해 구글시트 로직 전체가 포함되어야 합니다.
    # 안전을 위해 임시로 빈 데이터프레임을 리턴합니다. (실제 사용 시 수정 필요)
    
    # --- 임시 반환 (실제 사용 시 extract_gsheet 전체 코드로 대체 필수) ---
    print("⚠️ 경고: extract_gsheet 함수는 실행 가능하도록 전체 코드를 포함해야 합니다.")
    return pd.DataFrame([{'channel': 'coupangeats', 'login_url': 'http://store.coupangeats.com/merchant/login', 'id': 'temp_id', 'pw': 'temp_pw'}])
    # -----------------------------------------------------------------


# ==========================================
# 메인 함수: 수동 세션 저장 및 로드 (Firefox 버전)
# ==========================================
def run_manual_session_first_firefox():
    """
    1. 일반 Firefox로 수동 로그인 후 쿠키 저장
    2. 자동화 시 저장된 쿠키 로드 (Firefox 드라이버 사용)
    """
    print("="*60)
    print("방법 3: 수동 세션 저장 후 자동화 (Firefox)")
    print("="*60)
    
    # 계정 정보 로드
    try:
        acount_df = extract_gsheet(
            url="https://docs.google.com/spreadsheets/d/1P9QAcEJX0AIFGFbK_hS_vArCIVMNYaf-0_FVxHnE5hs/edit?usp=sharing"
        )
        coupang_account = acount_df[acount_df["channel"] == "coupangeats"].iloc[0]
        LOGIN_URL = coupang_account['login_url']
        MAIN_URL = LOGIN_URL.replace("/login", "/main")
    except Exception as e:
        print(f"❌ 계정 정보 로드 실패: {e}")
        return False
    
    # ★ Firefox Options 사용 ★
    options = FirefoxOptions()
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    
    driver = None
    
    try:
        # --- 쿠키 파일이 없으면 수동 로그인 모드 (최초 1회) ---
        if not COOKIE_FILE.exists():
            print("\n★ 모드 A: 쿠키 파일이 없습니다. 수동 로그인 모드 시작...")
            print("  1. Firefox 브라우저가 열리면 **직접 ID/PW를 입력하고 로그인**해주세요.")
            print("  2. 로그인 완료 후 **이 콘솔에서 Enter**를 눌러주세요.")
            
            # ★ Firefox 드라이버 실행 ★
            driver = webdriver.Firefox(
                service=Service(GeckoDriverManager().install()),
                options=options
            )
            
            driver.get(LOGIN_URL)
            input("\n✅ 로그인 완료 후 Enter를 누르세요...")
            
            # 쿠키 저장
            cookies = driver.get_cookies()
            with open(COOKIE_FILE, 'wb') as f:
                pickle.dump(cookies, f)
            
            print(f"✅ 쿠키 저장 완료: {COOKIE_FILE}")
            print(f"  저장된 쿠키 수: {len(cookies)}개")
            driver.quit()
            return True
            
        # --- 쿠키 파일이 있으면 자동 로그인 시도 모드 ---
        else:
            print(f"\n★ 모드 B: 저장된 쿠키 발견 ({COOKIE_FILE})")
            print("  쿠키를 로드하여 자동 로그인 시도...")
            
            # ★ Firefox 드라이버 실행 ★
            driver = webdriver.Firefox(
                service=Service(GeckoDriverManager().install()),
                options=options
            )
            
            # 1. 먼저 도메인에 접속 (쿠키 설정을 위해)
            driver.get(MAIN_URL)
            time.sleep(2)
            
            # 2. 쿠키 로드 및 주입
            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
            
            for cookie in cookies:
                try:
                    # Firefox 드라이버는 'expiry' 키 처리가 Chrome과 다를 수 있으므로 안전하게 변환
                    if 'expiry' in cookie and isinstance(cookie['expiry'], (float, int)):
                         cookie['expiry'] = int(cookie['expiry']) 
                    driver.add_cookie(cookie)
                except Exception as e:
                    # 'domain'이나 'path' 문제로 쿠키 추가 실패 가능
                    pass
            
            print(f"  쿠키 로드 완료: {len(cookies)}개")
            
            # 3. 페이지 새로고침하여 로그인 상태 확인
            driver.refresh()
            time.sleep(3)
            
            current_url = driver.current_url
            print(f"\n📍 현재 URL: {current_url}")
            
            if "login" not in current_url.lower() and "access denied" not in driver.title.lower():
                print("🎉 쿠키 기반 자동 로그인 성공!")
                success = True
            else:
                print("⚠️ 세션 만료 또는 로그인 실패. 쿠키 파일 삭제 후 수동 로그인 필요")
                COOKIE_FILE.unlink()  # 쿠키 파일 삭제
                success = False
            
            print("\n브라우저 수동 확인 (30초 대기)...")
            time.sleep(30)
            
            driver.quit()
            return success
            
    except Exception as e:
        print(f"❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            driver.quit()
        return False

# ==========================================
# 실행
# ==========================================
if __name__ == "__main__":
    print("="*60)
    print("🛡️ 쿠팡이츠 로그인 자동화 - 방법 3 (Firefox) 실행")
    print("="*60)
    
    input("준비가 되었으면 Enter를 누르세요...")
    print()
    
    run_manual_session_first_firefox()
    
