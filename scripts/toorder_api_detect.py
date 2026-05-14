"""
toorder CEO 사이트 채널별 일매출 보고서 API 탐지
Selenium + CDP performance logging + 버튼 인터랙션
"""
import sys
import os
import json
import time

sys.path.insert(0, "/mnt/c/airflow")
os.environ["AIRFLOW_HOME"] = "/mnt/c/airflow"
os.environ["DOWNLOAD_DIR"] = "/tmp/toorder_detect"
os.makedirs("/tmp/toorder_detect", exist_ok=True)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

ACCOUNT_ID = "doridang15"
PASSWORD = "ehfl5233!"
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"


def launch_browser():
    options = Options()
    chrome_bin = "/usr/bin/google-chrome"
    if os.path.exists(chrome_bin):
        options.binary_location = chrome_bin
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=ko-KR")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    prefs = {"download.default_directory": "/tmp/toorder_detect", "download.prompt_for_download": False}
    options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def do_login(driver):
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="id"]')))
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, 'input[name="id"]').send_keys(ACCOUNT_ID)
    driver.find_element(By.CSS_SELECTOR, 'input[name="password"]').send_keys(PASSWORD)
    driver.execute_script("document.querySelector('input[name=\"isCompany\"]').click()")
    time.sleep(0.3)
    driver.execute_script("document.querySelector('button[type=\"submit\"]').click()")
    for i in range(20):
        time.sleep(1)
        if "/dashboard" in driver.current_url and "auth" not in driver.current_url:
            return True
    return False


def flush_and_parse_logs(driver):
    """performance 로그 파싱 → {rid: {url, method, post_data, headers}}"""
    reqs = {}
    resps = {}
    try:
        logs = driver.get_log("performance")
    except Exception:
        return reqs, resps
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            method = msg.get("method", "")
            params = msg.get("params", {})
            if method == "Network.requestWillBeSent":
                req = params.get("request", {})
                rtype = params.get("type", "")
                if rtype in ("XHR", "Fetch"):
                    rid = params.get("requestId", "")
                    reqs[rid] = {
                        "url": req.get("url", ""),
                        "method": req.get("method", ""),
                        "headers": req.get("headers", {}),
                        "post_data": req.get("postData", ""),
                    }
            elif method == "Network.responseReceived":
                rtype = params.get("type", "")
                if rtype in ("XHR", "Fetch"):
                    rid = params.get("requestId", "")
                    resp = params.get("response", {})
                    resps[rid] = {"status": resp.get("status"), "url": resp.get("url", "")}
        except Exception:
            pass
    return reqs, resps


def print_api_results(driver, reqs, resps, label=""):
    skip_hosts = ["iconify", "cognito-idp"]
    print(f"\n=== {label} XHR/Fetch 요청 {len(reqs)}건 ===")
    for rid, req in reqs.items():
        url = req["url"]
        if any(s in url for s in skip_hosts):
            continue
        resp_info = resps.get(rid, {})
        print(f"\n[{req['method']}] {url}")
        print(f"  status: {resp_info.get('status', '?')}")
        if req.get("post_data"):
            try:
                pd = json.loads(req["post_data"])
                print(f"  POST: {json.dumps(pd, ensure_ascii=False, indent=2)[:600]}")
            except Exception:
                print(f"  POST: {req['post_data'][:300]}")
        for hk in ("Authorization", "authorization"):
            if hk in req.get("headers", {}):
                v = req["headers"][hk]
                print(f"  header[{hk}]: {v[:80]}...")
        # 응답 body
        try:
            result = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
            body_str = result.get("body", "")
            if body_str.strip().startswith(("{", "[")):
                data = json.loads(body_str)
                if isinstance(data, dict):
                    print(f"  resp keys: {list(data.keys())}")
                    for k, v in data.items():
                        if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                            print(f"  list['{k}'] len={len(v)} | [0] fields: {list(v[0].keys())}")
                            print(f"  [0] sample: {json.dumps(v[0], ensure_ascii=False)[:400]}")
                    full = json.dumps(data, ensure_ascii=False, indent=2)
                    print(full[:3000] if len(full) > 3000 else full)
                elif isinstance(data, list):
                    print(f"  resp list len={len(data)}")
                    if data and isinstance(data[0], dict):
                        print(f"  [0] fields: {list(data[0].keys())}")
                    print(json.dumps(data[:5], ensure_ascii=False, indent=2)[:3000])
        except Exception:
            pass


def detect_api():
    driver = launch_browser()
    print(f"Chrome: {driver.capabilities.get('browserVersion','?')}")

    print("\n=== 로그인 ===")
    ok = do_login(driver)
    print(f"로그인: {ok} | {driver.current_url}")
    if not ok:
        driver.quit()
        return

    # ── 목표 페이지 이동 ─────────────────────────────────────────────
    print("\n=== 목표 페이지 이동 ===")
    driver.get_log("performance")  # flush

    driver.get(SALES_REPORT_URL)
    time.sleep(8)
    print(f"URL: {driver.current_url}")

    reqs1, resps1 = flush_and_parse_logs(driver)
    print_api_results(driver, reqs1, resps1, "페이지 로드 시")

    # ── 페이지 구조 파악 ─────────────────────────────────────────────
    print("\n=== 페이지 HTML 요소 파악 ===")
    # 버튼, 입력 필드, 날짜 관련 요소 확인
    page_info = driver.execute_script("""
        var result = {};
        // 버튼 목록
        var btns = document.querySelectorAll('button');
        result.buttons = Array.from(btns).map(b => ({
            text: b.innerText.trim().substring(0, 30),
            class: b.className.substring(0, 60),
            type: b.type
        })).filter(b => b.text);

        // input 목록
        var inputs = document.querySelectorAll('input');
        result.inputs = Array.from(inputs).map(i => ({
            type: i.type, name: i.name, placeholder: i.placeholder, value: i.value.substring(0, 30)
        }));

        // select 목록
        var selects = document.querySelectorAll('select');
        result.selects = Array.from(selects).map(s => ({
            name: s.name, id: s.id, options: s.options.length
        }));

        return result;
    """)
    print(f"버튼: {len(page_info.get('buttons', []))}개")
    for b in page_info.get("buttons", [])[:20]:
        print(f"  [{b['type']}] '{b['text']}' class={b['class'][:50]}")
    print(f"inputs: {page_info.get('inputs', [])}")
    print(f"selects: {page_info.get('selects', [])}")

    # ── "보고서 생성" 버튼 클릭 ─────────────────────────────────────
    print("\n=== '보고서 생성' 버튼 클릭 ===")
    driver.get_log("performance")  # flush

    clicked = False
    click_targets = [
        ("XPATH", "//button[contains(text(),'보고서 생성')]"),
        ("XPATH", "//button[contains(text(),'조회')]"),
        ("XPATH", "//button[contains(text(),'검색')]"),
    ]
    for selector_type, selector in click_targets:
        try:
            el = driver.find_element(By.XPATH, selector)
            if el and el.is_displayed():
                print(f"  클릭: text='{el.text}'")
                driver.execute_script("arguments[0].click();", el)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        print("  버튼 못 찾음")

    time.sleep(8)
    reqs2, resps2 = flush_and_parse_logs(driver)
    print_api_results(driver, reqs2, resps2, "보고서 생성 클릭 후")

    # ── 날짜 변경 + 조회 ────────────────────────────────────────────
    print("\n=== 날짜 관련 요소 + 직접 API 탐지 ===")
    # 날짜 입력 필드 찾기
    date_els = driver.execute_script("""
        var els = [];
        document.querySelectorAll('input[type="date"], input[placeholder*="날짜"], input[placeholder*="date"], [class*="date"], [class*="Date"]').forEach(function(el) {
            els.push({tag: el.tagName, type: el.type || '', class: el.className.substring(0,60), placeholder: el.placeholder || '', value: el.value || ''});
        });
        return els;
    """)
    print(f"날짜 관련 요소: {date_els}")

    # ── 오늘 날짜로 데이터 조회 시도 (React 상태 디버깅) ────────────
    print("\n=== React 컴포넌트 상태 확인 ===")
    state_info = driver.execute_script("""
        // __NEXT_DATA__ 또는 window.__redux_store 또는 React fiber 확인
        var info = {};
        if (window.__NEXT_DATA__) info.next_data_keys = Object.keys(window.__NEXT_DATA__);
        if (window.__redux_store) info.redux = true;
        // title 확인
        info.title = document.title;
        info.url = location.href;
        // 페이지 내 텍스트 (매출, 채널 키워드)
        var bodyText = document.body.innerText;
        info.has_sales_text = bodyText.includes('매출') || bodyText.includes('주문') || bodyText.includes('채널');
        info.body_snippet = bodyText.substring(0, 300);
        return info;
    """)
    print(f"페이지 상태: {json.dumps(state_info, ensure_ascii=False, indent=2)}")

    # localStorage 토큰
    print("\n=== localStorage 인증 토큰 ===")
    try:
        ls = driver.execute_script("return Object.entries(localStorage)")
        for k, v in ls:
            if any(kw in k.lower() for kw in ["token", "auth", "access", "cognito", "user"]):
                print(f"  {k}: {str(v)[:200]}")
        # Cognito IdToken 전체 출력 (API 직접 호출에 필요)
        id_token = driver.execute_script(
            "var k = Object.keys(localStorage).find(k => k.includes('idToken'));"
            "return k ? localStorage.getItem(k) : null;"
        )
        if id_token:
            print(f"\nIdToken (전체): {id_token[:300]}...")
    except Exception as e:
        print(f"  접근 실패: {e}")

    # ── API 직접 호출 (브라우저 내에서) ────────────────────────────
    print("\n=== API 직접 호출 탐지 ===")
    driver.get_log("performance")  # flush

    driver.set_script_timeout(20)
    api_result = driver.execute_async_script("""
        var callback = arguments[arguments.length - 1];
        var token = null;
        Object.keys(localStorage).forEach(function(k) {
            if (k.includes('idToken')) token = localStorage.getItem(k);
        });
        if (!token) { callback({error: 'no idToken'}); return; }
        var today = new Date();
        var ymd = today.toISOString().split('T')[0];
        fetch('https://5yb80agx2l.execute-api.ap-northeast-2.amazonaws.com/main/sales/getDailySalesByOrderKind', {
            method: 'POST',
            headers: {'Content-Type': 'application/json', 'Authorization': token},
            body: JSON.stringify({'COMPANY_ID': '10071', 'START_DATE': ymd, 'END_DATE': ymd})
        })
        .then(function(r) { return r.json().then(function(d) { return {status: r.status, data: d}; }); })
        .then(callback)
        .catch(function(e) { callback({error: e.toString()}); });
    """)

    print(f"직접 API 결과: {json.dumps(api_result, ensure_ascii=False, indent=2)[:1500]}")

    # performance 로그에서 새 요청 캡처
    time.sleep(2)
    reqs3, resps3 = flush_and_parse_logs(driver)
    print_api_results(driver, reqs3, resps3, "직접 API 호출")

    driver.quit()
    print("\n=== 완료 ===")


if __name__ == "__main__":
    detect_api()
