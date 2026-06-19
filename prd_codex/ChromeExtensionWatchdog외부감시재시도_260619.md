# ChromeExtensionWatchdog 외부 감시 + 실패/건너뜀 재시도

## Task
`coupang_host_chrome.ps1`이 Chrome port 9222로 `runner.html`을 자동 실행하지만, 배치 완료 후 일부 매장이 "건너뜀" 상태로 남거나 페이지가 종료되어도 재시도가 이루어지지 않는다. 외부 Python 감시 스크립트를 만들어 port 9222 Chrome에 Selenium attach → 배치 완료 감지 → 실패/건너뜀 행만 클릭 → 재수집, 최대 5회 반복한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- scripts 폴더는 분석/자동화용 단독 실행 스크립트, DAG runtime 로직 없음
- 파라미터는 `argparse` 사용
- Selenium: `webdriver.Chrome(options=opts)` + `opts.debugger_address = "localhost:9222"` (기존 Chrome attach 방식)
- Chrome DevTools Protocol 탭 탐색은 `http://127.0.0.1:9222/json` 활용 가능 (Reference Code 참고)

## Files to Create / Modify
- **신규**: `C:\airflow\scripts\coupang_extension_watchdog.py` — 핵심 감시 스크립트
- **신규 (선택)**: `C:\airflow\scripts\run_coupang.bat` — 시작 프로그램용 배치 래퍼
- `C:\airflow\scripts\coupang_host_chrome.ps1` — **변경 없음**

## Implementation Steps

### 1. 상수 및 logger 정의
```python
import argparse
import logging
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchWindowException, WebDriverException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RUNNER_URL = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
DEBUG_PORT = "localhost:9222"
MAX_RETRY = 5
POLL_SEC = 30
BATCH_TIMEOUT = 5400  # 90분
```

### 2. Chrome attach + runner.html 탭 찾기
```python
def connect_to_chrome() -> webdriver.Chrome:
    opts = Options()
    opts.debugger_address = DEBUG_PORT
    return webdriver.Chrome(options=opts)

def find_runner_tab(driver: webdriver.Chrome) -> bool:
    """window_handles 순회 → runner.html 탭으로 switch. 없으면 False."""
    for handle in driver.window_handles:
        try:
            driver.switch_to.window(handle)
            if "ocpdgnoaajajnlehamcalfcpholjhfbe" in driver.current_url:
                return True
        except WebDriverException:
            continue
    return False
```

### 3. 배치 완료 대기 (polling)
```python
def wait_for_batch_done(driver: webdriver.Chrome) -> None:
    """window.running === false 될 때까지 POLL_SEC 간격으로 대기.
    페이지 사라지면 재이동 후 계속."""
    deadline = time.time() + BATCH_TIMEOUT
    while time.time() < deadline:
        try:
            running = driver.execute_script("return window.running === true")
            if not running:
                return
        except (NoSuchWindowException, WebDriverException):
            logger.warning("runner.html 탭 사라짐 — 재이동 시도")
            if not find_runner_tab(driver):
                driver.get(RUNNER_URL)
            time.sleep(5)
        time.sleep(POLL_SEC)
    logger.warning("배치 타임아웃 %ds 초과", BATCH_TIMEOUT)
```

### 4. 실패/건너뜀 행 탐색
```python
def find_failed_row_indices(driver: webdriver.Chrome) -> list[str]:
    """tbody#rows 순회 → b-fail/b-warn 또는 b-wait+"건너뜀" 행의 index 반환."""
    rows = driver.find_elements(By.CSS_SELECTOR, "tbody#rows tr")
    failed: list[str] = []
    for row in rows:
        row_id = row.get_attribute("id")  # "row-25"
        if not row_id or not row_id.startswith("row-"):
            continue
        idx = row_id.replace("row-", "")
        for span in row.find_elements(By.CSS_SELECTOR, "span.badge"):
            cls = span.get_attribute("class") or ""
            text = span.text.strip()
            if "b-fail" in cls or "b-warn" in cls:
                failed.append(idx)
                break
            if "b-wait" in cls and text == "건너뜀":
                failed.append(idx)
                break
    return list(dict.fromkeys(failed))  # 중복 제거, 순서 유지
```

### 5. 실패 행 클릭 후 재시작
```python
def click_retry(driver: webdriver.Chrome, indices: list[str]) -> None:
    """각 tr#row-{idx} 클릭(테스트 모드 선택) → startBtn 클릭."""
    for idx in indices:
        driver.find_element(By.ID, f"row-{idx}").click()
        time.sleep(0.3)
    driver.find_element(By.ID, "startBtn").click()
    logger.info("재시도 시작: %s", indices)
```

### 6. 메인 루프 (최대 5회)
```python
def run(args: argparse.Namespace) -> None:
    driver = connect_to_chrome()
    if not find_runner_tab(driver):
        logger.info("runner.html 탭 없음 → 새로 열기")
        driver.get(RUNNER_URL)
        time.sleep(3)

    for attempt in range(args.max_retry + 1):
        logger.info("[%d/%d] 배치 완료 대기 중...", attempt, args.max_retry)
        wait_for_batch_done(driver)

        failed = find_failed_row_indices(driver)
        logger.info("실패/건너뜀 행: %s", failed)

        if not failed:
            logger.info("모든 매장 완료 — 종료")
            break
        if attempt >= args.max_retry:
            logger.warning("최대 재시도(%d) 도달 — 미완료 행: %s", args.max_retry, failed)
            break

        click_retry(driver, failed)
        time.sleep(3)  # 재시도 수집 시작 대기


def main() -> None:
    parser = argparse.ArgumentParser(description="Coupang extension runner.html 감시 + 재시도")
    parser.add_argument("--max-retry", type=int, default=MAX_RETRY)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
```

### 7. (선택) 시작 프로그램 배치 파일
`C:\airflow\scripts\run_coupang.bat`:
```batch
@echo off
powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1
timeout /t 10 /nobreak
python C:\airflow\scripts\coupang_extension_watchdog.py
```
이 `.bat` 파일을 `shell:startup` 폴더에 넣으면 로그인 시 자동 실행.

## Reference Code
### coupang_runner_autoclick.py (기존 DevTools attach 패턴)
```python
"""Auto click topHalfBtn on Coupang runner.html via Chrome DevTools Protocol."""
import logging
import time
import urllib.request
import json

logger = logging.getLogger(__name__)

RUNNER_URL_SUFFIX = "/runner.html"
RUNNER_URL_FALLBACK = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
DEBUG_ENDPOINT = "http://127.0.0.1:9222"

def _http_json(url: str):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())

def _fetch_tabs() -> list[dict]:
    return _http_json(f"{DEBUG_ENDPOINT}/json")

def _find_runner_url() -> str:
    tabs = _fetch_tabs()
    for tab in tabs:
        url = str(tab.get("url", ""))
        if url.startswith("chrome-extension://") and url.endswith(RUNNER_URL_SUFFIX):
            return url
    return RUNNER_URL_FALLBACK
```

## Test Cases
1. **Chrome attach 확인** — `coupang_host_chrome.ps1` 실행 후:
   `python -c "from selenium import webdriver; from selenium.webdriver.chrome.options import Options; o=Options(); o.debugger_address='localhost:9222'; d=webdriver.Chrome(options=o); print(d.current_url)"`
   → 기대: URL 출력 (에러 없음)

2. **runner 탭 감지** — 위 환경에서:
   `python -c "... find_runner_tab(driver) ..."`
   → 기대: `True` 반환, 탭 switch 성공 로그

3. **실패 행 탐색** — 배치 완료 직후:
   `python coupang_extension_watchdog.py --max-retry 0`
   → 기대: `실패/건너뜀 행: ['25', '13', ...]` 또는 `모든 매장 완료 — 종료`

4. **재시도 클릭** — 실패 행 있는 상태에서:
   `python coupang_extension_watchdog.py --max-retry 1`
   → 기대: 해당 행만 선택 후 startBtn 클릭 → 수집 재개 확인

5. **5회 루프 종료**:
   `python coupang_extension_watchdog.py --max-retry 5`
   → 기대: 5회 시도 후 `최대 재시도(5) 도달` 로그 또는 먼저 전체 완료

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL → 원인 분석 (NoSuchWindowException? CSS selector 불일치? running 변수 타이밍?)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `coupang_host_chrome.ps1` 수정 금지 — watchdog은 완전히 독립 프로세스
- `window.running` JS 변수는 배치 중 `true`, 완료 후 `false` — `is_batch_running()` 로직 기준
- `b-wait` 배지는 "대기"(수집 전)와 "건너뜀"(수집 후 skip) 두 가지 의미 → **텍스트를 반드시 확인**해야 함
- `b-ok` 행은 절대 클릭하지 않음 (이미 완료된 매장 재수집 금지)
- `running === true`인 동안 row 클릭 불가 (extension 내부 가드) → 반드시 완료 후에 클릭
- `selenium` 이미 설치됨, 추가 pip install 불필요

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (있으면 유지)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case
- ChromeDriver 버전 불일치 에러 시: `pip install --upgrade selenium` 후 재시도
