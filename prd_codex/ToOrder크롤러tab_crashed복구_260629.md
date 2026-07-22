# ToOrder 크롤러 "tab crashed" 복구

## Task
`DB_ToOrder_Daily_Store_Dags`가 Chrome 148(headless, Docker)에서 렌더러 크래시("tab crashed")로 수집에 실패한다. 크래시는 대체로 로그인 직후 `_do_login`의 `driver.get(LOGIN_URL)` 시점에 발생한다. 크래시를 줄이는 Chrome 옵션을 추가하고, 발생하더라도 retriable 오류로 인식해 브라우저를 새로 띄워 재시도하도록 복구 경로를 고친다.

핵심 원인: 크래시 예외를 `_do_login`이 삼키고 `return False` → 호출부는 `result["error"]="로그인 실패"; return result`로 즉시 종료 → 재시도 루프는 retriable **예외**에만 도므로 `PIPELINE_RETRIES=2`가 무의미하게 건너뛰어져 한 번의 크래시가 영구 실패가 된다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/extract/crawling_toorder_sales_report.py` (이 파일만 수정)

## Implementation Steps

1. **`_is_retriable_driver_error`에 크래시 키워드 추가 (line 179)**
   기존 네트워크 키워드 튜플을 **유지**하고 아래를 추가한다:
   ```python
   "tab crashed",
   "session deleted because of page crash",
   "page crash",
   "chrome not reachable",
   "disconnected: not connected to devtools",
   "no such window",
   ```
   `"renderer"` 단독 키워드는 오탐 위험으로 **추가하지 않는다**.

2. **`_do_login`의 get 블록 except를 retriable이면 re-raise (line 462-464)**
   ```python
   except Exception as exc:
       logger.error("[%s] 페이지 이동 실패: %s", account_id, exc)
       if _is_retriable_driver_error(str(exc)):
           raise
       return False
   ```
   re-raise하면 호출부의 `except Exception`(line 1058 등)이 잡아 retriable 판정 후 재시도 루프를 돈다. 호출부 `finally`의 `driver.quit()`이 죽은 브라우저를 정리하고 다음 attempt에서 새 브라우저로 시작한다.

3. **Chrome 크래시 방어 옵션 추가 — `_build_report_browser_options` (line 374 `--window-size` 다음 줄)**
   ```python
   options.add_argument("--disable-software-rasterizer")
   options.add_argument("--disable-renderer-backgrounding")
   options.add_argument("--disable-background-timer-throttling")
   options.add_argument("--disable-backgrounding-occluded-windows")
   options.add_argument("--disable-features=Translate,BackForwardCache")
   ```
   `--single-process` / `--no-zygote`는 오히려 크래시를 유발할 수 있어 **넣지 않는다**. 이 함수는 `_launch_report_browser`(line 386)에서만 쓰이며 다른 브라우저 경로(`_launch_browser`, line 283)는 건드리지 않는다.

4. **크래시로 인한 "로그인 실패" 단락 return 동작 확인 (line 1028-1030)**
   2번 적용 후 크래시는 예외로 올라오므로, `_do_login`이 `False`를 반환하는 경우는 순수 인증 실패(아이디/비번 오류 등)만 남는다. 따라서 `if not _do_login(...): result["error"]="로그인 실패"; return result`는 의미가 유지되어 별도 코드 변경이 불필요할 수 있다. 2번 적용 뒤 Test Cases로 동작을 확인한다. 동일 패턴이 `run_crawling_daily_date_page`(line 1112)에도 있으나 메커니즘은 동일하게 처리된다.

## Reference Code

### modules/extract/crawling_toorder_sales_report.py — `_is_retriable_driver_error` (line 179)
```python
def _is_retriable_driver_error(message: str) -> bool:
    msg = message.lower()
    return any(
        keyword in msg
        for keyword in (
            "urlopen error",
            "connection refused",
            "connection aborted",
            "max retries exceeded",
            "new connection",
            "name or service not known",
            "remote disconnected",
            "protocolerror",
            "connection reset by peer",
            "connecttimeouterror",
        )
    )
```

### modules/extract/crawling_toorder_sales_report.py — `_build_report_browser_options` (line 358)
```python
    if HEADLESS_MODE:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # ← 여기 아래에 step 3 옵션 5줄 추가

    prefs = {
        "download.default_directory": str(download_dir.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    return options
```

### modules/extract/crawling_toorder_sales_report.py — `_do_login` get 블록 (line 456)
```python
    try:
        driver.get(LOGIN_URL)
        if not _wait_for_react_load(driver, timeout=15):
            logger.error("[%s] React 앱 로드 타임아웃", account_id)
            return False
        time.sleep(1.0)
    except Exception as exc:
        logger.error("[%s] 페이지 이동 실패: %s", account_id, exc)
        return False   # ← step 2: retriable이면 raise 로 교체
```

### modules/extract/crawling_toorder_sales_report.py — single_date 재시도 루프 (line 1023)
```python
    for attempt in range(1, PIPELINE_RETRIES + 1):
        driver = None
        try:
            driver = _launch_report_browser(toorder_id, download_dir)
            if not _do_login(driver, toorder_id, toorder_pw):
                result["error"] = "로그인 실패"
                return result
            ...
        except Exception as exc:
            msg = str(exc)
            result["error"] = msg
            if attempt < PIPELINE_RETRIES and _is_retriable_driver_error(msg):
                time.sleep(PIPELINE_RETRY_BASE_SEC * attempt)
                continue
            return result
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
```

## Test Cases
1. [구문] `python -c "import ast; ast.parse(open('modules/extract/crawling_toorder_sales_report.py',encoding='utf-8').read())"` → 기대: 에러 없음
2. [retriable 판정] `python -c "import sys; sys.path.insert(0,'.'); from modules.extract.crawling_toorder_sales_report import _is_retriable_driver_error as f; assert f('Message: tab crashed'); assert f('session deleted because of page crash'); assert not f('element not found'); print('OK')"` → 기대: `OK`
3. [DAG import] `python -c "from dags.db.DB_ToOrder_Daily_Store_Dags import dag; print(dag.dag_id)"` → 기대: `DB_ToOrder_Daily_Store_Dags`, ImportError 없음
4. [실DAG, 도커 가용 시] `docker compose exec airflow-scheduler airflow dags trigger DB_ToOrder_Daily_Store_Dags -c '{"sale_date":"2026-06-28"}'` → 기대: "tab crashed" 발생 시 `run_crawling_single_date ... 재시도 1/2` 로그가 찍히고 새 브라우저로 재시도, 최종 `ANALYTICS_DB/toorder_daily_store/toorder_daily_store_20260628.parquet` 생성

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행 (4번은 도커 가용 시)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~3 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_is_retriable_driver_error` 기존 네트워크 키워드는 **삭제하지 않는다**.
- 새 Chrome 옵션은 `_build_report_browser_options`(report 전용)에만 추가한다. `_launch_browser`(line 283) / `_build_browser`는 손대지 않는다.
- `PIPELINE_RETRIES`(line 75, =2) 값 변경 금지.
- docker-compose.yaml / Dockerfile 수정 금지 (shm_size 등 인프라 변경 없음).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
