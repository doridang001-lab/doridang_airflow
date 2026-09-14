# ToOrder 일별매출 수집 "tab crashed" 복구

## Task
`DB_ToOrder_Daily_Store_Dags` 가 Chrome 148(headless, Docker) 렌더러 크래시("tab crashed")로 로그인 단계에서 실패한다. 브라우저는 정상 실행되지만 `driver.get(LOGIN_URL)` 직후 렌더러가 죽고, 현재 코드는 이 크래시를 재시도 대상으로 보지 않아 새 브라우저로 복구하지 못한다. 크래시 발생 시 자동 재시도(새 브라우저)로 흡수하고, 안정화 플래그로 크래시 빈도를 낮춘다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/extract/crawling_toorder_sales_report.py` (유일 대상)

## Implementation Steps

1. **크래시 시그니처를 재시도 대상에 추가 — `_is_retriable_driver_error` (line 179)**
   기존 키워드 튜플에 렌더러/세션 크래시 계열을 추가한다:
   ```python
   "tab crashed",
   "session deleted because of page crash",
   "page crash",
   "chrome not reachable",
   "disconnected: not connected to devtools",
   "no such window",
   ```
   (너무 광범위한 `"renderer"` 단독은 제외 — 오탐 위험)

2. **`_do_login` 이 크래시를 삼키지 않게 — line 462-464**
   `driver.get(LOGIN_URL)` 예외 처리에서, 메시지가 `_is_retriable_driver_error` 에 해당하면 `return False` 대신 **예외를 re-raise** 한다. 비크래시 실패는 기존대로 `False` 반환:
   ```python
   except Exception as exc:
       logger.error("[%s] 페이지 이동 실패: %s", account_id, exc)
       if _is_retriable_driver_error(str(exc)):
           raise
       return False
   ```
   효과: 상위 `run_crawling_single_date`(line 1058 `except Exception`) / `run_crawling_daily_date_page`(line 1171) / 880 루프(line 924)가 예외를 받아 `_is_retriable_driver_error` 판정 → `finally`에서 `driver.quit()` 후 **새 브라우저로 재시도**.

3. **Chrome 안정화 플래그 추가 — `_build_report_browser_options` (line 371-374, 활성 경로)**
   `--window-size` 다음에 렌더러 시작 크래시 빈도를 줄이는 보수적 플래그 추가:
   ```python
   options.add_argument("--disable-software-rasterizer")
   options.add_argument("--disable-renderer-backgrounding")
   options.add_argument("--disable-background-timer-throttling")
   options.add_argument("--disable-backgrounding-occluded-windows")
   options.add_argument("--disable-features=Translate,BackForwardCache")
   ```
   (`--single-process`/`--no-zygote` 는 제외 — 오히려 불안정.)
   레거시 `_build_browser`/`_launch_browser`(line 283, 단일날짜 경로 미사용)는 일관성 위해 동일 플래그를 같이 반영해도 무방.

4. **`run_crawling_single_date` 의 로그인 즉시 return 은 유지 (line 1028-1030)**
   2번으로 크래시는 예외 경로로 빠지므로, `_do_login`이 `False`를 반환하는 경우는 비크래시 실패만 남는다. 현재의 즉시 return 동작을 유지(무의미한 재시도 방지). 추가 수정 불필요.

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
        return False
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
1. [문법] `python -c "import ast; ast.parse(open('modules/extract/crawling_toorder_sales_report.py',encoding='utf-8').read())"` → 기대: 에러 없음
2. [크래시 재시도 판정] `python -c "import sys; sys.path.insert(0,'.'); from modules.extract.crawling_toorder_sales_report import _is_retriable_driver_error as f; assert f('Message: tab crashed'); assert f('session deleted because of page crash'); assert not f('element not found'); print('OK')"` → 기대: `OK`
3. [DAG import] `python -c "from dags.db.DB_ToOrder_Daily_Store_Dags import dag; print(dag.dag_id)"` → 기대: `DB_ToOrder_Daily_Store_Dags`, ImportError 없음
4. [실DAG, 컨테이너] `docker compose exec airflow-scheduler airflow dags trigger DB_ToOrder_Daily_Store_Dags -c '{"sale_date":"2026-06-28"}'` → 로그에 "tab crashed" 발생 시 `run_crawling_single_date ... 재시도 1/2` 후 새 브라우저로 다운로드 성공 → `ANALYTICS_DB/toorder_daily_store/toorder_daily_store_20260628.parquet` 생성

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행 (4는 컨테이너 환경에서만)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~3 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_is_retriable_driver_error` 의 기존 키워드는 절대 삭제하지 말고 **추가만** 한다.
- 비크래시 로그인 실패(필드 없음 등)는 기존대로 즉시 실패해야 한다 — 무의미한 재시도를 만들지 말 것.
- `PIPELINE_RETRIES`(line 75, =2) 값은 변경하지 않는다.
- docker-compose.yaml / Dockerfile 은 이번 작업에서 수정하지 않는다 (shm_size 2gb 이미 적용).
- 활성 경로는 `_launch_report_browser` → `_build_report_browser_options`. 레거시 `_launch_browser`(line 283)는 단일날짜 경로에서 미사용.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
