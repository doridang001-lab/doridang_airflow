# toorder_daily_sales 적재 중단 복구 (Chrome tab crashed)

## Task
`Sales_ToOrderSalesReport_Crawl_Dags`(매일 06:05 KST)가 6/25 이후 `ANALYTICS_DB/toorder_daily_sales/` 적재를 멈췄다. 원인은 Chrome 148 headless 렌더러 크래시("tab crashed")가 로그인 단계에서 발생해 `run_crawling_date_range`가 전체 날짜를 스킵한 것. 누락 sale_date = 2026-06-26·27·28. tab crashed 대응 수정은 이미 `crawling_toorder_sales_report.py`에 반영돼 있으나, (1) 사용자 경로 `run_crawling_date_range`에는 로그인 재시도(새 브라우저) 루프가 없고, (2) worker 컨테이너가 옛 코드를 import한 상태라 재시작이 필요하다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/extract/crawling_toorder_sales_report.py` (**유일 코드 수정 대상**)
- 무수정(참고): `dags/sales/Sales_ToOrderSalesReport_Crawl_Dags.py`, `docker-compose.yaml`(CeleryExecutor: airflow-worker/airflow-scheduler)

## Root Cause (확정 — 로그 증거)
3개 실패 run 모두 동일:
```
crawling_toorder_sales_report.py:403 WARNING Chrome download path CDP setup failed: Message: tab crashed
crawling_toorder_sales_report.py:463 ERROR  페이지 이동 실패: Message: tab crashed
crawling_toorder_sales_report.py:1235 ERROR 로그인 실패 - 전체 날짜 스킵
크롤링 완료 0/N건 성공
```
- 로그 줄번호(`:403/:454/:463/:1235`)가 현재 디스크 코드(`:453` 등)와 ~50줄 어긋남 → **worker가 수정 전 모듈을 들고 있음**. 재시작 없이는 수정이 적용되지 않음.
- 디스크에는 이미 반영됨: `_is_retriable_driver_error`에 `tab crashed` 키워드(line 194~), `_build_report_browser_options` 안정화 플래그(line 392~396), `_do_login` retriable re-raise(line 523~524), `_launch_report_browser` CDP 크래시 시 브라우저 재기동 retry(line 454~476).
- **잔존 갭**: `run_crawling_date_range`(line 1330)는 `run_crawling_single_date`와 달리 login/navigate를 1회만 시도하고 실패 시 전체 날짜 스킵(line 1364~1383). 새 브라우저 재시도 루프가 없다.

## Implementation Steps

1. **`run_crawling_date_range`에 로그인/이동 retry 루프 추가** (line 1330~)
   `run_crawling_single_date`(line 1149~1210)의 `for attempt in range(1, PIPELINE_RETRIES + 1)` 패턴을 mirror한다.
   - `_launch_report_browser → _do_login → _navigate_to_sales_report` 단계를 attempt 루프로 감싼다.
   - `_do_login` 실패 또는 retriable 예외(`_is_retriable_driver_error`) 발생 시: `finally`에서 `driver.quit()` 후 `time.sleep(PIPELINE_RETRY_BASE_SEC * attempt)` 하고 다음 attempt(새 브라우저)로 `continue`.
   - 비retriable 로그인 실패(아이디/비번 등)는 기존대로 전체 날짜 `"로그인 실패"`로 즉시 종료.
   - 로그인+이동 성공 시 기존 날짜 다운로드 루프(line 1385~1400) 그대로 진입.
   - 신규 상수 만들지 말고 기존 `PIPELINE_RETRIES`, `PIPELINE_RETRY_BASE_SEC` 재사용. 신규 selector 추측 금지.

2. **구문/판정 정적 검증** (Test Cases 1~3).

3. **수정 코드 배포 — worker 재시작 (필수)**
   코드는 volume mount이므로 rebuild 불필요. worker가 옛 모듈을 들고 있으므로 재시작으로 새 모듈 import.
   ```
   docker compose restart airflow-worker airflow-scheduler
   ```

4. **누락분 backfill 트리거** (2026-06-26 ~ 28)
   DAG는 conf 없으면 lookback 7일 누락분 자동 수집이나, 명시적 범위가 안전:
   ```
   docker compose exec airflow-scheduler airflow dags trigger Sales_ToOrderSalesReport_Crawl_Dags -c '{"start_date":"2026-06-26","end_date":"2026-06-28"}'
   ```

## Reference Code

### modules/extract/crawling_toorder_sales_report.py — run_crawling_single_date (line 1149~1210, mirror 대상 패턴)
```python
    for attempt in range(1, PIPELINE_RETRIES + 1):
        driver = None
        try:
            driver = _launch_report_browser(toorder_id, download_dir)
            if not _do_login(driver, toorder_id, toorder_pw):
                result["error"] = "로그인 실패"
                return result
            if not _navigate_to_sales_report(driver, toorder_id):
                result["error"] = "보고서 페이지 이동 실패"
                return result
            result = _download_report_for_date(driver, toorder_id, target_date, download_dir)
            if result.get("success"):
                return result
            error_msg = str(result.get("error") or "").lower()
            if attempt < PIPELINE_RETRIES and _is_retriable_driver_error(error_msg):
                logger.warning("[%s] run_crawling_single_date 오류 재시도 %d/%d: %s", toorder_id, attempt, PIPELINE_RETRIES, result["error"])
                time.sleep(PIPELINE_RETRY_BASE_SEC * attempt)
                continue
            return result
        except Exception as exc:
            msg = str(exc)
            result["error"] = msg
            if attempt < PIPELINE_RETRIES and _is_retriable_driver_error(msg):
                logger.warning("[%s] run_crawling_single_date 예외 재시도 %d/%d: %s", toorder_id, attempt, PIPELINE_RETRIES, msg)
                time.sleep(PIPELINE_RETRY_BASE_SEC * attempt)
                continue
            logger.error("[%s] run_crawling_single_date 오류: %s", toorder_id, exc)
            return result
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("[%s] 브라우저 종료", toorder_id)
                except Exception:
                    pass
    return result
```

### modules/extract/crawling_toorder_sales_report.py — run_crawling_date_range (line 1330~1404, 현재 = 수정 대상)
```python
def run_crawling_date_range(toorder_id, toorder_pw, date_list, download_dir):
    download_dir.mkdir(parents=True, exist_ok=True)
    all_results = []
    driver = None
    logger.info("[%s] run_crawling_date_range 시작 - 총 %d일", toorder_id, len(date_list))
    try:
        driver = _launch_report_browser(toorder_id, download_dir)
        if not _do_login(driver, toorder_id, toorder_pw):
            logger.error("[%s] 로그인 실패 - 전체 날짜 스킵", toorder_id)
            for d in date_list:
                all_results.append({"success": False, "file": None, "date": d, "error": "로그인 실패"})
            return all_results
        if not _navigate_to_sales_report(driver, toorder_id):
            logger.error("[%s] 보고서 페이지 이동 실패 - 전체 날짜 스킵", toorder_id)
            for d in date_list:
                all_results.append({"success": False, "file": None, "date": d, "error": "보고서 페이지 이동 실패"})
            return all_results
        for idx, target_date in enumerate(date_list):
            logger.info("[%s] [%d/%d] %s 다운로드 중", toorder_id, idx + 1, len(date_list), target_date)
            result = _download_report_for_date(driver, toorder_id, target_date, download_dir)
            all_results.append(result)
            if idx < len(date_list) - 1:
                _random_delay(*TIMING["date_interval"])
    except Exception as exc:
        logger.error("[%s] run_crawling_date_range 오류: %s", toorder_id, exc)
    # ... (finally driver.quit())
```
구현 시: 위 `try` 블록의 `_launch_report_browser → _do_login → _navigate_to_sales_report` 부분만 `for attempt in range(1, PIPELINE_RETRIES + 1)` 루프 + retriable 시 `driver.quit()` 후 새 브라우저 재시도로 감싼다. 날짜 다운로드 루프는 로그인/이동 성공 후 1회만 실행되도록 유지.

## Test Cases
1. [구문] `python -c "import ast; ast.parse(open('modules/extract/crawling_toorder_sales_report.py',encoding='utf-8').read())"` → 기대: 에러 없음
2. [retriable 판정] `python -c "import sys; sys.path.insert(0,'.'); from modules.extract.crawling_toorder_sales_report import _is_retriable_driver_error as f; assert f('Message: tab crashed'); assert not f('element not found'); print('OK')"` → 기대: `OK`
3. [retry 적용 확인] `python -c "import inspect, modules.extract.crawling_toorder_sales_report as m; src=inspect.getsource(m.run_crawling_date_range); assert 'PIPELINE_RETRIES' in src and 'for attempt' in src, src; print('OK')"` → 기대: `OK`
4. [DAG import] `python -c "from dags.sales.Sales_ToOrderSalesReport_Crawl_Dags import dag; print(dag.dag_id)"` → 기대: `Sales_ToOrderSalesReport_Crawl_Dags`, ImportError 없음
5. [E2E - 컨테이너] 재시작 후 위 trigger 명령 실행 → 기대:
   - `crawl_reports` 로그에 `tab crashed` 발생 시 재시도 로그 후 새 브라우저로 다운로드 성공, `크롤링 완료 3/3건 성공`
   - `ANALYTICS_DB/toorder_daily_sales/toorder_daily_sales_20260626.csv`, `_20260627.csv`, `_20260628.csv` 생성
   - 각 CSV가 `수집채널,주문일자,매장명,플랫폼명,매출액` 스키마 + 매출액>0 행 보유

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~4 정적 실행 (5번은 컨테이너 E2E)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~4 전체 PASS + Constraints 위반 없음
```

## Constraints
- `crawling_toorder_sales_report.py` **단일 파일만** 수정. DAG/파이프라인/래퍼/compose는 코드 변경하지 말 것.
- `_is_retriable_driver_error` 기존 키워드는 삭제 금지(추가만). `tab crashed` 등은 이미 반영됨.
- `PIPELINE_RETRIES`(=2), `PIPELINE_RETRY_BASE_SEC` 값 변경 금지 — 재사용만.
- 날짜 다운로드 루프는 로그인/이동 성공 후 1회만 — retry 루프가 다운로드까지 반복하게 만들지 말 것(중복 다운로드 방지).
- worker 재시작은 코드 검증(Test 1~4) 통과 후 수행.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
