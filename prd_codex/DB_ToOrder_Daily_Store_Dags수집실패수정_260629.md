# DB_ToOrder_Daily_Store_Dags 수집 실패 수정

## Task
`DB_ToOrder_Daily_Store_Dags`의 `collect_toorder_daily_store` 태스크가 `날짜 설정 실패`로 죽는다. 로그인은 정상이고, 실제 원인은 **잘못된 페이지로 이동**해서 날짜 입력칸을 0개 찾는 것이다. 일별매출보고서의 올바른 URL은 `https://ceo.toorder.co.kr/dashboard/sales-report/datedetail`(=`SALES_REPORT_DATEDETAIL_URL`, 이미 코드에 정의됨)인데 `run_crawling_daily_date_page`가 `orderkinds`(채널별 보고서)로 가고 있다. 이동 URL/검증/파일명을 datedetail 기준으로 교정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Root Cause (확정)
호출 체인:
- DAG `dags/db/DB_ToOrder_Daily_Store_Dags.py`
  → `run_ai_daily_collection_to_daily_parquet_dir` (`modules/transform/pipelines/sales/DB_ai_daily_collection_01_collect.py:306`)
  → `run_crawling_single_date` (import: `modules/extract/crawling_toorder_sales_report_daily_date.py` — 9줄 재export 래퍼)
  → 실제 함수 = `crawling_toorder_sales_report.run_crawling_daily_date_page` (`modules/extract/crawling_toorder_sales_report.py:1148`)

버그: `run_crawling_daily_date_page`가 `driver.get(SALES_REPORT_DATE_URL)`로 이동하는데 `SALES_REPORT_DATE_URL`(line 50) = `.../sales-report/orderkinds`(채널별 보고서, 날짜칸 없음) → `date input fields not found: 0`.

증거: 같은 파일의 `run_crawling_datedetail_months`(line 925) / `_download_datedetail_month`(line 805)가 **동일한 `_set_date_range`를 datedetail 페이지에서 정상 동작**시킨다. 즉 `_set_date_range` 로직은 정상이고 daily 함수의 이동 URL만 틀림.

## Files to Create / Modify
- 수정: `modules/extract/crawling_toorder_sales_report.py` (**유일 수정 대상**)
- 무수정(참고): `modules/extract/crawling_toorder_sales_report_daily_date.py`, `modules/transform/pipelines/sales/DB_ai_daily_collection_01_collect.py`, `dags/db/DB_ToOrder_Daily_Store_Dags.py`

## Implementation Steps
`run_crawling_daily_date_page`(line 1148~1258)를 datedetail 경로로 교정한다.

1. **이동 URL 교정** (line 1178): `driver.get(SALES_REPORT_DATE_URL)` → `driver.get(SALES_REPORT_DATEDETAIL_URL)`.
2. **url 검증 교정** (line 1180): `if "sales-report/orderkinds" not in driver.current_url:` → `if "sales-report/datedetail" not in driver.current_url:`.
3. **다운로드 파일명 정합** (line 1204, 1213~1214): datedetail 페이지는 `종합보고서_일별상세_매출보고서.xlsx`(`ORIG_DATEDETAIL_FILENAME`, line 62)을 생성한다. 현재 `ORIG_DATE_FILENAME`/`ORIG_FILENAME` stem을 기다리므로 → `ORIG_DATEDETAIL_FILENAME` 기준으로 변경.
   - `expected_stem=(Path(ORIG_DATE_FILENAME).stem, Path(ORIG_FILENAME).stem)` → `ORIG_DATEDETAIL_FILENAME` stem 포함하도록 수정
   - rename 시 `Path(ORIG_DATE_FILENAME)` → `Path(ORIG_DATEDETAIL_FILENAME)`
4. **(권장) 다운로드 로직 재사용**: 단순 XPATH `//button[contains(text(),'보고서 생성')]`(line 1189) 대신, 이미 검증된 `_download_datedetail_month`(line 805~912)의 버튼 후보 탐색 + `Keys.ENTER` 제출 + 다운로드 폴링 패턴을 재사용한다. 가장 깔끔한 방법은 `run_crawling_daily_date_page` 내부 다운로드 블록을 `_download_datedetail_month(driver, toorder_id, target_date, target_date, download_dir)` 호출로 대체하는 것(단일 날짜 = month_start=month_end=target_date). 단, 결과 파일명 토큰이 `YYYY-MM`(월)로 rename되므로 daily 흐름에서 필요한 날짜 토큰 처리만 맞춘다.

## Reference Code

### modules/extract/crawling_toorder_sales_report.py — 상수 (line 49~62)
```python
SALES_REPORT_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"
SALES_REPORT_DATE_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/orderkinds"   # ← 버그: orderkinds로 잘못 설정
SALES_REPORT_DATEDETAIL_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/datedetail"  # ← 올바른 일별매출 URL
ORIG_FILENAME = "종합보고서_채널별(일)매출보고서.xlsx"
ORIG_DATE_FILENAME = "종합보고서_일별매출보고서.xlsx"
ORIG_DATEDETAIL_FILENAME = "종합보고서_일별상세_매출보고서.xlsx"   # ← datedetail 페이지가 생성하는 파일
```

### run_crawling_daily_date_page (line 1148~1186) — 수정 대상
```python
def run_crawling_daily_date_page(toorder_id, toorder_pw, target_date, download_dir):
    # ...
    for attempt in range(1, PIPELINE_RETRIES + 1):
        driver = None
        try:
            driver = _launch_report_browser(toorder_id, download_dir)
            if not _do_login(driver, toorder_id, toorder_pw):
                result["error"] = "로그인 실패"; return result

            logger.info("[%s] 일별매출보고서 페이지 이동", toorder_id)
            driver.get(SALES_REPORT_DATE_URL)              # ← SALES_REPORT_DATEDETAIL_URL 로 변경
            time.sleep(4.0)
            if "sales-report/orderkinds" not in driver.current_url:   # ← "sales-report/datedetail" 로 변경
                result["error"] = f"페이지 이동 실패: {driver.current_url}"; return result

            if not _set_date_range(driver, toorder_id, target_date):
                result["error"] = "날짜 설정 실패"; return result
            # ... 보고서 생성 버튼 클릭 / 다운로드 / rename (line 1204, 1213~1214의 파일명 상수 교정)
```

### _download_datedetail_month (line 805~851) — 재사용할 검증된 datedetail 패턴
```python
def _download_datedetail_month(driver, account_id, month_start, month_end, download_dir):
    if not _set_date_range(driver, account_id, month_start, month_end):
        result["error"] = "date range set failed"; return result
    wait = WebDriverWait(driver, 15)
    report_btn = None
    candidates = driver.find_elements(By.XPATH, "//button[not(@disabled)] | //*[@role='button' and not(@disabled)]")
    for candidate in candidates:
        text = (candidate.text or candidate.get_attribute("innerText") or "").strip()
        if "보고서 생성" in text:
            report_btn = candidate; break
    report_btn = wait.until(EC.element_to_be_clickable(report_btn))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", report_btn)
    driver.execute_script("arguments[0].focus();", report_btn)
    report_btn.send_keys(Keys.ENTER)
    # ... ORIG_DATEDETAIL_FILENAME stem으로 다운로드 폴링 + rename
```

### run_crawling_datedetail_months (line 944~951) — datedetail 이동/검증 정답 패턴
```python
logger.info("[%s] opening datedetail report page", toorder_id)
driver.get(SALES_REPORT_DATEDETAIL_URL)
time.sleep(4.0)
if "sales-report/datedetail" not in driver.current_url:
    return _datedetail_failure_results(month_spans, f"datedetail page navigation failed: {driver.current_url}")
```

## Test Cases
1. [모듈 import] `python -c "from modules.extract.crawling_toorder_sales_report import run_crawling_daily_date_page"` → 기대: ImportError 없음
2. [재export 래퍼] `python -c "from modules.extract.crawling_toorder_sales_report_daily_date import run_crawling_single_date"` → 기대: ImportError 없음
3. [URL 교정 확인] `python -c "import inspect, modules.extract.crawling_toorder_sales_report as m; src=inspect.getsource(m.run_crawling_daily_date_page); assert 'SALES_REPORT_DATEDETAIL_URL' in src and 'datedetail' in src, src"` → 기대: AssertionError 없음
4. [DAG import] `python -c "from dags.db.DB_ToOrder_Daily_Store_Dags import dag"` → 기대: ImportError 없음 (단, airflow 환경 필요 시 컨테이너에서 실행)
5. [E2E - 컨테이너] Airflow UI에서 `DB_ToOrder_Daily_Store_Dags`를 `conf {"sale_date": "2026-06-28"}`로 수동 트리거 → 기대:
   - 로그에 `date input fields not found: 0` 에러 없음 + `datedetail download complete` 출력
   - `종합보고서_일별상세_매출보고서` xlsx 다운로드
   - `ANALYTICS_DB/toorder_daily_store/toorder_daily_store_20260628.parquet` 생성, 태스크 success

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~4 정적 실행 (5번은 컨테이너 E2E)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `crawling_toorder_sales_report.py` **단일 파일만** 수정. DAG/파이프라인/래퍼는 건드리지 말 것.
- `SALES_REPORT_DATE_URL`/`SALES_REPORT_URL` 상수 값은 다른 곳(채널별 흐름 `run_crawling_single_date` line 1056)에서 사용되므로 **상수 자체를 바꾸지 말고**, `run_crawling_daily_date_page` 내부 참조만 `SALES_REPORT_DATEDETAIL_URL`로 교체한다.
- 기존 datedetail 패턴(`_download_datedetail_month`, `run_crawling_datedetail_months`)을 최대한 재사용 — 새 selector 추측 금지.
- datedetail(일별상세) 보고서 컬럼 스키마가 파이프라인 `_ingest_xlsx_to_daily_parquet`(collect.py)의 파싱과 맞는지 E2E(Test 5)에서 확인. 불일치 시 헤더 파싱 보정 필요(이 경우 collect.py 수정은 별도 승인 후).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
