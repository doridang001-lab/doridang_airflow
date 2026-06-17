# ToOrder datedetail 다운로드 재설계 (로그인 1회 + 월별 순차 다운로드)

## Task
`DB_Toorder_store_platform_daily` DAG이 다운로드 단계에서 매월 실패한다. 원인은 `_download_datedetail_month()`가 **정의되지 않은 상수 `ORIG_DATEDETAIL_FILENAME`** 를 참조해 rename 단계에서 `NameError`가 터지기 때문(다운로드 자체는 성공). 또한 파이프라인이 월마다 브라우저를 새로 띄우고 재로그인한다. 이 작업은 (1) 누락 상수 추가로 다운로드를 정상화하고 (2) 로그인 1회 + 한 세션에서 월별 순차 다운로드로 재설계한다. 우선 **2025-07 한 달** 다운로드 검증이 목표(파싱·parquet 적재는 기존 로직 유지).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/extract/crawling_toorder_sales_report.py`
  - 누락 상수 `ORIG_DATEDETAIL_FILENAME` 추가
  - 신규 함수 `run_crawling_datedetail_months()` (로그인 1회 + 월별 루프)
  - 기존 `run_crawling_datedetail_month()` → 신규 함수의 단일-span 래퍼로 변경
  - mojibake 한글 로그 복구
- 수정: `modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py`
  - 월 루프를 "필요한 월 선별 → 1회 다운로드 호출 → 파싱" 구조로 리팩터
- 비변경: `dags/sales/DB_Toorder_store_platform_daily_Dags.py` (conf 인터페이스 이미 존재)

## Implementation Steps

### 1. 누락 상수 정의 (`crawling_toorder_sales_report.py` 상수 섹션, 59~60행 근처)
`ORIG_FILENAME`, `ORIG_DATE_FILENAME` 옆에 추가:
```python
ORIG_DATEDETAIL_FILENAME = "종합보고서_일별상세_매출보고서.xlsx"
```
- 실제 다운로드 파일명 = `종합보고서_일별상세_매출보고서.xlsx` (호스트 `E:\d_down\종합보고서_일별상세_매출보고서.xlsx`).
- rename 결과 = `종합보고서_일별상세_매출보고서_2025-07.xlsx` (`{stem}_{month_token}{suffix}`, month_token=`2025-07`).
- **하이픈 형식(`2025-07`) 유지** — 파이프라인 `_DATEDETAIL_PREFIX` 및 `_find_pending_datedetail_file`의 `month_token in xlsx.stem` 매칭과 일치해야 함. 언더스코어(`2025_07`) 사용 금지.

### 2. 로그인 1회 다운로드 함수 신설 (`crawling_toorder_sales_report.py`)
신규 공개 함수 `run_crawling_datedetail_months(toorder_id, toorder_pw, month_spans, download_dir)`:
- `month_spans`: `list[tuple[str, str]]` = `[("2025-07-01","2025-07-31"), ...]`
- 동작: `_launch_report_browser()` 1회 → `_do_login()` 1회 → `driver.get(SALES_REPORT_DATEDETAIL_URL)` 진입 1회 → URL 검증 → `month_spans` 루프 돌며 **세션 유지한 채** 기존 `_download_datedetail_month(driver, account_id, start, end, download_dir)` 재사용.
- 반환: `list[dict]` — 각 원소 `{"success": bool, "file": str|None, "month": str, "error": str|None}`.
- `run_crawling_datedetail_month()`의 retriable 에러 재시도(`PIPELINE_RETRIES`, `_is_retriable_driver_error`) 패턴을 함수 전체 단위로 감싸 유지. 로그인 실패/페이지 진입 실패 시 모든 span에 실패 결과 채워 반환.
- `finally`에서 `driver.quit()`.
- 기존 `run_crawling_datedetail_month(...)`는 `run_crawling_datedetail_months(..., [(month_start, month_end)], ...)[0]` 호출하는 **얇은 래퍼**로 변경(하위호환).

### 3. 파이프라인 리팩터 (`DB_Toorder_store_platform_daily.py`, `run_toorder_store_platform_daily` 66~101행)
월 루프 구조 변경:
1. `_iter_month_spans(resolved_from, resolved_to)`로 전체 span 생성.
2. 각 span에 대해 `_find_pending_datedetail_file(resolved_manual, month_token)`로 이미 받은 월 제외 → **다운로드 필요한 month_spans만 선별**.
3. 선별 목록이 있으면 `run_crawling_datedetail_months(...)`를 **1회 호출** → `month_token -> file_path` 매핑 확보.
4. 전체 span을 다시 돌며: pending 파일 또는 신규 다운로드 파일 경로를 결정 → `_parse_datedetail_xlsx()` → `month_start ~ month_end` 필터 → 누적.
5. 누적분 있으면 `_upsert_parquet()`. 다운로드 실패 월은 기존처럼 `logger.warning` 후 skip.
- import 변경: `from modules.extract.crawling_toorder_sales_report import run_crawling_datedetail_months` (단일 함수 import도 호환되면 유지 가능).

### 4. mojibake 한글 로그 복구 (`crawling_toorder_sales_report.py`)
`_set_date_range`, `_download_datedetail_month`, `run_crawling_datedetail_month` 내 `"?? ??"`, `"???? ??"`, `"??? ?? ?? ??"` 등 깨진 로그 문자열을 읽을 수 있는 한글로 교체(동작 무관, 가독성). 예: `"[%s] 날짜 설정: %s ~ %s"`, `"[%s] datedetail 다운로드 완료: %s"`.

## Reference Code

### modules/extract/crawling_toorder_sales_report.py (현행 핵심 시그니처)
```python
ORIG_FILENAME = "종합보고서_채널별(일)매출보고서.xlsx"
ORIG_DATE_FILENAME = "종합보고서_일별매출보고서.xlsx"
# ORIG_DATEDETAIL_FILENAME 누락 → 추가 대상

SALES_REPORT_DATEDETAIL_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/datedetail"
PIPELINE_RETRIES = 2
PIPELINE_RETRY_BASE_SEC = 6.0

def _launch_report_browser(account_id: str, download_dir: Path) -> uc.Chrome: ...
def _do_login(driver, account_id: str, password: str) -> bool: ...
def _set_date_range(driver, account_id, start_date, end_date=None) -> bool: ...

def _download_datedetail_month(driver, account_id, month_start, month_end, download_dir) -> Dict[str, Any]:
    # ... 다운로드 대기 루프 통과 후:
    month_token = month_start[:7]
    stem = Path(ORIG_DATEDETAIL_FILENAME).stem   # <-- NameError 발생 지점
    suffix = Path(ORIG_DATEDETAIL_FILENAME).suffix
    new_path = download_dir / f"{stem}_{month_token}{suffix}"
    # ...
    return {"success": bool, "file": str|None, "month": month_token, "error": str|None}

def run_crawling_datedetail_month(toorder_id, toorder_pw, month_start, month_end, download_dir) -> Dict[str, Any]:
    # 현재: 매 호출마다 _launch_report_browser + _do_login (= 재로그인 문제)
    # 변경: run_crawling_datedetail_months(...)[0] 래퍼로
```

### modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py (현행 월 루프)
```python
from modules.extract.crawling_toorder_sales_report import run_crawling_datedetail_month
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR

_DATEDETAIL_PREFIX = "종합보고서_일별상세_매출보고서"

def run_toorder_store_platform_daily(*, target_date=None, date_from=None, date_to=None, ...) -> str:
    resolved_from, resolved_to = _resolve_date_bounds(...)
    resolved_manual = Path(manual_dir) if manual_dir else DOWN_DIR
    all_dfs = []
    for month_start, month_end in _iter_month_spans(resolved_from, resolved_to):  # <-- 매월 재로그인 발생
        month_token = month_start[:7]
        xlsx_path = _find_pending_datedetail_file(resolved_manual, month_token)
        if xlsx_path is None:
            result = run_crawling_datedetail_month(month_start=month_start, month_end=month_end, ...)
            if not result.get("success"): continue
            xlsx_path = Path(result["file"])
        month_df = _parse_datedetail_xlsx(xlsx_path)
        # ... 필터 → all_dfs.append → _rename_to_raw
    if all_dfs: _upsert_parquet(pd.concat(all_dfs), parquet_path)

def _iter_month_spans(date_from, date_to) -> list[tuple[str, str]]: ...
def _find_pending_datedetail_file(manual_dir, month_token) -> Path | None: ...
def _parse_datedetail_xlsx(xlsx_path) -> pd.DataFrame: ...
def _upsert_parquet(new_df, parquet_path) -> None: ...
```

## Test Cases
1. [상수 존재] `python -c "from modules.extract.crawling_toorder_sales_report import ORIG_DATEDETAIL_FILENAME; print(ORIG_DATEDETAIL_FILENAME)"` → 기대: `종합보고서_일별상세_매출보고서.xlsx`, NameError 없음
2. [신규 함수 import] `python -c "from modules.extract.crawling_toorder_sales_report import run_crawling_datedetail_months, run_crawling_datedetail_month; print('ok')"` → 기대: `ok`
3. [파이프라인 import] `python -c "from modules.transform.pipelines.sales.DB_Toorder_store_platform_daily import run_toorder_store_platform_daily; print('ok')"` → 기대: `ok`
4. [DAG import] `python -c "import importlib.util,sys; spec=importlib.util.spec_from_file_location('d','dags/sales/DB_Toorder_store_platform_daily_Dags.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); print(m.dag.dag_id)"` → 기대: `DB_Toorder_store_platform_daily_Dags`, ImportError 없음
5. [7월 다운로드 E2E — Docker/WSL] `docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_Toorder_store_platform_daily_Dags --conf '{"month": "2025-07"}'` 후 `collect_and_save` 태스크 로그 확인 → 기대:
   - `datedetail report button submitted: 2025-07-01 ~ 2025-07-31`
   - **NameError 없음**, `datedetail 다운로드 완료: 종합보고서_일별상세_매출보고서_2025-07.xlsx`
   - 로그인 로그가 (여러 달 트리거 시) 월 수만큼이 아닌 1회만 출력
6. [파일 생성] 다운로드 폴더(`DOWN_DIR` = 컨테이너 `/opt/airflow/download`, 호스트 `E:\d_down`)에 `종합보고서_일별상세_매출보고서_2025-07.xlsx`(또는 파싱 후 `_2025-07_raw.xlsx`) 존재 → 기대: 파일 존재

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~4 (정적 import) 먼저 실행
  2. PASS 후 Test Case 5~6 (E2E, Docker/WSL) 실행
  3. FAIL 항목 → 원인 분석
  4. 코드 수정
  5. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- rename 파일명 month_token은 **하이픈(`2025-07`)** — 언더스코어 금지(파서 매칭 깨짐).
- `run_crawling_single_date`, `run_crawling_daily_date_page`, `run_crawling_date_range`(채널별·일별 페이지)는 **건드리지 말 것**.
- parquet 스키마/upsert 로직(`_parse_datedetail_xlsx`, `_upsert_parquet`, `_OUTPUT_COLUMNS`) 변경 금지.
- 한 브라우저 세션 안에서 로그인은 1회만. 월별 재로그인 발생 금지.
- 다운로드 실패 월은 예외 전파 대신 `logger.warning` 후 skip(기존 동작 유지).
- `run_crawling_datedetail_month` 시그니처/반환 dict 키(`success/file/month/error`)는 하위호환 유지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
