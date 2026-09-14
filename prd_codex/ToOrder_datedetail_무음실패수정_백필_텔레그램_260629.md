# ToOrder datedetail DAG — 무음 실패 수정 + 백필 분리 + 텔레그램 알림

## Task
`DB_Toorder_store_platform_daily` parquet이 2026-06-25에서 멈춤(오늘 6/29). 원인은 `run_toorder_store_platform_daily()`가 다운로드/파싱이 전부 실패해도 예외 없이 parquet 경로를 반환 → `collect_and_save` 태스크가 항상 성공 처리 → `on_failure_callback`(텔레그램)이 안 울림. 실패를 실제 태스크 실패로 surface 하고, 매일 기본 수집을 가벼운 트레일링 윈도우로 바꾸고 전체 재수집은 `backfill` conf로 분리한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/sales/`
- DAG 파일 위치: `dags/sales/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py` — 실패를 raise (핵심)
- 수정: `dags/sales/DB_Toorder_store_platform_daily_Dags.py` — `resolve_dates`에 트레일링 윈도우 기본 + `backfill` conf
- 변경 없음(확인만): `modules/transform/utility/notifier.py` — 텔레그램 경로는 이미 연결됨

## Implementation Steps

### 1. 파이프라인: 실패를 삼키지 말고 raise
파일: `modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py`
함수: `run_toorder_store_platform_daily`

- 실패 사유를 수집할 리스트 2개를 준비:
  - `failed_download_months`: `missing_spans`에 있었는데 `downloaded_by_month`에 들어오지 못한 월(=다운로드 실패).
  - `empty_months`: 워크북은 확보됐으나 해당 월 in-range 파싱 결과가 0행인 월(현재 "parsed empty"/"Skipping ... without workbook" 경고만 찍는 경로).
- 데이터 저장은 **기존 순서 그대로 먼저** 수행한다: `all_dfs`가 있으면 `_upsert_parquet(new_df, parquet_path)`로 부분 성공분을 살린다.
- `_upsert_parquet` **이후**(= 함수 return 직전), 아래 중 하나라도 참이면 `RuntimeError`를 raise:
  - `failed_download_months`가 비어있지 않음
  - `empty_months`가 비어있지 않음
  - `all_dfs`가 완전히 비어있음
- 에러 메시지에 실패 월·요청 구간을 명시하고, `notifier.classify_failure`가 분류할 수 있도록 한국어 키워드("데이터 없음" 등)를 포함. 예:
  ```python
  raise RuntimeError(
      f"ToOrder datedetail 수집 실패(데이터 없음): "
      f"download_failed={failed_download_months}, empty={empty_months}, "
      f"range={resolved_from}~{resolved_to}, parquet={parquet_path}"
  )
  ```
- 구체 위치:
  - 다운로드 루프(현재 `results` 순회, 90~100행 부근)에서 `result.get("success")`가 False인 월 토큰을 `failed_download_months`에 추가.
  - 월별 파싱 루프(102~121행)에서 `xlsx_path is None`(107행)인 월 → `failed_download_months`에 이미 없으면 추가. 워크북은 있으나 in-range `month_df`가 empty면 → `empty_months`에 추가.
  - `collect_and_save`(DAG)는 예외를 전파만 하므로 수정 불필요.

### 2. DAG: 트레일링 윈도우 기본 + backfill conf 분리
파일: `dags/sales/DB_Toorder_store_platform_daily_Dags.py`
함수: `resolve_dates`

conf 분기(우선순위 유지):
- `month` 또는 `sale_date_from`+`sale_date_to`: 기존 동작 유지.
- `{"backfill": true}`: `DEFAULT_DATE_FROM`(2025-07-01) ~ 어제(KST) 전체 재수집.
- conf 없음(기본 일일 실행): **전월 1일 ~ 어제(KST)** 트레일링 윈도우(이번달+전월 2개 월 span). late-edit 보정 위해 전월 포함. 끊긴 6월 구간은 이 기본 실행만으로 자동 복구.
- `month`와 `backfill` 동시 지정 시 `ValueError`(기존 month+range 충돌 검사와 동일 패턴).
- 구현 힌트(전월 1일 계산):
  ```python
  import pendulum
  today = pendulum.now("Asia/Seoul")
  date_to = today.subtract(days=1).format("YYYY-MM-DD")
  date_from = today.start_of("month").subtract(months=1).format("YYYY-MM-DD")
  ```
- docstring/주석을 새 기본 동작에 맞게 갱신.

### 3. 텔레그램 알림(인프라 확인, 코드 추가 없음)
- `modules/transform/utility/notifier.on_failure_callback`이 이미 `send_telegram` 호출. 태스크가 실제 실패하면 자동 발화.
- Airflow Variable `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`가 설정돼 있어야 함(`send_telegram`은 미설정 시 조용히 skip). 실행 단계에서 점검.
- (선택) 재시도 가시성 위해 DAG `default_args`에 `on_retry_callback`(notifier에 이미 존재) 추가 가능.

## Reference Code

### modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py (현재 핵심)
```python
def run_toorder_store_platform_daily(*, target_date=None, date_from=None, date_to=None,
        sale_dates=None, dest_dir=None, toorder_id=None, toorder_pw=None,
        manual_dir=None, log_prefix="", **_) -> str:
    resolved_from, resolved_to = _resolve_date_bounds(...)
    ...
    downloaded_by_month: dict[str, Path] = {}
    if missing_spans:
        results = run_crawling_datedetail_months(...)
        for result in results:
            month_token = str(result.get("month") or "")
            if result.get("success") and result.get("file"):
                downloaded_by_month[month_token] = Path(str(result["file"]))
            else:
                logger.warning("%sDatedetail download failed: %s (%s)", ...)  # ← 삼킴
    all_dfs = []
    for month_start, month_end in month_spans:
        xlsx_path = pending_by_month.get(mt) or downloaded_by_month.get(mt)
        if xlsx_path is None:
            logger.warning("%sSkipping ... without workbook: %s", ...)  # ← 삼킴
            continue
        month_df = _parse_datedetail_xlsx(xlsx_path)
        ...  # in-range 필터 후 비면 "parsed empty" 경고만  # ← 삼킴
    if all_dfs:
        _upsert_parquet(pd.concat(all_dfs, ignore_index=True), parquet_path)
    else:
        logger.warning("%sNo ToOrder datedetail rows collected ...")  # ← 삼킴
    return str(parquet_path)   # ← 실패해도 성공 반환
```

### dags/sales/DB_Toorder_store_platform_daily_Dags.py (현재 resolve_dates 기본 분기)
```python
def resolve_dates(**context) -> str:
    conf = context["dag_run"].conf or {}
    month = conf.get("month")
    sale_date_from = conf.get("sale_date_from")
    sale_date_to = conf.get("sale_date_to")
    if month and (sale_date_from or sale_date_to):
        raise ValueError("month and sale_date_from/sale_date_to cannot be used together.")
    if month:
        ... # 해당 월 1일~말일
    elif sale_date_from or sale_date_to:
        ... # 범위
    else:
        date_from = DEFAULT_DATE_FROM  # "2025-07-01"  ← 매일 전체 재수집(무거움)
        date_to = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
    context["ti"].xcom_push(key="date_from", value=date_from)
    context["ti"].xcom_push(key="date_to", value=date_to)
    return f"date_from={date_from}, date_to={date_to}, parquet={PARQUET_PATH}"
```

### modules/transform/utility/notifier.py (이미 연결됨, 변경 없음)
```python
def send_telegram(text: str) -> None:
    token, chat_id = _get_telegram_creds()   # Airflow Variable TELEGRAM_BOT_TOKEN/CHAT_ID
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return
    ...  # POST sendMessage

def on_failure_callback(context) -> None:
    ...
    _send_email_alert(subject, body)
    send_telegram(f"{body}\n{_TELEGRAM_TRIGGER}")   # 태스크 실제 실패 시 발화
    enqueue_heal_task(context)
```

## Test Cases
1. [모듈 import] `python -c "import modules.transform.pipelines.sales.DB_Toorder_store_platform_daily"` → 기대: 에러 없음
2. [DAG 파싱] `python dags/sales/DB_Toorder_store_platform_daily_Dags.py` → 기대: SyntaxError/ImportError 없음
3. [기본 트레일링 윈도우] `resolve_dates`를 conf 없이 호출 → 기대: date_from=2026-05-01, date_to=2026-06-28
4. [backfill conf] conf `{"backfill": true}` → 기대: date_from=2025-07-01, date_to=어제
5. [충돌 검사] conf `{"month":"2026-06","backfill":true}` → 기대: ValueError
6. [백필 실행] `docker compose exec airflow-airflow-scheduler-1 airflow dags trigger DB_Toorder_store_platform_daily -c '{"month":"2026-06"}'` → 기대: 태스크 성공, 로그에 6월 다운로드 성공
7. [데이터 복구] `python -c "import pandas as pd; from modules.transform.utility.paths import ANALYTICS_DB; print(pd.read_parquet(ANALYTICS_DB/'toorder_daily_store_platform'/'toorder_store_platform_daily.parquet')['date'].max())"` → 기대: 2026-06-28
8. [실패 surface] 다운로드 실패 시 collect_and_save 태스크가 failed 상태가 되고 텔레그램 메시지 수신(TELEGRAM Variable 설정 전제)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 (정적/단위) 먼저 실행
  2. FAIL 항목 → 원인 분석 → 코드 수정 → 재실행
  3. 전부 PASS 후 Test Cases 6~7 (백필 실제 실행) 수행
종료 조건: Test Cases 1~7 PASS + Constraints 위반 없음
```

## Constraints
- `_upsert_parquet`로 **부분 성공분을 먼저 저장한 뒤** raise (성공한 월 데이터는 보존).
- 기존 conf 인터페이스(`month`, `sale_date_from/to`) 동작·우선순위 변경 금지.
- `_resolve_date_bounds`(파이프라인) 시그니처는 그대로 재사용 — DAG에서 해석한 date_from/date_to를 XCom으로 넘기는 기존 흐름 유지.
- print 금지, logging 사용. 경로/스케줄 하드코딩 금지.
- 컨테이너명은 `airflow-airflow-scheduler-1`.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(기존 구조 보존)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게(`str | None` 스타일 유지)
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 트레일링 윈도우 길이가 모호하면: 전월 1일~어제(2개월)로 결정
