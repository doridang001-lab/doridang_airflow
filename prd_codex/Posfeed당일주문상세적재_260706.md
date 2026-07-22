# Posfeed 당일 주문상세 적재 (Today Detail)

## Task
`dags/db/DB_Posfeed_Sales_Today_Dags.py`는 현재 당일 **주문(orders)만** 수집하고 주문 **상세(order item detail)** 는 적재하지 않는다. 상세는 매일 03:15 본 DAG에서만 채워져 당일 낮 시간대엔 비어 있다. 당일 주문 수집이 끝난 직후 **당일 수집분에 한해** 주문 상세를 크롤링·적재하도록 today DAG에 상세 태스크를 이어붙인다(당일기준).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_Posfeed_Sales_Detail.py` — 함수 `scrape_today_details` 1개 추가 (기존 함수는 절대 수정 금지)
- **수정** `dags/db/DB_Posfeed_Sales_Today_Dags.py` — 상세 태스크 2개(`extract_order_codes`, `scrape_order_details`) append

## Implementation Steps

### 1. `DB_Posfeed_Sales_Detail.py` — `scrape_today_details` 함수 추가
파일 맨 끝(`scrape_missing_order_details` 아래)에 추가. `AirflowSkipException`은 이미 import됨(line 24). `scrape_order_details`는 `task_ids="check_undetailed_orders"`로 XCom을 읽으므로, `_TiShim`으로 payload를 직접 주입해 우회한다(기존 `scrape_missing_order_details`와 동일 기법).

```python
def scrape_today_details(**context) -> str:
    """당일 extract_order_codes 결과만 상세 크롤링 (당일기준 경로).
    check_undetailed_orders(전체 스캔)를 거치지 않고 오늘 신규 코드만 적재한다."""
    payload = context["ti"].xcom_pull(task_ids="extract_order_codes", key="order_codes")
    if not payload or not payload.get("stores"):
        raise AirflowSkipException("당일 신규 주문 코드 없음")

    class _TiShim:
        def __init__(self, p):
            self._p = p

        def xcom_pull(self, *a, **k):
            return self._p

    return scrape_order_details(ti=_TiShim(payload))
```

### 2. `DB_Posfeed_Sales_Today_Dags.py` — import 추가
기존 import 블록에 추가:
```python
from modules.transform.pipelines.db.DB_Posfeed_Sales_Detail import (
    extract_order_codes,
    scrape_today_details,
)
```

### 3. `DB_Posfeed_Sales_Today_Dags.py` — 태스크 append & 체인 수정
`with DAG(...)` 블록 내부 기존 `t1 >> t2 >> t3` 를 아래로 교체:
```python
    t4 = PythonOperator(
        task_id="extract_order_codes",
        python_callable=extract_order_codes,
        op_kwargs={"collect_mode": "yesterday"},  # 최신 등록날짜 자동감지 = 당일
    )
    t5 = PythonOperator(
        task_id="scrape_order_details",
        python_callable=scrape_today_details,
        pool="selenium_pool",
        execution_timeout=timedelta(hours=2),
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
    )

    t1 >> t2 >> t3 >> t4 >> t5
```

### 설계 근거 (수정 판단 기준)
- `collect_mode="yesterday"` = **최신 등록날짜 자동감지** 모드. today DAG는 방금 오늘 주문을 `posfeed_sales` 파티션에 적재했으므로 최신 등록날짜 = 오늘 → 오늘 주문 코드만 추출된다.
- 오늘 주문이 0건이면 어제 데이터를 잡지만, 어제 상세는 이미 적재돼 `collected_set` 필터로 전량 제외 → `AirflowSkipException`으로 안전 종료.
- `check_undetailed_orders`(전체 파티션 스캔 → 과거 미수집분 전부 재수집)는 당일기준에 위배되므로 today 경로에서는 **사용하지 않는다**.
- `scrape_order_details`는 내부에 (본 수집 → 실패 재시도 → 빈 상세 재수집) 3중 로직을 포함하므로 today 경로는 단일 스크레이프 태스크로 충분(별도 `scrape_missing` 불필요).
- `extract_order_codes`는 없는 상위 태스크(`check_monthly_collection`, `ingest_manual_files`)의 XCom을 pull하지만 `if failed_yms:` / `if backfill_items:` 가드로 None 반환 시 무해.

## Reference Code

### dags/db/DB_Posfeed_Sales_Today_Dags.py (현재 상태 — 수정 대상)
```python
from modules.transform.pipelines.db.DB_Posfeed_Sales import collect_posfeed_today, partition_to_onedrive
from modules.transform.utility.schedule import DB_POSFEED_SALES_TODAY_TIME
# ... _on_failure_callback, resolve_today, default_args 생략 ...

with DAG(
    dag_id=dag_id,
    schedule=DB_POSFEED_SALES_TODAY_TIME,   # "15 13,15,17,19,21 * * *" (5회/일)
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "posfeed", "today", "selenium"],
) as dag:
    t1 = PythonOperator(task_id="resolve_today", python_callable=resolve_today)
    t2 = PythonOperator(
        task_id="move_to_storage",
        python_callable=collect_posfeed_today,
        pool="selenium_pool",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    t3 = PythonOperator(task_id="partition_to_onedrive", python_callable=partition_to_onedrive)

    t1 >> t2 >> t3
```

### modules/transform/pipelines/db/DB_Posfeed_Sales_Detail.py (재사용 함수 시그니처)
```python
from airflow.exceptions import AirflowSkipException  # line 24 (이미 존재)

def extract_order_codes(collect_mode: str = "yesterday", force_rescrape: bool = False, **context) -> str:
    """collect_mode에 따라 수집 대상 주문 코드를 매장별 그룹화하여 XCom(key="order_codes")에 저장.
    collect_mode="yesterday"이면 posfeed_sales 전체에서 최신 등록날짜를 자동 감지."""
    # ... 신규 코드 없으면 raise AirflowSkipException("수집할 신규 주문 코드 없음") ...
    context["ti"].xcom_push(key="order_codes", value={"stores": stores})

def scrape_order_details(**context) -> str:
    """XCom order_codes(stores)를 매장별 순차 크롤링 → posfeed_sales_detail 파티션 CSV 적재.
    본 수집 → 실패 재시도 → 빈 상세 재수집 3단계 내장.
    payload = context["ti"].xcom_pull(task_ids="check_undetailed_orders", key="order_codes")"""

# _TiShim 패턴 참고: scrape_missing_order_details 내부에서 payload 주입에 사용됨
def scrape_missing_order_details(collect_mode: str = "yesterday", **context) -> str:
    payload = context["ti"].xcom_pull(task_ids="check_undetailed_orders", key="order_codes")
    # ... class _TiShim: xcom_pull → payload 반환 ...
    return scrape_order_details(ti=_TiShim({"stores": missing_stores}))
```

## Test Cases
1. [파이프라인 파싱] `python -c "import ast,io; ast.parse(io.open(r'modules/transform/pipelines/db/DB_Posfeed_Sales_Detail.py',encoding='utf-8').read())"` → 기대: 오류 없음(exit 0)
2. [DAG 파싱] `python -c "import ast,io; ast.parse(io.open(r'dags/db/DB_Posfeed_Sales_Today_Dags.py',encoding='utf-8').read())"` → 기대: 오류 없음
3. [함수 존재] `python -c "from modules.transform.pipelines.db.DB_Posfeed_Sales_Detail import scrape_today_details, extract_order_codes; print('ok')"` → 기대: `ok`
4. [DAG import] `python -c "import dags.db.DB_Posfeed_Sales_Today_Dags as m; print(m.dag.task_ids)"` → 기대: `['resolve_today','move_to_storage','partition_to_onedrive','extract_order_codes','scrape_order_details']`
5. [의존성] 위 4의 dag에서 `t3(partition_to_onedrive) >> t4(extract_order_codes) >> t5(scrape_order_details)` 연결 확인 → 기대: `dag.get_task('scrape_order_details').upstream_task_ids == {'extract_order_codes'}`
6. [DAG import 에러 스캔] `airflow dags list-import-errors` (환경에서 가능 시) → 기대: `DB_Posfeed_Sales_Today_Dags` 관련 신규 오류 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `DB_Posfeed_Sales_Detail.py`의 **기존 함수는 절대 수정하지 않는다** — `scrape_today_details` 함수만 신규 추가.
- today 경로에 `check_undetailed_orders` / `scrape_missing_order_details` 를 **넣지 않는다** (전체 스캔 → 당일기준 위배).
- `collect_mode`는 반드시 `"yesterday"` 문자열 사용 (특정 날짜 하드코딩 금지 — op_kwargs는 parse-time 정적이므로 실제 날짜를 박으면 안 됨).
- 스케줄/경로 상수는 기존 import 유지 (`DB_POSFEED_SALES_TODAY_TIME`), 하드코딩 금지.
- `timedelta`는 이미 `from datetime import timedelta`로 import됨 — 중복 import 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 해당 위치에 append/수정
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`-> str`, `**context`)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
