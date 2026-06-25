# Launch DAG 네이밍 정리 + dags/launch/ 이동

## Task

`dags/db/` 에 오타·공백·비일관 네이밍으로 흩어진 Launch 파일 5개를 정리한다.
`dags/launch/` 폴더를 신설하고, **송파삼전점 1개 매장**을 매일 **15:00** 에 재수집한 뒤
**15:15** 에 UnifiedSales 집계를 실행하는 점심 매출 누락 확인용 DAG 4개로 재작성한다.

---

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/launch/` (신규 폴더)
- 스케줄 상수: 수집 DAG는 `DB_LAUNCH_TIME`, UnifiedSales 집계 DAG는 `DB_LAUNCH_UNIFIED_TIME` 사용 (하드코딩 금지)
- `dag_id = Path(__file__).stem` 유지 (파일명 = dag_id)
- `catchup=False`, `max_active_runs=1` 기본

---

## Files to Create / Modify

### 수정
- `modules/transform/utility/schedule.py` — `DB_LAUNCH_TIME`, `DB_LAUNCH_UNIFIED_TIME` 상수 추가
- `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — 함수 3개에 `target_stores=None` 파라미터 추가
- `modules/transform/pipelines/db/DB_Posfeed_Sales.py` — `partition_to_onedrive` 에 `target_store=None` 파라미터 추가

### 생성 (dags/launch/)
- `dags/launch/__init__.py` (빈 파일)
- `dags/launch/DB_OKPOS_Sales_Launch_Dags.py`
- `dags/launch/DB_Posfeed_Sales_Launch_Dags.py`
- `dags/launch/DB_UnifiedSales_Launch_Dags.py`
- `dags/launch/DB_Baemin_Macro_Launch_Dags.py`

### 삭제 (dags/db/ 기존 파일)
- `dags/db/DB_OKPOS_Sales_Launch_Dags .py` (파일명에 공백)
- `dags/db/DB_Posfeed_Sales_lauch_Dags.py`
- `dags/db/DB_Posfeed_Sales_Detail_laucnch_Dags.py` (스텁, 주석 1줄뿐)
- `dags/db/DB_UnifiedSales_launch.py`
- `dags/db/DB_Beamin_Macro_Launch_Dags.py`

---

## Implementation Steps

### Step 1 — schedule.py에 Launch 스케줄 상수 추가

`modules/transform/utility/schedule.py` 의 DB 스케줄 블록 끝에 추가:

```python
DB_LAUNCH_TIME = "0 15 * * *"           # 매일 15:00 KST (송파삼전점 점심 매출 수집)
DB_LAUNCH_UNIFIED_TIME = "15 15 * * *"  # 매일 15:15 KST (송파삼전점 점심 매출 집계)
```

---

### Step 2 — OKPOS 파이프라인 함수에 target_stores 파라미터 추가

`modules/transform/pipelines/db/DB_OKPOS_Sales.py` — 함수 3개 수정:

```python
# 현재 (1582번째 줄 근처)
def download_today_stores(**context) -> str:
    ...
    pending_keys = _filter_missing_keys(sale_dates, "okpos_order")  # 내부에서 STORES 사용

# 변경 후
def download_today_stores(target_stores: list | None = None, **context) -> str:
    stores = _resolve_target_stores(target_stores)
    # 기존 로직에서 이 함수가 수집 대상을 순회하는 STORES 참조는 stores 로 교체
    ...
    pending_keys = _filter_missing_keys(sale_dates, "okpos_order", stores=stores)
```

동일 패턴으로:
- `download_receipt_stores(target_stores=None, **context)`
- `download_daily_stores(target_stores=None, **context)`

`target_stores` 는 DAG에서 `["송파삼전점"]` 같은 문자열 리스트로 전달한다. 파이프라인 내부에서는
문자열을 그대로 순회하지 말고, 기존 `STORES` 중 `target in store["name"]` 인 store dict만 골라야 한다.

```python
def _resolve_target_stores(target_stores: list | None = None) -> list[dict]:
    if not target_stores:
        return STORES
    targets = [str(target).strip() for target in target_stores if str(target).strip()]
    stores = [
        store
        for store in STORES
        if any(target in store["name"] for target in targets)
    ]
    if not stores:
        raise ValueError(f"OKPOS target_stores 매칭 없음: {target_stores!r}")
    return stores
```

`_filter_missing_keys` 는 `stores` 인자를 받도록 확장하고, 내부 순회 대상을 `stores or STORES` 로 둔다.

```python
def _filter_missing_keys(sale_dates: list, csv_name: str, stores: list[dict] | None = None) -> list:
    stores_to_check = stores or STORES
    ...
    for store in stores_to_check:
        ...
```

각 `download_*_stores` 함수의 `force` 재다운로드 경로도 `STORES` 가 아니라 필터된 `stores` 를 순회해야 한다.

---

### Step 3 — Posfeed 파이프라인에 target_store 파라미터 추가

`modules/transform/pipelines/db/DB_Posfeed_Sales.py` 의 `partition_to_onedrive` 함수:

```python
# 현재 (726번째 줄)
def partition_to_onedrive(**context) -> str:

# 변경 후
def partition_to_onedrive(target_store: str | None = None, **context) -> str:
    # 기존 코드 그대로, 지점명 생성 이후 target_store 있으면 추가 필터
    ...
    df = df[df["브랜드"].notna()].copy()
    ...
    df["지점명"] = df.apply(lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1)
    if target_store:
        before = len(df)
        df = df[df["지점명"] == target_store].copy()
        logger.info("target_store 필터 적용: %s → %d → %d건", target_store, before, len(df))
        if df.empty:
            return f"적재 대상 없음: target_store={target_store}"
    ...
```

필터 기준 컬럼은 현재 코드에서 생성하는 `지점명` 으로 고정한다.

---

### Step 4 — dags/launch/ 폴더 생성 및 __init__.py

```bash
mkdir -p dags/launch
touch dags/launch/__init__.py
```

---

### Step 5 — DB_OKPOS_Sales_Launch_Dags.py 작성

`dags/db/DB_OKPOS_Sales_Launch_Dags .py` 의 내용을 기반으로 아래만 변경:

```python
# 변경 1: import
from modules.transform.utility.schedule import DB_LAUNCH_TIME

# 변경 2: 상수
MANUAL_DATE_RANGE = None
LOOKBACK_DAYS: int | None = 1       # 어제 1일만
TARGET_STORES = ["송파삼전점"]

# 변경 3: DAG schedule
with DAG(
    dag_id=dag_id,
    schedule=DB_LAUNCH_TIME,
    ...

# 변경 4: download task 3개에 op_kwargs 추가
t2 = PythonOperator(
    task_id="download_today",
    python_callable=download_today_stores,
    op_kwargs={"target_stores": TARGET_STORES},
    retries=2,
    retry_delay=timedelta(minutes=3),
)
t3 = PythonOperator(
    task_id="download_receipt",
    python_callable=download_receipt_stores,
    op_kwargs={"target_stores": TARGET_STORES},
    retries=2,
    retry_delay=timedelta(minutes=3),
)
t3b = PythonOperator(
    task_id="download_daily",
    python_callable=download_daily_stores,
    op_kwargs={"target_stores": TARGET_STORES},
    retries=2,
    retry_delay=timedelta(minutes=3),
)
```

기존 `trigger_unified_sales_after_okpos` 를 유지할 경우 트리거 대상 DAG는 반드시
`DB_UnifiedSales_Launch_Dags` 로 바꾼다. 기존 `"DB_UnifiedSales"` 대상이 남으면 Launch 수집 후
원본 집계 DAG가 실행될 수 있으므로 금지한다. 기본 동작은 기존처럼
`trigger_unified_sales=false` 일 때 스킵으로 유지한다.

나머지 task 및 의존성 체인은 원본과 동일하게 유지.

---

### Step 6 — DB_Posfeed_Sales_Launch_Dags.py 작성

`dags/db/DB_Posfeed_Sales_lauch_Dags.py` 기반, 아래만 변경:

```python
# 변경 1: import
from modules.transform.utility.schedule import DB_LAUNCH_UNIFIED_TIME

# 변경 2: 상수 추가
TARGET_STORE = "송파삼전점"

# 변경 3: DAG schedule
schedule=DB_LAUNCH_TIME,

# 변경 4: partition_to_onedrive task에 op_kwargs 추가
onedrive_task = PythonOperator(
    task_id='partition_to_onedrive',
    python_callable=partition_to_onedrive,
    op_kwargs={"target_store": TARGET_STORE},
)
```

download_excel 은 변경 없음 (포스피드는 전체 Excel 1파일 다운 후 필터).

---

### Step 7 — DB_UnifiedSales_Launch_Dags.py 작성

`dags/db/DB_UnifiedSales_launch.py` 기반, 아래만 변경:

```python
# 변경 1: import
from modules.transform.utility.schedule import DB_LAUNCH_TIME

# 변경 2: 상수
LOOKBACK_DAYS: int | None = 1          # 어제 1일
TEST_STORES: list[str] = ["송파삼전점"]
UPSTREAM_POS_TASKS = []                # 상류 대기 없음 (수동 실행처럼 동작)

# 변경 3: DAG schedule
schedule=DB_LAUNCH_UNIFIED_TIME,
```

나머지 로직 동일. `wait_for_upstream_pos` 는 `UPSTREAM_POS_TASKS=[]` 이면 즉시 통과.

---

### Step 8 — DB_Baemin_Macro_Launch_Dags.py 작성

`dags/db/DB_Beamin_Macro_Launch_Dags.py` 기반, 아래만 변경:

```python
# 변경 1: import
from modules.transform.utility.schedule import DB_LAUNCH_TIME

# 변경 2: 상수
TARGET_STORES = ["송파삼전점"]
COLLECT_RANGE: str | None = None       # '상위'/'하위' 분할 없이 TARGET_STORES 직접 사용
ORDERS_BACKFILL_DAYS: int | None = None  # 어제 1일 (유지)

# 변경 3: DAG schedule
schedule=DB_LAUNCH_TIME,
```

`load_accounts` 에서 `pipeline_load_accounts(target_stores=TARGET_STORES, exact=True)` 이미 지원됨.
새 DAG 파일명과 `dag_id` 는 `Baemin` 으로 정리하지만, 파이프라인 import 경로는 현재 코드 기준
`DB_Beamin_*` 를 유지한다.

---

### Step 9 — 기존 dags/db/ Launch 파일 삭제

```bash
# PowerShell (공백 있는 파일명 주의)
Remove-Item "C:\airflow\dags\db\DB_OKPOS_Sales_Launch_Dags .py"
Remove-Item "C:\airflow\dags\db\DB_Posfeed_Sales_lauch_Dags.py"
Remove-Item "C:\airflow\dags\db\DB_Posfeed_Sales_Detail_laucnch_Dags.py"
Remove-Item "C:\airflow\dags\db\DB_UnifiedSales_launch.py"
Remove-Item "C:\airflow\dags\db\DB_Beamin_Macro_Launch_Dags.py"
```

---

## Reference Code

### modules/transform/pipelines/db/DB_OKPOS_Sales.py (1582줄 근처)

```python
STORES = [
    {"name": "도리당 동두천지행점", "shopCd": "RL2725"},
    {"name": "도리당 삼송점",       "shopCd": "UE6850"},
    {"name": "도리당 평택비전점",    "shopCd": "UW4935"},
    {"name": "도리당 송파삼전점",    "shopCd": "LQ9726"},
]

def download_today_stores(**context) -> str:
    """today 페이지 매장별 엑셀 다운로드"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    ...
    # 내부에서 STORES 상수 직접 참조
    pending_keys = _filter_missing_keys(sale_dates, "okpos_order")

def download_receipt_stores(**context) -> str: ...
def download_daily_stores(**context) -> str: ...
```

수정 시 `target_stores` 문자열 리스트를 store dict 리스트로 해석하는 helper를 추가하고,
`_filter_missing_keys(..., stores=stores)` 및 `force` 경로까지 같은 `stores` 를 사용한다.

### modules/transform/pipelines/db/DB_Posfeed_Sales.py (726줄 근처)

```python
def partition_to_onedrive(**context) -> str:
    """포스피드 주문 CSV → 브랜드/지점/월 파티션 CSV로 누적 저장"""
    csv_path = context['ti'].xcom_pull(task_ids='move_to_storage', key='saved_file_path')
    df = pd.read_csv(str(src), dtype=str, encoding="utf-8-sig")
    df["브랜드"] = df["가맹점명"].apply(_classify_brand)
    df = df[df["브랜드"].notna()].copy()
    df["지점명"] = df.apply(lambda r: _normalize_store(r["가맹점명"], r["브랜드"]), axis=1)
    # target_store 필터는 지점명 생성 이후에 삽입
```

### modules/transform/utility/schedule.py (관련 상수)

```python
DB_POSFEED_SALES_TIME  = "15 7 * * *"   # 매일 07:15
DB_OKPOS_SALES_TIME    = "0 6 * * *"    # 매일 06:00
DB_UNIFIED_SALES_TIME  = "40 8 * * *"   # 매일 08:40
SMD_BAEMIN_COLLECT_TIME = "10 3 * * *"  # 매일 03:10

# 추가할 것:
DB_LAUNCH_TIME = "0 15 * * *"           # 매일 15:00 (수집)
DB_LAUNCH_UNIFIED_TIME = "15 15 * * *"  # 매일 15:15 (집계)
```

---

## Test Cases

1. **schedule.py import 확인**
   ```bash
   docker compose exec airflow-webserver python -c "from modules.transform.utility.schedule import DB_LAUNCH_TIME, DB_LAUNCH_UNIFIED_TIME; print(DB_LAUNCH_TIME, DB_LAUNCH_UNIFIED_TIME)"
   ```
   → 기대: `0 15 * * * 15 15 * * *`

2. **DAG 파싱 오류 없는지 (4개 파일)**
   ```bash
   docker compose exec airflow-webserver python -c "
   import importlib.util, pathlib
   for f in pathlib.Path('dags/launch').glob('DB_*.py'):
       spec = importlib.util.spec_from_file_location(f.stem, f)
       m = importlib.util.module_from_spec(spec)
       spec.loader.exec_module(m)
       print(f.name, 'ok')
   "
   ```
   → 기대: 4줄 `ok`

3. **dag_id 4개 등록 확인**
   ```bash
   docker compose exec airflow-webserver airflow dags list | grep Launch
   ```
   → 기대:
   ```
   DB_OKPOS_Sales_Launch_Dags
   DB_Posfeed_Sales_Launch_Dags
   DB_UnifiedSales_Launch_Dags
   DB_Baemin_Macro_Launch_Dags
   ```

4. **스케줄 확인**
   ```bash
   docker compose exec airflow-webserver python -c "
   import importlib.util, pathlib
   expected = {
       'DB_OKPOS_Sales_Launch_Dags': '0 15 * * *',
       'DB_Posfeed_Sales_Launch_Dags': '0 15 * * *',
       'DB_Baemin_Macro_Launch_Dags': '0 15 * * *',
       'DB_UnifiedSales_Launch_Dags': '15 15 * * *',
   }
   for f in pathlib.Path('dags/launch').glob('DB_*.py'):
       spec = importlib.util.spec_from_file_location(f.stem, f)
       m = importlib.util.module_from_spec(spec)
       spec.loader.exec_module(m)
       actual = str(getattr(m.dag, 'schedule_interval', getattr(m.dag, 'schedule', '')))
       print(f.stem, actual)
       assert expected[f.stem] in actual or actual == expected[f.stem]
   "
   ```
   → 기대: 수집 DAG 3개는 `0 15 * * *`, UnifiedSales DAG 1개는 `15 15 * * *`

5. **기존 dags/db/ Launch 파일 삭제 확인**
   ```bash
   rg --files dags/db | rg -i "launch|lauch|laucnch"
   ```
   → 기대: 출력 없음

---

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1→5 순서대로 실행
  2. FAIL → 원인 분석 후 해당 파일 수정
  3. 전체 Test Cases 재실행
종료 조건: 5개 PASS + 기존 dags/db/ 원본 DAG(DB_OKPOS_Sales_Dags.py 등) 정상 등록 유지
```

---

## Constraints

- `dags/db/DB_OKPOS_Sales_Dags.py` 등 원본 DAG(비-Launch)는 절대 수정·삭제하지 않는다
- pipeline 함수(`download_today_stores` 등) 수정 시 기본값 `target_stores=None` 으로 **하위 호환 유지** (None이면 기존 STORES 전체 사용)
- OKPOS `target_stores` 는 문자열 리스트 입력을 store dict 리스트로 변환해서 사용한다. 문자열 리스트를 직접 `store["name"]` 로 접근하지 않는다
- `partition_to_onedrive` 수정 시 `target_store=None` 기본값으로 **하위 호환 유지**
- 수집 DAG 스케줄은 `DB_LAUNCH_TIME`, UnifiedSales 집계 DAG 스케줄은 `DB_LAUNCH_UNIFIED_TIME` 사용, 문자열 하드코딩 금지
- 송파삼전점 매장 이름은 OKPOS `STORES` 에서 `"도리당 송파삼전점"` 이지만 DAG 상수는 `["송파삼전점"]` — pipeline에서 `target_stores` 매칭 시 `store["name"]` 에 `target` 이 포함되는지 확인
- 배민 Launch DAG 파일명은 `Baemin` 으로 정리하되, 파이프라인 import 경로는 기존 `DB_Beamin_*` 를 유지

---

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- `_filter_missing_keys` 는 `stores=None` 인자를 명시적으로 추가하고, 내부에서 `stores_to_check = stores or STORES` 로 순회
- 주석: 최소화 (WHY만, WHAT 설명 금지)
- 변수명: snake_case, 기존 파일과 동일
