# 쿠팡 CollectionCompare 역할 분리

## Task
`load_coupang_macro_partition` 함수가 `DB_CollectionCompare_Dags`(07:45)와 `DB_CoupangMacro_Load_Dags`(09:00) 두 DAG에서 중복 호출되어 설계가 혼란스럽다. 쿠팡 파티션 적재는 `DB_CoupangMacro_Load_Dags`만 담당하도록 역할을 분리하고, `DB_CollectionCompare_Dags`는 이미 적재된 파티션을 읽기만 하도록 정리한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Background (점검 결과)

**배민 — 변경 없음 (이미 안전)**
- Macro DAG(03:10): `orders_2026-06-21.parquet` (일별) 저장
- Manual load(07:45): `orders_2026-06.parquet` (월별) 저장
- 같은 파티션 경로에 두 파일 공존하지만, `DB_CollectionCompare.py` `load_baemin_macro()` 내에
  `주문번호` 기준 dedup(L165-169)이 이미 존재 → 중복 집계 없음
- **배민은 코드 수정 불필요**

**쿠팡 — 이중 호출 설계 혼란**
- `load_coupang_macro_partition`이 두 DAG에서 모두 호출됨
- 07:45 실행 시 파일이 아카이브/삭제 → 09:00 실행 시 처리 대상 없음(빈 실행)
- 데이터 손상은 없지만 09:00 DAG가 매일 무의미한 빈 실행 발생

## Files to Create / Modify

- `modules/transform/utility/schedule.py` — `DB_COUPANG_MACRO_TIME` 값 수정
- `dags/db/DB_CollectionCompare_Dags.py` — `ingest_coupangeats_orders` 태스크 제거, 의존성 재연결

## Implementation Steps

### Step 1 — schedule.py: 쿠팡 스케줄 앞당기기

파일: `C:\airflow\modules\transform\utility\schedule.py`

현재:
```python
DB_COUPANG_MACRO_TIME = "0 9 * * *"  # 매일 09:00 실행 (쿠팡이츠 매크로 적재)
```

변경 후:
```python
DB_COUPANG_MACRO_TIME = "0 7 * * *"  # 매일 07:00 실행 (쿠팡이츠 매크로 적재, CollectionCompare 07:45 전)
```

### Step 2 — DB_CollectionCompare_Dags.py: 쿠팡 load 태스크 제거

파일: `C:\airflow\dags\db\DB_CollectionCompare_Dags.py`

**제거할 것:**
1. import에서 `load_coupang_macro_partition` 제거
2. `ingest_coupangeats` PythonOperator 태스크 블록 전체 제거
3. 의존성 재연결

현재 의존성:
```python
ingest_baemin >> cleanup_baemin
[cleanup_baemin, ingest_coupangeats] >> build_compare
```

변경 후:
```python
ingest_baemin >> cleanup_baemin >> build_compare
```

**현재 import (제거 대상 포함):**
```python
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
from modules.transform.pipelines.db.DB_CoupangMacro_load import load_coupang_macro_partition  # ← 제거
```

**제거 후:**
```python
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
```

**제거할 태스크 블록:**
```python
    ingest_coupangeats = PythonOperator(
        task_id="ingest_coupangeats_orders",
        python_callable=load_coupang_macro_partition,
    )
```

## Reference Code

### schedule.py (현재 관련 라인)
```python
DB_COUPANG_MACRO_TIME = "0 9 * * *"  # 매일 09:00 실행 (쿠팡이츠 매크로 적재)
DB_COLLECTION_COMPARE_TIME   = "45 7 * * *"  # 매일 07:45 실행 (수집 비교 마트)
```

### DB_CollectionCompare_Dags.py (현재 전체)
```python
from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
from modules.transform.pipelines.db.DB_CoupangMacro_load import load_coupang_macro_partition
from modules.transform.utility.schedule import DB_COLLECTION_COMPARE_TIME

# ... DAG 정의 ...

    ingest_baemin = PythonOperator(task_id="ingest_manual_baemin_orders", ...)
    cleanup_baemin = PythonOperator(task_id="cleanup_manual_baemin_orders", ...)
    ingest_coupangeats = PythonOperator(task_id="ingest_coupangeats_orders", ...)
    build_compare = PythonOperator(task_id="build_collection_compare", ...)

    ingest_baemin >> cleanup_baemin
    [cleanup_baemin, ingest_coupangeats] >> build_compare
```

## Test Cases

1. **import 검증** — 수정 후 두 파일 모두 에러 없어야 함
   ```
   python -c "from dags.db.DB_CollectionCompare_Dags import dag; print('OK')"
   python -c "from modules.transform.utility.schedule import DB_COUPANG_MACRO_TIME; print(DB_COUPANG_MACRO_TIME)"
   ```
   기대: `OK` / `0 7 * * *`

2. **task graph 확인** — `ingest_coupangeats_orders` 태스크가 없어야 함
   ```
   python -c "
   from dags.db.DB_CollectionCompare_Dags import dag
   task_ids = [t.task_id for t in dag.tasks]
   assert 'ingest_coupangeats_orders' not in task_ids, f'태스크 아직 있음: {task_ids}'
   assert 'build_collection_compare' in task_ids
   print('PASS:', task_ids)
   "
   ```

3. **의존성 순서 확인** — `build_collection_compare`의 upstream이 `cleanup_manual_baemin_orders`여야 함
   ```
   python -c "
   from dags.db.DB_CollectionCompare_Dags import dag
   build = dag.get_task('build_collection_compare')
   upstream = [t.task_id for t in build.upstream_list]
   assert 'cleanup_manual_baemin_orders' in upstream, f'upstream: {upstream}'
   assert 'ingest_coupangeats_orders' not in upstream
   print('PASS upstream:', upstream)
   "
   ```

4. **Airflow UI 확인** (수동)
   - Airflow UI → `DB_CollectionCompare_Dags` Graph View → `ingest_coupangeats_orders` 노드 없음 확인
   - `DB_CoupangMacro_Load_Dags` Schedule 값이 `0 7 * * *`인지 확인

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1-3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `DB_COUPANG_MACRO_TIME` 변경은 `schedule.py`만 수정 — DAG 파일 내 하드코딩 금지
- `DB_CoupangMacro_Load_Dags.py` 자체는 수정하지 않음 (스케줄 상수만 변경하면 자동 반영)
- `DB_CollectionCompare_Dags.py`에서 배민 관련 태스크(`ingest_manual_baemin_orders`, `cleanup_manual_baemin_orders`)는 그대로 유지
- `build_collection_compare` 함수(`DB_CollectionCompare.py`)는 수정 없음 — 이미 파티션을 읽기만 함

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
