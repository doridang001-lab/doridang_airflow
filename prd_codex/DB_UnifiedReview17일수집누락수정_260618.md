# DB_UnifiedReview 2026-06-17 수집 누락 수정

## Task

`DB_UnifiedReview_Dags`가 `Sales_ToOrder_Review_Dags`의 완료 여부를 기다리지 않고 07:33에 바로 실행돼, 소스 DAG가 지연된 날(17일, 13:46 생성)에 `toorder_voc_20260617.parquet`가 없어 집계가 Skip됐다. `ExternalTaskSensor`를 추가해 소스 DAG 완료를 대기하도록 수정하고, 누락된 17일 데이터를 수동 복구한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- DAG 파일 위치: `dags/db/`
- `dag_id = Path(__file__).stem` 패턴 유지
- `catchup=False`, `max_active_runs=1` 기본

## Files to Create / Modify

- **수정**: `dags/db/DB_UnifiedReview_Dags.py` — ExternalTaskSensor task 추가, task 의존관계 업데이트

## Root Cause (역순 추적)

| 단계 | 내용 |
|------|------|
| ① 증상 | `unified_review_count.parquet`에 06-17 작성일자 데이터 없음 |
| ② 직접 원인 | DB_UnifiedReview 07:33 실행 시점에 `toorder_voc_20260617.parquet` 미존재 |
| ③ 상위 원인 | `Sales_ToOrder_Review_Dags` 예정(07:30)보다 6시간 늦은 13:46에 파일 생성 |
| ④ 근본 원인 | 두 DAG 간 의존관계가 3분 스케줄 gap에만 암묵적으로 의존, ExternalTaskSensor 없음 |

> **구조적 특성 (버그 아님)**: `toorder_voc_20260617.parquet`는 설계상 06-16까지의 리뷰만 포함. 06-17 작성 리뷰는 `toorder_voc_20260618.parquet`에 담김. 문제는 17일 upsert 자체가 Skip된 것.

## Implementation Steps

### Step 1: ExternalTaskSensor 추가

`dags/db/DB_UnifiedReview_Dags.py` 수정:

**추가할 import** (기존 import 블록 하단에 추가):
```python
from airflow.sensors.external_task import ExternalTaskSensor
```

**DAG 블록 내 task 추가** (`with DAG(...) as dag:` 블록에서 t1 정의 앞에 삽입):
```python
    wait_for_toorder_review = ExternalTaskSensor(
        task_id="wait_for_toorder_review",
        external_dag_id="Sales_ToOrder_Review_Dags",
        external_task_id=None,        # DAG 전체 완료 대기
        allowed_states=["success"],
        execution_delta=timedelta(0), # 같은 execution_date
        timeout=60 * 60 * 6,          # 최대 6시간 대기
        poke_interval=60,
        mode="reschedule",
    )
```

**task 의존관계 수정** (파일 마지막 줄):
```python
# 변경 전
t1 >> t2 >> t3

# 변경 후
wait_for_toorder_review >> t1 >> t2 >> t3
```

### Step 2: 17일 누락 데이터 수동 복구

구현 완료 후 Airflow UI에서 수동 트리거:
```json
{"sale_date": "2026-06-16"}
```
- DAG: `DB_UnifiedReview_Dags`
- `toorder_voc_20260617.parquet`가 이미 존재하므로 16일자 집계 복구 가능
- `wait_for_toorder_review` 센서는 과거 실행이므로 `allowed_states`에 따라 즉시 통과하거나 `execution_delta` 조정 필요

> **수동 트리거 시 센서 우회 방법**: `execution_delta` 대신 과거 날짜로 트리거하면 Sales DAG의 해당 날짜 run이 success 상태여야 센서를 통과한다. 복구 트리거 시 센서가 block되면 `wait_for_toorder_review` task만 skip 처리 후 t1부터 수동 실행한다.

## Reference Code

### dags/db/DB_UnifiedReview_Dags.py (현재 구조)
```python
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_UNIFIED_REVIEW_TIME
from modules.transform.pipelines.db.DB_UnifiedReview import (
    run_review as pipeline_run_review,
    backfill_review as pipeline_backfill_review,
    build_daily_review_count as pipeline_count_reviews,
)

dag_id = Path(__file__).stem

with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_REVIEW_TIME,   # "33 7 * * *"
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "review", "toorder", "unified_review"],
) as dag:

    t1 = PythonOperator(task_id="resolve_mode", python_callable=resolve_mode)
    t2 = PythonOperator(task_id="build_review_mart", python_callable=build_review_mart)
    t3 = PythonOperator(task_id="count_daily_reviews", python_callable=count_daily_reviews)

    t1 >> t2 >> t3   # ← 이 줄을 wait_for_toorder_review >> t1 >> t2 >> t3 로 변경
```

### dags/sales/Sales_ToOrder_Review_Dags.py (소스 DAG 확인용)
```python
# dag_id = Path(__file__).stem → "Sales_ToOrder_Review_Dags"
# schedule = "30 7 * * *"  (매일 07:30)
with DAG(
    dag_id=Path(__file__).stem,
    schedule="30 7 * * *",
    ...
) as dag:
    prepare  = PythonOperator(task_id="t1_prepare",  ...)
    collect  = PythonOperator(task_id="t2_collect",  retries=3)
    save     = PythonOperator(task_id="t3_save",     retries=1)
    validate = PythonOperator(task_id="t4_validate", retries=1)
```

## Test Cases

1. **Import 검증**
   ```bash
   python -c "from dags.db.DB_UnifiedReview_Dags import dag; print('OK')"
   ```
   → 기대: `OK` (ImportError 없음)

2. **DAG task 목록 확인**
   ```bash
   airflow tasks list DB_UnifiedReview_Dags
   ```
   → 기대: `wait_for_toorder_review`, `resolve_mode`, `build_review_mart`, `count_daily_reviews` 4개

3. **task 의존관계 확인**
   ```bash
   airflow tasks list DB_UnifiedReview_Dags --tree
   ```
   → 기대: `wait_for_toorder_review → resolve_mode → build_review_mart → count_daily_reviews` 순서

4. **누락 데이터 복구 확인**
   Airflow UI에서 `conf={"sale_date": "2026-06-16"}` 트리거 후:
   ```python
   import pandas as pd
   df = pd.read_parquet("OneDrive/data/mart/unified_review/unified_review_count.parquet")
   print(df[df["작성일자"] == "2026-06-16"])
   ```
   → 기대: 2026-06-16 행이 존재

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `ExternalTaskSensor`의 `external_dag_id`는 반드시 `"Sales_ToOrder_Review_Dags"` (파일명 stem 그대로)
- `execution_delta=timedelta(0)`: 두 DAG 모두 매일 실행이므로 같은 execution_date 기준
- `mode="reschedule"`: worker slot 점유 방지 (poke 방식 금지)
- `timeout=60 * 60 * 6`: 소스 DAG가 최대 6시간 지연 가능 케이스 커버
- `timedelta`는 이미 `from datetime import timedelta`로 import되어 있음 — 중복 import 금지
- 기존 `default_args`, `_on_failure_callback` 등은 수정하지 않음

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
