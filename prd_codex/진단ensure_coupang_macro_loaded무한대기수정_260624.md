# ensure_coupang_macro_loaded 무한 대기 수정

## Task
`DB_CollectionCompare_Dags`의 `ensure_coupang_macro_loaded` 태스크가 `TriggerDagRunOperator`를 사용해 항상 새 run을 생성하는데, `DB_CoupangMacro_Load_Dags`의 `max_active_runs=1` 제약으로 스케줄 run이 살아있을 때 triggered run이 Queue에 걸려 무한 대기한다. `ExternalTaskSensor`로 교체해 이미 완료된 매크로 run을 감지하도록 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- `dag_id=Path(__file__).stem`, `catchup=False`, `max_active_runs=1` 기본

## Files to Create / Modify
- **수정**: `dags/db/DB_CollectionCompare_Dags.py`
  - `TriggerDagRunOperator` → `ExternalTaskSensor` 교체
  - `execution_date_fn`으로 가장 최근 macro success run 감지

## Implementation Steps

1. **import 교체**
   ```python
   # 제거
   from airflow.operators.trigger_dagrun import TriggerDagRunOperator

   # 추가
   from airflow.sensors.external_task import ExternalTaskSensor
   from datetime import timedelta
   ```

2. **execution_date_fn 헬퍼 정의** (DAG 정의 위, `with DAG(...)` 바깥에 배치)
   ```python
   def _latest_macro_execution_date(dt, **kwargs):
       """가장 최근 성공한 CoupangMacro run의 execution_date 반환."""
       from airflow.models import DagRun
       from airflow.utils.state import DagRunState

       runs = DagRun.find(
           dag_id="DB_CoupangMacro_Load_Dags",
           state=DagRunState.SUCCESS,
       )
       eligible = sorted(
           [r for r in runs if r.execution_date <= dt],
           key=lambda r: r.execution_date,
           reverse=True,
       )
       return eligible[0].execution_date if eligible else dt
   ```

3. **태스크 교체** (`with DAG(...)` 블록 안)
   ```python
   # 기존 TriggerDagRunOperator 교체
   ensure_coupang_macro_loaded = ExternalTaskSensor(
       task_id="ensure_coupang_macro_loaded",
       external_dag_id="DB_CoupangMacro_Load_Dags",
       external_task_id=None,               # DAG 레벨 success 감지
       execution_date_fn=_latest_macro_execution_date,
       allowed_states=["success"],
       failed_states=["failed"],
       poke_interval=60,
       timeout=3600,                         # 1시간 타임아웃
       mode="poke",
   )
   ```

4. **태스크 의존성 유지** (변경 없음)
   ```python
   ingest_baemin >> cleanup_baemin
   [cleanup_baemin, ensure_coupang_macro_loaded] >> build_compare
   ```

## Reference Code

### dags/db/DB_CollectionCompare_Dags.py (현재 전체)
```python
"""Collection comparison mart DAG."""

from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # ← 교체 대상

from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    load_manual_baemin_orders,
    cleanup_manual_baemin_orders,
)
from modules.transform.utility.schedule import DB_COLLECTION_COMPARE_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_COLLECTION_COMPARE_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "dashboard", "collection_compare", "powerbi"],
) as dag:
    ingest_baemin = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_orders,
    )
    cleanup_baemin = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_orders,
    )
    ensure_coupang_macro_loaded = TriggerDagRunOperator(  # ← 교체
        task_id="ensure_coupang_macro_loaded",
        trigger_dag_id="DB_CoupangMacro_Load_Dags",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    build_compare = PythonOperator(
        task_id="build_collection_compare",
        python_callable=build_collection_compare,
    )
    ingest_baemin >> cleanup_baemin
    [cleanup_baemin, ensure_coupang_macro_loaded] >> build_compare
```

## Test Cases

1. **DAG import 검증**
   ```bash
   python -c "from dags.db.DB_CollectionCompare_Dags import dag; print('OK')"
   ```
   → 기대: `OK` (ImportError 없음)

2. **ExternalTaskSensor 태스크 타입 확인**
   ```bash
   python -c "
   from dags.db.DB_CollectionCompare_Dags import dag
   t = dag.get_task('ensure_coupang_macro_loaded')
   print(type(t).__name__)
   "
   ```
   → 기대: `ExternalTaskSensor`

3. **Airflow DAG parse 검증** (Airflow 컨테이너 안에서)
   ```bash
   airflow dags list | grep DB_CollectionCompare
   ```
   → 기대: `DB_CollectionCompare_Dags` 출력, 파싱 오류 없음

4. **수동 trigger 테스트** (Airflow UI)
   - `DB_CollectionCompare_Dags` 수동 trigger
   - `ensure_coupang_macro_loaded` 태스크가 `DB_CoupangMacro_Load_Dags`의 최근 success run 감지 후 통과하는지 확인
   - 새 `DB_CoupangMacro_Load_Dags` run이 추가 생성되지 않는지 확인

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: Test Cases 1~3 전체 PASS + Test Case 4 Airflow UI에서 확인
```

## Constraints

- `TriggerDagRunOperator` 완전 제거 (import 포함)
- `execution_date_fn`은 DAG 블록 **외부** (모듈 레벨)에 정의
- `timeout=3600` 필수 — 무한 대기 방지
- `mode="poke"` 유지 (reschedule 모드 전환 금지)
- 기존 태스크 의존성 `[cleanup_baemin, ensure_coupang_macro_loaded] >> build_compare` 변경 금지
- `DB_CoupangMacro_Load_Dags` DAG 자체는 수정하지 않는다

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
