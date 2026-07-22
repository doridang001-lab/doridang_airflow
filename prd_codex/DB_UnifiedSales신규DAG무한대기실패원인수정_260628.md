# DB_UnifiedSales 신규 DAG 무한 대기 / 실패 원인 수정

## Task

리팩토링으로 `dags/db/DB_UnifiedSales.py`(구) → `dags/db/DB_UnifiedSales_Dags.py`(신, `dag_id="DB_UnifiedSales"`)로 교체했는데, 새 DAG의 선두 센서 태스크 `wait_for_upstream_pos`가 상류 POS 수집 DAG 완료를 영영 확인하지 못해 전체 파이프라인이 막혀 있다. 상류 매칭 기준을 `logical_date(execution_date)` 윈도우 → 벽시계 `start_date` 최근성 기준으로 바꾸고, 6시간 워커 슬롯 점유를 막기 위해 PythonSensor(`mode="reschedule"`)로 전환한다.

### 근본 원인 (진단 완료)
- `wait_for_upstream_pos`(`DB_UnifiedSales_Dags.py:130~215`)는 상류 런을 `DagRun.execution_date` 윈도우(`logical_date - 18h <= execution_date <= logical_date`)로 찾는다.
- 상류 POS DAG들은 일 1회 새벽 실행 → `execution_date`가 벽시계보다 ~24h 뒤처진다(일별 DAG의 data_interval_start 특성). 반면 unified DAG는 `0 12,15`(3h 간격)라 ~3h만 뒤처진다. 두 기준 프레임이 ~21h 어긋나 윈도우가 거의 절대 겹치지 않음 → 센서 로그에 4개 상류 모두 `dag_run 없음`.
- 메타DB 실측 예: `DB_Posfeed_Sales_Dags` `execution_date=2026-06-26 18:15Z`인데 `start_date=2026-06-27 18:15Z`(24h 차).
- spec의 `execution_delta`(오프셋 보정용)를 정의해두고도 쿼리에서 전혀 사용하지 않음 — 미완성 리팩토링.
- 증상: `scheduled__2026-06-27T03:00:00` 런 = 6h timeout 실패(하위 19개 `upstream_failed`), `scheduled__2026-06-27T06:00:00` 런 = 17h째 running(while 루프로 슬롯 점유), `max_active_runs=1`이라 이후 런 전부 막힘.
- 동일 계열 과거 수정 선례: `prd_codex/FixUnifiedReviewSensorDelta_260619.md`, `prd_codex/wait_toorder_menu센서무한대기버그수정_260616.md`.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- sensor는 `mode="reschedule"` 사용 (repo 관례: `DB_UnifiedReview_Dags.py`, `DB_ToOrderMenu_LLM_Dags.py`)

## Files to Create / Modify
- **수정**: `dags/db/DB_UnifiedSales_Dags.py`
  - `UPSTREAM_POS_TASKS` (line 98~119): `execution_delta` 필드 제거
  - `wait_for_upstream_pos` 콜백 (line 130~215): 단발 poke 함수로 재작성, 윈도우 쿼리 교체
  - 태스크 정의 (line 466~469): `PythonOperator` → `PythonSensor(mode="reschedule")`
  - import 추가: `from airflow.sensors.python import PythonSensor`

## Implementation Steps

### 1. import 추가
파일 상단 import 블록(line 21 부근 `from airflow.operators.python import PythonOperator` 아래)에 추가:
```python
from airflow.sensors.python import PythonSensor
```

### 2. `UPSTREAM_POS_TASKS`에서 `execution_delta` 필드 제거 (line 98~119)
`dag_id`/`task_id`만 유지. 상류 task_id는 실측으로 모두 존재 확인됨.
```python
UPSTREAM_POS_TASKS = [
    {"dag_id": "DB_OKPOS_Sales_Dags", "task_id": "write_log"},
    {"dag_id": "DB_UnionPOS_Receipt_Dags", "task_id": "write_log"},
    {"dag_id": "DB_EasyPOS_Sales_Dags", "task_id": "save_easypos_product"},
    {"dag_id": "DB_Posfeed_Sales_Dags", "task_id": "scrape_missing_order_details"},
]
```

### 3. `wait_for_upstream_pos` 콜백 재작성 (line 130~215)
`while True` / `time.sleep` / `deadline` / `logical_col = DagRun.execution_date` 제거. 단발 poke 함수로 변경 — 미완료면 `return False`(reschedule), 전부 완료면 `return True`, 상류 실패면 `AirflowException`(fail-fast). manual 스킵 로직 유지(스킵=`return True`).

```python
def wait_for_upstream_pos(**context) -> bool:
    """정기 실행에서 상류 POS 수집 DAG 완료를 1회 poke로 확인한다 (reschedule sensor용).

    상류 런 매칭은 logical_date가 아니라 벽시계 start_date 최근성으로 한다.
    상류 POS DAG는 일 1회 새벽 실행이라 execution_date가 벽시계보다 ~24h 뒤처져
    logical_date 윈도우로는 매칭되지 않기 때문이다.

    수동 정정 실행은 기본적으로 대기하지 않는다. dag_run.conf에
    {"wait_upstreams": true}를 넣으면 수동에서도 상류 완료를 강제한다.
    """
    from airflow import settings
    from airflow.exceptions import AirflowException
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    run_type = str(getattr(dag_run, "run_type", "") or "").lower()
    is_manual = "manual" in run_type or bool(getattr(dag_run, "external_trigger", False))

    if is_manual and not _truthy(conf.get("wait_upstreams")):
        logger.info("수동 실행: 상류 POS 대기 스킵 (wait_upstreams=true 아님)")
        return True

    max_upstream_age_hours = int(conf.get("upstream_max_age_hours") or 18)
    cutoff = pendulum.now("UTC") - timedelta(hours=max_upstream_age_hours)

    pending: list[str] = []
    completed: list[str] = []
    session = settings.Session()
    try:
        for spec in UPSTREAM_POS_TASKS:
            label = f"{spec['dag_id']}.{spec['task_id']} since={cutoff.isoformat()}"
            upstream_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == spec["dag_id"])
                .filter(DagRun.start_date >= cutoff)
                .order_by(DagRun.start_date.desc())
                .first()
            )
            if upstream_run is None:
                pending.append(f"{label}: 최근 dag_run 없음")
                continue
            run_label = f"{label} run_id={upstream_run.run_id}"

            ti = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == spec["dag_id"])
                .filter(TaskInstance.task_id == spec["task_id"])
                .filter(TaskInstance.run_id == upstream_run.run_id)
                .one_or_none()
            )
            if ti is None:
                pending.append(f"{run_label}: task_instance 없음")
                continue

            state = str(ti.state or "").lower()
            if state in {"success", "skipped"}:
                completed.append(f"{run_label}: state={state}")
                continue
            if state in {"failed", "upstream_failed"}:
                raise AirflowException(f"상류 POS 실패: {run_label} state={ti.state}")
            pending.append(f"{run_label}: state={ti.state}")
    finally:
        session.close()

    if not pending:
        logger.info("상류 POS 완료 확인: %s", " | ".join(completed[:10]))
        return True

    logger.info("상류 POS 대기 중: %s", " | ".join(pending[:10]))
    return False
```

### 4. 태스크 정의를 PythonSensor로 변경 (line 466~469)
```python
t_wait = PythonSensor(
    task_id="wait_for_upstream_pos",
    python_callable=wait_for_upstream_pos,
    mode="reschedule",
    poke_interval=60,
    timeout=60 * 60 * 6,
)
```
의존관계(`t1 >> t_wait >> ...`)는 변경하지 않는다.

### 5. 운영 조치 (코드 배포 후 1회)
파일 수정 시 스케줄러가 자동 파싱한다. 그 후:
1. 멈춘 `scheduled__2026-06-27T06:00:00` 런의 `wait_for_upstream_pos` task를 **Clear**(또는 런 fail 처리) → 슬롯 해제(`max_active_runs=1` 해소).
2. 필요 시 해당 런 Clear로 재실행하여 새 로직 통과 확인. 오늘(06-28) 정기 런은 자동 진행.

명령 예:
```bash
docker exec <scheduler> airflow tasks clear DB_UnifiedSales -t wait_for_upstream_pos -s 2026-06-27 -e 2026-06-27 -y
```

## Reference Code

### dags/db/DB_UnifiedSales_Dags.py (상단 import + DAG 정의)
```python
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
# ↑ 여기에 from airflow.sensors.python import PythonSensor 추가

from modules.transform.utility.schedule import DB_UNIFIED_SALES_TIME
# ...
with DAG(
    dag_id=dag_id,                                  # "DB_UnifiedSales"
    schedule=DB_UNIFIED_SALES_TIME,                 # "0 12,15 * * *"
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "unionpos", "easypos", "posfeed", "unified_sales"],
) as dag:
    t1 = PythonOperator(task_id="resolve_date", python_callable=resolve_date)
    t_wait = PythonOperator(task_id="wait_for_upstream_pos", python_callable=wait_for_upstream_pos)  # ← 교체 대상
```

### dags/db/DB_UnifiedReview_Dags.py (reschedule sensor 관례)
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_toorder_review = ExternalTaskSensor(
    task_id="wait_for_toorder_review",
    external_dag_id="Sales_ToOrder_Review_Dags",
    external_task_id="t4_validate",
    execution_date_fn=_latest_toorder_review_execution_date,
    allowed_states=["success"],
    timeout=60 * 60 * 6,
    poke_interval=60,
    mode="reschedule",
)
```

## Test Cases
1. **콜백 정의 import** `docker exec <scheduler> python -c "from dags.db.DB_UnifiedSales_Dags import wait_for_upstream_pos; print('OK')"` → 기대: `OK`
2. **DAG import** `docker exec <scheduler> python -c "from dags.db.DB_UnifiedSales_Dags import dag; print('OK')"` → 기대: ImportError 없음
3. **파싱 에러 없음** `docker exec <scheduler> airflow dags list-import-errors` → 기대: `No data found` (DB_UnifiedSales 관련 에러 없음)
4. **센서 타입 확인** `docker exec <scheduler> python -c "from dags.db.DB_UnifiedSales_Dags import dag; print(type(dag.get_task('wait_for_upstream_pos')).__name__)"` → 기대: `PythonSensor`
5. **Clear 후 poke 로그** — stuck 런 Clear 후, poke 로그에 `상류 POS 완료 확인` 또는 미완 시 `상류 POS 대기 중`이 찍히고, `dag_run 없음`이 **사라져야** 함(벽시계 최근 런을 가리킴).
6. **reschedule 동작** — 대기 중 task 상태가 `up_for_reschedule`(슬롯 미점유)인지 UI/로그로 확인.
7. **통과 확인** — 상류 4개 key task가 모두 success면 센서 즉시 통과 → 하위 `build_okpos` 등 진행.

## Verification Loop
구현 완료 후 아래 루프를 모든 Test Cases PASS까지 반복한다.
```
LOOP until all PASS:
  1. Test Cases 1~4 (정적/import) 실행
  2. FAIL 항목 → 원인 분석 → 코드 수정 → 재실행
  3. 통과 시 운영 조치(Implementation Step 5) 후 Test Cases 5~7 실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `logical_date`/`execution_date` 기반 윈도우 매칭으로 되돌리지 말 것 — 상류 일별 DAG의 execution_date가 벽시계보다 ~24h 뒤처져 영구 실패하는 것이 이 버그의 근본 원인이다.
- manual(정정) 실행은 기본 스킵 동작 유지 — `conf.wait_upstreams=true`일 때만 대기.
- `pendulum`, `timedelta`는 이미 파일 상단에서 import됨(추가 불필요). `PythonSensor`만 추가.
- 상류 실패 상태(`failed`/`upstream_failed`)는 `AirflowException`으로 fail-fast(무한 대기 금지).
- 의존관계·하위 태스크 정의는 건드리지 않는다.
- 구 파일 `dags/db/DB_UnifiedSales.py`는 `.airflowignore`로 가려진 상태 유지(건드리지 않음).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 해당 블록만 수정
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `upstream_max_age_hours` 기본값: 18 (기존 값 유지)
