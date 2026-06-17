# wait_toorder_menu 센서 무한 대기 버그 수정

## Task
`DB_ToOrderMenu_LLM_Dags`의 첫 태스크 `wait_toorder_menu`(`ExternalTaskSensor`)가 끝나지 않고 poke만 무한 반복한다. 원인은 **logical_date 불일치**: 센서가 `execution_delta`/`execution_date_fn` 없이 자기와 동일한 logical_date의 외부 DAG 런을 찾는데, 대기 대상 `DB_ToOrderMenu_Dags`(스케줄 07:00)와 본 DAG(스케줄 08:30)의 data_interval 시작 시각이 달라 매칭이 영원히 실패한다. 레포에 이미 존재하는 검증된 `execution_date_fn` 패턴(`Sales_Orders_02_Review_Dags.py`)을 적용해 해결한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `dags/db/DB_ToOrderMenu_LLM_Dags.py` (단일 파일만 수정)
- schedule.py / 외부 DAG / 파이프라인 모듈은 **변경 없음**

## Implementation Steps

### 1. `from airflow import settings` import 추가
파일 상단 import 블록에 추가 (`from airflow import DAG` 인근).

### 2. execution_date 해석 함수 추가
import 블록 아래, `with DAG(...)` 정의 위에 추가. DB에서 외부 태스크의 최신 success TaskInstance의 execution_date를 직접 조회한다 (스케줄 시각 차이와 무관하게 동작).

```python
def _latest_toorder_menu_execution_date(dt, **context):
    """DB_ToOrderMenu_Dags의 save_menu_detail_parquet가 성공한 최신 execution_date 반환."""
    from airflow.models.taskinstance import TaskInstance as TI

    session = settings.Session()
    try:
        ti = (
            session.query(TI)
            .filter(
                TI.dag_id == "DB_ToOrderMenu_Dags",
                TI.task_id == "save_menu_detail_parquet",
                TI.state == "success",
            )
            .order_by(TI.execution_date.desc())
            .first()
        )
        if ti:
            logger.info("[sensor] 메뉴 DAG 성공 실행 발견: %s", ti.execution_date)
            return ti.execution_date
    except Exception as exc:
        logger.warning("[sensor] TaskInstance 조회 실패: %s", exc)
    finally:
        session.close()
    logger.info("[sensor] 성공 실행 없음, 기본값 사용: %s", dt)
    return dt
```

### 3. 센서 정의 교체
기존 `wait_toorder_menu = ExternalTaskSensor(...)` (현재 task_id/external_dag_id/external_task_id/poke_interval/timeout/mode만 있음) 를 아래로 교체.

```python
    wait_toorder_menu = ExternalTaskSensor(
        task_id="wait_toorder_menu",
        external_dag_id="DB_ToOrderMenu_Dags",
        external_task_id="save_menu_detail_parquet",
        execution_date_fn=_latest_toorder_menu_execution_date,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=120,
        timeout=14400,
        mode="reschedule",
        soft_fail=True,
    )
```

변경 핵심:
- `execution_date_fn` 추가 → logical_date 불일치 해소 (**핵심 수정**)
- `allowed_states`/`failed_states` 명시 → 외부 실패 시 무한 대기 대신 빠른 판정
- `soft_fail=True` → 외부 런을 못 찾아도 LLM 파이프라인 진행 (기존 02 패턴과 동일)

## Reference Code
### dags/sales/Sales_Orders_02_Review_Dags.py
```python
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import State
from airflow import settings

def _latest_smd01_execution_date(dt, **context):
    """SMD_01의 save_to_csv 태스크가 성공한 최신 execution_date를 찾는다."""
    from airflow.models.taskinstance import TaskInstance as TI
    session = settings.Session()
    try:
        ti = session.query(TI).filter(
            TI.dag_id == 'Sales_Orders_01_Extract_Dags',
            TI.task_id == 'save_to_csv',
            TI.state == 'success'
        ).order_by(TI.execution_date.desc()).first()
        if ti:
            print(f"[sensor] SMD_01 save_to_csv 성공 찾음: {ti.execution_date}")
            return ti.execution_date
    except Exception as e:
        print(f"[sensor] TaskInstance 조회 실패: {e}")
    finally:
        session.close()
    return dt

    # 센서 사용부
    wait_for_smd_01 = ExternalTaskSensor(
        task_id='wait_for_smd_01_completion',
        external_dag_id='Sales_Orders_01_Extract_Dags',
        external_task_id='save_to_csv',
        execution_date_fn=_latest_smd01_execution_date,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
        soft_fail=True,
    )
```

## Test Cases
1. [DagBag import] `python -c "from airflow.models.dagbag import DagBag; b=DagBag('dags'); print(b.import_errors)"` → 기대: `DB_ToOrderMenu_LLM_Dags` 관련 에러 없음 (빈 dict 또는 무관 항목만)
2. [센서 속성 확인] DAG 로드 후 `wait_toorder_menu.execution_date_fn`이 `None`이 아니고, `allowed_states == ['success']`, `soft_fail is True` 인지 확인
3. [수동 트리거] Airflow UI에서 `DB_ToOrderMenu_LLM_Dags` 트리거 → 메뉴 DAG `save_menu_detail_parquet`가 한 번이라도 success였다면 `wait_toorder_menu`가 즉시 success로 전환되는지 확인
4. [무한대기 미발생] 메뉴 DAG 성공 이력이 없어도 `soft_fail`로 다운스트림이 막히지 않고 진행되는지 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 단일 파일(`dags/db/DB_ToOrderMenu_LLM_Dags.py`)만 수정. 외부 DAG·schedule.py·파이프라인 모듈 변경 금지.
- print 금지 — 모든 로그는 기존 `logger` 사용.
- `execution_delta` 단순 대안은 채택하지 않음 (스케줄 변경·backfill·수동 트리거에 취약).
- task_id `wait_toorder_menu`, external_dag_id `DB_ToOrderMenu_Dags`, external_task_id `save_menu_detail_parquet`, `poke_interval=120`, `timeout=14400`, `mode="reschedule"` 값은 그대로 유지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(=수정)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
