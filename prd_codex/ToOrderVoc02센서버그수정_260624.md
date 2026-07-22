# ToOrderVoc02 센서 버그 수정

## Task

`Strategy_ToOrderVoc_02_Transform_Dags`의 `wait_for_crawling` ExternalTaskSensor가 두 가지 이유로 항상 실패한다:
(1) postgres 메타DB 일시 장애 시 poke 자체가 crash되고,
(2) `execution_delta` 미설정으로 01 DAG의 07:30 KST 실행을 09:30 KST logical_date로 탐색해 존재하지 않는 실행을 3600초 기다리다 timeout된다.
두 파라미터(`execution_delta`, `mode`)를 수정하여 구조적 원인을 제거한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- DAG 파일 위치: `dags/strategy/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- DAG에는 오케스트레이션만 — 비즈니스 로직은 `modules/transform/pipelines/`

## Files to Create / Modify

- **수정**: `dags/strategy/Strategy_ToOrderVoc_02_Transform_Dags.py`
  - `wait_for_crawling` ExternalTaskSensor 파라미터 2개 변경

## Implementation Steps

### 스케줄 불일치 배경

| DAG | schedule (KST 기준) | UTC logical_date (2026-06-23 실행) |
|-----|---------------------|--------------------------------------|
| `Strategy_ToOrderVoc_01_Crawl_Dags` | `"30 7 * * *"` (07:30 KST) | `2026-06-22T22:30:00Z` |
| `Strategy_ToOrderVoc_02_Transform_Dags` | `"30 9 * * *"` (09:30 KST) | `2026-06-23T00:30:00Z` |

`execution_delta` 없이 ExternalTaskSensor는 02의 logical_date(`00:30Z`)와 동일한 01 실행을 탐색하지만,  
01은 `22:30Z`에만 실행되므로 해당 실행이 존재하지 않아 항상 timeout 발생.  
필요한 delta = `timedelta(hours=2)` (02 logical_date - 2h = 01 logical_date).

### 1. ExternalTaskSensor 수정

파일: `dags/strategy/Strategy_ToOrderVoc_02_Transform_Dags.py`, `wait_for_crawling` 태스크 블록

```python
# 변경 전 (lines 116-125)
wait_for_crawling = ExternalTaskSensor(
    task_id='wait_for_crawling',
    external_dag_id='Strategy_ToOrderVoc_01_Crawl_Dags',
    external_task_id='move_voc_files',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='poke',
    timeout=3600,
    poke_interval=60,
)

# 변경 후
wait_for_crawling = ExternalTaskSensor(
    task_id='wait_for_crawling',
    external_dag_id='Strategy_ToOrderVoc_01_Crawl_Dags',
    external_task_id='move_voc_files',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    execution_delta=timedelta(hours=2),  # 01=07:30KST, 02=09:30KST → 2시간 차이
    mode='reschedule',                   # 워커 슬롯 점유 방지
    timeout=7200,                        # 크롤링 최대 대기 2시간
    poke_interval=60,
)
```

- `from datetime import timedelta`는 이미 line 10에 import됨 → 추가 불필요
- `mode='reschedule'`: poke 사이에 워커를 반환하므로 DB 일시 장애 시 다음 poke에서 자동 재시도됨

## Reference Code

### Strategy_ToOrderVoc_02_Transform_Dags.py (현재 sensors 블록)

```python
from datetime import timedelta  # line 10 — 이미 존재
from airflow.sensors.external_task import ExternalTaskSensor  # line 15

# 현재 wait_for_crawling (lines 116-125):
wait_for_crawling = ExternalTaskSensor(
    task_id='wait_for_crawling',
    external_dag_id='Strategy_ToOrderVoc_01_Crawl_Dags',
    external_task_id='move_voc_files',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='poke',
    timeout=3600,
    poke_interval=60,
)
```

## Test Cases

1. **DAG 파싱 오류 없음**  
   `python -c "from dags.strategy.Strategy_ToOrderVoc_02_Transform_Dags import dag"`  
   → 기대: ImportError / SyntaxError 없음

2. **올바른 logical_date 탐색 확인 (UI)**  
   Airflow UI → `Strategy_ToOrderVoc_02_Transform_Dags` → `wait_for_crawling` Clear 후 재실행  
   → 로그에서 확인:
   - **이전(버그)**: `Poking for ... on 2026-06-23T00:30:00+00:00`
   - **수정 후**: `Poking for ... on 2026-06-22T22:30:00+00:00`

3. **센서 즉시 통과 확인**  
   01 DAG가 어제(2026-06-23) 성공했다면, 센서가 첫 poke에서 success 상태를 찾아 즉시 통과해야 함.

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

- `timedelta` import는 추가하지 말 것 (이미 있음)
- `mode='reschedule'` 사용 시 task에 `pool`이나 `pool_slots` 설정 필요 없음 (기본값 사용)
- timeout을 7200 이상으로 설정 (01 크롤링이 최대 2시간 소요될 수 있음)
- `execution_delta` 방향: 양수 `timedelta(hours=2)` → 02 logical_date에서 2시간 과거를 탐색

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
