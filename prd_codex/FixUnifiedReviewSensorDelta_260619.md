# FixUnifiedReviewSensorDelta

## Task

`DB_UnifiedReview_Dags`의 `wait_for_toorder_review` ExternalTaskSensor가 `execution_delta=timedelta(0)` 때문에 존재하지 않는 execution_date를 영구 탐색 중이다. 두 DAG 스케줄 차이(3분)에 맞게 `execution_delta=timedelta(minutes=3)`으로 수정하여 무한 루프를 해소한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)

## Files to Create / Modify

- **수정:** `C:\airflow\dags\db\DB_UnifiedReview_Dags.py` (line 147)

## Root Cause

```
Sales_ToOrder_Review_Dags:  cron "30 7 * * *"  → execution_date 22:30:00 UTC
DB_UnifiedReview_Dags:      cron "33 7 * * *"  → execution_date 22:33:00 UTC

현재 execution_delta=timedelta(0)
  → 센서가 Sales_ToOrder_Review_Dags의 22:33:00 UTC 런을 찾음
  → 그런 런은 존재하지 않음 → 무한 reschedule
```

## Implementation Steps

1. `C:\airflow\dags\db\DB_UnifiedReview_Dags.py` 의 `wait_for_toorder_review` 정의 블록에서 `execution_delta` 수정

```python
# 변경 전 (line ~147)
execution_delta=timedelta(0),

# 변경 후
execution_delta=timedelta(minutes=3),
```

`22:33:00 - 3분 = 22:30:00 UTC` → Sales_ToOrder_Review_Dags 실행 시각과 일치.

## Test Cases

1. **import 검증** `python -c "from dags.db.DB_UnifiedReview_Dags import dag; print('OK')"` → 기대: `OK` (ImportError 없음)
2. **Airflow UI 검증** — 현재 stuck된 `DB_UnifiedReview_Dags` run의 `wait_for_toorder_review` task를 **Clear** 후 재실행
3. **로그 확인** — 다음 poke 로그에 `Poking for DAG 'Sales_ToOrder_Review_Dags' on 2026-06-17T22:30:00+00:00` 가 찍히면 성공
4. **통과 확인** — `Sales_ToOrder_Review_Dags`의 해당 날짜 run이 success 상태면 센서가 즉시 통과

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL → 원인 분석 후 코드 수정
  3. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints

- `execution_delta` 값은 두 cron 스케줄 차이(33-30=3분)에서 계산된 것. 스케줄 변경 시 함께 갱신 필요
- `timedelta`는 이미 파일 상단에서 `from datetime import timedelta`로 import 되어 있으므로 추가 import 불필요

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 해당 줄만 수정
- 다른 ExternalTaskSensor가 있으면: 이 태스크와 무관하므로 건드리지 않음
- 주석 여부: 추가 주석 없이 코드만 수정
