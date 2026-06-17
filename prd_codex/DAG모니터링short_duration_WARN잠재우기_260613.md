# DAG 모니터링 short_duration WARN 잠재우기

## Task
일일 DAG 모니터링(`Strategy_DagMonitoring_01_Alert`)이 데이터 게이팅으로 정상 no-op(success + SKIP 메시지 반환)하는 3개 DAG을 "실행시간이 너무 짧음"(short_duration) WARN으로 오탐한다. 실제 결과 파일(`{MART_DB}/dags_monitoring/dags_monitoring_20260610.csv`, `..._20260611.csv`)로 검증한 결과 short_duration WARN은 이 3개뿐이고 매일 반복된다. 화이트리스트 상수를 추가해 이 3개의 short_duration WARN만 제거하고, 다른 DAG의 비정상 짧은 실행은 계속 감지한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- (수정) `modules/transform/pipelines/strategy/dag_monitoring_pipeline.py` — 단일 파일

## Implementation Steps

1. **모듈 상단 상수 추가** — `RUNNING_STATES = {...}` (라인 27 부근) 바로 아래에 화이트리스트 set 추가:
   ```python
   # short_duration WARN 제외 대상:
   # 데이터 게이팅으로 정상 no-op(success + SKIP 메시지 반환)하는 DAG들.
   # 원천 데이터 미존재/이미 처리 완료 시 의도적으로 빠르게 종료한다.
   SHORT_DURATION_WHITELIST = {
       "DB_UnifiedReview_Dags",
       "Sales_StoreSales_01_Report_Dags",
       "Strategy_FdamCS_02_FlowMacro_Dags",
   }
   ```

2. **short_duration 판정에 화이트리스트 가드 추가** — `apply_failure_rules()` 내부 라인 362의 short_duration 분기를 수정한다. `dag_id`는 같은 루프 상단(라인 286)에서 이미 `dag_id = str(dag_run["dag_id"])`로 확보되어 있으므로 그대로 참조한다.

   변경 전:
   ```python
   elif 0 < duration_sec < 10:
       status = "WARN"
       fail_type = "short_duration"
       detail = f"실행시간이 너무 짧음 ({duration_sec:.1f}초)"
   ```
   변경 후:
   ```python
   elif 0 < duration_sec < 10 and dag_id not in SHORT_DURATION_WHITELIST:
       status = "WARN"
       fail_type = "short_duration"
       detail = f"실행시간이 너무 짧음 ({duration_sec:.1f}초)"
   ```

   화이트리스트에 든 DAG은 이 분기를 건너뛰어 `status="OK"`, `detail="Healthy"`로 유지된다. `dag_failed` / `high_skip_ratio` / `upstream_failed` 등 진짜 실패 판정은 이 분기보다 위에서 처리되므로 영향 없음.

## Reference Code
### modules/transform/pipelines/strategy/dag_monitoring_pipeline.py
```python
"""Daily Airflow DAG monitoring pipeline."""
from __future__ import annotations
import io, json, logging, os
from datetime import UTC
from pathlib import Path
from typing import Any
import pandas as pd
from modules.common.config import ADMIN_EMAIL
from modules.transform.utility.onedrive import save_to_onedrive_csv
from modules.transform.utility.paths import DASHBOARD_DB, MART_DB

logger = logging.getLogger(__name__)
RUNNING_STATES = {"queued", "running", "scheduled", "deferred", "up_for_retry"}

# ----- apply_failure_rules() 내부 판정 로직 (라인 327~365) -----
status = "OK"
fail_type = None
detail = "Healthy"

intentional_skip_run = (
    total_tasks > 0 and skipped_tasks > 0 and failed_tasks == 0
    and upstream_failed_tasks == 0 and dag_run["state"] == "success"
    and (success_tasks + skipped_tasks) == total_tasks
)
if dag_run["state"] == "failed":
    status = "FAIL"; fail_type = "dag_failed"
    detail = f"DAG 실행 실패 (실패 Task {failed_tasks}건)"
if status != "FAIL":
    if total_tasks > 0 and (skipped_tasks / total_tasks) >= 0.5 and not intentional_skip_run:
        status = "FAIL"; fail_type = "high_skip_ratio"; detail = f"스킵 비율 과다 ({skipped_tasks}/{total_tasks})"
    elif upstream_failed_tasks > 0:
        status = "FAIL"; fail_type = "upstream_failed"; detail = f"선행 Task 실패 감지 ({upstream_failed_tasks}건)"
if status == "OK":
    if total_tasks > 0 and skipped_tasks == total_tasks:
        status = "WARN"; fail_type = "all_skipped"; detail = f"전체 Task 스킵 ({total_tasks}건)"
    elif intentional_skip_run:
        detail = f"의도된 스킵 포함 완료 ({skipped_tasks}/{total_tasks})"
    elif 0 < duration_sec < 10:   # ← 여기에 화이트리스트 가드 추가
        status = "WARN"; fail_type = "short_duration"; detail = f"실행시간이 너무 짧음 ({duration_sec:.1f}초)"
```

## Test Cases
1. [import 무결성] `python -c "import modules.transform.pipelines.strategy.dag_monitoring_pipeline as m; print(sorted(m.SHORT_DURATION_WHITELIST))"` → 기대: `['DB_UnifiedReview_Dags', 'Sales_StoreSales_01_Report_Dags', 'Strategy_FdamCS_02_FlowMacro_Dags']` 출력, 에러 없음
2. [DAG import] `python -c "import importlib.util,sys; spec=importlib.util.spec_from_file_location('d','dags/strategy/Strategy_DagMonitoring_01_Alert_Dags.py'); print('ok')"` → 기대: `ok` (구문 오류 없음)
3. [가드 동작 검증] 아래 인라인 스니펫으로 화이트리스트 dag_id가 short_duration을 건너뛰는지 확인:
   ```
   python -c "from modules.transform.pipelines.strategy.dag_monitoring_pipeline import SHORT_DURATION_WHITELIST as W; \
   dag_id='DB_UnifiedReview_Dags'; duration_sec=9.1; \
   hit = (0 < duration_sec < 10 and dag_id not in W); \
   print('SKIP_WARN' if not hit else 'WOULD_WARN')"
   ```
   → 기대: `SKIP_WARN`
4. [비화이트리스트는 여전히 감지] 위 스니펫에서 `dag_id='Some_Other_Dags'`로 바꿔 실행 → 기대: `WOULD_WARN`

## Verification Loop
구현 완료 후 아래 루프를 모든 Test Cases PASS까지 반복한다.
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `collect_dag_runs()`의 기존 `excluded_prefixes`("Strategy_DagMonitoring")는 건드리지 않는다. 역할이 다름: `excluded_prefixes`는 수집 자체 제외(대시보드에서 사라짐), `SHORT_DURATION_WHITELIST`는 수집·표시는 하되 short_duration WARN만 OK 처리(대시보드 가시성 유지).
- short_duration 외 다른 판정 분기(`dag_failed`/`high_skip_ratio`/`upstream_failed`/`all_skipped`)는 절대 수정하지 않는다.
- 3개 dag_id 문자열은 파일명(확장자 제외)과 정확히 일치해야 한다. 오타 시 가드가 동작하지 않음.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
