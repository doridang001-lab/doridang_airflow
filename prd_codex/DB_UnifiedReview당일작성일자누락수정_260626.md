# DB_UnifiedReview 당일 작성일자 누락 수정

## Task
`DB_UnifiedReview`(07:49)가 생산자 `Sales_ToOrder_Review_Dags`(09:30, 파일 ~09:32 생성)보다 먼저 돌아서, 당일 작성일자(예: 06-25)가 마트에 빠진다. 데이터·로직은 정상이고 순수 스케줄 순서 문제다(항상 D-1 지연). 소비자 스케줄을 수집 완료 이후로 옮겨 당일 작성일자까지 같은 날 반영한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/transform/utility/schedule.py:56` — `DB_UNIFIED_REVIEW_TIME` 변경
- 수정(주석만): `dags/db/DB_UnifiedReview_Dags.py:66-70` — `_latest_toorder_review_execution_date` 주석을 새 스케줄 기준으로 갱신

## Implementation Steps

1. **스케줄 상수 변경** — `modules/transform/utility/schedule.py:56`
   ```python
   # 변경 전
   DB_UNIFIED_REVIEW_TIME         = "49 7 * * *"  # 매일 07:49 실행 (ToOrderVOC 완료 기준으로 직전 실행 추적)
   # 변경 후
   DB_UNIFIED_REVIEW_TIME         = "0 10 * * *"  # 매일 10:00 실행 (ToOrderVOC 09:30 수집 완료 후)
   ```
   근거: `Sales_ToOrder_Review_Dags`(09:30)가 ~09:32에 당일 `toorder_voc_YYYYMMDD.parquet` 생성 → `ExternalTaskSensor`(mode=reschedule, timeout 6h)가 당일 성공을 잡고 통과 → `lookback days=2`가 `[전날, 당일]`을 잡아 당일 작성일자 포함.

2. **DAG 주석 정합성** — `dags/db/DB_UnifiedReview_Dags.py:66-70`
   `_latest_toorder_review_execution_date`는 "최신 성공"을 반환하므로 기능 변경 불필요(당일 건을 잡음). 단 66-70행 주석의 "07:50 이전 ... 직전 1일 이내" 문구를 새 스케줄(10:00, 당일 수집 후 실행) 기준으로 갱신만 한다. 로직 코드는 건드리지 않는다.

## Reference Code

### modules/transform/utility/schedule.py (대상 라인 주변)
```python
SMD_TOORDER_SALES_REPORT_TIME = "5 6 * * *"  # 매일 6:05 실행
DB_TOORDER_STORE_PLATFORM_TIME = "10 7 * * *"  # 매일 07:10 실행 (SMD_TOORDER_SALES_REPORT 이후)
DB_UNIFIED_REVIEW_TIME         = "49 7 * * *"  # 매일 07:49 실행 (ToOrderVOC 완료 기준으로 직전 실행 추적)
# 생산자: SMP_TOORDER_VOC_TIME = "30 9 * * *"  (Sales_ToOrder_Review_Dags)
```

### dags/db/DB_UnifiedReview_Dags.py (센서 핵심)
```python
LOOKBACK_DAYS = 2

def _latest_toorder_review_execution_date(dt, **context):
    """Sales_ToOrder_Review_Dags.t4_validate의 최신 성공 execution_date를 최신순으로 조회한다."""
    # ... 최신 success를 [dt-1day, now] 범위에서 반환 (66-70행 주석이 07:50 전제) ...

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
1. [상수 변경] `python -c "from modules.transform.utility import schedule as s; print(s.DB_UNIFIED_REVIEW_TIME)"` → 기대: `0 10 * * *`
2. [로직 재현/누락 채움] `python -c "from modules.transform.pipelines.db import DB_UnifiedReview as R; df=R._load_and_aggregate(R._collect_files('lookback',2),'lookback'); print(df['작성일자'].astype(str).value_counts().sort_index().tail(5))"` → 기대: 당일 직전 작성일자까지 출력(최신 toorder_voc 파일 기준)
3. [DAG import] `python -c "from dags.db.DB_UnifiedReview_Dags import dag; print(dag.schedule_interval)"` → 기대: ImportError 없음, `0 10 * * *`

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_latest_toorder_review_execution_date`의 **로직 코드는 변경 금지**(주석만 갱신). 기능은 이미 당일 최신 성공을 잡음.
- 다른 스케줄 상수/DAG 영향 없음 — `DB_UNIFIED_REVIEW_TIME`은 `DB_UnifiedReview_Dags`만 사용.
- lookback days 확대는 무의미(07:49 시점엔 당일 파일 미생성). days=2 유지.
- 즉시 백필이 필요하면(과거 누락일 채우기): `DB_UnifiedReview` DAG를 `{"sale_date": "YYYY-MM-DD"}` conf로 trigger (mode=date 경로 지원).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
