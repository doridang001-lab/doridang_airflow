# DB_UnifiedSales 오전 9시 전 적재 실패 수정

## Task

DB_UnifiedSales DAG가 매일 08:40에 시작하지만 9시 전에 완료되지 않아 okpos 미달 등 부실 적재가 반복되고 있다.
원인은 2가지 버그: ① OKPOS 상류 DAG 대기 시 execution_delta 오설정으로 71분 폴링 발생, ② LOOKBACK_DAYS=None으로 매일 전체 백필 실행.
`dags/db/DB_UnifiedSales.py` 파일 3줄만 수정하면 해결된다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG 파일 위치: `dags/db/`

## Files to Modify

- `dags/db/DB_UnifiedSales.py` — 3곳 수정 (아래 Implementation Steps 참조)

## Implementation Steps

### 수정 1: OKPOS execution_delta 수정 (줄 97)

**원인**: `wait_for_upstream_pos`가 `logical_date - execution_delta`로 상류 DAG run을 찾는다.
- UnifiedSales logical_date = 08:40 KST = 23:40 UTC
- OKPOS 실행 시간 = 06:00 KST = **21:00 UTC**
- 올바른 delta = 23:40 - 21:00 = **2h 40m**
- 현재 잘못된 값 1h 5m → target = 22:35 UTC → OKPOS run 없음 → 71분 폴링

```python
# 수정 전 (줄 93-98)
UPSTREAM_POS_TASKS = [
    {
        "dag_id": "DB_OKPOS_Sales_Dags",
        "task_id": "write_log",
        "execution_delta": timedelta(hours=1, minutes=5),   # ← 잘못됨
    },

# 수정 후
UPSTREAM_POS_TASKS = [
    {
        "dag_id": "DB_OKPOS_Sales_Dags",
        "task_id": "write_log",
        "execution_delta": timedelta(hours=2, minutes=40),  # ← 08:40-06:00=2h40m
    },
```

나머지 3개 상류 DAG delta는 정확하므로 **건드리지 않는다**:
- UnionPOS (07:55 KST): delta 45m → target 22:55 UTC ✓
- EasyPOS (07:50 KST): delta 50m → target 22:50 UTC ✓
- Posfeed (07:15 KST): delta 1h25m → target 22:15 UTC ✓

---

### 수정 2: LOOKBACK_DAYS 복원 (줄 88)

**원인**: `None`으로 설정 시 매 실행마다 전체 소스 CSV 백필 → build_okpos 단독 10분+, 18개 task 순차 누적으로 9시 초과.

```python
# 수정 전 (줄 87-88)
# LOOKBACK_DAYS: int | None = 7
LOOKBACK_DAYS: int | None = None  # 전체 백필 모드는 None으로 변경

# 수정 후
LOOKBACK_DAYS: int | None = 7
```

> 주석 제거 가능 (또는 유지 무방). 핵심은 값을 `7`로 바꾸는 것.
> `conf={"backfill": true}` 수동 실행 시 전체 백필 여전히 가능하므로 기능 손실 없음.

---

### 수정 3: retry_delay 단축 (줄 77, 선택)

간헐적 DB DNS 오류 발생 시 retry 대기 시간 단축으로 총 지연 감소.

```python
# 수정 전 (줄 77)
"retry_delay": timedelta(minutes=5),

# 수정 후
"retry_delay": timedelta(minutes=3),
```

## Reference Code

### dags/db/DB_UnifiedSales.py (수정 대상 구간)

```python
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),   # ← 수정 3: 3으로
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}

# LOOKBACK_DAYS:
# - int  : 최근 N일 누락분 보충
# - None : 전체 소스 CSV 백필
# LOOKBACK_DAYS: int | None = 7
LOOKBACK_DAYS: int | None = None  # ← 수정 2: 7로

UPSTREAM_POS_TASKS = [
    {
        "dag_id": "DB_OKPOS_Sales_Dags",
        "task_id": "write_log",
        "execution_delta": timedelta(hours=1, minutes=5),  # ← 수정 1: hours=2, minutes=40
    },
    {
        "dag_id": "DB_UnionPOS_Receipt_Dags",
        "task_id": "write_log",
        "execution_delta": timedelta(minutes=45),          # 정확 — 수정 불필요
    },
    {
        "dag_id": "DB_EasyPOS_Sales_Dags",
        "task_id": "save_easypos_product",
        "execution_delta": timedelta(minutes=50),          # 정확 — 수정 불필요
    },
    {
        "dag_id": "DB_Posfeed_Sales_Dags",
        "task_id": "scrape_missing_order_details",
        "execution_delta": timedelta(hours=1, minutes=25), # 정확 — 수정 불필요
    },
]
```

## Test Cases

1. **import 검증**
   ```
   python -c "from dags.db.DB_UnifiedSales import dag; print('ok')"
   ```
   → 기대: `ok` (ImportError 없음)

2. **execution_delta 계산 검증**
   ```python
   from datetime import timedelta
   import pendulum
   # UnifiedSales 08:40 KST logical_date
   unified_ld = pendulum.datetime(2026, 6, 23, 23, 40, tz="UTC")
   delta = timedelta(hours=2, minutes=40)
   target = unified_ld - delta
   print(target)  # → 2026-06-23 21:00:00+00:00 (= OKPOS 06:00 KST ✓)
   ```

3. **LOOKBACK_DAYS 확인**
   ```
   python -c "from dags.db.DB_UnifiedSales import LOOKBACK_DAYS; print(LOOKBACK_DAYS)"
   ```
   → 기대: `7`

4. **다음날 정기 실행 후 로그 확인**
   - `wait_for_upstream_pos` task: 1분 이내 완료 (이전: 71분)
   - `build_okpos` task: 5분 이내 완료 (이전: 10분+)
   - 전체 DAG: 09:00 전 완료

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

- 수정 범위: `dags/db/DB_UnifiedSales.py` 3줄만 — `modules/` 코드 수정 없음
- 나머지 `UPSTREAM_POS_TASKS` 항목 3개(UnionPOS, EasyPOS, Posfeed)의 delta 값 변경 금지
- `conf={"backfill": true}` 수동 실행 경로는 반드시 유지 (LOOKBACK_DAYS=7이어도 동작해야 함)
- 기존 주석 스타일 유지 (줄 84-86 주석 블록 포맷 그대로)

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
