# Sales_ToOrder_Review - LOOKBACK_DAYS=7 에러 내성 강화

## Task

`t2_collect`에서 일부 날짜 수집 실패 시 `raise AirflowException`으로 인해 `t3_save`가 실행되지 않아 성공한 날짜 데이터까지 손실되는 버그를 수정한다. 부분 실패 시에도 성공한 날짜는 저장하고, 실패 날짜는 다음 실행(LOOKBACK_DAYS=7)에서 자동 재시도되도록 한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/sales/`
- DAG 파일 위치: `dags/sales/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- **수정**: `modules/transform/pipelines/sales/Sales_ToOrder_Review_collect.py`
  - `t2_collect` 함수 내 2곳 변경

## Implementation Steps

### 1. `t2_collect` — Airflow 재시도 시 중복 수집 방지

`session_temp_dir` 생성 직후, account 루프 시작 전에 아래 블록을 삽입한다.

**삽입 위치**: `session_temp_dir.mkdir(parents=True, exist_ok=True)` 다음 줄 (현재 line 755 이후)

```python
# Airflow 재시도 시 이미 수집된 temp parquet이 있으면 skip
already_collected: dict[str, str] = {}
for d in list(dates_to_collect):
    tmp_path = session_temp_dir / f"tmp_{d.replace('-', '')}.parquet"
    if tmp_path.exists() and tmp_path.stat().st_size > 0:
        logger.info("[TOORDER_VOC] %s already collected in prev attempt, skip re-download", d)
        already_collected[d] = str(tmp_path)

dates_to_collect_now = [d for d in dates_to_collect if d not in already_collected]
temp_parquet_map.update(already_collected)
```

그리고 아래 `for account in account_records:` 루프 안의

```python
remaining = list(dates_to_collect)
```
를

```python
remaining = list(dates_to_collect_now)
```

로 변경한다.

### 2. `t2_collect` — 부분 실패 시 raise 제거, warning으로 교체

**현재 코드 (line 809-810):**
```python
if failed_dates:
    raise AirflowException(f"수집 실패 날짜: {failed_dates}")
```

**변경 후:**
```python
if failed_dates:
    logger.warning(
        "[TOORDER_VOC] 수집 실패 날짜 %d건: %s → 다음 실행(LOOKBACK_DAYS=7)에서 자동 재시도",
        len(failed_dates),
        failed_dates,
    )
```

return 문은 기존 그대로 유지:
```python
return f"collected={len(temp_parquet_map)}"
```

## Reference Code

### Sales_ToOrder_Review_collect.py (핵심 구조)

```python
# 현재 t2_collect 함수 구조 요약 (line 741~812)
def t2_collect(**context: Any) -> str:
    ti = context["ti"]
    dates_to_collect: list[str] = ti.xcom_pull(task_ids="t1_prepare", key="dates_to_collect") or []
    account_records: list[dict] = ti.xcom_pull(task_ids="t1_prepare", key="account_records") or []

    if not dates_to_collect:
        ti.xcom_push(key="temp_parquet_map", value={})
        ti.xcom_push(key="failed_dates", value=[])
        return "수집 대상 없음"

    run_id = (context.get("run_id") or "manual").replace(":", "_").replace("+", "_")
    session_temp_dir = TEMP_DIR / "toorder_voc_downloads" / run_id
    session_temp_dir.mkdir(parents=True, exist_ok=True)

    # ↑ 여기 아래에 Step 1의 already_collected 블록 삽입

    temp_parquet_map: dict[str, str] = {}
    failed_dates: list[str] = []
    failure_details: list[dict] = []

    for account in account_records:
        account_id = str(account["id"])
        password = str(account["pw"])

        remaining = list(dates_to_collect)   # ← dates_to_collect_now 로 변경
        for attempt in range(1, MAX_DATE_ATTEMPTS + 1):
            ...  # 기존 재시도 로직 유지

    ti.xcom_push(key="temp_parquet_map", value=temp_parquet_map)
    ti.xcom_push(key="failed_dates", value=failed_dates)
    ti.xcom_push(key="failure_details", value=failure_details)

    # ↓ Step 2: raise → logger.warning 으로 교체
    if failed_dates:
        raise AirflowException(f"수집 실패 날짜: {failed_dates}")  # 이 줄 제거

    return f"collected={len(temp_parquet_map)}"
```

## Test Cases

1. **import 검증**
   ```
   python -c "from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import t2_collect"
   ```
   → 기대: ImportError 없음

2. **부분 실패 시 t3_save 실행 확인**
   Airflow UI → `Sales_ToOrder_Review_Dags` → Trigger DAG w/ config:
   ```json
   {"start_date": "2026-06-12", "end_date": "2026-06-18"}
   ```
   → 기대: t2_collect에서 일부 날짜 실패해도 t3_save, t4_validate 모두 실행됨

3. **저장 파일 확인**
   ```
   python -c "
   from modules.transform.utility.paths import ANALYTICS_DB
   import os
   d = ANALYTICS_DB / 'toorder_review'
   print(sorted(os.listdir(d))[-7:])
   "
   ```
   → 기대: 수집 성공한 날짜의 `toorder_voc_YYYYMMDD.parquet` 파일 존재

4. **Airflow 재시도 중복 방지 확인**
   - 실행 로그에서 `already collected in prev attempt, skip re-download` 메시지 확인
   - 동일 날짜가 두 번 다운로드되지 않음

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1번 실행 (import)
  2. Test Cases 2번 — DAG 수동 실행, t3_save 실행 여부 확인
  3. Test Cases 3번 — parquet 파일 존재 확인
  4. FAIL 항목 → 원인 분석 후 코드 수정
  5. 전체 재실행
종료 조건: 전체 PASS
```

## Constraints

- `t1_prepare`, `t3_save`, `t4_validate` 는 변경하지 않는다
- `dags/sales/Sales_ToOrder_Review_Dags.py` 는 변경하지 않는다 (`LOOKBACK_DAYS = 7` 이미 설정됨)
- `raise AirflowException`을 제거하되, XCom push (`failed_dates`, `failure_details`)는 그대로 유지
- `already_collected` 블록은 `temp_parquet_map: dict[str, str] = {}` 선언 **이전**에 위치해야 한다 (`update` 호출 순서 때문)
- `already_collected` 블록은 `temp_parquet_map` 초기화 이후에 `temp_parquet_map.update(already_collected)` 를 호출한다

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
