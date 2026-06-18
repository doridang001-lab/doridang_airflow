# DB_UnifiedSales qty 타입 오류 수정

## Task

`build_okpos` Task가 `pyarrow.lib.ArrowInvalid: Could not convert '2' with type str: tried to convert to int64` 오류로 실패하고 있다.
`qty` 컬럼이 파이프라인 단계에서 string으로 변환된 채 공통 저장 함수에 전달되지만, 저장 함수가 `qty`를 int로 캐스팅하지 않고 `to_parquet()`을 호출하기 때문이다.
`DB_UnifiedSales_common.py`의 `_save_unified_daily`에 한 줄 추가하면 okpos/easypos/unionpos/posfeed 전 채널이 수정된다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_common.py` (line ~396)

## Implementation Steps

1. `DB_UnifiedSales_common.py`의 `_save_unified_daily` 함수에서 `unit_price` 캐스팅 직전에 `qty` int 캐스팅 한 줄 추가

   **현재 코드 (line ~396):**
   ```python
   merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
   if "total_price" in merged.columns:
       merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int)
   if "discount_amount" in merged.columns:
       merged["discount_amount"] = pd.to_numeric(merged["discount_amount"], errors="coerce").fillna(0).astype(int)
   merged.to_parquet(daily_path, index=False, engine="pyarrow")
   ```

   **변경 후:**
   ```python
   merged["qty"] = pd.to_numeric(merged["qty"], errors="coerce").fillna(0).astype(int)
   merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
   if "total_price" in merged.columns:
       merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int)
   if "discount_amount" in merged.columns:
       merged["discount_amount"] = pd.to_numeric(merged["discount_amount"], errors="coerce").fillna(0).astype(int)
   merged.to_parquet(daily_path, index=False, engine="pyarrow")
   ```

## Reference Code

### DB_UnifiedSales_common.py (수정 대상 구간, line 396–401)

```python
    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
    if "total_price" in merged.columns:
        merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int)
    if "discount_amount" in merged.columns:
        merged["discount_amount"] = pd.to_numeric(merged["discount_amount"], errors="coerce").fillna(0).astype(int)
    merged.to_parquet(daily_path, index=False, engine="pyarrow")
    logger.info("저장(일별): %s | 전체 %d행 (신규 %d행)", daily_path, len(merged), new_count)
    return new_count
```

## Test Cases

1. **파일 수정 후 import 확인**
   ```
   python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import _save_unified_daily"
   ```
   → 기대: ImportError 없음

2. **qty string → parquet 변환 단위 검증**
   ```python
   python -c "
   import pandas as pd, tempfile, os
   from modules.transform.pipelines.db.DB_UnifiedSales_common import _save_unified_daily
   df = pd.DataFrame({'qty': ['2', '3'], 'unit_price': [1000, 2000], 'source': ['okpos', 'okpos'], 'sale_date': ['2026-06-15', '2026-06-15']})
   with tempfile.TemporaryDirectory() as d:
       import unittest.mock as m
       # _save_unified_daily 내부에서 daily_path를 임시 경로로 패치하거나 직접 parquet 저장 테스트
       print('OK')
   "
   ```
   → 기대: `ArrowInvalid` 없이 `OK` 출력

3. **Airflow Task 수동 재실행**
   ```
   airflow tasks run DB_UnifiedSales build_okpos <run_id> --local
   ```
   → 기대: 로그에 `pyarrow.lib.ArrowInvalid` 없음, `저장(일별)` 또는 `변경 없음, 스킵` 메시지

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

- `DB_UnifiedSales_common.py` 외 다른 파이프라인 파일(okpos, easypos, unionpos)은 건드리지 않는다. 공통 저장 레이어에서만 수정.
- `qty`의 `fillna(0)` — NaN이 섞여도 안전하게 0으로 채운다.
- 기존 `unit_price` 캐스팅 패턴과 동일한 스타일로 작성.
- 파일명 변경(`DB_UnifiedSales.py` → `DB_UnifiedSales_Dag.py`)은 `dag_id = Path(__file__).stem` 패턴으로 dag_id가 바뀌어 Airflow 메타DB 이력 orphan 발생 — **하지 않는다**.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
