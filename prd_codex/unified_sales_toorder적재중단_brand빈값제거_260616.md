# unified_sales toorder 적재 중단 + brand 빈값 과거 보정

## Task
`DB_UnifiedSales` 수집 결과의 `brand` 컬럼 빈값(null/"")을 제거한다. 빈 brand는 전부 `source=toorder`에서 발생하며(다른 소스는 0건), toorder 상위 소스가 지점별 도리당+나홀로 매출을 합산해 기록해 행 단위 브랜드 구분이 불가능하다. 따라서 **toorder를 unified_sales에 적재하지 않도록** DAG/파이프라인에서 제거하고, **이미 저장된 과거 parquet의 toorder 행도 전체 삭제**한다. 보정 후 brand는 정확히 `{나홀로, 도리당}`만 남는다(검증 완료).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `dags/db/DB_UnifiedSales.py` — toorder 태스크/함수/import/태그 제거
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py` — 진입점 3개 no-op(SKIP)
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_common.py` — 과거 보정 admin 함수 추가
- (실행) 추가한 보정 함수 1회 실행해 과거 toorder 행 제거

## Implementation Steps

### 1. DAG에서 toorder 적재 제거 — `dags/db/DB_UnifiedSales.py`
- 49~51행 import 제거: `backfill_toorder as pipeline_backfill_toorder`, `run_toorder as pipeline_run_toorder`, `run_lookback_toorder as pipeline_lookback_toorder`
- `build_toorder(**context)` 함수 전체 삭제 (226~239행)
- 태스크 정의 `t5a2 = PythonOperator(task_id="build_toorder", ...)` 삭제 (310~313행)
- 의존성 체인(351행)에서 `t5a2` 제거:
  ```python
  t1 >> t3 >> t3a >> t4 >> t5 >> t5a >> t5a3 >> t5b >> t5c >> t6 >> t7 >> t8 >> t9
  ```
- `tags=[...]`(277행)에서 `"toorder"` 문자열 제거

### 2. 파이프라인 진입점 무력화 — `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py`
DAG 외 경로/수동 호출에서도 저장되지 않도록 `_save_unified_daily` 호출 경로를 끊는다. 파일·facade export(`DB_UnifiedSales.py`의 `__all__`)는 import 호환 위해 유지.
- `run_toorder(date_str, overwrite=False)` 본문을 즉시 SKIP 반환으로 교체:
  ```python
  def run_toorder(date_str: str, overwrite: bool = False) -> str:
      logger.info("toorder 채널 비활성화: unified_sales 적재 제외 (%s)", date_str)
      return f"SKIP: toorder 채널 비활성화 (unified_sales 제외) {date_str}"
  ```
- `run_lookback_toorder(days=7)`, `backfill_toorder()`도 동일하게 즉시 SKIP 반환(파일 로드·저장 없이):
  ```python
  def run_lookback_toorder(days: int = 7) -> str:
      return "SKIP: toorder 채널 비활성화 (unified_sales 제외)"

  def backfill_toorder() -> str:
      return "SKIP: toorder 채널 비활성화 (unified_sales 제외)"
  ```

### 3. 과거 보정 admin 함수 추가 — `modules/transform/pipelines/db/DB_UnifiedSales_common.py`
기존 `resave_existing_unified_sales` / `repartition_unified_sales_by_sale_date` 패턴을 그대로 따른 1회용 함수 추가:
```python
def purge_source_from_unified_sales(source: str = "toorder") -> str:
    """모든 unified_sales parquet에서 지정 source 행을 제거하고 파일별로 재저장 (멱등)."""
    src = str(source).strip().lower()
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    total_removed = 0
    changed_files = 0
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if "source" not in df.columns:
            continue
        mask = df["source"].fillna("").astype(str).str.strip().str.lower() == src
        removed = int(mask.sum())
        if removed == 0:
            continue
        df = df[~mask].reset_index(drop=True)
        df.to_parquet(path, index=False, engine="pyarrow")
        total_removed += removed
        changed_files += 1
        logger.info("%s 행 제거: %s (%d행)", src, path.name, removed)

    result = f"OK: purge source={src} | files={changed_files} removed={total_removed}"
    logger.info(result)
    return result
```

### 4. 과거 보정 1회 실행
구현 후 아래 명령으로 과거 toorder 행(약 2,650행) 제거.

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_common.py (기존 패턴/상수)
```python
import logging
import hashlib
import re
from datetime import datetime
import pandas as pd
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, MART_DB, ONEDRIVE_DB, POSFEED_WHITELIST_CSV_PATH

logger = logging.getLogger(__name__)
UNIFIED_ROOT = MART_DB / "unified_sales_grp"

def resave_existing_unified_sales() -> str:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
    ...
    for path in files:
        df = pd.read_parquet(path)
        if "source" in df.columns:
            df = df.copy()
            df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
        parts.append(df)
    ...  # to_parquet(path, index=False, engine="pyarrow") 패턴 사용
```

### modules/transform/pipelines/db/DB_UnifiedSales_toorder.py (현재 진입점)
```python
def run_toorder(date_str: str, overwrite: bool = False) -> str:
    df_all = _load_parquet()
    if df_all.empty:
        raise FileNotFoundError(f"toorder parquet not found: {TOORDER_PARQUET}")
    df = _transform_df(df_all, date_str)
    if df.empty:
        return f"SKIP: toorder {date_str} no matching rows"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    return f"toorder {date_str}: {saved} rows"

def run_lookback_toorder(days: int = 7) -> str: ...
def backfill_toorder() -> str: ...
```

### dags/db/DB_UnifiedSales.py (현재 toorder 태스크/체인)
```python
from modules.transform.pipelines.db.DB_UnifiedSales import (
    backfill_toorder as pipeline_backfill_toorder,
    run_toorder as pipeline_run_toorder,
    run_lookback_toorder as pipeline_lookback_toorder,
    ...
)

def build_toorder(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if conf.get("backfill"):
        return pipeline_backfill_toorder()
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_toorder(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_toorder()
    return pipeline_lookback_toorder(days=LOOKBACK_DAYS)

    t5a2 = PythonOperator(task_id="build_toorder", python_callable=build_toorder)
    # 체인: t1 >> t3 >> t3a >> t4 >> t5 >> t5a >> t5a3 >> t5a2 >> t5b >> t5c >> t6 >> t7 >> t8 >> t9
    tags=["db", "toorder", "okpos", "unionpos", "easypos", "posfeed", "unified_sales"]
```

## Test Cases
1. [과거 보정 실행 + 결과 검증]
   ```bash
   PYTHONIOENCODING=utf-8 python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import purge_source_from_unified_sales, UNIFIED_ROOT; import pandas as pd; print(purge_source_from_unified_sales('toorder')); parts=[pd.read_parquet(f, columns=['source','brand']) for f in UNIFIED_ROOT.glob('unified_sales_*.parquet')]; df=pd.concat(parts, ignore_index=True); df['brand']=df['brand'].fillna('').astype(str).str.strip(); assert (df['brand']=='').sum()==0; assert (df['source'].str.lower()=='toorder').sum()==0; print('OK brands=', sorted(df['brand'].unique()))"
   ```
   → 기대: 마지막 줄 `OK brands= ['나홀로', '도리당']`, AssertionError 없음
2. [toorder 진입점 무력화] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import run_toorder; print(run_toorder('2026-06-01'))"` → 기대: `SKIP: toorder 채널 비활성화 ...`, 저장/예외 없음
3. [facade import 호환] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales import run_toorder, run_lookback_toorder, backfill_toorder; print('ok')"` → 기대: `ok`
4. [DAG import] `python -c "from dags.db.DB_UnifiedSales import dag; print('ok')"` → 기대: ImportError 없음, `ok` (build_toorder 미참조)
5. [멱등성] Test 1 명령 재실행 → 기대: `removed=0`, 빈 brand 여전히 0

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
- `purge_source_from_unified_sales`는 toorder 행만 제거하고 다른 source 행은 절대 건드리지 않는다.
- `to_parquet(..., index=False, engine="pyarrow")` — 기존 저장 방식과 동일하게.
- `DB_UnifiedSales_toorder.py`의 파일/함수는 삭제하지 말고 본문만 SKIP 처리(다운스트림 import 호환).
- facade(`DB_UnifiedSales.py`)의 `__all__` toorder export는 유지.
- 다운스트림(`DB_UnifiedSales_validate.py` 등)이 toorder source를 필수 가정하지 않는지 확인.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
