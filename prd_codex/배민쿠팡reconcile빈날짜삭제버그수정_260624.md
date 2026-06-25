# 배민쿠팡reconcile빈날짜삭제버그수정

## Task

`reconcile_baemin_for_test_stores`와 `reconcile_coupang_for_test_stores`에서, 월별 파일은 존재하지만 특정 날짜 행이 없을 때(`df_day.empty`) 기존 unified_sales 행을 삭제하는 버그를 수정한다. 배민 매크로는 주 1회 수집이라 lookback 7일 범위 안 최근 1~2일은 아직 미수집 상태인데 이를 "주문 없음"으로 오판해 기존 데이터를 지워버리는 것이 원인이다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify

- `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py`
- `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py`

## Implementation Steps

### 1. DB_UnifiedSales_baemin.py — 날짜 레벨 빈 데이터 처리 (line 103~114)

**현재 코드 (버그):**
```python
df_day = df_raw[df_raw["sale_date"] == date].copy()
if df_day.empty:
    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
    total_removed += removed
    total_added += added
    logger.info(
        "배민 데이터 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
        store,
        date,
        removed,
        added,
    )
    continue
```

**수정 후 (skip):**
```python
df_day = df_raw[df_raw["sale_date"] == date].copy()
if df_day.empty:
    logger.info("배민 데이터 없음, 기존 배달의민족 행 유지: store=%s date=%s", store, date)
    continue
```

### 2. DB_UnifiedSales_baemin.py — 파일 없음 케이스도 쿠팡처럼 유지로 통일 (line 58~72)

쿠팡은 파일 자체 없을 때 `continue` (보존), 배민은 삭제. 일관성 맞춤.

**현재 코드 (버그):**
```python
if not baemin_files:
    for date in dates:
        if date[:7] != ym:
            continue
        removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
        total_removed += removed
        total_added += added
        logger.info(
            "배민 수집 파일 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
            store,
            date,
            removed,
            added,
        )
    continue
```

**수정 후:**
```python
if not baemin_files:
    logger.warning("배민 수집 파일 없음, 기존 배달의민족 행 유지: store=%s ym=%s", store, ym)
    continue
```

### 3. DB_UnifiedSales_baemin.py — df_raw 비어있는 케이스도 동일하게 (line 75~89)

**현재 코드 (버그):**
```python
if df_raw.empty:
    for date in dates:
        if date[:7] != ym:
            continue
        removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
        total_removed += removed
        total_added += added
        logger.info(
            "배민 수집 데이터 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
            store,
            date,
            removed,
            added,
        )
    continue
```

**수정 후:**
```python
if df_raw.empty:
    logger.warning("배민 수집 데이터 없음, 기존 배달의민족 행 유지: store=%s ym=%s", store, ym)
    continue
```

### 4. DB_UnifiedSales_coupang.py — 날짜 레벨 빈 데이터 처리 (line 88~93)

**현재 코드 (버그):**
```python
df_day = df_raw[df_raw["sale_date"] == date].copy()
if df_day.empty:
    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
    total_removed += removed
    total_added += added
    logger.info("쿠팡 데이터 없음, 기존 쿠팡이츠 행 제거: store=%s date=%s | 제거=%d", store, date, removed)
    continue
```

**수정 후:**
```python
df_day = df_raw[df_raw["sale_date"] == date].copy()
if df_day.empty:
    logger.info("쿠팡 데이터 없음, 기존 쿠팡이츠 행 유지: store=%s date=%s", store, date)
    continue
```

## Reference Code

### DB_UnifiedSales_baemin.py (현재 전체 함수 구조)

```python
import hashlib
import logging
import re
from datetime import timedelta
from glob import glob

import pandas as pd
import pendulum

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    _apply_posfeed_blacklist,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _to_int_series,
    _unified_daily_path,
)

logger = logging.getLogger(__name__)

BAEMIN_SOURCE = "배민수동"
BAEMIN_PLATFORM = "배달의민족"


def reconcile_baemin_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    dates = _resolve_baemin_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "배민수동 교정 스킵 | 대상 날짜 없음"
    ym_list = sorted({d[:7] for d in dates})

    total_added = 0
    total_removed = 0
    store_map = _load_store_map()

    for store in stores:
        for ym in ym_list:
            baemin_files = _find_baemin_files(store, ym)
            if not baemin_files:
                # ← step 2 수정 위치
                ...
                continue

            df_raw = _read_baemin_files(baemin_files)
            if df_raw.empty:
                # ← step 3 수정 위치
                ...
                continue

            _assert_required_columns(df_raw, store, ym)
            brand = _parse_brand_from_path(baemin_files[0])
            parsed = df_raw["주문시각"].map(_parse_baemin_datetime)
            df_raw = df_raw.copy()
            df_raw["sale_date"] = parsed.map(lambda v: v[0])
            df_raw["order_time"] = parsed.map(lambda v: v[1])

            for date in dates:
                if date[:7] != ym:
                    continue
                df_day = df_raw[df_raw["sale_date"] == date].copy()
                if df_day.empty:
                    # ← step 1 수정 위치
                    continue
                ...
```

### DB_UnifiedSales_coupang.py (현재 전체 함수 구조)

```python
from __future__ import annotations
import hashlib
import logging
import re
from datetime import timedelta
from glob import glob

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, UNIFIED_ROOT, _load_store_map, _lookup_store_meta,
    _make_unified_pk, _to_int_series, _unified_daily_path,
)
from modules.transform.utility.paths import ANALYTICS_DB

logger = logging.getLogger(__name__)

def reconcile_coupang_for_test_stores(stores, sale_date=None, lookback_days=7):
    ...
    for store in stores:
        for ym in ym_list:
            coupang_files = _find_coupang_files(store, ym)
            if not coupang_files:
                logger.warning("쿠팡 수집 파일 없음, 기존 쿠팡이츠 행 유지: store=%s ym=%s", store, ym)
                continue  # ← 쿠팡은 이미 안전 (수정 불필요)

            df_raw = _deduplicate_raw_orders(_read_coupang_files(coupang_files))
            if df_raw.empty:
                logger.warning("쿠팡 수집 데이터 없음, 기존 쿠팡이츠 행 유지: store=%s ym=%s", store, ym)
                continue  # ← 쿠팡은 이미 안전 (수정 불필요)

            for date in dates:
                ...
                df_day = df_raw[df_raw["sale_date"] == date].copy()
                if df_day.empty:
                    # ← step 4 수정 위치 (현재는 여기서 삭제함)
                    continue
```

## Test Cases

1. **배민 baemin.py import 확인**
   ```
   python -c "from modules.transform.pipelines.db.DB_UnifiedSales_baemin import reconcile_baemin_for_test_stores"
   ```
   → 기대: ImportError 없음

2. **쿠팡 coupang.py import 확인**
   ```
   python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import reconcile_coupang_for_test_stores"
   ```
   → 기대: ImportError 없음

3. **Airflow DAG import 확인**
   ```
   python -c "from dags.db.DB_UnifiedSales import dag"
   ```
   → 기대: ImportError 없음

4. **수동 DAG 실행 후 로그 확인**
   - reconcile_baemin 태스크 로그에 "기존 배달의민족 행 유지" 메시지 출력되어야 함
   - "제거=N 추가=0" 패턴 사라져야 함 (빈 날짜에서)

5. **데이터 보존 확인**
   - 수정 전: 배민/쿠팡 행 수 = 감소
   - 수정 후: 배민/쿠팡 행 수 = 유지 또는 증가

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~3 (import 확인) 먼저 실행
  2. DB_UnifiedSales DAG 수동 실행 (Airflow UI 또는 airflow tasks run)
  3. reconcile_baemin / reconcile_coupang 로그에서 "제거=N 추가=0" 패턴 검색
     → FAIL: 아직 삭제 로직 남아있음
     → PASS: "행 유지" 메시지만 출력
  4. unified_sales parquet에서 TEST_STORES 배민/쿠팡 행 수 비교
종료 조건: 전체 PASS
```

## Constraints

- `_upsert_daily` 함수 자체는 수정하지 않는다 (공통 함수, 다른 곳에서도 사용)
- `enforce_baemin_manual_only` / `enforce_coupang_manual_only` 함수는 이번 수정 범위 외
- 수정 후 docstring에서 "직수집 파일 없는 날짜: 기존 배달의민족 행 제거" 문구도 "유지"로 업데이트

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
