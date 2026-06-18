# validate_toorder 경로 및 알림 수정

## Task
`validate_toorder` 태스크의 ToOrder 데이터 소스를 날짜별 CSV에서 단일 parquet 파일(`toorder_store_platform_daily.parquet`)로 교체한다.
컬럼명이 한글(`플랫폼명`, `매장명`, `매출액`) → 영문(`platform`, `store`, `price`)으로 바뀌었고, 날짜 필터도 파일명 방식에서 `date` 컬럼 필터로 변경된다.
추가로 `validate_toorder` 알림에서 이메일(`_send_alert`) 호출을 제거하고 텔레그램만 유지한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ANALYTICS_DB` (하드코딩 금지)

## Files to Modify
- `modules/transform/pipelines/db/DB_Beamin_Macro_validate.py` — `_toorder_baemin_by_store` 함수 교체
- `dags/db/DB_Beamin_Macro_Dags.py` — `validate_toorder` 함수에서 `_send_alert` 제거

## Implementation Steps

### Step 1 — `DB_Beamin_Macro_validate.py`: 상수 교체

파일 상단(line 36)의 상수를 교체한다:

```python
# 제거
TOORDER_DAILY_DIR = ANALYTICS_DB / "toorder_daily_sales"

# 추가
TOORDER_PARQUET_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
```

### Step 2 — `DB_Beamin_Macro_validate.py`: `_toorder_baemin_by_store` 함수 교체

현재 함수(line ~136):
```python
def _toorder_baemin_by_store(target_date: str) -> dict:
    date_str = target_date.replace("-", "")
    csv_path = TOORDER_DAILY_DIR / f"toorder_daily_sales_{date_str}.csv"
    if not csv_path.exists():
        logger.warning("ToOrder CSV 없음: %s", csv_path)
        return {}
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        mask = df["플랫폼명"].isin(["배달의민족", "배민1", "배민 포장"])
        by_store = (
            df.loc[mask]
            .groupby("매장명")["매출액"]
            .sum()
            .apply(int)
            .to_dict()
        )
        logger.info("ToOrder 배민 매장 %d개 (target_date=%s)", len(by_store), target_date)
        return by_store
    except Exception as exc:
        logger.error("ToOrder CSV 읽기 실패: %s / %s", csv_path, exc)
        return {}
```

변경 후:
```python
def _toorder_baemin_by_store(target_date: str) -> dict:
    if not TOORDER_PARQUET_PATH.exists():
        logger.warning("ToOrder parquet 없음: %s", TOORDER_PARQUET_PATH)
        return {}
    try:
        df = pd.read_parquet(TOORDER_PARQUET_PATH)
        df = df[df["date"].astype(str) == target_date]
        mask = df["platform"].isin(["배달의민족", "배민1", "배민 포장"])
        by_store = (
            df.loc[mask]
            .groupby("store")["price"]
            .sum()
            .apply(int)
            .to_dict()
        )
        logger.info("ToOrder 배민 매장 %d개 (target_date=%s)", len(by_store), target_date)
        return by_store
    except Exception as exc:
        logger.error("ToOrder parquet 읽기 실패: %s / %s", TOORDER_PARQUET_PATH, exc)
        return {}
```

`sum_toorder_baemin_from_csv` 함수(line ~164)는 `_toorder_baemin_by_store`를 내부 호출하므로 **변경 불필요**.

### Step 3 — `dags/db/DB_Beamin_Macro_Dags.py`: 활성 `validate_toorder` 이메일 제거

파일에 `validate_toorder` 함수가 두 번 정의되어 있다. **Python은 마지막 정의를 사용**하므로 line ~1100 이후의 두 번째 정의가 활성 버전이다.

활성 버전(line ~1147-1153) 현재:
```python
    if compared > 0 and (not matched or gap_stores or missing_brand_stores):
        send_telegram(summary)
    if mismatched or missing_brand_stores:
        _send_alert(
            subject=f"[검증 불일치 {target_date}] mismatch={len(mismatched)} missing_brand={len(sorted(set(missing_brand_stores)))}",
            body=summary,
        )
    return summary
```

변경 후:
```python
    if compared > 0 and (not matched or gap_stores or missing_brand_stores):
        send_telegram(summary)
    return summary
```

### Step 4 — `dags/db/DB_Beamin_Macro_Dags.py`: dead-code 첫 번째 `validate_toorder` 이메일 교체

line ~452의 첫 번째(dead-code) 정의도 일관성을 위해 수정한다.

현재(line ~500-503):
```python
    if compared > 0 and (not matched or gap_stores or missing_brand_stores):
        _send_alert(subject=f"[배민↔토더 불일치] {target_date}", body=summary)

    _save_validate_log(target_date, "ToOrder 교차검증", summary)
```

변경 후:
```python
    if compared > 0 and (not matched or gap_stores or missing_brand_stores):
        send_telegram(summary)

    _save_validate_log(target_date, "ToOrder 교차검증", summary)
```

## Reference Code

### `DB_Beamin_Macro_validate.py` (현재 imports + 상수)
```python
import logging
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_04_orders import collect_orders_for_account, _COLUMNS
from modules.transform.utility.paths import ANALYTICS_DB, BAEMIN_ORDERS_DB
from modules.transform.pipelines.db.beamin_store_io import (
    find_tables, read_file, read_table, write_table,
)
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")

TOORDER_DAILY_DIR = ANALYTICS_DB / "toorder_daily_sales"  # ← 이 줄을 교체
```

### `toorder_store_platform_daily.parquet` 컬럼 스키마
파이프라인 `DB_Toorder_store_platform_daily.py`에 정의:
```python
_OUTPUT_COLUMNS = ["date", "store", "platform", "price", "receipts_num"]
# date: "YYYY-MM-DD"  store: 매장명  platform: 플랫폼명  price: 매출액  receipts_num: 주문수
```

배민 플랫폼 필터값: `["배달의민족", "배민1", "배민 포장"]`

## Test Cases
1. **parquet 읽기 확인**
   ```
   python -c "
   from modules.transform.pipelines.db.DB_Beamin_Macro_validate import _toorder_baemin_by_store
   import pendulum
   yesterday = pendulum.yesterday('Asia/Seoul').format('YYYY-MM-DD')
   result = _toorder_baemin_by_store(yesterday)
   print(type(result), len(result), 'stores')
   "
   ```
   → 기대: `<class 'dict'> N stores` (N≥0, 오류 없음)

2. **import 확인**
   ```
   python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print('OK')"
   ```
   → 기대: `OK` (ImportError 없음)

3. **`_send_alert` 미호출 확인**
   ```
   python -c "
   import ast, sys
   with open('dags/db/DB_Beamin_Macro_Dags.py') as f:
       src = f.read()
   # validate_toorder 두 번째 정의 이후에 _send_alert 가 있으면 FAIL
   idx = src.rfind('def validate_toorder(')
   snippet = src[idx:]
   assert '_send_alert' not in snippet, 'FAIL: _send_alert still in validate_toorder'
   print('PASS')
   "
   ```
   → 기대: `PASS`

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints
- `TOORDER_DAILY_DIR` 상수를 참조하는 다른 함수가 있는지 파일 내 grep 후 없으면 제거, 있으면 유지하고 `TOORDER_PARQUET_PATH`만 추가
- `send_telegram` import는 이미 파일 상단에 있으므로 추가 불필요
- dead-code 첫 번째 `validate_toorder`(line ~452)에는 `send_telegram` import가 범위 내에 없을 수 있으니 파일 상단 import 확인

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
