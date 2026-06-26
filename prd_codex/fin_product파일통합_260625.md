# fin_product 파일 통합 (7→5)

## Task
`data/mart/fin_product/` 폴더의 7개 파일을 5개로 줄인다.
`fin_product_mart.csv`(grp의 파생물)를 제거하고 `launch_date` 컬럼을 `fin_product_grp.csv`에 흡수시키며,
WRITE-ONLY인 `fin_product_map.json`도 제거한다.
unified sales 파이프라인(66개 DAG)은 mart.csv를 읽지 않으므로 영향 없음.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 저장: `_safe_replace(tmp, dst)` 패턴 사용 (OneDrive FUSE 안전 저장)
- 인코딩: `encoding="utf-8-sig"`

## Files to Modify

- `modules/transform/pipelines/db/DB_FinProduct.py`
- `modules/transform/pipelines/db/DB_FinProduct_Map.py`
- `modules/transform/utility/paths.py`

## Implementation Steps

### Step 1 — paths.py: 두 상수 제거

`C:\airflow\modules\transform\utility\paths.py` line 299, 303 제거:
```python
# 제거
"FIN_PRODUCT_MART_CSV_PATH":    lambda: _get("MART_DB") / "fin_product" / "fin_product_mart.csv",
"FIN_PRODUCT_MAP_JSON_PATH":    lambda: _get("MART_DB") / "fin_product" / "fin_product_map.json",
```

### Step 2 — DB_FinProduct.py: build_fin_product_mart() 수정

**import 수정 (line 22-28):**
```python
# 변경 전
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    FIN_PRODUCT_MART_CSV_PATH,   # ← 제거
    MART_DB,
)

# 변경 후
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    MART_DB,
)
```

**build_fin_product_mart() 저장 부분 수정 (line 1075-1095):**

기존 mart DataFrame은 그대로 만든다. 저장만 grp.csv에 launch_date를 write-back하는 방식으로 변경.

```python
# 변경 전 (lines 1075-1095): mart.csv에 저장
mart = (
    df[["source", "상품코드", "상품명", "is_main_candidate", "수동분류", "중복_수동분류"]]
    .drop_duplicates(subset=["source", "상품코드"], keep="first")
    .merge(launch_dates, on=["source", "상품코드"], how="left")
    .sort_values(["source", "상품코드"])
    .reset_index(drop=True)
)

FIN_PRODUCT_MART_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
tmp = FIN_PRODUCT_MART_CSV_PATH.with_suffix(".tmp")
try:
    mart.to_csv(tmp, index=False, encoding="utf-8-sig")
    _safe_replace(tmp, FIN_PRODUCT_MART_CSV_PATH)
finally:
    try:
        tmp.unlink(missing_ok=True)
    except Exception:
        pass

conflict_msg = f", 충돌 {len(conflicts)}건 알림" if not conflicts.empty else ""
logger.info("fin_product_mart.csv 저장: %d행%s", len(mart), conflict_msg)
```

```python
# 변경 후: launch_date를 grp.csv에 write-back
mart = (
    df[["source", "상품코드", "상품명", "is_main_candidate", "수동분류", "중복_수동분류"]]
    .drop_duplicates(subset=["source", "상품코드"], keep="first")
    .merge(launch_dates, on=["source", "상품코드"], how="left")
    .sort_values(["source", "상품코드"])
    .reset_index(drop=True)
)

# launch_date를 grp.csv에 write-back
launch_date_map = mart.set_index(["source", "상품코드"])["launch_date"].to_dict()
updated_master = master.copy()
updated_master["source_"] = updated_master["source"].astype(str).str.strip()
updated_master["상품코드_"] = updated_master["상품코드"].astype(str).str.strip()
updated_master["launch_date"] = updated_master.apply(
    lambda r: launch_date_map.get((r["source_"], r["상품코드_"]), r.get("launch_date", "")),
    axis=1,
)
updated_master = updated_master.drop(columns=["source_", "상품코드_"])

tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
try:
    updated_master.to_csv(tmp, index=False, encoding="utf-8-sig")
    _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
finally:
    try:
        tmp.unlink(missing_ok=True)
    except Exception:
        pass

conflict_msg = f", 충돌 {len(conflicts)}건 알림" if not conflicts.empty else ""
logger.info("fin_product_grp.csv launch_date write-back: %d행%s", len(mart), conflict_msg)
```

### Step 3 — DB_FinProduct.py: build_launch_tracking() 수정

```python
# 변경 전 (lines 1140-1170): mart.csv 읽기
def build_launch_tracking(**context) -> str:
    """신규 메인 메뉴의 출시 후 30일 매출 성과 집계."""
    if not FIN_PRODUCT_MART_CSV_PATH.exists():
        logger.warning("fin_product_mart.csv 없음 - 스킵")
        return "스킵: mart 없음"

    mart = pd.read_csv(FIN_PRODUCT_MART_CSV_PATH, dtype=str).fillna("")
    is_main = (
        mart["is_main_candidate"].astype(str).str.strip().str.upper()
        if "is_main_candidate" in mart.columns
        else pd.Series(["N"] * len(mart), index=mart.index)
    )
    launch_date = (
        mart["launch_date"].astype(str).str.strip()
        if "launch_date" in mart.columns
        else pd.Series([""] * len(mart), index=mart.index)
    )
    mart = mart[(is_main == "Y") & (launch_date != "")]
    ...
    mart = mart.drop_duplicates(subset=["source", "상품코드", "상품명", "수동분류", "launch_date"])
```

```python
# 변경 후: grp.csv 읽기 + is_latest=Y + dedup
def build_launch_tracking(**context) -> str:
    """신규 메인 메뉴의 출시 후 30일 매출 성과 집계."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        logger.warning("fin_product_grp.csv 없음 - 스킵")
        return "스킵: grp 없음"

    grp = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
    if "is_latest" in grp.columns:
        grp = grp[grp["is_latest"].astype(str).str.upper() == "Y"]
    grp = grp.drop_duplicates(subset=["source", "상품코드"], keep="first")

    mart = grp  # 이후 로직에서 mart 변수명 그대로 사용

    is_main = (
        mart["is_main_candidate"].astype(str).str.strip().str.upper()
        if "is_main_candidate" in mart.columns
        else pd.Series(["N"] * len(mart), index=mart.index)
    )
    launch_date = (
        mart["launch_date"].astype(str).str.strip()
        if "launch_date" in mart.columns
        else pd.Series([""] * len(mart), index=mart.index)
    )
    mart = mart[(is_main == "Y") & (launch_date != "")]
    ...
    mart = mart.drop_duplicates(subset=["source", "상품코드", "상품명", "수동분류", "launch_date"])
    # 이후 로직 동일
```

### Step 4 — DB_FinProduct_Map.py: export_json() map.json 저장 제거

```python
# 변경 전 (lines 674-678)
FIN_PRODUCT_MAP_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
FIN_PRODUCT_MAP_JSON_PATH.write_text(
    json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
)
logger.info("JSON 저장: %s (%d 표준명)", FIN_PRODUCT_MAP_JSON_PATH, len(result))

# 변경 후: 해당 4줄 전부 삭제
# (FIN_PRODUCT_MAP_JSON_PATH import도 line 14에서 제거)
```

### Step 5 — OneDrive 파일 정리

코드 수정 완료 후 수동으로 삭제:
- `C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product\fin_product_mart.csv`
- `C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product\fin_product_map.json`

## Reference Code

### DB_FinProduct.py (imports + 저장 패턴)
```python
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    FIN_PRODUCT_MART_CSV_PATH,
    MART_DB,
)
from modules.transform.utility.mailer import send_email

logger = logging.getLogger(__name__)


def _safe_replace(src: Path, dst: Path) -> None:
    """os.replace with fallback for FUSE/OneDrive mounts where rename may fail."""
```

### paths.py (fin_product 상수 현재 상태)
```python
"FIN_PRODUCT_CSV_PATH":         lambda: _get("MART_DB") / "fin_product" / "fin_product_grp.csv",
"FIN_PRODUCT_REVIEW_CSV_PATH":  lambda: _get("MART_DB") / "fin_product" / "fin_product_review.csv",
"FIN_PRODUCT_ALIAS_CSV_PATH":   lambda: _get("MART_DB") / "fin_product" / "fin_product_alias.csv",
"FIN_PRODUCT_MART_CSV_PATH":    lambda: _get("MART_DB") / "fin_product" / "fin_product_mart.csv",   # 제거
"POSFEED_WHITELIST_CSV_PATH":   lambda: _get("MART_DB") / "fin_product" / "fin_product_posfeed_whitelist.csv",
"FIN_PRODUCT_MAP_CSV_PATH":     lambda: _get("MART_DB") / "fin_product" / "fin_product_map.csv",
"FIN_PRODUCT_MAP_REVIEW_CSV_PATH": lambda: _get("MART_DB") / "fin_product" / "fin_product_map_review.csv",
"FIN_PRODUCT_MAP_JSON_PATH":    lambda: _get("MART_DB") / "fin_product" / "fin_product_map.json",   # 제거
```

## Test Cases

1. **paths.py import 오류 없음**
   ```
   python -c "from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH; print('ok')"
   ```
   → 기대: `ok`

2. **FIN_PRODUCT_MART_CSV_PATH 제거 확인**
   ```
   python -c "from modules.transform.utility.paths import FIN_PRODUCT_MART_CSV_PATH"
   ```
   → 기대: `ImportError` 발생

3. **DB_FinProduct.py import 오류 없음**
   ```
   python -c "from modules.transform.pipelines.db.DB_FinProduct import build_fin_product_mart, build_launch_tracking; print('ok')"
   ```
   → 기대: `ok`

4. **DB_FinProduct_Map.py import 오류 없음**
   ```
   python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map; print('ok')"
   ```
   → 기대: `ok`

5. **build_fin_product_mart() 실행 후 grp.csv에 launch_date 컬럼 존재**
   ```
   python -c "
   import pandas as pd
   from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH
   df = pd.read_csv(FIN_PRODUCT_CSV_PATH)
   print('launch_date' in df.columns, df['launch_date'].notna().sum())
   "
   ```
   → 기대: `True [양수]`

6. **fin_product_mart.csv가 더 이상 생성되지 않음**
   ```
   python -c "
   from pathlib import Path
   p = Path(r'C:/Users/민준/OneDrive - 주식회사 도리당/data/mart/fin_product/fin_product_mart.csv')
   print('exists:', p.exists())
   "
   ```
   → 기대: `exists: False`

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~4 (import 검증) 먼저 실행
  2. FAIL 시 해당 파일 import 정리
  3. Test Cases 5~6 (데이터 검증)은 DAG 실행 후 확인
종료 조건: 전체 PASS + fin_product/ 폴더에 5개 파일만 존재
```

## Constraints
- `_safe_replace(tmp, dst)` 패턴 반드시 유지 (OneDrive FUSE 환경)
- build_launch_tracking() 내부 `mart` 변수명은 그대로 유지 (이후 로직 변경 최소화)
- grp.csv write-back 시 기존 컬럼 순서 변경 금지 (다른 모듈이 컬럼 위치 기준으로 읽을 수 있음)
- `중복_수동분류` write-back 로직(기존)과 `launch_date` write-back 로직을 하나의 저장 트랜잭션으로 합칠 것
- OneDrive 파일 삭제(Step 5)는 코드 배포 및 DAG 1회 정상 실행 확인 후 수동으로 진행

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
