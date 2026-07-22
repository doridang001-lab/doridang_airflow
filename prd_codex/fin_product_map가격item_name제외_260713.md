# fin_product_map: 가격이 item_name으로 들어간 행 제외

## Task
`fin_product_map_review_input.csv`에 상품명(item_name) 자리에 가격(순수 숫자)이 들어간 2개 행이 검수 대상으로 계속 방치된다. 이 순수-숫자 item_name 행을 상품 매핑(map) 레이어에서 제외하여 map/review/join/recently 모든 출력에서 사라지게 한다.

문제 행 (`fin_product_map_review_input.csv` 331~332):
```
200008080,16900,송파삼전점,배민수동,도리당,16900,12000,16900,,N,1,수동분류 미입력
200008081,23500,송파삼전점,배민수동,도리당,23500,23500,23500,,N,1,수동분류 미입력
```
item_name·item_key·표준_메뉴명_edit 이 모두 `16900`/`23500` (가격). 근본 출처는 `DB_UnifiedSales_baemin.py:491` (`out["item_name"] = df["주문옵션상세"]`)에서 배민 원천의 주문옵션상세에 옵션명 대신 가격이 유입된 것이나, **이번 작업은 원천 교정이 아니라 map 레이어 제외로 처리한다.**

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_FinProduct_Map.py` (단일 파일)

## Implementation Steps

### 1. `import re` 추가
`DB_FinProduct_Map.py` 상단 import 블록에 `re`가 없으므로 추가한다 (표준 라이브러리 import 그룹, `import os` 근처).

### 2. 순수 숫자(가격) item_name 제거 헬퍼 추가
기존 `find_llm_targets()`의 `item_name.str.fullmatch(r"\d+")` 시맨틱과 일치. 모듈 상단 헬퍼 영역(예: `_empty_map()` 근처)에 추가:

```python
def _drop_price_item_names(df: pd.DataFrame) -> pd.DataFrame:
    """item_name이 순수 숫자(가격)인 행을 제거한다."""
    if df.empty or "item_name" not in df.columns:
        return df
    names = df["item_name"].fillna("").astype(str).str.strip()
    is_price = names.str.fullmatch(r"\d+")
    if bool(is_price.any()):
        logger.info("가격 item_name 행 제외: %d행", int(is_price.sum()))
    return df[~is_price].copy()
```

### 3. 신규 스캔 차단 — `scan_target_items()` 의 `scoped` 마스크
unified 파켓에서 새로 유입되는 것을 막는다. 기존 `scoped` 필터에 `& ~df["item_name"].str.fullmatch(r"\d+")` 추가:

```python
scoped = df[
    df["store"].isin(TARGET_STORE_SET)
    & (df["item_name"] != "")
    & ~df["item_name"].str.fullmatch(r"\d+")
    & (df["item_id"] != "")
    & (df["item_id"] != OKPOS_ADJUSTMENT_ITEM_ID)
]
```

### 4. 기존 지속 행 제거 — `load_map()`
이미 `fin_product_map.csv`에 저장된 2개 행이 재기록 시 빠지도록, `df = df.reindex(columns=MAP_COLUMNS, fill_value="")` 바로 다음 줄에 삽입:

```python
df = df.reindex(columns=MAP_COLUMNS, fill_value="")
df = _drop_price_item_names(df)
```

이 두 소스(`scan_target_items` → `build_initial_map`, `load_map`)가 `migrate_product_map`/`llm_product_map`의 `result`/`map_df`를 구성하므로 이후 `write_map`·`write_review_map`·`write_join_map`·`write_recently_map` 출력 전부에서 해당 행이 사라진다.

`find_llm_targets`의 기존 `\d+` 가드는 그대로 둔다(중복이지만 무해).

## Reference Code
### DB_FinProduct_Map.py (관련 함수 현재 상태)
```python
import json
import logging
import os
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
# ... (paths / allocator import 생략)

logger = logging.getLogger(__name__)

def _empty_map() -> pd.DataFrame:
    return pd.DataFrame(columns=MAP_COLUMNS)

def scan_target_items(*, persist_identity: bool = True) -> pd.DataFrame:
    ...
    for file_path in iter_unified_sales_files():
        ...
        scoped = df[
            df["store"].isin(TARGET_STORE_SET)
            & (df["item_name"] != "")
            & (df["item_id"] != "")
            & (df["item_id"] != OKPOS_ADJUSTMENT_ITEM_ID)
        ]
        ...

def load_map() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_CSV_PATH.exists():
        return _empty_map()
    df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_review_status_aliases(df)
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=MAP_COLUMNS, fill_value="")
    df = _fill_item_identity_columns(df, persist=False)
    ...

# 참고: 기존 가드 (변경 없음)
def find_llm_targets(all_items, map_df):
    ...
    invalid_item_name = item_name.str.fullmatch(r"\d+")
    ...
```

## Test Cases
1. [헬퍼 회귀] dry-run 실행 시 제외 로그 확인:
   `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map; s=migrate_product_map(dry_run=True); print(s['target_rows'], s['review_rows'])"`
   → 기대: 로그에 `가격 item_name 행 제외: 2행` 출력, 에러 없음
2. [import 정상] `python -c "import modules.transform.pipelines.db.DB_FinProduct_Map"` → 기대: ImportError 없음
3. [실제 재생성 후 검증] `migrate_product_map()` 실행 후 `fin_product_map_review_input.csv`에서 `200008080`, `200008081`, item_name `16900`/`23500` 행이 없어야 함
4. [map 원본 검증] `fin_product_map.csv`에서도 item_id `200008080`/`200008081` 이 빠졌는지 확인

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
- 변경은 `DB_FinProduct_Map.py` 단일 파일로 한정한다.
- `DB_UnifiedSales_baemin.py` 등 원천 파이프라인은 이번 작업에서 건드리지 않는다.
- 순수 숫자 판별은 기존 `find_llm_targets`와 동일한 `str.fullmatch(r"\d+")` 사용 (콤마/소수점 확장 금지).
- `find_llm_targets`의 기존 `\d+` 가드는 제거하지 말 것 (중복 무해).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(덮어쓰기)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
