# DB_UnifiedSales build_okpos SIGTERM 해결 — Allocator 성능 수정

## Task
`DB_UnifiedSales.build_okpos` 태스크가 `AirflowTaskTerminated: Task received SIGTERM`로 실패한다.
근본 원인은 `DB_ItemIdAllocator.allocate_manual_item_ids`가 **O(입력행 × 마스터행)** 로 동작하는 것(마스터 ≈19,557행, okpos 입력 ≈507행).
결과(배정 코드)를 **완전히 동일하게 유지**하면서 복잡도를 O(입력행 + 마스터행)로 낮춰 SIGTERM을 없애고 모든 build 태스크를 가속한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_ItemIdAllocator.py` (단일 파일)
- **생성**: `tests/test_item_id_allocator.py` (골든 동등성 테스트)

## Implementation Steps

### 1. Fix 1 — 스칼라 파싱에서 1-원소 Series 생성 제거
`_item_seq_token`(L172), `_code_in_source_range`(L229), `_numeric_value`(L242)의
`pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]` 를 순수 스칼라 파싱으로 교체.
모듈 상단에 헬퍼 추가:
```python
def _coerce_float(text: str) -> float:
    try:
        return float(text)
    except (TypeError, ValueError):
        return float("nan")
```
- 예) `_item_seq_token`:
```python
def _item_seq_token(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return ""
    numeric = _coerce_float(text)
    if pd.isna(numeric):
        return ""
    seq = int(numeric)
    if 0 <= seq < MAX_ITEM_BLOCK_SIZE:
        return str(seq)
    return ""
```
- `_code_in_source_range`, `_numeric_value`도 동일하게 `pd.to_numeric(pd.Series([text]))...` → `_coerce_float(text)` 로 교체.
- 공백은 이미 `.strip()` 처리됨 → 정상 숫자 문자열에 대해 `float()` 결과 == `pd.to_numeric` 스칼라 결과라 동작 동등.

### 2. Fix 2 — `.map()` 호출부 벡터화
마스터 전체에 도는 `.map(_item_seq_token)` (`_manual_master_key_series` L211, `validate_fin_product_codes` L513/L535)를 벡터화 함수로 교체:
```python
def _item_seq_token_series(values: pd.Series) -> pd.Series:
    text = values.fillna("").astype(str).str.strip()
    numeric = pd.to_numeric(text, errors="coerce")
    valid = numeric.notna() & (numeric >= 0) & (numeric < MAX_ITEM_BLOCK_SIZE)
    result = pd.Series("", index=values.index, dtype="object")
    result[valid] = numeric[valid].astype("int64").astype(str)
    return result
```
- `_manual_master_key_series`: `item_seq = _clean_series(master, "item_seq").map(_item_seq_token)` → `item_seq = _item_seq_token_series(_clean_series(master, "item_seq"))`
- `validate_fin_product_codes` L513/L535의 `.map(_item_seq_token)` 도 동일 교체.
- 스칼라 `_item_seq_token`은 `_raw_item_token`(입력행 507회, Fix 1 후 저렴)용으로 **유지**.

### 3. Fix 3 — 마스터 스캔을 루프 밖으로 hoist + 증분 상태 유지 (핵심)
`allocate_manual_item_ids` per-row 루프(L437~465)에서 마스터 재스캔·concat 제거.
현재 매 행마다 호출되는 `_next_store_seq`(L327), `_pick_item_seq`/`_next_item_seq`(L341,354),
`_consistent_allocator_mask`(L265) + L463 `pd.concat` 가 O(행×마스터)의 원인.

루프 **진입 전** `work_master`에서 1회만 계산해 dict 상태 구성:
- `store_seq_by_key: dict[(source,brand,store) -> int]` — 기존 store_seq(마지막 값). `_next_store_seq` 대체
- `max_store_seq_by_source: dict[source -> int]` — 신규 store_seq 배정용(없으면 0, 있으면 max+1)
- `used_item_seq: dict[(source, store_seq) -> set[int]]` — `_consistent_allocator_mask` 통과 행만 집계. `_pick_item_seq`/`_next_item_seq` 대체

루프 내부는 dict 조회로 배정하고, 배정 직후 dict 갱신(신규 store_seq 등록 → `max_store_seq_by_source` 증가, item_seq를 used set에 add).
`work_master`는 루프 중 mutate 금지. `new_rows` 리스트만 모아 **루프 종료 후 1회** `pd.concat` → `validate_fin_product_codes` / `_write_master`(L467~469)에 전달.

**동등성 규칙(기존 함수 semantics 그대로 재현):**
- store_seq: (source,brand,store) 기존 있으면 재사용, 없으면 `max_store_seq_by_source.get(source, -1)+1` (첫 배정 0)
- item_seq: `preferred`가 digit이고 [0,block) 이며 미사용이면 preferred, 아니면 used 최대+1. `next_seq >= block_size`면 기존과 동일하게 `ValueError`
- 같은 배치 내 새로 만든 store/seq를 뒤 행이 재사용하도록 dict 즉시 갱신
- `block_size = item_block_size(source)` (okpos=10000, 그 외 1000)

## Reference Code
### modules/transform/pipelines/db/DB_ItemIdAllocator.py (imports + 핵심 상수/시그니처)
```python
from __future__ import annotations
import logging, os, re, time
from datetime import datetime
from pathlib import Path
import pandas as pd
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, existing_fin_product_csv_path

logger = logging.getLogger(__name__)
DEFAULT_ITEM_BLOCK_SIZE = 1000
SOURCE_ITEM_BLOCK_SIZE = {"okpos": 10000}
MAX_ITEM_BLOCK_SIZE = max(SOURCE_ITEM_BLOCK_SIZE.values(), default=DEFAULT_ITEM_BLOCK_SIZE)
SOURCE_ITEM_BASE = {"easypos":1_000_000,"unipos":2_000_000,"okpos":10_000_000,
                    "posfeed":100_000_000,"배민수동":200_000_000,"쿠팡수동":300_000_000}
MANUAL_SOURCES = set(SOURCE_ITEM_BASE)
ALLOCATOR_COLUMNS = ["brand","store","item_key","store_seq","item_seq"]

def canonical_source(source: object) -> str: ...
def item_block_size(source: object) -> int:  # SOURCE_ITEM_BLOCK_SIZE.get(canonical_source(source), 1000)
def _clean_series(df, col) -> pd.Series:      # df[col].fillna("").astype(str).str.strip()
def _source_base(source: str) -> int:         # SOURCE_ITEM_BASE[canonical_source(source)]
def _consistent_allocator_mask(master, source) -> pd.Series:  # code == base + store_seq*block + item_seq 인 행만 True
def _existing_manual_assignments(master, source) -> dict[tuple, str]: ...
def _next_store_seq(master, source, brand, store) -> int: ...   # 기존 매칭 마지막값 or max+1(0)
def _next_item_seq(master, source, store_seq) -> int: ...       # consistent used 최대+1, >=block 이면 ValueError
def _pick_item_seq(master, source, store_seq, preferred) -> int: ...
def _make_manual_master_row(columns, row, code, store_seq, item_seq) -> dict[str,str]: ...
def allocate_manual_item_ids(rows: pd.DataFrame, *, persist: bool = True) -> pd.Series: ...
```
### allocate_manual_item_ids per-row 루프 (교체 대상, 현재)
```python
for idx, row in rows.iterrows():
    source = canonical_source(row.get("source", ""))
    brand = str(row.get("brand","")).strip(); store = str(row.get("store","")).strip()
    item_name = str(row.get("item_name","")).strip(); item_token = _raw_item_token(row)
    key = (source, brand, store, item_token)
    existing = assignment_cache[source].get(key)
    if existing: out.at[idx] = existing; continue
    store_seq = _next_store_seq(work_master, source, brand, store)   # ← 매 행 전체 스캔
    preferred_seq = item_token if item_token.isdigit() else ""
    item_seq = _pick_item_seq(work_master, source, store_seq, preferred_seq)  # ← 매 행 3~5회 스캔
    code = str(_source_base(source) + store_seq * item_block_size(source) + item_seq)
    master_row = _make_manual_master_row(columns, row_for_master, code, store_seq, item_seq)
    new_rows.append(master_row)
    work_master = pd.concat([work_master, pd.DataFrame([master_row], columns=columns)], ...)  # ← 매 행 전체 복사
    assignment_cache[source][key] = code
    out.at[idx] = code
```

## Test Cases
1. [골든 동등성] `tests/test_item_id_allocator.py` 신규 작성 후 실행 → 기대: PASS
   `python -m pytest tests/test_item_id_allocator.py -q`
   - 합성 마스터(source/brand/store/item_seq/store_seq/상품코드/상품명/item_key/is_latest 컬럼) + 입력행으로 `allocate_manual_item_ids(persist=False)` 호출
   - 케이스: (a) 신규 store 배정, (b) 기존 store_seq 재사용, (c) preferred item_seq 충돌 시 다음 슬롯, (d) MANUAL_SOURCES 여러 개 혼합, (e) block 초과 시 `ValueError`
   - 리팩터 전 코드로 배정한 결과(하드코딩 기대값)와 반환 Series가 정확히 일치
2. [import 정상] `python -c "from modules.transform.pipelines.db.DB_ItemIdAllocator import allocate_manual_item_ids, _item_seq_token_series, _coerce_float"` → 기대: 에러 없음
3. [벡터 헬퍼 동작] `python -c "import pandas as pd; from modules.transform.pipelines.db.DB_ItemIdAllocator import _item_seq_token_series; print(_item_seq_token_series(pd.Series(['0','5','abc','','99999999','-1'])).tolist())"` → 기대: `['0','5','','','','']`
4. [DAG import 회귀] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음
5. [관련 테스트 회귀] `python -m pytest tests/test_unified_sales_manual_delivery_cleanup.py -q` → 기대: PASS
6. [실데이터 성능/동등성] 실제 마스터 사본 + 507행 샘플로 before/after 배정 결과 diff 0 확인, 실행 1~2초 내

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 Test 1의 배정 코드 diff)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `allocate_manual_item_ids` 반환값(배정 코드)은 **리팩터 전과 100% 동일**해야 한다. 성능만 개선, 로직 결과 변경 금지.
- store_seq/item_seq 배정 semantics(`_next_store_seq`/`_next_item_seq`/`_pick_item_seq`)와 `ValueError` 조건을 정확히 재현.
- 마스터는 OneDrive 대용량 CSV → 루프 내 재스캔·`pd.concat` 절대 금지. 스캔은 루프 밖 1회.
- `_consistent_allocator_mask` 통과 행만 item_seq used 집계에 포함(기존 동작 유지).
- 스칼라 `_item_seq_token` 은 삭제하지 말 것(`_raw_item_token` 이 사용).
- 병렬화/DAG 구조/저장구조 변경은 **범위 밖** — 손대지 말 것.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`from __future__ import annotations` 사용 중)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
