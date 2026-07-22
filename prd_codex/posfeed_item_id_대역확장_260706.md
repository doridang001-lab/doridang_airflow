# posfeed item_id 대역 확장 (번호 용량 확장 + 코드 정합)

## Task
posfeed item_id는 매장별로 상품명 기준 숫자코드를 발급하는데(`allocate_manual_item_ids`), 상품명 변형 때문에 코드가 기하급수적으로 늘어(현재 12,008행) **store_seq 89/99로 대역 한계에 임박**했다. posfeed/배민수동/쿠팡수동 코드 대역을 9자리로 확장(okpos 8자리보다 +1자리 → 겹침 0)하고, 기존 fin_product_grp.csv·unified_sales parquet·fin_product_map의 옛 코드를 새 대역으로 재번호(가산 오프셋) 마이그레이션한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 발급식은 `base + store_seq*1000 + item_seq` (block=1000 불변)

## Files to Create / Modify
**수정 (코드 3곳):**
- `modules/transform/pipelines/db/DB_ItemIdAllocator.py` — `SOURCE_CODE_RANGES` / `MANUAL_SOURCE_BASE` 새 대역
- `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` — 하드코딩 `- 100000` 제거(48행 import, 1098행)
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` — `_source_base_for_item_id` 하드코딩 제거(21행 import, 179-187행)

**생성 (마이그레이션 스크립트):**
- `scripts/migrate_posfeed_item_id_band.py` — 일회성, dry-run 우선, 멱등

## Implementation Steps

### 1. `DB_ItemIdAllocator.py` — 대역 상수 변경
`SOURCE_CODE_RANGES`의 manual 3줄과 `MANUAL_SOURCE_BASE` 3줄만 교체. okpos/easypos/unipos·`OKPOS_ADJUSTMENT_ITEM_ID="19999999"`·`MANUAL_ITEM_BLOCK_SIZE=1000` 은 그대로.
```python
SOURCE_CODE_RANGES: dict[str, tuple[int, int]] = {
    "easypos": (1, 99),
    "unipos": (100, 999),
    "unionpos": (100, 999),
    "posfeed": (100_000_000, 199_999_999),
    "배민수동": (200_000_000, 299_999_999),
    "쿠팡수동": (300_000_000, 399_999_999),
    "okpos": (10_000_000, 19_999_999),
}
MANUAL_SOURCE_BASE: dict[str, int] = {
    "posfeed": 100_000_000,
    "배민수동": 200_000_000,
    "쿠팡수동": 300_000_000,
}
```
근거: okpos 8자리(10M–19.99M) vs posfeed 9자리(100M–199.99M) → 자릿수 +1, 구간 완전 분리. store_seq 수용량 100 → 100,000개.

### 2. `DB_UnifiedSales_posfeed.py` — 하드코딩 base 제거
- 48행 import에 `MANUAL_SOURCE_BASE` 추가:
  `from modules.transform.pipelines.db.DB_ItemIdAllocator import MANUAL_ITEM_BLOCK_SIZE, MANUAL_SOURCE_BASE, allocate_manual_item_ids`
- 1098행: `offset = int(code) - 100000` → `offset = int(code) - MANUAL_SOURCE_BASE["posfeed"]`

### 3. `DB_FinProduct_Map.py` — 하드코딩 base 제거
- 21행 import 블록(`from ...DB_ItemIdAllocator import (...)`)에 `MANUAL_SOURCE_BASE` 추가
- 179-187행 `_source_base_for_item_id` 함수를 아래로 교체:
```python
def _source_base_for_item_id(source: object) -> int | None:
    return MANUAL_SOURCE_BASE.get(canonical_source(source))
```

### 4. `scripts/migrate_posfeed_item_id_band.py` — 재번호 마이그레이션
가산 오프셋(`delta = NEW_BASE - OLD_BASE`): posfeed `+99_900_000`, 배민수동 `+199_800_000`, 쿠팡수동 `+299_700_000`.
- **멱등성**: `item_id`(또는 `상품코드`)가 이미 새 base 이상이면 skip.
- **dry-run 먼저**: `--apply` 없으면 source별 변환 건수·min/max·대역이탈 0 만 출력하고 파일 미변경.
- 대상 (MART_DB = `paths.MART_DB`):
  1. `MART_DB/fin_product/fin_product_grp.csv` — `source ∈ {posfeed,배민수동,쿠팡수동}` 행 `상품코드 += delta`. 쓰기 전 `.bak_YYYYmmdd_HHMMSS` 백업.
  2. `MART_DB/unified_sales_grp/unified_sales_*.parquet` — `.bak_` 제외(`iter_unified_sales_files()` 사용). manual source 행 `item_id += delta`.
  3. `MART_DB/fin_product/fin_product_map.csv`, `_recently.csv`, `_review.csv`, `_train.json` — manual source 행 `item_id += delta` (검수 상태 연결 보존). 파일 없으면 skip.
- 적용 후 `validate_fin_product_codes(new_grp)` 호출해 새 대역 통과 확인(예외 없으면 PASS).

### 5. 실행 순서 (DAG 일시정지 상태 단일 창)
1. `DB_UnifiedSales` / `DB_FinProduct*` DAG 일시정지
2. 스크립트 dry-run → 수치 확인 → `--apply` 실제 적용 (grp → parquet → map 순)
3. 코드 3곳 변경
4. `validate_fin_product_codes` 로 grp 검증
5. DAG 재개

> 주의: **grp 재번호가 코드 배포보다 먼저**여야 함. 코드가 먼저 배포되고 grp이 옛 6자리면 `allocate_manual_item_ids` persist 경로(275행) 검증이 "대역 이탈"로 실패.

## Reference Code
### DB_ItemIdAllocator.py (현재 head + 핵심 함수 시그니처)
```python
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH  # MART_DB/fin_product/fin_product_grp.csv

MANUAL_ITEM_BLOCK_SIZE = 1000
SOURCE_CODE_RANGES: dict[str, tuple[int, int]] = { ... }   # ← 수정 대상
MANUAL_SOURCE_BASE: dict[str, int] = { ... }               # ← 수정 대상
MANUAL_SOURCES = set(MANUAL_SOURCE_BASE)
OKPOS_ADJUSTMENT_ITEM_ID = "19999999"                       # okpos 대역, 유지

def canonical_source(source: object) -> str: ...            # unionpos→unipos 정규화
def allocate_manual_item_ids(rows: pd.DataFrame, *, persist: bool = True) -> pd.Series: ...
    # code = str(MANUAL_SOURCE_BASE[source] + store_seq * MANUAL_ITEM_BLOCK_SIZE + item_seq)
def validate_fin_product_codes(df: pd.DataFrame, *, allow_legacy_blank: bool = False) -> None: ...
    # for src, (start, end) in SOURCE_CODE_RANGES.items(): 대역 이탈 시 ValueError
```
### DB_UnifiedSales_common.py (parquet 목록)
```python
from modules.transform.utility.paths import MART_DB
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
def iter_unified_sales_files() -> list:
    return sorted(p for p in UNIFIED_ROOT.glob("unified_sales_*.parquet") if ".bak_" not in p.name)
```

## Test Cases
1. [상수 변경] `python -c "from modules.transform.pipelines.db.DB_ItemIdAllocator import SOURCE_CODE_RANGES as r; assert r['posfeed']==(100_000_000,199_999_999) and r['okpos']==(10_000_000,19_999_999); print('OK')"` → 기대: `OK`
2. [겹침 0] `python -c "from modules.transform.pipelines.db.DB_ItemIdAllocator import SOURCE_CODE_RANGES as r; import itertools; ivs=list(r.values()); assert all(a[1]<b[0] or b[1]<a[0] for a,b in itertools.combinations(ivs,2)); print('NO_OVERLAP')"` → 기대: `NO_OVERLAP`
3. [posfeed 임포트] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_posfeed"` → 기대: 예외 없음
4. [map 임포트] `python -c "import modules.transform.pipelines.db.DB_FinProduct_Map"` → 기대: 예외 없음
5. [dry-run] `python scripts/migrate_posfeed_item_id_band.py` → 기대: source별 변환건수·min/max 출력, 파일 미변경, 대역이탈 0
6. [apply 후 검증] `--apply` 실행 후 grp 재로드 → `validate_fin_product_codes(grp)` 예외 없음 + posfeed min≥100_000_000, max≤199_999_999, okpos 8자리 무변, 겹치는 코드 0
7. [재번호 정합] 임의 posfeed 상품 1건: 옛 코드 + 99_900_000 == 새 grp/parquet 코드 일치
8. [멱등] 스크립트 2회째 실행 → 변환 건수 0 (이미 새 base 이상이라 skip)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~8 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드/스크립트 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `MANUAL_ITEM_BLOCK_SIZE=1000` / okpos·easypos·unipos 대역 / `OKPOS_ADJUSTMENT_ITEM_ID` 변경 금지
- store_seq/item_seq 컬럼 값은 재번호 시 불변 (base만 바뀌므로 그대로 유효)
- 마이그레이션은 반드시 dry-run → apply 순, 실제 파일 변경 전 백업 필수
- **grp 재번호를 코드 배포보다 먼저** (validate 하드 실패 방지)
- 스크립트는 멱등: 이미 새 대역이면 무변경
- 파일 경로·함수명은 플랜 그대로 보존 (하드코딩 신규 금지, MANUAL_SOURCE_BASE 상수 참조)

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
