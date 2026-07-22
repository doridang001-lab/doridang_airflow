# DB_FinProduct_Dags 상품코드 오류 수정

## Task
`DB_FinProduct_Dags`가 두 태스크에서 실패한다: `update_product_master`의 "상품코드가 서로 다른 상품키에 중복 사용됨" 충돌과 `load_okpos_product_xlsx`의 "okpos store_seq=3 상품 슬롯 초과: 1001/1000". 두 오류 모두 최근 도입한 매장 스코프 상품코드에서 비롯됐다. 공용 allocator(`DB_ItemIdAllocator.py`)의 블록 크기 1000이 okpos raw 코드 offset(0~1009 > 1000)을 못 담는 것이 근본 원인이며, `DB_UnifiedSales`·`DB_FinProduct`·`DB_FinProduct_Map` 세 DAG가 이 allocator를 공유한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Root Cause (실데이터 확인 완료)
- master(`fin_product_grp_input.csv`) 단독 검증은 통과. 충돌은 `update_product_master`의 append 단계에서 발생.
- okpos raw 상품코드 실측: `min=10000000 max=10001009` → offset 0~1009 (778개 코드 전부 okpos 대역 in-band).
- store_seq=3 매장: item_seq가 999까지 차 있어 offset≥1000 신규 상품이 들어오면 1000으로 초과.
- allocator는 okpos **sales**(`DB_UnifiedSales_okpos.py:839`, persist=True)와 fin_product 양쪽에서 호출 → master okpos 행에 allocator 컬럼(store_seq/item_seq/item_key)이 채워져 있음.
- 시뮬레이션 검증: allocator 컬럼 없이 append → `FAIL ['10001047']`(보고된 오류 재현), 컬럼 포함 append → `PASS`.

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_FinProduct.py` (Fix 1 + Fix 2 offset 계산)
- 수정: `modules/transform/pipelines/db/DB_ItemIdAllocator.py` (Fix 2 source별 블록)
- 수정: `modules/transform/pipelines/db/DB_FinProduct_Map.py` (offset 계산 source별 블록)
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` (offset 계산 source별 블록)
- 생성(1회성 마이그레이션 스크립트): `scratch/migrate_okpos_item_block.py` (또는 프로젝트 관례 위치) — old→new okpos 코드 매핑 및 데이터 rewrite
- DAG 파일(`dags/db/DB_FinProduct_Dags.py`)은 변경 없음 (오케스트레이션만)

## Implementation Steps

### Fix 1 — 충돌 해소 (마이그레이션 불필요, 단독 배포 가능)
1. `DB_FinProduct.py`의 `update_product_master` 함수(약 906~932줄) `new_rows.append({...})` dict에 allocator 컬럼 3개를 추가한다. classified `item`은 load 태스크 `_apply_scoped_product_codes` 결과(XCom)에서 왔으므로 값을 이미 갖고 있다.
   ```python
   new_rows.append({
       ...  # 기존 필드 유지
       "brand": item.get("brand", ""),
       "store": item.get("store", ""),
       "launch_date": _normalize_launch_date(item.get("launch_date", "")),
       "store_seq": item.get("store_seq", ""),
       "item_seq": item.get("item_seq", ""),
       "item_key": item.get("item_key", ""),
   })
   ```
   → append 행 identity 토큰이 master와 동일(item_seq 기반)해져 "서로 다른 상품키에 중복" 충돌이 사라진다.
   (참고: `finalize_unionpos_pending`·`apply_review_approvals`는 `row.to_dict()`로 복사하므로 이미 allocator 컬럼 보존됨 — 수정 불필요.)

### Fix 2 — okpos 슬롯 초과 해소 (source별 블록 확대, okpos만 10000)
2. `DB_ItemIdAllocator.py` 상단에 source별 블록 도입 (기존 `MANUAL_ITEM_BLOCK_SIZE = 1000`을 대체/보강):
   ```python
   DEFAULT_ITEM_BLOCK_SIZE = 1000
   SOURCE_ITEM_BLOCK_SIZE = {"okpos": 10000}

   def item_block_size(source) -> int:
       return SOURCE_ITEM_BLOCK_SIZE.get(canonical_source(source), DEFAULT_ITEM_BLOCK_SIZE)

   MANUAL_ITEM_BLOCK_SIZE = DEFAULT_ITEM_BLOCK_SIZE  # 하위호환 (okpos 외 그대로)
   ```
3. `DB_ItemIdAllocator.py`에서 블록 상수를 source 인지 함수로 교체할 지점 (모두 source 값 가용):
   - `allocate_manual_item_ids`: 코드 조립 `_source_base(source) + store_seq * item_block_size(source) + item_seq`.
   - `_next_item_seq(master, source, store_seq)`: 상한 `item_block_size(source)`.
   - `_pick_item_seq(master, source, store_seq, preferred)`: 상한 `item_block_size(source)`.
   - `_raw_item_token(row)`: `% item_block_size(canonical_source(row.get("source","")))`.
   - `validate_fin_product_codes`: slot 초과 체크 상한을 source별 블록으로.
   - `_item_seq_token(value)`: 현재 `0 <= seq < MANUAL_ITEM_BLOCK_SIZE(1000)` 캡 → okpos item_seq가 최대 9999까지 가능하므로 상한을 `max(SOURCE_ITEM_BLOCK_SIZE.values(), default=DEFAULT_ITEM_BLOCK_SIZE)` 등으로 **완화**해 identity 토큰이 유지되게 한다. (source 컨텍스트 없이 호출되므로 전역 최대치로 완화하는 방식 권장.)
4. offset//·% 계산을 source별 블록으로 통일:
   - `DB_FinProduct.py::_apply_scoped_product_codes`(약 594~596줄): `bs = item_block_size(source)` 후 `store_seq = offset // bs`, `item_seq = offset % bs`.
   - `DB_FinProduct_Map.py:209-210`: 동일하게 행 source의 `item_block_size` 사용.
   - `DB_UnifiedSales_posfeed.py:1142-1143`: posfeed는 기본 1000이라 실질 불변이지만 안전하게 `item_block_size(source)`로 통일.
5. 밴드 안전성: 모든 source가 기존 `SOURCE_CODE_RANGES` 내에 그대로 수용됨(실측 확인). 밴드/`SOURCE_ITEM_BASE` 수정 불필요.

### Fix 2 마이그레이션 (이번에 함께 진행) — okpos 코드만 변경
6. 블록 변경 후 okpos 코드를 **raw 원천에서 재도출**한다. old item_seq는 `%1000`로 상위비트 유실 → old 코드만으로 복원 불가하므로, unified sales okpos + fin_product load를 block=10000으로 재실행하면 새 okpos 코드가 결정론적으로 재생성된다.
7. **old→new okpos 코드 매핑**을 identity `(source, brand, store, item_key 또는 item_seq_token)`로 생성.
8. `fin_product_grp_input.csv`: okpos 행을 매핑으로 rewrite하되 **수동 검수 분류(llm_check=N, 수동분류, exclude_check, launch_date 등) 보존**. (매핑 join 후 상품코드만 교체, 나머지 컬럼 유지)
9. `fin_product_mart.csv`, `fin_product_map*.csv`: 각 DAG 태스크로 재생성(또는 매핑 update).
10. 적재 DB의 okpos item_id: 매핑 기준 UPDATE 또는 해당 파티션 재적재.

## Reference Code

### modules/transform/pipelines/db/DB_ItemIdAllocator.py (핵심 함수)
```python
MANUAL_ITEM_BLOCK_SIZE = 1000
SOURCE_ITEM_BASE = {"easypos":1_000_000,"unipos":2_000_000,"okpos":10_000_000,
    "posfeed":100_000_000,"배민수동":200_000_000,"쿠팡수동":300_000_000}
MANUAL_SOURCES = set(SOURCE_ITEM_BASE)

def _next_item_seq(master, source, store_seq):  # DB_ItemIdAllocator.py:268
    ...
    next_seq = 0 if used.empty else int(used.max()) + 1
    if next_seq >= MANUAL_ITEM_BLOCK_SIZE:   # ← okpos에서 overflow 발생 지점
        raise ValueError(f"{source} store_seq={store_seq} 상품 슬롯 초과: {next_seq+1}/{MANUAL_ITEM_BLOCK_SIZE}")
    return next_seq

def validate_fin_product_codes(df, *, allow_legacy_blank=False):  # DB_ItemIdAllocator.py:398
    ...
    # manual identity = (source, brand, store, token)
    # token = item_seq_token(item_seq) 우선, 없으면 item_key/normalize_item_key(상품명)
    for code_value, idxs in df[manual_target].groupby(code[manual_target]).groups.items():
        identities = {identity.at[i] for i in idxs}
        if len(identities) > 1:
            conflict_codes.append(code_value)     # ← 충돌 감지 지점
    if conflict_codes:
        raise ValueError(f"상품코드가 서로 다른 상품키에 중복 사용됨: {conflict_codes}")
```

### modules/transform/pipelines/db/DB_FinProduct.py (수정 대상)
```python
def _apply_scoped_product_codes(df):  # DB_FinProduct.py:569
    ...
    for idx in result.index:
        source = canonical_source(result.at[idx, "source"])
        base = SOURCE_ITEM_BASE.get(source)
        offset = int(str(result.at[idx, "상품코드"]).strip()) - base
        result.at[idx, "store_seq"] = str(offset // MANUAL_ITEM_BLOCK_SIZE)  # ← source별 블록으로
        result.at[idx, "item_seq"]  = str(offset % MANUAL_ITEM_BLOCK_SIZE)   # ← source별 블록으로

def update_product_master(**context):  # DB_FinProduct.py:888
    ...
    for item in classified:
        new_rows.append({           # ← 여기 store_seq/item_seq/item_key 누락이 충돌 원인 (Fix 1)
            "source": item.get("source", SOURCE_CODE),
            ...
        })
    df_out = _mark_is_latest(pd.concat([df_master, pd.DataFrame(new_rows)], ignore_index=True))
    validate_fin_product_codes(df_out, allow_legacy_blank=True)  # ← 여기서 실패
```

## Test Cases
1. [allocator import] `python -c "from modules.transform.pipelines.db.DB_ItemIdAllocator import item_block_size; print(item_block_size('okpos'), item_block_size('easypos'))"` → 기대: `10000 1000`
2. [DAG import] `python -c "from dags.db.DB_FinProduct_Dags import dag"` → 기대: ImportError 없음
3. [Fix 1 충돌 검증] master 로드 후 okpos 변경분(allocator 컬럼 포함) append → `validate_fin_product_codes(df_out, allow_legacy_blank=True)` → 기대: 예외 없음(PASS). (컬럼 누락 시 `['10001047']` 충돌 재현되던 것이 사라짐)
4. [Fix 2 초과 검증] okpos 전 매장·전 offset(0~1009)에 `allocate_manual_item_ids` 재실행 → 기대: `_next_item_seq` 초과 예외 없음, offset 1000~1009가 0~9와 충돌하지 않음
5. [전 source 검증] `validate_fin_product_codes(master, allow_legacy_blank=True)`가 okpos 외 source 코드 불변 상태로 통과
6. [마이그레이션 무결성] old→new 매핑 적용 후 `fin_product_grp_input.csv`의 llm_check=N 수동분류 행 개수·값이 마이그레이션 전후 동일(코드만 변경, 분류 보존)
7. [최종 DAG] 컨테이너에서 `DB_FinProduct_Dags` 수동 트리거 → t1(load)·t4(update)·t8(mart) 성공 로그

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
- okpos만 블록 10000. 다른 source(easypos/unipos/posfeed/배민수동/쿠팡수동)는 1000 유지 → 코드 절대 불변.
- `SOURCE_CODE_RANGES`, `SOURCE_ITEM_BASE`(밴드) 수정 금지 — 모든 source가 기존 밴드 내 수용됨.
- 세 DAG(`DB_UnifiedSales`·`DB_FinProduct`·`DB_FinProduct_Map`)가 동일 allocator 상수를 공유하므로, 블록 로직은 `DB_ItemIdAllocator.py` 한 곳에서 `item_block_size(source)`로 일원화하고 다른 파일은 이를 호출만 한다.
- 마이그레이션은 okpos 수동 검수 분류(llm_check=N) 보존이 최우선. 코드만 교체하고 분류/플래그 컬럼은 손대지 않는다.
- 실 데이터 파일 경로는 `modules.transform.utility.paths`의 `FIN_PRODUCT_CSV_PATH`, `FIN_PRODUCT_MART_CSV_PATH`, `RAW_OKPOS_SALES` 상수 사용 (하드코딩 금지).
- DAG 파일은 오케스트레이션만 — 로직은 modules/pipelines에 둔다.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `_item_seq_token` 캡 완화 방식이 모호하면: 전역 최대 블록(`max(SOURCE_ITEM_BLOCK_SIZE.values(), default=1000)`) 기준으로 완화
