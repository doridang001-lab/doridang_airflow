# DB_UnifiedSales 테스트 매장 배달행 이중집계 버그 수정

## Task
`DB_UnifiedSales` DAG는 9개 테스트 매장(`DELIVERY_MANUAL_TEST_STORES`)의 배달(배달의민족/쿠팡이츠) 매출을 직수집값(배민수동/쿠팡수동)만 남기고 POS·posfeed 배달행은 제거하도록 설계됐다. 그러나 POS·posfeed가 `배민 포장`, `배민 사장`, `쿠팡 포장` 같은 **변형 platform**으로 적재하면 제거 로직이 정확 일치(`배달의민족`/`쿠팡이츠`)만 잡아서 변형 행이 살아남아 배민수동/쿠팡수동과 **이중집계**된다 → 검증 오차가 크게 발생. platform 값은 그대로 유지하고 **source로만 구분**하도록 제거 로직을 플랫폼 패밀리 기준으로 일반화한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_common.py` (핵심: 길목 필터)
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py` (제거/수집 마스크)
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` (제거/수집 마스크)
- **수정(선택)** `modules/transform/pipelines/db/DB_CollectionCompare.py` (패밀리 정의 단일출처화)
- **생성** `scripts/diag_test_store_delivery.py` (진단/검증 read-only 스크립트)

## Implementation Steps

### 1. 진단 스크립트 생성 (read-only)
`scripts/diag_test_store_delivery.py`:
- `MART_DB/unified_sales_grp/unified_sales_*.parquet` 전부 로드 (`from modules.transform.utility.paths import MART_DB`)
- `DELIVERY_MANUAL_TEST_STORES`(common에서 import)로 store 필터
- `(platform, source)`별 행수 + `total_price` 합계 출력
- 배달 계열 platform에서 `source ∉ {배민수동, 쿠팡수동}`인 조합을 강조 출력 → 어떤 변형 platform/source가 살아남는지 특정
- 수정 전/후 동일 스크립트로 비교

### 2. 핵심 수정 — 길목 필터 일반화 (`DB_UnifiedSales_common.py`)
기존 `MANUAL_SOURCE_BY_PLATFORM`(정확 일치 dict)을 **플랫폼 패밀리** 기반으로 교체:
```python
DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡수동": {"쿠팡이츠", "쿠팡 포장"},
}
PLATFORM_TO_MANUAL_SOURCE = {
    p: src for src, plats in DELIVERY_PLATFORM_FAMILIES.items() for p in plats
}
```
`filter_manual_delivery_sources_for_test_stores`에서 `expected_source = platform.map(MANUAL_SOURCE_BY_PLATFORM)` → `platform.map(PLATFORM_TO_MANUAL_SOURCE)`로 변경. 나머지 로직(store 셋 ∩ expected.notna() ∩ source≠expected → 제거) 유지.
- 1단계 진단에서 실제 등장한 변형 platform이 더 있으면 패밀리 집합에 보강.
- 이 필터는 `_save_unified_daily`(매 채널 저장)와 DAG 최종 `enforce_manual_delivery_sources_for_test_stores`(전체 파일 스윕)에서 호출되므로 여기만 고치면 모든 일자 파일이 최종 정리됨.

### 3. 일관성 — reconcile/enforce 제거 마스크도 패밀리로
`DB_UnifiedSales_baemin.py` / `DB_UnifiedSales_coupang.py`에서 `platform == BAEMIN_PLATFORM`(또는 `== COUPANG_PLATFORM`) 정확 일치 마스크를 패밀리 `isin`으로 교체:
- baemin: `_upsert_daily` remove_mask, `enforce_baemin_manual_only_for_test_stores` remove_mask, `_collect_existing_unified_baemin_manual_dates`/`_collect_existing_baemin_duplicate_dates`의 platform 비교
- coupang: `_upsert_daily` remove_mask, `enforce_coupang_manual_only_for_test_stores` remove_mask, `_collect_existing_dates`의 platform 비교
→ `DELIVERY_PLATFORM_FAMILIES["배민수동"]` / `["쿠팡수동"]` 집합으로 `platform.isin(...)`. common에서 import.

### 4. 단일 출처화 (선택)
`DB_CollectionCompare.py`의 로컬 `PLATFORM_GROUPS`를 `DELIVERY_PLATFORM_FAMILIES` 기준으로 재구성(또는 import)해 배달 패밀리 정의를 한 곳에서만 관리. (값 동일: 배달의민족={배달의민족,배민1,배민 포장,배민 사장}, 쿠팡이츠={쿠팡이츠,쿠팡 포장})

## Reference Code

### DB_UnifiedSales_common.py (수정 대상 핵심부)
```python
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, MART_DB, ONEDRIVE_DB

UNIFIED_ROOT = MART_DB / "unified_sales_grp"

DELIVERY_MANUAL_TEST_STORES = [
    "해운대중동점", "법흥리점", "송파삼전점", "동탄영천점", "중랑면목점",
    "시흥배곧점", "동두천지행점", "평택비전점", "부산장림점",
]

MANUAL_SOURCE_BY_PLATFORM = {        # ← 교체 대상
    "배달의민족": "배민수동",
    "배민1": "배민수동",
    "쿠팡이츠": "쿠팡수동",
}

def filter_manual_delivery_sources_for_test_stores(df, stores=None):
    if df.empty or not {"store", "platform", "source"}.issubset(df.columns):
        return df
    store_set = {str(s).strip() for s in (stores or DELIVERY_MANUAL_TEST_STORES) if str(s).strip()}
    if not store_set:
        return df
    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    platform = out["platform"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    expected_source = platform.map(MANUAL_SOURCE_BY_PLATFORM)   # ← PLATFORM_TO_MANUAL_SOURCE
    remove_mask = (store.isin(store_set) & expected_source.notna() & source.ne(expected_source))
    removed = int(remove_mask.sum())
    if removed:
        logger.warning("테스트 매장 배달 수동 source 필터: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)

# enforce_manual_delivery_sources_for_test_stores(stores=None): 전체 parquet 스윕하며 위 필터 적용
```

### DB_UnifiedSales_baemin.py (제거 마스크 패턴)
```python
BAEMIN_SOURCE = "배민수동"
BAEMIN_PLATFORM = "배달의민족"

# _upsert_daily 내부 (정확 일치 → 패밀리 isin 으로 교체)
remove_mask = (
    df_existing["store"].fillna("").astype(str).str.strip().eq(store)
    & df_existing["platform"].fillna("").astype(str).str.strip().eq(BAEMIN_PLATFORM)
)

# enforce_baemin_manual_only_for_test_stores 내부
remove_mask = (
    df["store"].fillna("").astype(str).str.strip().isin(store_set)
    & df["platform"].fillna("").astype(str).str.strip().eq(BAEMIN_PLATFORM)
    & ~df["source"].fillna("").astype(str).str.strip().eq(BAEMIN_SOURCE)
)
```

### DB_UnifiedSales_coupang.py (제거 마스크 패턴)
```python
COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"

# _upsert_daily / enforce_coupang_manual_only_for_test_stores 의 platform.eq(COUPANG_PLATFORM)
# → platform.isin(DELIVERY_PLATFORM_FAMILIES["쿠팡수동"]) 로 교체
```

### DB_CollectionCompare.py (기존 패밀리 정의 — 동일 기준)
```python
PLATFORM_GROUPS = {
    "배달의민족": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡이츠": {"쿠팡이츠", "쿠팡 포장"},
}
```

## Test Cases
1. [진단 전] `python scripts/diag_test_store_delivery.py` → 기대: 테스트 매장 배달 계열 platform에서 `source ∉ {배민수동,쿠팡수동}`인 행/금액이 **존재**(버그 재현) 확인.
2. [common import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import PLATFORM_TO_MANUAL_SOURCE, DELIVERY_PLATFORM_FAMILIES; print(PLATFORM_TO_MANUAL_SOURCE)"` → 기대: `배민 포장`/`배민 사장`/`쿠팡 포장` 키 포함된 dict 출력, 에러 없음.
3. [facade import] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales"` → 기대: ImportError 없음.
4. [일괄 정리] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import enforce_manual_delivery_sources_for_test_stores as f; print(f())"` → 기대: "제거=N행" (N>0 또는 0) 정상 반환.
5. [진단 후] `python scripts/diag_test_store_delivery.py` → 기대: 테스트 매장 배달 계열 platform의 `source`가 `배민수동/쿠팡수동`만 남음 (비매뉴얼 행 0건).
6. [검증] `validate_sales` / `validate_monthly_sales` 재실행 → `MART_DB/unified_sales_grp_error_list/` CSV에서 9개 매장 오차율 정상(<2%) 확인.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 진단(5)에서 남은 변형 platform이 있으면 DELIVERY_PLATFORM_FAMILIES에 추가)
  3. 코드 수정
  4. 전체 재실행
종료 조건: 진단(5) 비매뉴얼 배달행 0건 + import 정상
```

## Constraints
- platform 값은 절대 변경 금지 — `배달의민족`/`쿠팡이츠` 그대로 유지하고 **source로만 구분**한다.
- 패밀리 정의는 한 곳(`DB_UnifiedSales_common.DELIVERY_PLATFORM_FAMILIES`)에서만 관리하고 baemin/coupang/CollectionCompare는 import 재사용.
- `MANUAL_SOURCE_BY_PLATFORM`을 참조하는 다른 곳이 없는지 grep 후 교체 (`grep -rn MANUAL_SOURCE_BY_PLATFORM modules/`).
- 기존 컬럼 스키마(`UNIFIED_COLUMNS`)·`_pk` 생성 로직 변경 금지.
- 진단 스크립트는 read-only (parquet write 금지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석: 최소화 (WHY만)
- 변수명·함수명: snake_case, 기존 파일과 동일하게
