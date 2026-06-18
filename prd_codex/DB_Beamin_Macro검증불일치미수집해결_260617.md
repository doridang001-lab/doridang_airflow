# DB_Beamin_Macro 검증 불일치/미수집 해결

## Task
`DB_Beamin_Macro` 수집 파이프라인에서 (1) 나홀로 브랜드 매장(대신점·간석중앙점)이 store 키 불일치로 수집 대상에서 빠지는 문제와 (2) 사용자가 `영업관리부_수집` 폴더에 넣은 수동 수집 CSV가 orders DB로 import되지 않아 검증(ToOrder 교차검증)이 계속 불일치로 뜨는 문제를 해결한다. 기존 SSOT 유틸 `lookup_store_key`와 이미 구현된 `import_manual_baemin_csvs`를 재사용해 고친다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 매장명 정규화는 반드시 `modules.transform.utility.store_normalize` 재사용

## 작업 전 체크(실제 코드 반영 전)
- `DB_Beamin_combined.py`에서 나홀로 드롭다운 파싱/필터에 `lookup_store_key`가 적용되지 않았는지 점검
- `DB_Beamin_Macro_validate.py`에서 `validate_toorder_orders`에 manual import 호출이 없는지 점검
- `DB_Beamin_Macro_Dags.py`의 `validate_toorder`가 `manual_dir` 미전달인지 점검

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_Beamin_combined.py` — 매장명 정규화로 나홀로 누락 해결
- 수정: `modules/transform/pipelines/db/DB_Beamin_Macro_validate.py` — 수동 CSV import 연결 + 재수집 보호
- 수정: `dags/db/DB_Beamin_Macro_Dags.py` — validate에 `manual_dir` 전달

## Background (근본 원인)
- **(A) 매장명 정규화 누락**: `_parse_store_option`이 드롭다운 텍스트를 정규식으로만 파싱해 나홀로는 `대신점`, 도리당은 `부산대신점`으로 **서로 다른 store 키**가 됨. 그 결과 같은 지점 sister 브랜드를 남기는 `_filter_store_list_for_request`가 store 문자열이 안 맞아 나홀로를 **버림**. 수집돼도 다른 파티션에 저장돼 검증 합산도 안 됨.
- **(B) 수동 import 미연결**: `import_manual_baemin_csvs()`(이미 구현됨)가 **아무 데서도 호출되지 않음**. DAG의 `precheck_manual_baemin_orders`는 비교/리포트만 하고 orders DB에 쓰지 않음.
- `store_normalize.lookup_store_key`에 이미 매핑 존재: `"나홀로 대신점"→"나홀로 부산대신점"`, `"나홀로 간석중앙점"→"나홀로 인천간석중앙점"`. `lookup_store_key(brand, store)`는 브랜드 접두어를 뗀 지점 키를 반환.

## Implementation Steps

### 1. 나홀로 매장 누락 해결 — `DB_Beamin_combined.py`
1. 파일 상단 import 블록에 추가:
   ```python
   from modules.transform.utility.store_normalize import lookup_store_key
   ```
2. `_parse_store_option`(현재 716행대): 정규식으로 뽑은 `store`를 정규화하여 반환.
   ```python
   def _parse_store_option(opt: dict) -> dict | None:
       """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None."""
       text = opt.get("text", "")
       brand = next(
           (b for b in KNOWN_BRANDS if re.search(rf"(?<![가-힣]){re.escape(b)}", text)),
           None,
       )
       if not brand:
           return None
       matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
       store = matches[-1] if matches else text[:20]
       store = lookup_store_key(brand, store)   # ← 추가: 나홀로 대신점→부산대신점 등
       return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}
   ```
3. `_filter_store_list_for_request`(현재 743행대): 요청 매장명도 정규화 후 비교(양쪽 모두 정규화).
   ```python
   def _filter_store_list_for_request(store_list: list[dict], requested_store_name: str) -> list[dict]:
       """대표 매장명이 주어지면 같은 지점의 sister brand 옵션까지 함께 남긴다."""
       brand, requested_store = _split_requested_store_name(requested_store_name)
       requested_store = lookup_store_key(brand or "", requested_store)  # ← 추가
       if not requested_store:
           return list(store_list)
       return [
           store_info
           for store_info in store_list
           if requested_store == str(store_info.get("store") or "").strip()
       ]
   ```
   - 효과: 같은 지점의 나홀로·도리당이 동일 store 키("부산대신점")를 가져 (i) 필터에서 살아남고 (ii) 같은 파티션 키로 저장돼 검증 합산이 맞음.

### 2. 수동 CSV import 연결 + 재수집 보호 — `DB_Beamin_Macro_validate.py`
1. `_manual_baemin_filename_fallback`(현재 54행대): `_unknown_20260616_1`처럼 `_N` 접미사 대응.
   ```python
   stem = re.sub(r"_unknown_\d{8}(?:_\d+)?$", "", stem)   # 기존: r"_unknown_\d{8}$"
   ```
2. `validate_toorder_orders`(현재 388행대) 시그니처에 인자 추가:
   ```python
   def validate_toorder_orders(
       account_list: list,
       store_info_per_account: list,
       target_date: str,
       manual_dir: Path | None = None,
   ) -> dict:
   ```
3. 함수 초입에서, `toorder_by_store`/`baemin_by_store`를 읽기 **전에** 수동 CSV import:
   ```python
   manual_stores: set[str] = set()
   if manual_dir is not None and Path(manual_dir).exists():
       imported = import_manual_baemin_csvs(target_date, Path(manual_dir))
       manual_stores = {k.split("/", 1)[1] for k in imported}  # "brand/store" → store
       if imported:
           logger.info("수동 import 반영 매장: %s", sorted(manual_stores))
   ```
   - 이후 기존 `baemin_by_store = _baemin_orders_by_store(target_date)`가 수동분까지 합산해서 읽음(순서 중요: import가 먼저).
4. 불일치 재수집 단계(현재 527-528행)에서 **수동 반영 매장 제외**:
   ```python
   recollect_targets = [s for s in mismatched_first if s not in manual_stores]
   _delete_orders_for_stores(target_date, recollect_targets)
   _recollect_stores(store_info_per_account, account_list, target_date, set(recollect_targets))
   ```
   - 이유: 수동 데이터가 정답(authoritative). 셀레늄 재수집은 또 누락되고, 삭제 단계가 수동 행까지 지워버림. 수동 매장은 import 결과를 그대로 최종 비교에 사용.
   - 재비교 루프(현재 547행 `for store in mismatched_first:`)는 **그대로 둔다**. 수동 매장은 `baemin_after`에 수동분이 포함돼 일치 시 자동으로 `mismatched_final`에서 빠지고, 여전히 다르면 정상적으로 플래그 유지.

### 3. DAG에서 manual_dir 전달 — `DB_Beamin_Macro_Dags.py`
1. `validate_toorder`(현재 1100행대)의 호출(현재 1109행):
   ```python
   result = validate_toorder_orders(
       account_list, store_info_per_account, target_date,
       manual_dir=MANUAL_BAEMIN_ORDERS_DIR,
   )
   ```
   - `MANUAL_BAEMIN_ORDERS_DIR = COLLECT_DB / "영업관리부_수집"` 는 이미 정의됨(87행).
2. `precheck_manual_baemin_orders`(260행)는 사전 비교 리포트 용도로 **그대로 유지**(import는 upsert라 중복 무해, 실제 반영은 validate 단계 담당).

## Reference Code

### modules/transform/utility/store_normalize.py
```python
STORE_NAME_MAP: dict[str, str] = {
    "도리당 인천간석점":       "도리당 인천간석중앙점",
    "도리당 간석중앙점":       "도리당 인천간석중앙점",
    "나홀로 간석중앙점":       "나홀로 인천간석중앙점",
    "나홀로 대신점":           "나홀로 부산대신점",
    # ... (기타 매핑)
}
_MAP = _build_expanded_map(STORE_NAME_MAP)

def strip_brand(series: pd.Series) -> pd.Series:
    """브랜드 접두어('도리당 ','나홀로 ') 제거 → 지점명만."""
    return series.map(lambda s: _BRAND_PREFIX_RE.sub("", str(s)).strip())

def lookup_store_key(brand: str, store: str) -> str:
    """브랜드/지점명을 매핑 기반 정규화. 매핑 있으면 접두어 뗀 지점 키, 없으면 입력 그대로."""
    normalized_store = f"{brand} {store}".strip()
    if not normalized_store:
        return ""
    normalized = _MAP.get(normalized_store, normalized_store)
    prefix = f"{brand} ".strip()
    if brand and normalized.startswith(prefix):
        return normalized[len(prefix):].strip()
    return store.strip()
```

### modules/transform/pipelines/db/DB_Beamin_Macro_validate.py (import_manual_baemin_csvs — 이미 구현됨)
```python
def import_manual_baemin_csvs(
    target_date: str, base_dir: Path, file_pattern: str | None = None,
) -> dict[str, int]:
    """수동 배민 주문 CSV를 월별 orders DB 파티션으로 import. 반환 {"brand/store": rows}."""
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    pattern = str(file_pattern or "baemin_orders_*.csv")
    imported: dict[str, int] = {}
    for csv_path in sorted(base_dir.glob(pattern)):
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        # store_name 컬럼 또는 파일명 fallback → _manual_baemin_store_meta(raw, fallback)
        # 주문상태=='배달완료' & 주문시각.startswith(date_prefix) 필터
        # brand={brand}/store={store_key}/ym={ym}/orders_{ym} 에 주문번호 upsert
        ...
    return imported
```
- `BAEMIN_ORDERS_DB` 파티션: `analytics/baemin_macro/orders/brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM}`
- `_baemin_orders_by_store`는 `brand=*/store=*` 전부 읽어 store별 합산(주문번호 dedup)하므로, import만 되면 검증에 자동 반영됨.

### dags/db/DB_Beamin_Macro_Dags.py (관련 상수/호출)
```python
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB
MANUAL_BAEMIN_ORDERS_DIR = COLLECT_DB / "영업관리부_수집"   # 이미 정의됨(87행)

def validate_toorder(**context) -> str:
    ...
    result = validate_toorder_orders(account_list, store_info_per_account, target_date)  # ← manual_dir 추가
```

## Test Cases
1. [정규화] `python -c "from modules.transform.utility.store_normalize import lookup_store_key; print(lookup_store_key('나홀로','나홀로 대신점'), lookup_store_key('나홀로','나홀로 간석중앙점'))"` → 기대: `부산대신점 인천간석중앙점`
2. [combined import] `python -c "import modules.transform.pipelines.db.DB_Beamin_combined"` → 기대: ImportError 없음
3. [validate import] `python -c "import modules.transform.pipelines.db.DB_Beamin_Macro_validate"` → 기대: ImportError 없음
4. [수동 import 동작] `python -c "from pathlib import Path; from modules.transform.pipelines.db.DB_Beamin_Macro_validate import import_manual_baemin_csvs; from modules.transform.utility.paths import COLLECT_DB; print(import_manual_baemin_csvs('2026-06-16', COLLECT_DB/'영업관리부_수집'))"` → 기대: dict 반환, `도리당/송파삼전점` 등 키 포함(파일 존재 시)
5. [DAG import] `python -c "from dags.db.DB_Beamin_Macro_Dags import dag"` → 기대: ImportError 없음
6. [검증 재실행] target_date=2026-06-16으로 `validate_toorder_orders(..., manual_dir=MANUAL_BAEMIN_ORDERS_DIR)` 실행 → 기대: 초월점/송파삼전점이 일치 전환, 부산대신점/인천간석중앙점은 나홀로 파티션 합산으로 일치, `store_results`에서 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_parse_store_option`/`_filter_store_list_for_request`의 기존 반환 구조(`{"store_id","brand","store"}`)·정렬 키(`_store_collection_sort_key`)는 유지.
- 매장명 정규화는 반드시 `lookup_store_key` 사용 — 새 매핑 하드코딩 금지(필요 시 `store_normalize.STORE_NAME_MAP`에만 추가).
- import는 반드시 비교(`_baemin_orders_by_store`) **전에** 실행. 순서 바뀌면 수동분 미반영.
- 삭제/재수집(`_delete_orders_for_stores`/`_recollect_stores`) 대상에서 `manual_stores`는 반드시 제외 — 미제외 시 수동 데이터가 삭제됨.
- `precheck_manual_baemin_orders` 및 그 안의 `_collect_manual_baemin_orders`(DAG-local 비교 함수)는 건드리지 말 것.
- "테이블 로드 실패" 셀레늄 안정화는 이번 범위 밖 — 건드리지 말 것.

## 운영 적용 체크리스트
- 수동 CSV 업로드 폴더 경로(`COLLECT_DB/영업관리부_수집`)에 파일이 들어와도 기존 동작은 동일하게 유지되며, `validate_toorder`에서만 최종 반영으로 전달됨.
- 수동 임포트 매장 건너뛰기 정책으로 인해 `manual_stores`는 재수집/삭제 대상에서 제외됨(데이터 손실 방지).
- 수집 누락 이슈(대신점/간석중앙점) 종료 조건: compare 단계부터 store key가 `부산대신점`, `인천간석중앙점`로 통일되는지 로그 확인.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(해당 함수만 교체)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
