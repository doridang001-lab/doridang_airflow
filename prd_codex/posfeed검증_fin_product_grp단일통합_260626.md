# posfeed 검증 fin_product_grp.csv 단일 통합

## Task
posfeed 상품 유효성(블랙리스트) 관리가 두 파일(`fin_product_posfeed_whitelist.csv`, `fin_product_grp.csv`)로 쪼개져 데이터가 어긋났다. whitelist에는 사용자가 끝낸 신뢰 분류(`is_valid` Y/N)가 있는데, grp의 posfeed 행(2613개)은 외부/수동 실행 시 LLM이 처음부터 재분류한 `exclude_check` 값이라 whitelist와 전혀 맞지 않고, 상품코드가 상품명과 중복 채워져 있다. `fin_product_grp.csv`를 **단일 운영 소스**로 만든다: (1) whitelist 분류를 grp `exclude_check`로 복원, (2) posfeed `상품코드`를 비움(상품명으로 식별), (3) 파이프라인이 grp의 `source=posfeed & exclude_check=Y`를 블랙리스트로 읽고 쓰게 변경. whitelist CSV는 폐기.

매핑: `is_valid=N` ⇄ `exclude_check=Y`(제외), `is_valid=Y` ⇄ `exclude_check=N`(유지). 식별 키: `_normalize_item_key(상품명)` ⇄ `_normalize_item_key(item_name)`. whitelist `store` 컬럼은 전부 비어 있어 매장별 제외 로직 불필요(전체 매장 제외).

데이터 현황: whitelist 2417행(N:2260, Y:157) / grp posfeed 2613행(exclude_check Y:802, N:1811). 마이그레이션 후 grp `exclude_check=Y`가 ~2260으로 whitelist와 정합되어야 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 일회성 스크립트 위치: `scripts/` (기존 `rollback_fin_product_move.py`, `check_fin_product_unique.py`와 동일)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 저장: `utf-8-sig` + `.tmp` 작성 후 `os.replace` (OneDrive 마운트 폴백은 `_safe_replace` 패턴)

## Files to Create / Modify
**생성:**
- `scripts/migrate_posfeed_grp.py` — 일회성 데이터 마이그레이션

**수정:**
- `modules/transform/pipelines/db/DB_UnifiedSales_common.py` — `_load_posfeed_blacklist`, `_apply_posfeed_blacklist`
- `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` — `generate_posfeed_whitelist_draft`, `_auto_fix_dori_whitelist`, `report_posfeed_exclusions`, `sync_posfeed_blacklist`(docstring)

**손대지 않음:** `paths.py`의 `POSFEED_WHITELIST_CSV_PATH` 상수 정의는 남겨도 무방(읽기/쓰기 참조만 제거). `dags/db/DB_UnifiedSales_Dags.py`는 docstring이 이미 grp 기준이라 코드 변경 없음.

## Implementation Steps

1. **일회성 마이그레이션 스크립트 `scripts/migrate_posfeed_grp.py`**
   - `FIN_PRODUCT_CSV_PATH`(grp), `POSFEED_WHITELIST_CSV_PATH`(whitelist)를 `paths`에서 import, `_normalize_item_key`를 common에서 import.
   - whitelist 로드 → `{_normalize_item_key(item_name): is_valid.upper()}` 맵 생성.
   - grp 로드(`dtype=str`, fillna("")). `source=="posfeed"` 행에 대해:
     - whitelist 매칭 시 `exclude_check = "Y" if is_valid=="N" else "N"` (**whitelist 우선, 기존 값 덮어씀**).
     - whitelist 미존재(grp가 ~196개 더 많음) → grp 현재 `exclude_check` 유지.
     - 모든 posfeed 행 `상품코드 = ""` (공백).
     - `updated_at`을 현재 시각으로 갱신.
   - 비-posfeed 행은 그대로 둠. `utf-8-sig` + atomic replace로 저장.
   - 로그: 매칭/덮어쓴 행수, 상품코드 비운 행수, 최종 exclude_check Y/N 분포.

2. **`DB_UnifiedSales_common.py` `_load_posfeed_blacklist()` repoint**
   - 읽기 소스: `POSFEED_WHITELIST_CSV_PATH` → `FIN_PRODUCT_CSV_PATH`. 캐시 mtime도 grp 기준.
   - 필터: `source=="posfeed"` (소문자 strip) & `exclude_check.str.upper()=="Y"`.
   - 키: `_normalize_item_key(상품명)`. store 컬럼 없음 → 반환 dict 값은 항상 `None`(전체 매장 제외). normalize 후 중복 상품명은 하나라도 Y면 제외.
   - 반환 타입 `dict[str, set[str] | None]` 유지(전부 `None`).

3. **`_apply_posfeed_blacklist()`** — 변경 최소. 블랙리스트 dict 값 `None`=전체 매장 제외 분기 그대로 동작(store 컬럼 없어도 무해).

4. **`DB_UnifiedSales_posfeed.py` `generate_posfeed_whitelist_draft()` repoint**
   - 출력 대상 whitelist CSV → grp. unified_sales parquet에서 신규 posfeed `item_name` 추출 후, grp `source=posfeed` 상품명에 **없는 항목만** 신규 행 append:
     `source="posfeed", 구분="배달", 상품코드="", 상품명=item_name, exclude_check=(LLM is_valid N→"Y" / Y→"N"), llm_check="Y"`, `updated_at`=now.
   - **기존 grp 행의 `exclude_check`는 절대 재분류·덮어쓰지 않음**(값 소실 근본 원인 차단).
   - 기존 마스터 로드/저장은 `_read_master`/atomic replace 패턴(`DB_FinProduct._safe_replace`) 재사용. 이메일 알림은 유지(경로 표기만 grp). 공개 함수명은 DAG 호환 위해 유지.

5. **`_auto_fix_dori_whitelist()` repoint** — whitelist 대신 grp에서 `source=posfeed` & `상품명`에 "도리" 포함 행의 `exclude_check`를 N으로 보정(미존재 시 새 posfeed 행 append). `(fixed_count, added_count)` 반환 시그니처 유지.

6. **`report_posfeed_exclusions()` repoint** — 도리 자동보정/`still_blocked` 재조회가 읽는 소스를 whitelist → grp(`exclude_check`)로 교체. 제외 로그/리포트 저장 로직(detail/summary CSV)은 그대로.

7. **`sync_posfeed_blacklist()`** — `_load_posfeed_blacklist`(grp 기반)로 자동 정합. docstring/주석을 grp(`source=posfeed & exclude_check=Y`) 기준으로 갱신.

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_common.py (현재 — whitelist 기반, grp로 교체 대상)
```python
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, POSFEED_WHITELIST_CSV_PATH, MART_DB

def _load_posfeed_blacklist() -> dict[str, set[str] | None]:
    """현재: fin_product_posfeed_whitelist.csv의 is_valid=N 항목 → store-aware dict.
       변경: FIN_PRODUCT_CSV_PATH에서 source=posfeed & exclude_check=Y 를 normalize(상품명) 키로."""
    global _POSFEED_WHITELIST_CACHE, _POSFEED_WHITELIST_CACHE_MTIME
    try:
        mtime = POSFEED_WHITELIST_CSV_PATH.stat().st_mtime   # → FIN_PRODUCT_CSV_PATH 로 교체
    except FileNotFoundError:
        _POSFEED_WHITELIST_CACHE = {}; _POSFEED_WHITELIST_CACHE_MTIME = None
        return _POSFEED_WHITELIST_CACHE
    # ... df = pd.read_csv(...); 필요 컬럼: item_name,is_valid  → source,exclude_check,상품명
    result: dict[str, set[str] | None] = {}
    for _, row in df[df["is_valid"].str.strip().str.upper() == "N"].iterrows():
        key = _normalize_item_key(str(row["item_name"]).strip())
        if not key: continue
        result[key] = None   # store 비면 전체 매장
    return result

def _apply_posfeed_blacklist(df: pd.DataFrame) -> pd.DataFrame:
    blacklist = _load_posfeed_blacklist()
    if not blacklist or df.empty: return df
    item_keys = df["item_name"].fillna("").astype(str).str.strip().map(_normalize_item_key)
    # r is None → 전체 매장 제외
    remove_mask = pd.Series([k in blacklist for k in item_keys], index=df.index, dtype=bool)
    return df[~remove_mask].copy()
```

### modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py (현재 — whitelist 기록, grp로 교체 대상)
```python
from modules.transform.utility.paths import ANALYTICS_DB  # POSFEED_WHITELIST_CSV_PATH 참조 제거 대상
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, UNIFIED_ROOT, _apply_posfeed_blacklist, _load_posfeed_blacklist,
    _make_unified_pk, _normalize_item_key, ...)

_WHITELIST_COLUMNS = ["item_name", "is_valid", "store", "review_needed", "classified_by"]

def generate_posfeed_whitelist_draft() -> str:
    """현재: parquet의 posfeed item_name → POSFEED_WHITELIST_CSV_PATH에 LLM 분류 append.
       변경: grp(source=posfeed)에 없는 신규 item_name만 append, 기존 exclude_check 보존."""
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    # ... norm_items 수집 → 기존 grp posfeed 상품명과 비교 → 신규만 _classify_item_with_llm → grp append

def _auto_fix_dori_whitelist(doridori_df) -> tuple[int, int]:
    """현재: POSFEED_WHITELIST_CSV_PATH의 '도리' 항목 is_valid=Y 보정.
       변경: grp source=posfeed '도리' 행 exclude_check=N 보정."""

def sync_posfeed_blacklist() -> str:
    blacklist = _load_posfeed_blacklist()   # 이제 grp 기반 → 자동 정합
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    # source=posfeed & item_name ∈ blacklist 행 제거 → menu_name/order_cnt/_pk 재계산 → 저장
```

### modules/transform/utility/paths.py (상수 — 변경 없음, 참조용)
```python
FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp.csv"
POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product" / "fin_product_posfeed_whitelist.csv"
```

## Test Cases
1. [마이그레이션 실행] `python scripts/migrate_posfeed_grp.py` → 기대: 매칭/덮어쓴 행수·상품코드 비운 행수 로그 출력, 에러 없음
2. [상품코드 공백] `python -c "import pandas as pd; from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH as p; df=pd.read_csv(p,dtype=str).fillna(''); pf=df[df['source']=='posfeed']; print('blank_code', (pf['상품코드'].str.strip()=='').all(), 'excludeY', (pf['exclude_check'].str.upper()=='Y').sum())"` → 기대: `blank_code True`, excludeY ≈ 2260
3. [블랙리스트 grp 로드] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import _load_posfeed_blacklist as f; print(len(f()))"` → 기대: ImportError 없음, ~2260 항목
4. [퍼사드 import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales import sync_posfeed_blacklist, generate_posfeed_whitelist_draft, run_posfeed"` → 기대: ImportError 없음
5. [whitelist 참조 제거] `grep -rn "POSFEED_WHITELIST_CSV_PATH" modules/transform/pipelines/db/` → 기대: 읽기/쓰기 참조 0건 (paths.py 상수 정의만 허용)
6. [소급 적용] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales import sync_posfeed_blacklist as s; print(s())"` → 기대: 파일 N개 수정 / 총 M행 제거 요약 문자열

> 모든 명령은 `C:\airflow`를 작업 디렉터리(또는 PYTHONPATH)로 두고 실행.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석 (경로 소스 미교체 / normalize 키 불일치 / 캐시 mtime)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- whitelist `is_valid` 값이 grp `exclude_check`보다 **우선**한다(기존 LLM 값 덮어쓰기). 반대 방향 금지.
- `generate_posfeed_whitelist_draft`는 **신규 item_name만** append하고 기존 grp `exclude_check`를 절대 재분류/덮어쓰지 않는다.
- posfeed 행은 `상품코드`를 비우고 `상품명`으로 식별한다(이후 신규 행도 동일).
- blank 상품코드 부작용 없음 확인: `_load_fin_product_latest`/`_fin_code_to_name_map`(상품코드!="" 필터, posfeed는 item_id=md5 매칭), `build_fin_product_mart`(수동분류 공백 행 제외), `_mark_is_latest`(블랙리스트는 is_latest 비의존) — 모두 영향 없음.
- CSV 저장은 `utf-8-sig` + atomic replace, OneDrive PermissionError 폴백 유지.
- `paths.py`/`schedule.py`/DAG 파일 통째 덮어쓰기 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
