# fin_product 마스터 통합 (Step 1: grp에 map 컬럼 흡수)

## Task
`data/mart/fin_product/`에 상품 분류 마스터가 grp/map 2개로 병렬 존재해 헷갈린다.
map(fin_product_map.csv, 송파삼전점 표준화)은 downstream 소비가 전혀 없는 상세 분류 파일럿이다.
grp(fin_product_grp.csv)을 단일 canonical 마스터로 삼기 위해, map이 사람이 정리한 표준명·상세분류
컬럼을 grp에 item_id(=상품코드) left join으로 흡수한다. (단계적 통합의 Step 1 — 안전·가역·추가형)

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 저장: `to_csv(..., index=False, encoding="utf-8-sig")` + `.tmp` 후 `_safe_replace`

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_FinProduct.py` (이 파일 하나만)
- 변경 대상 산출물: `fin_product_grp.csv` 에 컬럼 4개 추가 (표준상품명, 메뉴대분류, 메뉴중분류, 대표메뉴)
- **변경 금지**: `fin_product_mart.csv` 산출물 스키마(source/상품코드/수동분류/중복_수동분류) 그대로

## Column Design (검토 완료 — 이 4개만 추가)
| map 원천 | grp 신규 | 값 예시 |
|---|---|---|
| `표준_메뉴명_edit` | `표준상품명` | "1인 순살 닭도리탕" |
| `메뉴대분류_edit` | `메뉴대분류` | 메인메뉴/추가메뉴/사이드/기타 |
| `메뉴중분류_edit` | `메뉴중분류` | 인원추가 등 |
| `main_menu_edit` | `대표메뉴` | 옵션·추가메뉴의 부모 메뉴명 |

- map의 `수동분류_edit`(10종)는 추가 안 함 → grp 기존 `수동분류`(4종) 유지(덮어쓰기 금지).
- `부모메뉴_확정방식`/`대표_menu_name`/`menu_name_*`/`main_menu_confidence` 제외(내부 근거).

## Implementation Steps

1. **import 추가** — paths import 블록(현재 22~28행)에 `FIN_PRODUCT_MAP_CSV_PATH` 추가.
   ```python
   from modules.transform.utility.paths import (
       ANALYTICS_DB,
       FIN_PRODUCT_CSV_PATH,
       FIN_PRODUCT_REVIEW_CSV_PATH,
       FIN_PRODUCT_ALIAS_CSV_PATH,
       FIN_PRODUCT_MART_CSV_PATH,
       FIN_PRODUCT_MAP_CSV_PATH,
   )
   ```

2. **신규 helper `_load_map_enrichment()`** — 모듈 함수(helper 영역)에 추가.
   `fin_product_map.csv` → `{item_id: {"표준상품명","메뉴대분류","메뉴중분류","대표메뉴"}}`.
   검수완료(review_status_edit ∈ {1,y,yes,true,승인,검수완료}) 우선, 그 안에서 updated_at 최신.
   순환 import 회피 위해 DB_FinProduct_Map은 import하지 않고 approved 판별 inline.
   ```python
   _MAP_APPROVED_TOKENS = {"1", "1.0", "y", "yes", "true", "승인", "검수완료", "완료"}

   def _load_map_enrichment() -> dict[str, dict[str, str]]:
       """fin_product_map.csv → {item_id: {표준상품명, 메뉴대분류, 메뉴중분류, 대표메뉴}} (검수완료 우선, 최신)."""
       if not FIN_PRODUCT_MAP_CSV_PATH.exists():
           return {}
       try:
           df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
       except Exception as e:
           logger.warning("fin_product_map.csv 로드 실패: %s", e)
           return {}
       for col in ("item_id", "표준_메뉴명_edit", "메뉴대분류_edit", "메뉴중분류_edit",
                   "main_menu_edit", "review_status_edit", "updated_at"):
           if col not in df.columns:
               df[col] = ""
       df["item_id"] = df["item_id"].fillna("").astype(str).str.strip()
       df["_std"] = df["표준_메뉴명_edit"].fillna("").astype(str).str.strip()
       df = df[(df["item_id"] != "") & (df["_std"] != "")].copy()
       if df.empty:
           return {}
       df["_approved"] = df["review_status_edit"].fillna("").astype(str).str.strip().str.lower().isin(_MAP_APPROVED_TOKENS)
       df["_ts"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
       df = df.sort_values(["_approved", "_ts"], ascending=[False, False])
       best = df.groupby("item_id", as_index=False).first()
       enr: dict[str, dict[str, str]] = {}
       for _, r in best.iterrows():
           enr[r["item_id"]] = {
               "표준상품명": r["_std"],
               "메뉴대분류": str(r.get("메뉴대분류_edit", "") or "").strip(),
               "메뉴중분류": str(r.get("메뉴중분류_edit", "") or "").strip(),
               "대표메뉴": str(r.get("main_menu_edit", "") or "").strip(),
           }
       return enr
   ```

3. **`_read_master()` 하위호환 보강** — `exclude_check`/`중복_수동분류` 채우는 블록(현재 377~389행) 옆에:
   ```python
   for _c in ("표준상품명", "메뉴대분류", "메뉴중분류", "대표메뉴"):
       if _c not in df.columns:
           df[_c] = ""
   ```
   → 어떤 writer가 append/write해도 컬럼 보존. `ensure_allocator_columns`는 여분 컬럼을 drop하지 않음.

4. **`build_fin_product_mart()` write-back에 파생값 주입** — 전체 마스터 재읽기→`중복_수동분류`
   write-back 블록(현재 1069~1088행, `if FIN_PRODUCT_CSV_PATH.exists():` 안, `full_master` 계산 직후) 에:
   ```python
   enr = _load_map_enrichment()
   codes = full_master["상품코드"].fillna("").astype(str).str.strip()
   full_master["표준상품명"] = codes.map(lambda c: enr.get(c, {}).get("표준상품명", ""))
   full_master["메뉴대분류"] = codes.map(lambda c: enr.get(c, {}).get("메뉴대분류", ""))
   full_master["메뉴중분류"] = codes.map(lambda c: enr.get(c, {}).get("메뉴중분류", ""))
   full_master["대표메뉴"]   = codes.map(lambda c: enr.get(c, {}).get("대표메뉴", ""))
   # 가독성: 표준상품명을 상품명 바로 뒤로 이동
   cols = list(full_master.columns)
   if "상품명" in cols and "표준상품명" in cols:
       cols.remove("표준상품명")
       cols.insert(cols.index("상품명") + 1, "표준상품명")
       full_master = full_master[cols]
   ```
   `mart` DataFrame(하단 source/상품코드/수동분류/중복_수동분류) 로직은 건드리지 말 것.

## Reference Code
### modules/transform/pipelines/db/DB_FinProduct.py (현재 상태 발췌)
```python
# import 블록 (12~36행)
from modules.transform.utility.paths import (
    ANALYTICS_DB, FIN_PRODUCT_CSV_PATH, FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH, FIN_PRODUCT_MART_CSV_PATH,
)
# _read_master 하위호환 블록 (377~389행)
    if "exclude_check" not in df.columns: df["exclude_check"] = "N"
    if "updated_at" not in df.columns: df["updated_at"] = ""
    if "구분" not in df.columns: df["구분"] = ""
    if "is_latest" not in df.columns: df["is_latest"] = "N"
    if "표준_메뉴명" in df.columns: df = df.drop(columns=["표준_메뉴명"], errors="ignore")
    if "중복_수동분류" not in df.columns: df["중복_수동분류"] = "N"
# build_fin_product_mart write-back 블록 (1069~1088행)
    if FIN_PRODUCT_CSV_PATH.exists():
        full_master = _read_master()
        full_master["_key"] = list(zip(full_master["source"].astype(str), full_master["상품코드"].astype(str)))
        full_master["중복_수동분류"] = full_master["_key"].apply(lambda k: "Y" if k in conflict_keys else "N")
        full_master = full_master.drop(columns=["_key"])
        full_master = ensure_allocator_columns(full_master)
        validate_fin_product_codes(full_master, allow_legacy_blank=True)
        tmp_grp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
        try:
            full_master.to_csv(tmp_grp, index=False, encoding="utf-8-sig")
            _safe_replace(tmp_grp, FIN_PRODUCT_CSV_PATH)
        finally:
            try: tmp_grp.unlink(missing_ok=True)
            except Exception: pass
```
### fin_product_map.csv 헤더 (관련 컬럼)
```
item_id,item_key,...,item_name,unitprice,표준_메뉴명_edit,수동분류_edit,main_menu_edit,메뉴대분류_edit,메뉴중분류_edit,부모메뉴_확정방식,...,review_status_edit,classified_by,updated_at
```
### fin_product_grp.csv 헤더 (현재)
```
source,구분,대메뉴,중메뉴,상품코드,상품명,판매단가,수동분류,is_main_candidate,llm_check,exclude_check,updated_at,approve,is_latest,중복_수동분류,launch_date,brand,store,item_key,store_seq,item_seq
```

## Test Cases
1. [정적 파싱] `python -c "import ast; ast.parse(open(r'C:\airflow\modules\transform\pipelines\db\DB_FinProduct.py',encoding='utf-8').read()); print('OK')"` → 기대: `OK`
2. [enrichment 로드] `python -c "from modules.transform.pipelines.db.DB_FinProduct import _load_map_enrichment as f; d=f(); print(type(d), len(d)); print(list(d.items())[:2])"` → 기대: `<class 'dict'>` + 송파삼전점 item_id 몇 건 매핑(값에 표준상품명 키 존재)
3. [컬럼 반영] `fin_product_grp.csv` 백업 후 `airflow tasks test DB_FinProduct build_fin_product_mart 2026-07-06` 실행 → grp 헤더에 `표준상품명,메뉴대분류,메뉴중분류,대표메뉴` 추가, 송파삼전점 okpos 행은 값 채워지고 타 매장/타 source 행은 빈 값
4. [mart 불변] 위 실행 후 `fin_product_mart.csv` 헤더가 `source,상품코드,수동분류,중복_수동분류` 그대로인지 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→4 순서 실행
  2. FAIL 항목 원인 분석
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `mart` DataFrame 및 `fin_product_mart.csv` 스키마는 변경 금지 (다운스트림 소비처 존재 가정).
- grp 기존 `수동분류`(4종) 덮어쓰기 금지 — 신규 컬럼만 추가.
- `DB_FinProduct_Map` 모듈을 import 하지 말 것(순환 import 위험) — approved 판별은 inline.
- item_id는 allocator 설계상 전역 유일 → 상품코드만으로 join(안전). map 미매칭 행은 빈 값 유지.
- 실행 전 `fin_product_grp.csv` 백업 (`_safe_replace`가 제자리 덮어씀).
- Step 2(map DAG/파일 은퇴)는 이번 범위 아님 — 건드리지 말 것.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
