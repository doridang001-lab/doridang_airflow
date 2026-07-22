# fin_product 통합 (저리스크): mart에 합치고 원본 불변

## Task
상품 분류 마스터가 grp/map 2개로 병렬 존재해 헷갈린다. grp/map **원본은 손대지 않고**,
재생성 파생물인 `fin_product_mart.csv`에서만 grp 분류 + map 표준명/상세분류를 합쳐 "한 테이블에서 다 보기"를 만든다.
`build_fin_product_mart`의 mart 생성부만 확장한다. (grp write-back·map DAG는 그대로 살려둠)

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 저장은 기존 패턴(tmp + `_safe_replace`, `encoding="utf-8-sig"`) 유지

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_FinProduct.py` (이 파일 하나만)
- 산출물 변경: `fin_product_mart.csv` (컬럼 확장) — **원본 `fin_product_grp.csv`/`fin_product_map.csv`는 불변**
- 건드리지 말 것: `_read_master()`, grp write-back 블록(중복_수동분류), `DB_FinProduct_Map*`

## Implementation Steps

1. **import 추가** — paths import 블록(현재 22~28행)에 `FIN_PRODUCT_MAP_CSV_PATH` 추가.
   ```python
   from modules.transform.utility.paths import (
       ANALYTICS_DB, FIN_PRODUCT_CSV_PATH, FIN_PRODUCT_REVIEW_CSV_PATH,
       FIN_PRODUCT_ALIAS_CSV_PATH, FIN_PRODUCT_MART_CSV_PATH, FIN_PRODUCT_MAP_CSV_PATH,
   )
   ```

2. **신규 helper `_load_map_enrichment_df()`** (helper 영역에 추가) — DataFrame 반환.
   `fin_product_map.csv` → 컬럼 `[source, item_id, 표준상품명, 메뉴대분류, 메뉴중분류, 대표메뉴]` (source+item_id 유일).
   검수완료 우선 + updated_at 최신. 파일 없음/로드 실패 → 정확한 컬럼의 빈 DataFrame(merge no-op).
   순환 import 회피 위해 `DB_FinProduct_Map`은 import하지 않고 approved 판별 inline.
   ```python
   _MAP_APPROVED = {"1", "1.0", "y", "yes", "true", "승인", "검수완료", "완료"}
   _ENR_COLS = ["source", "item_id", "표준상품명", "메뉴대분류", "메뉴중분류", "대표메뉴"]

   def _load_map_enrichment_df() -> pd.DataFrame:
       """fin_product_map.csv → (source,item_id)별 표준명/상세분류. 검수완료 우선, 최신."""
       empty = pd.DataFrame(columns=_ENR_COLS)
       if not FIN_PRODUCT_MAP_CSV_PATH.exists():
           return empty
       try:
           df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
       except Exception as e:
           logger.warning("fin_product_map.csv 로드 실패, 표준명 합침 생략: %s", e)
           return empty
       for c in ("source", "item_id", "표준_메뉴명_edit", "메뉴대분류_edit",
                 "메뉴중분류_edit", "main_menu_edit", "review_status_edit", "updated_at"):
           if c not in df.columns:
               df[c] = ""
       df["source"] = df["source"].astype(str).str.strip()
       df["item_id"] = df["item_id"].astype(str).str.strip()
       df["표준상품명"] = df["표준_메뉴명_edit"].astype(str).str.strip()
       df = df[(df["item_id"] != "") & (df["표준상품명"] != "")].copy()
       if df.empty:
           return empty
       df["_ok"] = df["review_status_edit"].astype(str).str.strip().str.lower().isin(_MAP_APPROVED)
       df["_ts"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
       df = df.sort_values(["_ok", "_ts"], ascending=[False, False])
       df["메뉴대분류"] = df["메뉴대분류_edit"].astype(str).str.strip()
       df["메뉴중분류"] = df["메뉴중분류_edit"].astype(str).str.strip()
       df["대표메뉴"] = df["main_menu_edit"].astype(str).str.strip()
       return df.groupby(["source", "item_id"], as_index=False).first()[_ENR_COLS]
   ```

3. **`build_fin_product_mart()` mart 생성부 교체** — 현재 mart 생성(1090~1099행,
   `df[["source","상품코드","수동분류","중복_수동분류"]].drop_duplicates(...)` 블록)을 아래로 교체.
   `중복_수동분류` 계산·grp write-back(1069~1088행)·충돌 이메일 로직은 **그대로 둔다**.
   ```python
   base_cols = ["source", "상품코드", "수동분류", "중복_수동분류"]
   extra_src = ["상품명", "exclude_check"]
   for c in extra_src:
       if c not in df.columns:
           df[c] = ""
   mart = (
       df[base_cols + extra_src]
       .drop_duplicates(subset=["source", "상품코드"], keep="first")
       .sort_values(["source", "상품코드"])
       .reset_index(drop=True)
   )
   try:
       enr = _load_map_enrichment_df()
       mart = mart.merge(
           enr.rename(columns={"item_id": "상품코드"}),
           on=["source", "상품코드"], how="left",
       )
       for c in ["표준상품명", "메뉴대분류", "메뉴중분류", "대표메뉴"]:
           mart[c] = mart[c].fillna("") if c in mart.columns else ""
       matched = int((mart["표준상품명"].astype(str).str.strip() != "").sum())
       logger.info("mart 표준명 합침: %d/%d행 매칭", matched, len(mart))
   except Exception as e:
       logger.warning("표준명 합침 실패, mart 기본 컬럼만 생성: %s", e)
       for c in ["표준상품명", "메뉴대분류", "메뉴중분류", "대표메뉴"]:
           mart[c] = ""
   mart = mart[base_cols + ["상품명", "표준상품명", "메뉴대분류", "메뉴중분류", "대표메뉴", "exclude_check"]]
   ```

## 최종 mart 스키마
`source, 상품코드, 수동분류, 중복_수동분류, 상품명, 표준상품명, 메뉴대분류, 메뉴중분류, 대표메뉴, exclude_check`
(앞 4개는 기존과 동일 순서 — 위치 기반 소비처 안전. 뒤 6개가 이번에 합쳐지는 값.)

## Reference Code
### modules/transform/pipelines/db/DB_FinProduct.py (현재 상태 발췌)
```python
# import 블록 (22~28행)
from modules.transform.utility.paths import (
    ANALYTICS_DB, FIN_PRODUCT_CSV_PATH, FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH, FIN_PRODUCT_MART_CSV_PATH,
)
# build_fin_product_mart 현재 mart 생성부 (1090~1099행) — 이 블록을 교체
    df["중복_수동분류"] = df.apply(
        lambda r: "Y" if (str(r["source"]), str(r["상품코드"])) in conflict_keys else "N", axis=1
    )
    mart = (
        df[["source", "상품코드", "수동분류", "중복_수동분류"]]
        .drop_duplicates(subset=["source", "상품코드"], keep="first")
        .sort_values(["source", "상품코드"])
        .reset_index(drop=True)
    )
    FIN_PRODUCT_MART_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MART_CSV_PATH.with_suffix(".tmp")
    try:
        mart.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MART_CSV_PATH)
    finally:
        try: tmp.unlink(missing_ok=True)
        except Exception: pass
```
### fin_product_map.csv 헤더 (관련 컬럼)
```
item_id,item_key,...,item_name,unitprice,표준_메뉴명_edit,수동분류_edit,main_menu_edit,메뉴대분류_edit,메뉴중분류_edit,부모메뉴_확정방식,...,review_status_edit,classified_by,updated_at
```
### fin_product_grp.csv 헤더 (df 원천 — 이 컬럼들이 df에 있음)
```
source,구분,대메뉴,중메뉴,상품코드,상품명,판매단가,수동분류,is_main_candidate,llm_check,exclude_check,updated_at,approve,is_latest,중복_수동분류,launch_date,brand,store,item_key,store_seq,item_seq
```

## Test Cases
1. [정적 파싱] `python -c "import ast; ast.parse(open(r'C:\airflow\modules\transform\pipelines\db\DB_FinProduct.py',encoding='utf-8').read()); print('OK')"` → 기대: `OK`
2. [enrichment 로드] `python -c "from modules.transform.pipelines.db.DB_FinProduct import _load_map_enrichment_df as f; d=f(); print(d.shape); print(list(d.columns)); print(d.head())"` → 기대: 컬럼 6개(`source,item_id,표준상품명,메뉴대분류,메뉴중분류,대표메뉴`), source=okpos 송파삼전점 item_id 매핑
3. [mart 재생성] `fin_product_grp.csv`·`fin_product_map.csv` 백업 후 `airflow tasks test DB_FinProduct build_fin_product_mart 2026-07-06` → 기대: 로그 "mart 표준명 합침: N/M행 매칭", `fin_product_mart.csv` 헤더 10컬럼(앞 4개 순서 동일)
4. [교차 오염 0건] `python -c "import pandas as pd; from modules.transform.utility.paths import FIN_PRODUCT_MART_CSV_PATH as p; d=pd.read_csv(p,dtype=str).fillna(''); bad=d[(d['source']!='okpos') & (d['표준상품명'].str.strip()!='')]; print('bad', len(bad)); assert len(bad)==0"` → 기대: `bad 0`
5. [원본 불변] 실행 전후 `fin_product_grp.csv` 헤더 동일(신규 컬럼 없음) + `fin_product_map.csv` 무변경 → 기대: diff 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→5 순서 실행
  2. FAIL 항목 원인 분석
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- **원본 불변**: `fin_product_grp.csv`/`fin_product_map.csv`의 스키마·writer를 바꾸지 말 것. `_read_master()`·grp write-back 블록 수정 금지.
- **조인은 반드시 (source, 상품코드)** — item_id(상품코드)만으로 조인 금지(posfeed/unionpos 교차 오염).
- mart 앞 4컬럼(`source,상품코드,수동분류,중복_수동분류`)은 순서 그대로, 신규 컬럼은 뒤에만 append.
- `DB_FinProduct_Map`은 import 금지(순환) — approved 판별 inline. map 로드 실패는 try/except로 흡수(빈 값 진행).
- grp 기존 `수동분류`(4종) 덮어쓰기 금지, map `수동분류_edit`(10종)은 mart에 넣지 않음.
- `DB_FinProduct_Map_Dags`(map DAG)는 **종료/비활성화 금지** — 표준명 생산자라 이번 범위에서 살려둠.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
