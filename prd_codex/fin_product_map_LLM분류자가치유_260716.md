# fin_product_map LLM 분류 자가치유 수정

## Task
`DB_FinProduct_Map`의 LLM 상품 분류에서 일부 행이 `수동분류_edit` 공백(검수사유 "수동분류 미입력")으로 남고, **한번 그 상태가 되면 LLM이 다시 분류하지 않는다**. 서로 맞물린 2가지 원인(LLM 응답 item_name echo 미스매치 + 재시도 트랩)을 고쳐 LLM 분류 단계가 자가 치유되게 만든다. 단일 파일 수정.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 스케줄/경로 상수: `from modules.transform.utility.schedule|paths import ...` (하드코딩 금지)
- `normalize_item_key`는 `DB_FinProduct_Map.py` 상단(라인 35~44 import 블록)에 이미 import되어 있음 → 새 import 불필요

## Files to Create / Modify
- **수정만**: `modules/transform/pipelines/db/DB_FinProduct_Map.py`
- 신규 파일 없음. item_id 배정 로직(`DB_ItemIdAllocator.py`)·posfeed 파이프라인은 **건드리지 않는다**.

## Implementation Steps

### 1. `_classify_batch` — LLM 응답 매칭 강건화 (현재 라인 1281~1312)
LLM 응답을 입력과 item_name **완전일치**로만 되짚어, qwen이 상품명을 재포맷하면(`[소] 반마리+우거지 200g`의 괄호/무공백 `+`) 조회가 빗나가 `classified={}` → 라벨 공백이 된다.

현재:
```python
def _classify_batch(batch, examples, rules):
    results = call_llm(build_prompt(batch, examples, rules=rules))
    by_name = {str(r.get("item_name", "")).strip(): r for r in results}
    rows = []
    unmatched_count = 0
    for item in batch:
        classified = by_name.get(str(item.get("item_name", "")).strip(), {})
        if not classified:
            unmatched_count += 1
        ...
```

변경:
- 응답 인덱스를 **원문 키 + `normalize_item_key(원문)` 키** 두 가지로 등록(`setdefault`로 원문 우선 보존).
- 조회는 **원문 → normalize 폴백** 순.
- 둘 다 실패하고 `len(results) == len(batch)`이면 **위치기반(index) 폴백**을 마지막 안전망으로 사용(응답 순서가 배치 순서와 일치한다는 가정 하에서만).
- `unmatched_count`는 폴백까지 전부 실패한 건수만 집계하도록 유지.

구현 예:
```python
def _classify_batch(batch, examples, rules):
    results = call_llm(build_prompt(batch, examples, rules=rules))
    by_name: dict[str, dict] = {}
    for r in results:
        raw = str(r.get("item_name", "")).strip()
        if raw:
            by_name.setdefault(raw, r)
        nkey = normalize_item_key(raw)
        if nkey:
            by_name.setdefault(nkey, r)
    rows = []
    unmatched_count = 0
    positional_ok = len(results) == len(batch)
    for pos, item in enumerate(batch):
        raw_item = str(item.get("item_name", "")).strip()
        classified = by_name.get(raw_item) or by_name.get(normalize_item_key(raw_item))
        if not classified and positional_ok:
            classified = results[pos] if isinstance(results[pos], dict) else None
        if not classified:
            classified = {}
            unmatched_count += 1
        llm_normalized = _normalize_classification(item, classified)
        rule_result = classify_by_rules(item, rules)
        resolved = reconcile(rule_result, llm_normalized)
        normalized = _normalize_classification(item, resolved)
        rows.append({... 기존과 동일 ...})
    if unmatched_count:
        logger.warning("LLM 응답 item_name 미매칭: %d/%d건", unmatched_count, len(batch))
    return rows
```
(rows.append 내용·나머지 로직은 기존 그대로 유지)

### 2. `find_llm_targets` — 재시도 트랩 완화 (현재 라인 985~991)
라벨을 못 채운 행도 `classified_by="llm"`로 저장되고, `classified_by∈{llm,human}`이면 무조건 done 처리되어 **다음 실행에서 영구 제외**된다.

현재:
```python
    is_pending_other = manual_label.eq("기타") & review_status.ne(REVIEW_APPROVED)
    has_manual_value = (
        (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & manual_label.isin(VALID_CATEGORIES)
        & ~is_pending_other
    )
    classified_done = classified_by.isin(["llm", "human"]) & ~is_pending_other
```

변경:
```python
    is_pending_other = manual_label.eq("기타") & review_status.ne(REVIEW_APPROVED)
    has_valid_label = manual_label.isin(VALID_CATEGORIES)
    has_manual_value = (
        (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & has_valid_label
        & ~is_pending_other
    )
    # llm은 유효 라벨을 실제로 채운 경우만 done. 빈/무효 라벨 llm 행은 재분류 대상으로 남긴다.
    # human은 사람 결정이므로 라벨 유무와 무관하게 존중.
    llm_done = classified_by.eq("llm") & has_valid_label
    human_done = classified_by.eq("human")
    classified_done = (llm_done | human_done) & ~is_pending_other
```
반환 필터(`~invalid_item_name & ~(classified_done | (review_status == REVIEW_APPROVED) | has_manual_value)`)는 그대로 둔다.

## Reference Code

### DB_FinProduct_Map.py (import 블록 — normalize_item_key 이미 존재)
```python
from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    MANUAL_SOURCE_BASE,
    OKPOS_ADJUSTMENT_ITEM_ID,
    allocate_manual_item_ids,
    canonical_source,
    ensure_allocator_columns,
    is_manual_allocated_source,
    item_block_size,
    normalize_item_key,   # ← 재사용
)
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
KEY_COLUMNS = ["store", "source", "brand", "item_id"]
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
```

### normalize_item_key (DB_ItemIdAllocator.py:51)
```python
_ITEM_KEY_RE = re.compile(r"[^0-9A-Za-z가-힣一-龥]+")

def normalize_item_key(name: object) -> str:
    text = "" if name is None else str(name)
    text = text.strip().lower()
    if not text or text == "nan":
        return ""
    key = _ITEM_KEY_RE.sub("", text)
    if key:
        return key
    return re.sub(r"\s+", "", text)
```

### _review_reason — "수동분류 미입력" 발생 지점 (DB_FinProduct_Map.py:774, 참고용·수정 대상 아님)
```python
def _review_reason(row):
    ...
    if label not in VALID_CATEGORIES:
        reasons.append("수동분류 미입력")
    elif label == "기타":
        reasons.append("기타 분류 확인")
    ...
    if not reasons and classified_by == "llm":
        reasons.append("LLM 분류 확인")
    return ", ".join(dict.fromkeys(reasons))
```

## Test Cases
1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import _classify_batch, find_llm_targets"` → 기대: ImportError/SyntaxError 없음
2. [find_llm_targets 재시도] 아래 스니펫 실행 → 빈 라벨 llm 행이 대상에 **포함**되고, 유효 라벨 llm 행은 **제외**:
   ```python
   import pandas as pd
   from modules.transform.pipelines.db.DB_FinProduct_Map import find_llm_targets, MAP_COLUMNS, KEY_COLUMNS
   all_items = pd.DataFrame([
       {"store":"송파삼전점","source":"posfeed","brand":"도리당","item_id":"1","item_key":"a","store_seq":"","item_seq":"","item_name":"빈라벨행","unitprice":"","대표메뉴":""},
       {"store":"송파삼전점","source":"posfeed","brand":"도리당","item_id":"2","item_key":"b","store_seq":"","item_seq":"","item_name":"유효행","unitprice":"","대표메뉴":""},
   ])
   m = pd.DataFrame(columns=MAP_COLUMNS)
   m.loc[0] = {**{c:"" for c in MAP_COLUMNS},"store":"송파삼전점","source":"posfeed","brand":"도리당","item_id":"1","item_name":"빈라벨행","수동분류_edit":"","classified_by":"llm","review_status_edit":"0"}
   m.loc[1] = {**{c:"" for c in MAP_COLUMNS},"store":"송파삼전점","source":"posfeed","brand":"도리당","item_id":"2","item_name":"유효행","표준_메뉴명_edit":"유효행","수동분류_edit":"옵션","classified_by":"llm","review_status_edit":"0"}
   t = find_llm_targets(all_items, m)
   ids = set(t["item_id"])
   assert "1" in ids and "2" not in ids, ids
   print("PASS retry-trap")
   ```
   → 기대: `PASS retry-trap`
3. [매칭 강건화 회귀] `_classify_batch`가 LLM 응답 item_name을 재포맷해도 매칭되는지 — `call_llm`을 monkeypatch해 응답 item_name에 대괄호/공백을 변형시킨 뒤 라벨이 채워지는지 확인. → 기대: `unmatched_count == 0`, 결과 행의 `수동분류_edit`가 VALID_CATEGORIES에 포함.
4. [dry-run 대상 집계] DAG conf `{"dry_run": true}`로 `llm_product_map` 실행 → `summary["llm_targets"]`가 수정 전 대비 증가(과거 갇힌 미입력 행 포함). 로그에 LLM 미호출 확인.
5. [소량 실집행] conf `{"limit": 20}` 실행 후 review CSV(`fin_product_map_review_input.csv`) 검수사유 분포 확인 → "수동분류 미입력" 감소, 기존 검수완료 330/자동복사 40/자동승인 14 행 수 유지(회귀 없음).

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~3(단위) 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 재실행
  5. 통과 시 4~5(dry-run/소량 실집행)로 end-to-end 확인
종료 조건: 1~3 전체 PASS + 4~5에서 미입력 감소·회귀 없음 + Constraints 위반 없음
```

## Constraints
- `DB_ItemIdAllocator.py`, posfeed/okpos 파이프라인, item_id 배정 규칙은 **수정 금지**. posfeed 같은 메뉴가 표기 차이(`계란 1개 추가` vs `계란 추가`)로 item_id가 갈라지는 건 원본 상품명 차이라 불가피하며 표준_메뉴명_edit로 흡수 — 병합/정규화 변경하지 않는다.
- `human` classified_by 행은 라벨이 비어도 재분류 대상에 넣지 않는다(사람 결정 존중).
- 0원 자동승인(`_auto_approve_zero_price`), 동일상품명 자동복사(`_seed_split_rows_from_siblings`), 규칙/LLM reconcile 경로는 건드리지 않는다.
- 위치기반(index) 폴백은 `len(results) == len(batch)`일 때만 적용(순서 보장 없으면 미적용).
- print 금지, logging 유지. 기존 warning 로그(`LLM 응답 item_name 미매칭`) 포맷 보존.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재: 해당 함수만 인플레이스 수정(전체 덮어쓰기 금지)
- import 모호 시: 기존 import 블록 패턴 따라가기(normalize_item_key는 추가 import 불필요)
- 타입 힌트/주석: 기존 파일과 동일 수준(주석은 WHY만 최소화)
- 변수명 스타일: snake_case, 기존 파일과 동일
