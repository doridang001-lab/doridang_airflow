# fin_product_map_review 편집 반영 수정

## Task
`fin_product_map_review_input.csv` 의 셀(`표준_메뉴명_edit`/`수동분류_edit`)을 사람이 직접 고치고 `DB_FinProduct_Map_Dags` 를 재실행하면 편집이 옛 값으로 되돌아간다. 원인은 이 리뷰 파일이 매 실행마다 `fin_product_map.csv` 기준으로 통째 재생성되는데, `apply_review_edits()` 가 `검수유무=1`(승인)인 행만 map 에 반영하기 때문. `검수유무` 값과 무관하게 "사람이 실제로 바꾼 셀"을 감지해 보존하도록 write-back 로직을 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_FinProduct_Map.py`
  - `apply_review_edits(map_df, review_df)` 함수 (`:639-669`) 재작성
- 그 외 파일 변경 없음 (`find_llm_targets` 는 기존 필터로 자동 동작)

## Implementation Steps

### 1) `apply_review_edits()` 재작성 (`DB_FinProduct_Map.py:639-669`)

현재 동작: `검수유무` 승인(=`REVIEW_APPROVED`) 행만 map 에 반영하고, 반영 시 `검수유무`를 강제로 `REVIEW_APPROVED` 로 올림.

변경 후 동작 스펙:
- 리뷰 전체 행을 `KEY_COLUMNS`(`["store","source","brand","item_key"]`)로 map 과 매칭. `drop_duplicates(subset=KEY_COLUMNS, keep="last")` 유지.
- 각 행에서 리뷰 값과 map 현재 값 비교:
  - `rv_std = _strip_text(review_row.get("표준_메뉴명_edit"))`
  - `rv_label = _strip_text(review_row.get("수동분류_edit"))`
  - `cur_std = _strip_text(result.loc[idx, "표준_메뉴명_edit"].iloc[-1])`
  - `cur_label = _strip_text(result.loc[idx, "수동분류_edit"].iloc[-1])`
  - `is_approved = _normalize_review_status(review_row.get(REVIEW_STATUS_COLUMN)) == REVIEW_APPROVED`
  - `std_changed = rv_std != "" and rv_std != cur_std`
  - `label_changed = rv_label in VALID_CATEGORIES and rv_label != cur_label`
- `is_approved or std_changed or label_changed` 일 때만 갱신:
  - `rv_std != ""` → `result.loc[idx, "표준_메뉴명_edit"] = rv_std`
    (승인인데 `rv_std == ""` 이면 기존처럼 `item_name` fallback)
  - `rv_label in VALID_CATEGORIES` → `result.loc[idx, "수동분류_edit"] = rv_label`
  - `result.loc[idx, "classified_by"] = "human"`
  - `result.loc[idx, "updated_at"] = TODAY`
  - `검수유무`(`REVIEW_STATUS_COLUMN`)는 **사용자가 넣은 값 그대로 유지** — 강제 승격 금지.
    승인 행이면 `REVIEW_APPROVED` 로 세팅해도 되지만, 대기(`REVIEW_PENDING`) 행을 승인으로 바꾸지 말 것.
- 핵심: 사람이 손댄 행을 `classified_by="human"` 으로 마킹 → 재생성(`build_review_rows`)이 map 의 사람 값을 재현하고, `find_llm_targets` 의 `classified_by.isin(["llm","human"])` 필터에 걸려 LLM 재분류에서 제외됨 → 편집이 두 경로 모두 보존.

### 2) LLM 클로버 방지 확인 (코드 변경 불필요)
`find_llm_targets()` (`:672-704`) 는 이미 `classified_by.isin(["llm","human"])` 를 제외하므로, 1)에서 `classified_by="human"` 세팅만으로 LLM 재분류 대상에서 빠진다. 추가 수정 없이 동작하는지 Test Case 로만 검증.

## Reference Code

### modules/transform/pipelines/db/DB_FinProduct_Map.py — 수정 대상 (현재 코드)
```python
REVIEW_STATUS_COLUMN = "review_status_edit"
REVIEW_STATUS_DISPLAY_COLUMN = "검수유무"
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
KEY_COLUMNS = ["store", "source", "brand", "item_key"]
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
TODAY = str(date.today())

def _strip_text(value: object) -> str:
    return str(value or "").strip()

def apply_review_edits(map_df: pd.DataFrame, review_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty or review_df.empty:
        return map_df
    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    approved = review_df[
        review_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED
    ].copy()
    if approved.empty:
        return result
    approved = approved.drop_duplicates(subset=KEY_COLUMNS, keep="last")
    for _, review_row in approved.iterrows():
        key_mask = pd.Series(True, index=result.index)
        for col in KEY_COLUMNS:
            key_mask = key_mask & (result[col].fillna("").astype(str).str.strip() == _strip_text(review_row.get(col)))
        if not bool(key_mask.any()):
            continue
        idx = result.index[key_mask]
        std_name = _strip_text(review_row.get("표준_메뉴명_edit")) or _strip_text(review_row.get("item_name"))
        label = _strip_text(review_row.get("수동분류_edit"))
        if label not in VALID_CATEGORIES:
            continue
        result.loc[idx, "표준_메뉴명_edit"] = std_name
        result.loc[idx, "수동분류_edit"] = label
        result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED
        result.loc[idx, "classified_by"] = "human"
        result.loc[idx, "updated_at"] = TODAY
    return result
```

### find_llm_targets — LLM 대상 제외 필터 (변경 없음, 참고용)
```python
def find_llm_targets(all_items, map_df):
    ...
    classified_by = merged["classified_by"].fillna("").astype(str).str.strip()
    review_status = merged[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip()
    has_manual_value = (
        (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (merged["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    )
    return merged[
        ~(classified_by.isin(["llm", "human"]) | (review_status == REVIEW_APPROVED) | has_manual_value)
    ][[...]].reset_index(drop=True)
```

## Test Cases

1. [편집 보존 — 대기 상태(검수유무=0)에서 표준명만 변경]
   `python -c "..."` 로 아래 검증:
   - `map_df`: `송파삼전점` 1행, `표준_메뉴명_edit="옛표준"`, `수동분류_edit="기타"`, `classified_by="llm"`, `검수유무="0"`
   - `review_df`: 같은 key, `표준_메뉴명_edit="새표준"`, `검수유무="0"`
   - `apply_review_edits(map_df, review_df)` → 기대: 해당 행 `표준_메뉴명_edit == "새표준"` AND `classified_by == "human"`
   - (수정 전에는 `"옛표준"` 유지 → 실패해야 정상)

2. [LLM 재클로버 방지]
   위 결과 맵을 `find_llm_targets(all_items, 결과맵)` 에 넣었을 때 해당 행이 **미포함**인지 assert
   → 기대: LLM 대상에서 제외 (classified_by="human")

3. [승인 행 기존 동작 유지]
   `review_df` 의 `검수유무="1"`, `수동분류_edit="메인"` → `apply_review_edits` 결과 `수동분류_edit=="메인"`, `검수유무` 승인 유지

4. [모듈 import 무결성]
   `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import apply_review_edits, find_llm_targets, migrate_product_map, llm_product_map"` → 기대: ImportError 없음

5. [dry-run 스모크]
   `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map; migrate_product_map(dry_run=True)"` → 기대: 예외 없이 종료, 파일 미변경

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
- `KEY_COLUMNS` 매칭 방식/컬럼 순서 변경 금지 (`store/source/brand/item_key`).
- `수동분류_edit` 는 반드시 `VALID_CATEGORIES` 중 하나만 map 에 기록. 유효하지 않은 값은 무시.
- 대기(`REVIEW_PENDING`) 행을 승인(`REVIEW_APPROVED`)으로 **강제 승격하지 말 것** — 편집만 보존, 승인 게이트는 사람 몫.
- 기존 "승인=1 → 마트 승격" 게이트(`DB_FinProduct.py` 의 `_load_map_enrichment_df`)는 건드리지 않는다. 이번 수정은 **리뷰 파일 내 편집 보존**에 한정.
- `write_map`/`write_review_map`/`write_recently_map` 시그니처·저장 경로 변경 금지.
- print 금지, logging 사용. import 절대경로 유지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
