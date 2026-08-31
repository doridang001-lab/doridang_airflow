# fin_product_map 수동분류 빈칸 LLM 분류 수정

## Task

`DB_FinProduct_Map_Dags`는 수동분류가 빈 상품을 로컬 LLM(Ollama)으로 채워 검수 파일로 내보내야 하는데, 실제 산출물에는 수동분류 빈칸이 그대로 나간다.
원인은 (1) LLM 대상 산정이 unified_sales parquet에만 묶여 있어 POS 마스터로만 유입된 map 행을 아예 보지 못하고, (2) LLM이 무효 응답을 줘도 조용히 빈칸이 되기 때문이다.
판매이력 없는 map 행도 LLM이 분류하게 하고, LLM 실패는 표식·알림으로 드러나게 만든다.

### 실측 근거 (2026-07-23 11:00 산출물)

| 항목 | 값 |
| --- | --- |
| `fin_product_map.csv` 전체 | 774행 |
| `수동분류_edit` 빈칸 | 46행 (전부 `classified_by=""`, `검수유무=0`) |
| `fin_product_map_review_input.csv` 검수사유 | 46행 모두 "수동분류 미입력" |
| `scan_target_items()` (parquet 스캔) | 728행 |
| 빈칸 46행이 parquet 스캔에 존재? | **0/46 (전부 없음)** |
| `find_llm_targets(all_items, map_df)` 결과 | **0건** |

빈칸 46행은 전부 `송파삼전점 / posfeed / 도리당`이며, POS 마스터(`fin_product` CSV → `apply_recently_edits`)로만 map에 들어온 행이다.
마스터의 `대메뉴`가 공란이라 `_label_from_recently()`가 라벨을 못 만들고, 라벨 `""` + `classified_by=""` + `PENDING`으로 삽입된다.
`build_join_map()`은 `category != ""` 조건이라 이 46행은 `fin_product_map_join.csv`에서도 통째로 빠진다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

수정:
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` (핵심 변경 전부)
- `dags/db/DB_FinProduct_Map_Dags.py` (`run_llm()` 텔레그램 메시지에 실패 건수 한 줄 추가)

생성:
- `tests/test_fin_product_map_llm_targets.py` (신규 테스트)

## Implementation Steps

### 1. `find_llm_targets()` — 대상 집합을 map까지 확장 (`DB_FinProduct_Map.py:974`)

현재는 `all_items`(parquet 스캔)를 좌변으로 `map_df`를 left merge하므로 **map에만 있는 행은 후보에 들어가지 못한다.**
기존 merge 결과를 `parquet_targets`로 유지하고, 여기에 map 전용 미분류 행을 합친다.

`map_df`에서 다음 조건 행을 추출한다:
- `_normalize_review_status(review_status_edit) != REVIEW_APPROVED`
- `수동분류_edit`이 `VALID_CATEGORIES`에 없음(빈칸 포함), 또는 `기타` + 미승인 (기존 `is_pending_other`와 동일 규칙)
- `item_name != ""`, `~item_name.str.fullmatch(r"\d+")`, `item_id != ""`, `item_id != OKPOS_ADJUSTMENT_ITEM_ID`

출력 컬럼 `["item_id", "item_key", "store_seq", "item_seq", "store", "source", "brand", "item_name", "unitprice", "대표메뉴"]`로 reindex한 뒤
`pd.concat([parquet_targets, map_only]).drop_duplicates(subset=KEY_COLUMNS, keep="first").reset_index(drop=True)`.

`classified_by in {"llm", "human"}` 제외 판정은 기존 로직 그대로 둔다(재분류 폭주 방지).

### 2. `llm_product_map()` — parquet이 비어도 map만으로 동작 (`:1490`)

- `if all_items.empty: return summary` 조기 반환을 `if all_items.empty and map_df.empty:`로 완화.
- `summary["target_rows"]`는 `len(all_items)` 대신 `all_items` + map-only 대상 합계 기준.
- `already_llm_classified`는 음수가 나오지 않도록 `max(0, target_rows - llm_targets)`로 계산.

### 3. `migrate_product_map()` — parquet에 없는 map 행 보존 (`:1425`)

`existing.merge(current_values, on=KEY_COLUMNS, how="inner")`가 매 실행마다 parquet 미존재 행을 버린다.
1번을 고쳐 LLM이 분류해도 다음 migrate에서 결과가 사라지므로 함께 고친다.

- `how="inner"` → `how="left"`.
- 값 갱신 루프(`:1426-1433`)에서 `current_col`이 NaN/빈 문자열이면 기존 값을 유지:
  ```python
  current = existing[current_col]
  has_current = current.notna() & (current.astype(str).str.strip() != "")
  existing[col] = current.where(has_current, existing[col])
  ```
- 기존 `brand`/`unitprice`의 "빈값일 때만 채움" 분기는 그대로 유지.

### 4. `_classify_batch()` — 무효 응답 단건 재시도 + 실패 표식 (`:1336`)

- 시그니처에 `allow_retry: bool = True` 추가(재귀 방지용).
- 행 생성 후 `수동분류_edit`이 `VALID_CATEGORIES`에 없고 `rule_result`도 없는 항목을 모은다.
- `allow_retry`이고 `len(batch) > 1`이면 해당 항목만 `_classify_batch([item], examples, rules, allow_retry=False)`로 1회 단건 재요청해 결과를 교체.
- 재시도 후에도 무효면 **라벨은 빈칸 유지**하고 `classified_by = "llm_unresolved"`로 표시.
- `logger.warning`으로 재시도 건수 / 최종 실패 건수 기록.

### 5. `_review_reason()` — 실패 사유 노출 (`:789`)

- `classified_by == "llm_unresolved"` 분기 추가 → `"LLM 분류 실패, 수동분류 미입력"`.
- 나머지 로직은 그대로(빈 라벨이면 기존 "수동분류 미입력"도 계속 동작).

### 6. 요약/알림에 미해결 건수 노출

- `llm_product_map()` summary에 `"llm_unresolved": int` 추가 (`map_df["classified_by"] == "llm_unresolved"` 카운트).
- `dags/db/DB_FinProduct_Map_Dags.py`의 `run_llm()` 텔레그램 본문(`new_pending > 0` 알림)에 `LLM 분류 실패: {n}건` 한 줄 추가. 별도 알림은 만들지 않는다.

## Reference Code

### modules/transform/pipelines/db/DB_FinProduct_Map.py — 현재 `find_llm_targets` (수정 대상)

```python
def find_llm_targets(all_items: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    if all_items.empty:
        return all_items.copy()
    if map_df.empty:
        return all_items.copy()

    status = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴",
        REVIEW_STATUS_COLUMN, "classified_by",
    ):
        status[col] = status[col].fillna("").astype(str).str.strip()
    status[REVIEW_STATUS_COLUMN] = status[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    status = status.drop_duplicates(subset=KEY_COLUMNS, keep="last")

    merged = all_items.merge(
        status[KEY_COLUMNS + ["표준_메뉴명_edit", "수동분류_edit", REVIEW_STATUS_COLUMN, "classified_by"]],
        on=KEY_COLUMNS,
        how="left",
    )
    classified_by = merged["classified_by"].fillna("").astype(str).str.strip()
    review_status = merged[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip()
    manual_label = merged["수동분류_edit"].fillna("").astype(str).str.strip()
    item_name = merged["item_name"].fillna("").astype(str).str.strip()
    invalid_item_name = item_name.str.fullmatch(r"\d+")
    is_pending_other = manual_label.eq("기타") & review_status.ne(REVIEW_APPROVED)
    has_valid_label = manual_label.isin(VALID_CATEGORIES)
    has_manual_value = (
        (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & has_valid_label
        & ~is_pending_other
    )
    llm_done = classified_by.eq("llm") & has_valid_label
    human_done = classified_by.eq("human")
    classified_done = (llm_done | human_done) & ~is_pending_other
    return merged[
        ~invalid_item_name & ~(classified_done | (review_status == REVIEW_APPROVED) | has_manual_value)
    ][[
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "대표메뉴",
    ]].reset_index(drop=True)
```

### 현재 `_classify_batch` (수정 대상, `:1336`)

```python
def _classify_batch(batch: list[dict], examples: list[dict], rules: list[dict]) -> list[dict]:
    results = call_llm(build_prompt(batch, examples, rules=rules))
    by_name: dict[str, dict] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        raw = str(result.get("item_name", "")).strip()
        if raw:
            by_name.setdefault(raw, result)
        normalized_key = normalize_item_key(raw)
        if normalized_key:
            by_name.setdefault(normalized_key, result)
    rows = []
    unmatched_count = 0
    positional_ok = len(results) == len(batch)
    for pos, item in enumerate(batch):
        raw_item = str(item.get("item_name", "")).strip()
        classified = by_name.get(raw_item) or by_name.get(normalize_item_key(raw_item))
        if not classified and positional_ok and isinstance(results[pos], dict):
            classified = results[pos]
        if not classified:
            classified = {}
            unmatched_count += 1
        llm_normalized = _normalize_classification(item, classified)
        rule_result = classify_by_rules(item, rules)
        resolved = reconcile(rule_result, llm_normalized)
        normalized = _normalize_classification(item, resolved)
        rows.append({
            "item_id": item.get("item_id", ""),
            "item_key": item.get("item_key", ""),
            "store_seq": item.get("store_seq", ""),
            "item_seq": item.get("item_seq", ""),
            "store": item["store"],
            "source": item["source"],
            "brand": item.get("brand", ""),
            "item_name": item["item_name"],
            "unitprice": item.get("unitprice", ""),
            "대표메뉴": item.get("대표메뉴", ""),
            **normalized,
            REVIEW_STATUS_COLUMN: REVIEW_PENDING,
            "classified_by": resolved.get("classified_by", "llm"),
            "updated_at": TODAY,
        })
    if unmatched_count:
        logger.warning("LLM 응답 item_name 미매칭: %d/%d건", unmatched_count, len(batch))
    return rows
```

### 라벨을 조용히 비우는 지점 (`_normalize_classification`, `:258`)

```python
def _normalize_classification(item: dict, classified: dict) -> dict[str, str]:
    item_name = _strip_text(item.get("item_name"))
    values = dict(classified)
    values.update(_classification_override(item_name))

    label = _strip_text(values.get("수동분류_edit") or values.get("수동분류"))
    if label not in VALID_CATEGORIES:
        label = ""          # <-- 재시도·표식 없이 빈칸

    std_name = _strip_text(values.get("표준_메뉴명_edit") or values.get("표준_메뉴명")) or item_name
    return {"표준_메뉴명_edit": std_name, "수동분류_edit": label}
```

### 빈칸 행을 만드는 지점 (`apply_recently_edits`, `:529-552`)

```python
            else:
                result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_PENDING
                result.loc[idx, "classified_by"] = ""
...
        new_rows.append({
            ...
            "수동분류_edit": label if label in VALID_CATEGORIES else "",
            REVIEW_STATUS_COLUMN: REVIEW_APPROVED if label in VALID_CATEGORIES else REVIEW_PENDING,
            "classified_by": "human" if label in VALID_CATEGORIES else "",
            ...
        })
```

### `migrate_product_map`의 inner merge (`:1420-1440`, 수정 대상)

```python
    if not result.empty and not existing.empty:
        current_value_cols = [col for col in [
            "item_id", "store_seq", "item_seq", "item_name", "unitprice", "대표메뉴",
        ] if col not in KEY_COLUMNS]
        current_values = result[KEY_COLUMNS + current_value_cols].drop_duplicates(subset=KEY_COLUMNS, keep="last")
        existing = existing.merge(current_values, on=KEY_COLUMNS, how="inner", suffixes=("", "_current"))
        for col in current_value_cols:
            current_col = f"{col}_current"
            if current_col in existing.columns:
                if col in {"brand", "unitprice"}:
                    existing[col] = existing[col].where(existing[col].astype(str).str.strip() != "", existing[current_col])
                else:
                    existing[col] = existing[current_col]
                existing = existing.drop(columns=[current_col])
```

### 참고 상수 (`DB_FinProduct_Map.py` 상단)

```python
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
KEY_COLUMNS = ["store", "source", "brand", "item_id"]
REVIEW_STATUS_COLUMN = "review_status_edit"
REVIEW_STATUS_DISPLAY_COLUMN = "검수유무"
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
```

### 테스트 monkeypatch 패턴 (`tests/test_fin_product_manual_unknown_integration.py`)

```python
import pandas as pd
import pytest
from modules.transform.pipelines.db import DB_FinProduct_Map as M

def test_xxx(monkeypatch):
    monkeypatch.setattr(M, "scan_target_items", lambda **kw: pd.DataFrame(...))
    monkeypatch.setattr(M, "load_map", lambda: pd.DataFrame(...))
    monkeypatch.setattr(M, "load_review_map", lambda: M._empty_review_map())
    monkeypatch.setattr(M, "load_recently_map", lambda: pd.DataFrame(columns=M.RECENTLY_COLUMNS))
    monkeypatch.setattr(M, "call_llm", lambda prompt: [{"item_name": "...", "표준_메뉴명_edit": "...", "수동분류_edit": "메인"}])
```

## Test Cases

1. [map 전용 행이 LLM 대상에 포함] `python -m pytest tests/test_fin_product_map_llm_targets.py -q` → 기대: parquet에 없고 map에만 있는 미분류 행이 `find_llm_targets()` 결과에 포함, 전체 PASS
2. [승인/유효라벨 행 제외] 같은 테스트 파일 — `검수유무=1` 또는 유효 라벨 보유 map 행은 대상에서 제외되는지 → 기대: PASS
3. [parquet 비어도 동작] `all_items`가 빈 DataFrame이어도 `llm_product_map(dry_run=False)`가 map-only 행을 분류(`call_llm` monkeypatch) → 기대: `summary["llm_targets"] > 0`, `new_classified > 0`
4. [무효 응답 재시도] `call_llm`이 첫 호출에 `수동분류_edit="없는분류"`를 반환하도록 monkeypatch → 기대: 단건 재요청 1회 발생, 최종 라벨 빈칸 + `classified_by == "llm_unresolved"`
5. [migrate 보존] `migrate_product_map(dry_run=True)`에서 parquet에 없는 기존 map 행이 결과에 남는지 → 기대: 보존됨(행 수 감소 없음)
6. [기존 회귀] `python -m pytest tests/test_fin_product_manual_unknown_integration.py tests/test_fin_product_rule_staging.py -q` → 기대: 전체 PASS
7. [DAG import] `python -c "from dags.db.DB_FinProduct_Map_Dags import dag; print(dag.dag_id)"` → 기대: ImportError 없음, `DB_FinProduct_Map_Dags` 출력
8. [실측 확인 — 실행 후]
   ```
   python -c "import pandas as pd; from modules.transform.utility.paths import FIN_PRODUCT_MAP_CSV_PATH as p; d=pd.read_csv(p,dtype=str,encoding='utf-8-sig').fillna(''); e=d[d['수동분류_edit'].str.strip()=='']; print(len(d), len(e)); print(e.groupby('classified_by').size())"
   ```
   → 기대: 빈칸 46 → 0 (또는 `llm_unresolved`로만 소수 잔존), `fin_product_map_join.csv` 행 수 증가

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

- LLM 실패 시 `기타` 등으로 **임의 채움 금지** — 빈칸 유지 + `classified_by="llm_unresolved"` 표식만 남긴다(오분류 방지, 사용자 결정 사항).
- `classified_by in {"llm", "human"}` 이면서 유효 라벨이 있는 행은 재분류 대상에 넣지 않는다(LLM 호출 폭주 방지).
- `_classify_batch` 재시도는 **1회, 단건만** — 무한 재귀 금지(`allow_retry=False`로 재호출).
- 승인 완료(`검수유무=1`) 행의 사람이 입력한 값은 절대 덮어쓰지 않는다.
- 컬럼 순서·이름(`MAP_COLUMNS`, `REVIEW_COLUMNS`, `RECENTLY_COLUMNS`)은 변경 금지 — Excel 검수 파일 포맷 호환.
- CSV 인코딩 `utf-8-sig`, 원자적 교체(`_safe_replace`) 패턴 유지.
- 실 LLM 호출 없이 테스트가 돌도록 `call_llm`은 반드시 monkeypatch.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
