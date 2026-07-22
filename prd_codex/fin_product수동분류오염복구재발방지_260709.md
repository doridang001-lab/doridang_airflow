# fin_product 수동분류 오염 복구 + 재발 방지

## Task

`fin_product_map_review_input.csv`의 `수동분류_edit`가 379행 중 341행(90%)이 가짜 `기타`로 뭉개졌고, 그 전부가 `검수유무=1`(검수완료)로 도장되어 `fin_product_mart.csv`까지 오염됐다. 원인은 `DB_FinProduct_Map.py`의 map → recently → map 자가봉인 피드백 루프다. 코드를 먼저 고치고, 7/8 백업에서 사람 분류·표준명을 복구한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Root Cause (반드시 먼저 이해할 것)

사용자가 처음 지목한 `dags/db/DB_FinProduct_Dags.py`는 **무죄**다. 오염된 결과를 마트로 퍼뜨리는 하류일 뿐이다. `.tmp/migrate_fin_product_remaining_review_classes.py`도 무죄다 (`fin_product_grp_input.csv`만 건드림).

진범은 `dags/db/DB_FinProduct_Map_Dags.py` → `modules/transform/pipelines/db/DB_FinProduct_Map.py`:

1. `_label_from_recently()` (419-429행)는 `대메뉴`를 `메인메뉴→메인`, `사이드→사이드`, `추가메뉴→토핑`으로 매핑하고 **그 외 전부 `기타` 반환**. 그런데 `fin_product_grp.csv`의 `대메뉴` 실제 값은 `""`(13,434) / `"도리당"`(4,214) / `"[홀]도리당"`(275) — **브랜드 문자열이지 카테고리가 아니다.** 세 매핑 어느 것도 안 맞아 항상 `기타`.

2. `apply_recently_edits()` (480-483행, 501-502행)는 그 `기타`를 쓰면서 `classified_by="human"` + `review_status_edit=1`로 도장. 사람이 만진 적 없는 값에 사람 도장이 찍힌다.

3. `find_llm_targets()` (870-871행)는 `classified_by in ("llm","human")` 이거나 승인된 행을 영구 제외. → 한 번 `기타/human/승인`이 찍히면 LLM도 사람도 다시 손대지 않는다.

4. `build_recently_rows()`는 `map_df`의 `review_status_edit`를 그대로 `fin_product_map_recently.csv`에 복사하고, 그 파일을 다시 `apply_recently_edits()`가 읽는다. **map → recently → map 순환**으로 오염이 매일 재확인된다.

부수 결함:
- `_classify_batch()` (1076-1080행): LLM 응답에서 `item_name`으로 못 찾으면 `results[item_idx]` **위치 폴백** → 남의 분류가 붙는다.
- `build_prompt()` (884-887행): 각 항목에 `대표메뉴="..."`를 설명 없이 넣어줘 LLM이 표준명으로 베낀다 (현재 표준명==대표메뉴 22건). `대표메뉴`는 `scan_target_items()`가 `menu_name` 최빈값으로 만든 **상위 주문 메뉴**라 개별 상품 표준명과 무관하다 (예: `계란찜(포장)` → 대표메뉴 `1인 순살 닭도리탕`).

파괴 실측 (7/8 백업 대비 공통 239행 중 211행 파괴):

| 원래 분류 | 파괴된 건수 |
|---|---|
| 옵션 | 82 |
| 토핑 | 60 |
| 리뷰 | 21 |
| 음료 | 17 |
| 메인 | 13 |
| 사이드 | 13 |
| 1인 | 3 |
| 주류 | 2 |

## Files to Create / Modify

**수정:**
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` — 재발 방지 수정 6곳 (Part 1)

**생성 (일회성, 커밋 금지):**
- `.tmp/restore_fin_product_map_classes.py` — 데이터 복구 스크립트 (Part 2)

**건드리지 말 것:**
- `dags/db/DB_FinProduct_Dags.py`, `dags/db/DB_FinProduct_Map_Dags.py`, `modules/transform/pipelines/db/DB_FinProduct.py`

## Implementation Steps

### Part 1 — 코드 수정 (반드시 복구보다 먼저)

복구를 먼저 하면 `DB_FinProduct_Map_Dags`가 하루 만에 복구본을 다시 `기타`로 덮는다.
전부 `modules/transform/pipelines/db/DB_FinProduct_Map.py` 한 파일이다.

**1-1. `_label_from_recently()` — `기타` 강제 폴백 제거**

```python
def _label_from_recently(row: pd.Series, current_label: str = "") -> str:
    if current_label in VALID_CATEGORIES:
        return current_label
    major = _strip_text(row.get("대메뉴"))
    if major == "메인메뉴":
        return "메인"
    if major == "사이드":
        return "사이드"
    if major == "추가메뉴":
        return "토핑"
    return ""          # 기존: return "기타"
```

`current_label` 유효 시 그대로 반환하는 기존 가드는 유지.

**1-2. `apply_recently_edits()` — 승인/human 도장 조건화 (핵심)**

기존 행 갱신 분기(465-484행)와 신규 행 생성 분기(486-504행) 양쪽에서:

- 라벨이 `VALID_CATEGORIES`에 없으면 `수동분류_edit`를 **쓰지 않고** 기존 값 보존
- `classified_by="human"` / `REVIEW_APPROVED`는 **라벨이 실제로 도출됐을 때만** 찍는다. 아니면 `REVIEW_PENDING` + `classified_by=""` 유지
- `표준_메뉴명_edit` 반영은 현행 유지 (recently의 표준명은 사람이 쓴 값)

이것이 map → recently → map 피드백 루프를 끊는 핵심 수정이다.

**1-3. `find_llm_targets()` — `기타` 미검수 행을 재분류 대상으로**

`has_manual_value` (866-869행) 계산에서 `수동분류_edit == "기타"` 이면서 `review_status_edit != "1"` 인 행은 "분류됨"으로 치지 않는다. 제외 조건(870-871행)의 `classified_by.isin(["llm","human"])`도 승인되지 않은 `기타` 행에는 적용하지 않는다.

**1-4. `_classify_batch()` — 위치 폴백 제거**

```python
classified = by_name.get(item["item_name"], {})
# 아래 두 줄 삭제
# if not classified and item_idx < len(results):
#     classified = results[item_idx]
```

이름 매칭 실패 시 `_normalize_classification()`이 빈 dict를 받아 표준명=`item_name`, 라벨=`""`(1-5 수정 후)로 떨어지고 미검수로 남는다. 미매칭 건수를 `logger.warning`으로 남긴다.

**1-5. `_normalize_classification()` — 무효 라벨을 `기타`로 만들지 않기**

```python
label = _strip_text(values.get("수동분류_edit") or values.get("수동분류"))
if label not in VALID_CATEGORIES:
    label = ""        # 기존: label = "기타"
```

`_review_reason()` (749행)이 이미 빈 라벨을 "수동분류 미입력"으로 표시하므로 검수 시트에 자연히 드러난다.

**1-6. `build_prompt()` — 대표메뉴 오염 차단**

`items_text`에서 `대표메뉴="..."`를 **제거**한다. 현재 프롬프트에서 아무 역할도 못 하면서 22건을 오염시켰다. 남긴다면 규칙에 반드시 한 줄 추가:

```
- 대표메뉴는 이 상품이 함께 팔린 상위 주문 메뉴일 뿐입니다. 절대 표준_메뉴명_edit으로 사용하지 마세요.
```

### Part 2 — 데이터 복구 스크립트

`.tmp/restore_fin_product_map_classes.py` (일회성, 커밋하지 않음).
기존 `.tmp/migrate_fin_product_remaining_review_classes.py`의 백업·검증 패턴을 그대로 따른다.

- BASE: `C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product`
- 복구 소스: `_backup/fin_product_map_review_input.before_class_migrate_20260708_154650.csv`
- 조인 키: `["store", "source", "brand", "item_key"]` (= `DB_FinProduct_Map.KEY_COLUMNS`)

**`item_id`로 조인하면 안 된다.** 그 사이 scoped numeric item_id 마이그레이션이 있어 239건만 맞는다. `item_key`로는 **371/379가 매칭**되고 그중 343건이 비-`기타` 값을 갖는다.

절차:

1. `_backup/`에 `fin_product_map.csv`, `fin_product_map_review_input.csv`를 `*.before_class_restore_<stamp>.csv`로 `shutil.copy2` 백업
2. `fin_product_map.csv`를 읽고 백업본과 `KEY_COLUMNS`로 left join
3. 매칭된 371행: 백업의 `수동분류_edit`와 `표준_메뉴명_edit`를 **둘 다** 복원하고 `classified_by="human"`, `review_status_edit="1"` 설정
4. 미매칭 8행: `수동분류_edit=""`, `review_status_edit="0"`, `classified_by=""` 로 두어 다음 DAG 실행에서 LLM 재분류 대상이 되게 한다. 목록:
   `2인 순살 반반세트`, `메밀 물 막국수 세트`, `순살 닭도리 정식(2인이상)`, `16900`, `환타 오렌지 355ML (캔)`(×2), `미니 김가루`, `[복날한정] 1인 미나리 수삼 백숙`
5. 기존 헬퍼로 파생 파일 재생성 (**새로 짜지 말 것**):
   `write_map()` → `write_review_map(build_review_rows(map_df))` → `write_recently_map()` → `write_join_map()`
6. `DB_FinProduct.build_fin_product_mart()` 호출로 `fin_product_mart.csv` 재생성
7. 복구 전/후 `수동분류_edit.value_counts()` 출력

## Reference Code

### modules/transform/pipelines/db/DB_FinProduct_Map.py (핵심 상수 + 수정 대상 함수)

```python
from modules.transform.utility.paths import (
    FIN_PRODUCT_CSV_PATH, FIN_PRODUCT_MAP_JOIN_CSV_PATH, FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_RECENTLY_CSV_PATH, FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH, MART_DB,
    existing_fin_product_csv_path, existing_fin_product_map_review_csv_path,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    canonical_source, normalize_item_key, allocate_manual_item_ids, item_block_size,
)

REVIEW_STATUS_COLUMN = "review_status_edit"
REVIEW_STATUS_DISPLAY_COLUMN = "검수유무"
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
KEY_COLUMNS = ["store", "source", "brand", "item_key"]
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
MAP_COLUMNS = [
    "item_id", "item_key", "store_seq", "item_seq", "store", "source", "brand",
    "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴",
    REVIEW_STATUS_COLUMN, "classified_by", "updated_at",
]

def write_map(map_df: pd.DataFrame) -> None: ...
def write_review_map(review_df: pd.DataFrame) -> None: ...
def write_recently_map(map_df: pd.DataFrame) -> None: ...
def write_join_map(map_df: pd.DataFrame) -> dict: ...
def build_review_rows(map_df: pd.DataFrame) -> pd.DataFrame: ...
def load_map() -> pd.DataFrame: ...
def migrate_product_map(dry_run: bool = False, **context) -> dict: ...
def llm_product_map(dry_run: bool = False, limit: int | None = None, **context) -> dict: ...
```

### modules/transform/pipelines/db/DB_FinProduct.py (오염 전파 경로 — 수정 대상 아님)

```python
def _load_map_enrichment_df() -> pd.DataFrame:
    """fin_product_map_review_input.csv에서 검수 완료된 분석용 표준명/분류만 로드."""
    df = pd.read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df["검수유무"] = df["검수유무"].astype(str).str.strip()
    approved = df["검수유무"].str.lower().isin(_MAP_APPROVED)
    df = df[approved & (df["source"] != "") & (df["item_id"] != "") & (df["표준상품명"] != "")].copy()
    return df.drop_duplicates(subset=["source", "item_id"], keep="last")[_MAP_ENRICHMENT_COLS]

def build_fin_product_mart(**context) -> str: ...
```

### .tmp/migrate_fin_product_remaining_review_classes.py (백업 패턴 참고)

```python
from datetime import datetime
from pathlib import Path
import shutil
import sys
import pandas as pd

BASE = Path(r"C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product")
BACKUP = BASE / "_backup"

def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")

def main() -> int:
    sys.path.insert(0, str(Path(r"C:\airflow")))
    from modules.transform.pipelines.db.DB_FinProduct import build_fin_product_mart

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    BACKUP.mkdir(parents=True, exist_ok=True)
    shutil.copy2(GRP, BACKUP / f"fin_product_grp_input.before_remaining_class_migrate_{stamp}.csv")
    grp.to_csv(GRP, index=False, encoding="utf-8-sig", lineterminator="\n")
    print(build_fin_product_mart())
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

## Test Cases

1. **[모듈 import]** `python -c "import modules.transform.pipelines.db.DB_FinProduct_Map as m; print('ok')"` → 기대: ImportError/SyntaxError 없음

2. **[DAG import]** `python -c "from dags.db.DB_FinProduct_Map_Dags import dag; print(dag.dag_id)"` → 기대: `DB_FinProduct_Map_Dags`

3. **[`기타` 폴백 제거]** `python -c "import pandas as pd; from modules.transform.pipelines.db.DB_FinProduct_Map import _label_from_recently as f; print(repr(f(pd.Series({'대메뉴':'도리당'}))))"` → 기대: `''` (기존 코드는 `'기타'`)

4. **[유효 라벨 보존]** 같은 함수에 `current_label='옵션'` 전달 → 기대: `'옵션'`

5. **[무효 라벨 → 빈 값]** `_normalize_classification({'item_name':'테스트'}, {'수동분류_edit':'없는분류'})` → 기대: `수동분류_edit == ''` (기존 `'기타'`)

6. **[위치 폴백 제거 확인]** `_classify_batch` 소스에 `results[item_idx]` 문자열이 남아있지 않을 것

7. **[복구 검증]** 복구 스크립트 실행 후 `fin_product_map_review_input.csv`의 `수동분류_edit.value_counts()` → 기대: 7/8 백업과 일치. 대략 `옵션 120 / 토핑 98 / 메인 37 / 사이드 26 / 리뷰 23 / 음료 22 / 주류 14 / 기타 28 / 1인 4 / 세트 2`. **`기타`가 341 → 30 안팎으로 떨어져야 한다.**

8. **[마트 전파]** `fin_product_mart.csv`의 `수동분류` 분포가 위와 같이 따라 움직일 것 → 기대: `기타` 비중 90% → 10% 미만

9. **[재발 방지 — 핵심 회귀 테스트]**
   ```
   python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map; print(migrate_product_map(dry_run=True))"
   ```
   그다음 `dry_run=False`로 실제 실행 → `수동분류_edit` 분포 재비교 → **기대: 변화 없음.**
   (수정 전 코드로 돌리면 여기서 다시 `기타`로 무너진다. 이것이 회귀 테스트다.)

10. **[LLM 재분류 대상]** `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import llm_product_map; print(llm_product_map(dry_run=True, limit=20)['llm_targets'])"` → 기대: 미매칭 8건 + 미검수 `기타` 행이 포함된 양수

11. **[변경 범위]** `git status --short` → 기대: `modules/transform/pipelines/db/DB_FinProduct_Map.py` 외 수정 없음. `.tmp/restore_*.py`는 커밋 대상 아님

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~6 (코드 수정 검증) 실행
  2. Test Cases 7~8 (복구 검증) 실행
  3. Test Case 9 (회귀 테스트) 실행 — 가장 중요
  4. Test Cases 10~11 실행
  5. FAIL 항목 → 원인 분석 → 코드 수정 → 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- **순서 엄수**: Part 1(코드 수정) → Part 2(데이터 복구). 반대로 하면 다음 DAG 실행이 복구본을 다시 뭉갠다.
- 복구 조인 키는 반드시 `["store","source","brand","item_key"]`. `item_id`는 scoped numeric 마이그레이션으로 깨져 있다.
- 복구 스크립트는 파생 파일을 직접 쓰지 말고 **기존 헬퍼**(`write_map`, `write_review_map`, `write_recently_map`, `write_join_map`)를 호출한다.
- 복구 전 `fin_product_map.csv`, `fin_product_map_review_input.csv`를 `_backup/`에 `shutil.copy2`로 반드시 백업한다.
- `dags/db/DB_FinProduct_Dags.py`, `DB_FinProduct.py`는 수정하지 않는다 (무죄, 하류 소비자일 뿐).
- `.tmp/restore_fin_product_map_classes.py`는 커밋하지 않는다.
- `표준_메뉴명_edit`도 백업 값으로 복원한다 (사용자 승인 완료). 예: `[들깨] 우거지 닭도리탕` → `들깨 우거지 닭도리탕`, `펩시 제로 355ml` → `펩시제로 355ml`
- `item_name="16900"`(unified_sales에서 흘러든 쓰레기 행)과 `2인 순살 반반세트`의 `unitprice=-53900`(음수)는 **이번 복구 범위 밖**이다. 건드리지 말고 로그로만 남긴다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
