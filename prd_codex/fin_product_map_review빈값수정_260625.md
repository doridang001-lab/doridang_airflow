# fin_product_map_review 빈값 수정

## Task
DAG 실행 후 `fin_product_map_review.csv`가 헤더만 있고 데이터가 없는 버그를 수정한다.
원인: `_review_reason()` 함수가 LLM이 분류한 항목(`classified_by=llm`)에 대해 검수사유를 생성하지 않아, `build_review_rows`에서 전부 필터 아웃됨.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` — `_review_reason()` 함수 (line 365)

## Implementation Steps

1. `_review_reason()` 함수 끝 부분 수정 (line 383~384):

**현재 코드:**
```python
    if menu_count > 1 and label in {"메인", "1인"}:
        reasons.append("부모메뉴 후보 다수")
    return ", ".join(dict.fromkeys(reasons))
```

**수정 후:**
```python
    if menu_count > 1 and label in {"메인", "1인"}:
        reasons.append("부모메뉴 후보 다수")
    if not reasons and _strip_text(row.get("classified_by")) == "llm":
        reasons.append("LLM 분류 확인")
    return ", ".join(dict.fromkeys(reasons))
```

**핵심 논리**: `review_status_edit != 'Y'`이고 다른 검수사유가 없지만 LLM이 분류한 항목은 사람이 한 번 확인해야 하므로 "LLM 분류 확인" 사유를 부여한다.

## Reference Code
### modules/transform/pipelines/db/DB_FinProduct_Map.py (관련 함수)
```python
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트"]
KEY_COLUMNS = ["store", "source", "item_name"]

def _normalize_review_status(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() == "approved" or text.upper() == "Y":
        return "Y"
    return "N"

def _strip_text(value: object) -> str:
    return str(value or "").strip()

def _review_reason(row: pd.Series) -> str:
    if _normalize_review_status(row.get("review_status_edit")) == "Y":
        return ""
    reasons = []
    std_name = _strip_text(row.get("표준_메뉴명_edit"))
    label = _strip_text(row.get("수동분류_edit"))
    try:
        menu_count = int(float(_strip_text(row.get("menu_name_후보수")) or "0"))
    except ValueError:
        menu_count = 0

    if not std_name:
        reasons.append("표준명 미입력")
    if label not in VALID_CATEGORIES:
        reasons.append("수동분류 미입력")
    elif label == "기타":
        reasons.append("기타 분류 확인")
    if menu_count > 1 and label in {"메인", "1인"}:
        reasons.append("부모메뉴 후보 다수")
    return ", ".join(dict.fromkeys(reasons))  # ← 여기에 LLM 조건 추가

def build_review_rows(map_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty:
        return _empty_review_map()
    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    result["검수사유"] = result.apply(_review_reason, axis=1)
    result = result[result["검수사유"].fillna("").astype(str).str.strip() != ""]
    if result.empty:
        return _empty_review_map()
    return (
        result.reindex(columns=REVIEW_COLUMNS, fill_value="")
        .drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(["store", "source", "검수사유", "item_name"])
        .reset_index(drop=True)
    )
```

## Test Cases

1. **단위 테스트 - LLM 분류 항목 검수사유 생성 확인**
```python
python -c "
import sys; sys.path.insert(0, 'C:/airflow')
import pandas as pd
from modules.transform.pipelines.db.DB_FinProduct_Map import _review_reason
row = pd.Series({'review_status_edit': 'N', '표준_메뉴명_edit': '계란찜', '수동분류_edit': '사이드', 'menu_name_후보수': '1', 'classified_by': 'llm'})
print(repr(_review_reason(row)))
"
```
→ 기대: `'LLM 분류 확인'`

2. **Y 승인 항목은 여전히 빈값이어야 함**
```python
python -c "
import sys; sys.path.insert(0, 'C:/airflow')
import pandas as pd
from modules.transform.pipelines.db.DB_FinProduct_Map import _review_reason
row = pd.Series({'review_status_edit': 'Y', '표준_메뉴명_edit': '계란찜', '수동분류_edit': '사이드', 'classified_by': 'llm'})
print(repr(_review_reason(row)))
"
```
→ 기대: `''`

3. **dry_run으로 migrate 실행 → review_rows > 0**
```python
python -c "
import sys; sys.path.insert(0, 'C:/airflow')
from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map
result = migrate_product_map(dry_run=True)
print(result)
"
```
→ 기대: `review_rows: 272` (또는 그 이상)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_review_reason` 함수 인터페이스(인자, 반환 타입) 변경 금지
- `build_review_rows` 로직 변경 금지 — `_review_reason`만 수정
- `classified_by == "llm"` 조건은 다른 reasons가 없을 때만 추가 (`if not reasons`)
- 기존 Y 승인 항목은 review에서 여전히 제외되어야 함

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
