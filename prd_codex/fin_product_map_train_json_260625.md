# fin_product_map_train_json

## Task
`DB_FinProduct_Map_Dags.py`는 LLM이 상품 분류를 잘 하도록 학습 데이터를 유지하는 것이 목적이나,
현재는 `fin_product_grp_train.json`에 상응하는 `fin_product_map_train.json` 생성 단계가 없다.
`migrate_product_map` 이후, `llm_product_map` 이전에 승인된 분류를 JSON으로 저장해 LLM 예시 참조를 영속화한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 파일 저장: `_safe_replace(tmp, target)` 패턴으로 원자적 저장 (기존 함수 재사용)

## Files to Create / Modify

- **수정** `modules/transform/utility/paths.py` — `FIN_PRODUCT_MAP_TRAIN_JSON_PATH` 상수 추가
- **수정** `modules/transform/pipelines/db/DB_FinProduct_Map.py` — `build_fin_product_map_train_json()` 함수 추가 + `build_prompt()` 수정
- **수정** `dags/db/DB_FinProduct_Map_Dags.py` — 새 태스크 추가 및 체인 수정

## Implementation Steps

### 1. `paths.py` — 경로 상수 추가

`_RESOLVERS` 딕셔너리의 `# Derived — MART_DB 계열` 블록에 아래 항목 추가:

```python
"FIN_PRODUCT_MAP_TRAIN_JSON_PATH": lambda: _get("MART_DB") / "fin_product" / "fin_product_map_train.json",
```

위치: `FIN_PRODUCT_MAP_REVIEW_CSV_PATH` 항목 바로 아래.

### 2. `DB_FinProduct_Map.py` — import 추가

기존:
```python
from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    MART_DB,
)
```

변경:
```python
from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
    MART_DB,
)
```

### 3. `DB_FinProduct_Map.py` — `build_fin_product_map_train_json()` 함수 추가

`llm_product_map()` 함수 바로 앞에 아래 함수를 삽입한다:

```python
_TRAIN_EXAMPLES_PER_LABEL = 10


def _build_map_train_payload(map_df: pd.DataFrame) -> dict:
    from datetime import datetime
    approved = map_df[
        (map_df["review_status_edit"].fillna("").astype(str).str.strip() == "Y")
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
        & (map_df["item_name"].fillna("").astype(str).str.strip() != "")
    ].copy()

    counts = approved["수동분류_edit"].value_counts().to_dict() if not approved.empty else {}
    label_rules: dict[str, dict] = {}
    for label in VALID_CATEGORIES:
        label_df = approved[approved["수동분류_edit"] == label].copy()
        if label_df.empty:
            label_rules[label] = {"examples": []}
            continue
        label_df = (
            label_df
            .drop_duplicates(subset=["item_name"], keep="last")
            .head(_TRAIN_EXAMPLES_PER_LABEL)
        )
        label_rules[label] = {
            "examples": [
                {
                    "item_name": str(row["item_name"]).strip(),
                    "표준_메뉴명_edit": str(row.get("표준_메뉴명_edit", "")).strip(),
                    "수동분류_edit": str(row.get("수동분류_edit", "")).strip(),
                    "메뉴대분류_edit": str(row.get("메뉴대분류_edit", "")).strip(),
                    "main_menu_edit": str(row.get("main_menu_edit", "")).strip(),
                }
                for _, row in label_df.iterrows()
            ]
        }

    return {
        "version": "1.0",
        "source": str(FIN_PRODUCT_MAP_CSV_PATH),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "allowed_categories": VALID_CATEGORIES,
        "label_counts": {label: int(counts.get(label, 0)) for label in VALID_CATEGORIES},
        "label_rules": label_rules,
    }


def build_fin_product_map_train_json(dry_run: bool = False, **context) -> dict:
    map_df = load_map()
    payload = _build_map_train_payload(map_df)
    counts = payload.get("label_counts", {})
    count_msg = ", ".join(f"{label}={counts.get(label, 0)}" for label in VALID_CATEGORIES)

    if dry_run:
        logger.info("dry-run: train JSON 저장 생략 | %s", count_msg)
    else:
        FIN_PRODUCT_MAP_TRAIN_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = FIN_PRODUCT_MAP_TRAIN_JSON_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            _safe_replace(tmp, FIN_PRODUCT_MAP_TRAIN_JSON_PATH)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        logger.info("fin_product_map_train.json 저장: %s | %s", FIN_PRODUCT_MAP_TRAIN_JSON_PATH, count_msg)

    return {"label_counts": counts, "dry_run": dry_run}
```

### 4. `DB_FinProduct_Map.py` — `build_prompt()` 수정

기존 `build_prompt(batch, examples)` 시그니처는 유지하되, `build_examples()` 호출부를 감싸는 `_load_map_examples()` 헬퍼를 추가해
train JSON이 있으면 거기서 로드하도록 한다.

`build_examples()` 함수 바로 아래에 아래 헬퍼 추가:

```python
def _load_map_examples() -> list[dict]:
    if FIN_PRODUCT_MAP_TRAIN_JSON_PATH.exists():
        try:
            data = json.loads(FIN_PRODUCT_MAP_TRAIN_JSON_PATH.read_text(encoding="utf-8"))
            examples = []
            for label_data in data.get("label_rules", {}).values():
                examples.extend(label_data.get("examples", []))
            if examples:
                logger.info("train JSON에서 예시 %d건 로드", len(examples))
                return examples
        except Exception as e:
            logger.warning("train JSON 로드 실패, map_df fallback: %s", e)
    return []
```

`classify_unmapped()` 내부의 `examples = build_examples(map_df)` 호출을 아래로 교체:

```python
examples = _load_map_examples() or build_examples(map_df)
```

### 5. `DB_FinProduct_Map_Dags.py` — 태스크 추가 및 체인 수정

import 추가:
```python
from modules.transform.pipelines.db.DB_FinProduct_Map import (
    llm_product_map,
    migrate_product_map,
    build_fin_product_map_train_json,  # 추가
)
```

`run_llm` 함수 위에 새 래퍼 추가:
```python
def run_build_train_json(**context) -> dict:
    conf = _dag_conf(context)
    dry_run = _conf_bool(conf, "dry_run", DRY_RUN_BY_DEFAULT)
    logger.info("fin_product_map_train_json 빌드 시작: dry_run=%s", dry_run)
    return build_fin_product_map_train_json(dry_run=dry_run)
```

`with DAG` 블록에 태스크 추가 및 체인 수정:
```python
t1_5 = PythonOperator(
    task_id="build_fin_product_map_train_json",
    python_callable=run_build_train_json,
)

# 기존: t1 >> t2
t1 >> t1_5 >> t2
```

## Reference Code

### DB_FinProduct.py (패턴 참조)
```python
def _build_train_payload(master: pd.DataFrame) -> dict:
    # ... confirmed 필터링 후 label별 examples 구조화
    return {
        "version": _TRAIN_RULES["version"],
        "source": str(FIN_PRODUCT_CSV_PATH),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "allowed_labels": _CLASSIFY_LABELS,
        "label_counts": {label: int(counts.get(label, 0)) for label in _CLASSIFY_LABELS},
        "label_rules": label_rules,
    }

def build_fin_product_grp_train_json(**context) -> str:
    df_master = _read_master()
    payload = _build_train_payload(df_master)
    FIN_PRODUCT_GRP_TRAIN_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_GRP_TRAIN_JSON_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _safe_replace(tmp, FIN_PRODUCT_GRP_TRAIN_JSON_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
    counts = payload.get("label_counts", {})
    count_msg = ", ".join(f"{label}={counts.get(label, 0)}" for label in _CLASSIFY_LABELS)
    logger.info("fin_product_grp_train.json 저장: %s | %s", FIN_PRODUCT_GRP_TRAIN_JSON_PATH, count_msg)
    return f"train_json 저장: {count_msg}"
```

### DB_FinProduct_Map.py (현재 구조 참조)
```python
# imports
from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    MART_DB,
)
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트"]

def _safe_replace(tmp: Path, target: Path, retries: int = 3, delay_sec: float = 1.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            os.replace(tmp, target)
            return
        except PermissionError:
            if attempt == retries:
                raise
            time.sleep(delay_sec)

def build_examples(map_df: pd.DataFrame) -> list[dict]:
    # review_status_edit==Y 행에서 최대 20개 예시 반환
    ...

def classify_unmapped(unmapped, map_df, limit, dry_run) -> list[dict]:
    examples = build_examples(map_df)  # ← 이 줄을 _load_map_examples() or build_examples(map_df) 로 교체
    ...
```

## Test Cases

1. **paths import 확인**
   ```
   python -c "from modules.transform.utility.paths import FIN_PRODUCT_MAP_TRAIN_JSON_PATH; print(FIN_PRODUCT_MAP_TRAIN_JSON_PATH)"
   ```
   기대: `OneDrive/.../fin_product/fin_product_map_train.json` 경로 출력, ImportError 없음

2. **함수 import 확인**
   ```
   python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import build_fin_product_map_train_json; print('OK')"
   ```
   기대: `OK`

3. **dry_run 실행**
   ```
   python -c "
   from modules.transform.pipelines.db.DB_FinProduct_Map import build_fin_product_map_train_json
   result = build_fin_product_map_train_json(dry_run=True)
   print(result)
   "
   ```
   기대: `{'label_counts': {...}, 'dry_run': True}`, JSON 파일 미생성

4. **실제 실행 및 파일 생성**
   ```
   python -c "
   from modules.transform.pipelines.db.DB_FinProduct_Map import build_fin_product_map_train_json
   result = build_fin_product_map_train_json(dry_run=False)
   print(result)
   import json
   from modules.transform.utility.paths import FIN_PRODUCT_MAP_TRAIN_JSON_PATH
   data = json.loads(FIN_PRODUCT_MAP_TRAIN_JSON_PATH.read_text(encoding='utf-8'))
   print('categories:', data['allowed_categories'])
   print('counts:', data['label_counts'])
   "
   ```
   기대: JSON 파일 생성, `allowed_categories`에 9개 카테고리 존재, `label_counts`에 승인 항목 수 반영

5. **DAG import 확인**
   ```
   python -c "from dags.db.DB_FinProduct_Map_Dags import dag; print(dag.dag_id)"
   ```
   기대: `DB_FinProduct_Map_Dags`, ImportError 없음

6. **DAG 태스크 체인 확인**
   ```
   python -c "
   from dags.db.DB_FinProduct_Map_Dags import dag
   tasks = {t.task_id: t for t in dag.tasks}
   print(list(tasks.keys()))
   print(tasks['build_fin_product_map_train_json'].upstream_task_ids)
   print(tasks['llm_product_map'].upstream_task_ids)
   "
   ```
   기대: `migrate_product_map → build_fin_product_map_train_json → llm_product_map` 체인 확인

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

- `_safe_replace()` 는 이미 `DB_FinProduct_Map.py`에 존재 — 재정의 금지, 그대로 재사용
- `build_examples()` 는 삭제하지 말 것 — `_load_map_examples()` 의 fallback으로 유지
- `FIN_PRODUCT_MAP_TRAIN_JSON_PATH`는 `paths.py`에만 정의, 파이프라인에서 하드코딩 금지
- 기존 `migrate_product_map`, `llm_product_map` 함수 시그니처 변경 금지
- DAG task_id: `"build_fin_product_map_train_json"` 정확히 사용

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
