# FinProduct 분류 학습 루프 개선 (자동 키워드 규칙 + 규칙·LLM 병행 검증)

## Task
두 상품 분류 파이프라인(`DB_FinProduct.py` 통합 마스터, `DB_FinProduct_Map.py` 송파삼전점 표준화)이
Ollama LLM으로 메뉴를 10개 카테고리로 분류하지만, 사람이 수동 교정을 쌓아도 학습이 반영되지 않는다.
원인은 (1) `DB_FinProduct.py`의 하드코딩 키워드 규칙이 LLM보다 먼저 적용되어 교정을 덮어씀,
(2) few-shot 예시가 라벨당 8~10개로 캡, (3) `include_keywords/ignore_words` 같은 명시적 규칙 구조 부재,
(4) A(`수동분류`)/B(`수동분류_edit`) 학습 데이터 분리다.
**목표: 사람은 지금처럼 `*_input.csv`에서만 재분류하고, 파이프라인이 확정 분류에서 키워드 규칙 JSON을 자동 채굴하여
규칙이 누적 성장하고, 분류 시 규칙과 LLM을 병행 검증해 불일치를 검수 대상으로 노출한다.**

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 원자적 파일 저장: tmp 파일 write → `os.replace` (기존 `_safe_replace` 패턴)
- 카테고리 10종(두 시스템 동일): `메인, 1인, 사이드, 기타, 주류, 음료, 토핑, 옵션, 세트, 리뷰`

## Files to Create / Modify
**생성:**
- `modules/transform/pipelines/db/DB_FinProduct_Rules.py` — 공통 규칙 엔진/채굴 모듈 (선례: `DB_ItemIdAllocator.py`)

**수정:**
- `modules/transform/utility/paths.py` — 규칙 JSON 경로 상수 2개 추가 (283행 근처)
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` — 규칙 채굴·주입·병행 검증 연결
- `modules/transform/pipelines/db/DB_FinProduct.py` — 하드코딩 규칙 → 데이터 규칙 교체, 병행 검증
- `dags/db/DB_FinProduct_Map_Dags.py` — (필요 시 로그/태스크명만) 규칙 생성 포함 반영

## Implementation Steps

### 1. `paths.py` 경로 상수 추가 (283행 `FIN_PRODUCT_MAP_TRAIN_JSON_PATH` 근처)
```python
FIN_PRODUCT_RULES_JSON_PATH = MART_DB / "fin_product" / "fin_product_rules.json"
FIN_PRODUCT_RULES_MANUAL_JSON_PATH = MART_DB / "fin_product" / "fin_product_rules_manual.json"  # 선택적 수기 override
```

### 2. 신규 모듈 `DB_FinProduct_Rules.py` 작성

**모듈 상수:**
```python
CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
DEFAULT_IGNORE_WORDS = ["추가", "곱빼기", "선택", "맛있는", "변경", "기본", "많이", "적게"]
```

**규칙 스키마 (사용자 요청 형태, JSON 배열의 원소):**
```json
{
  "priority": 1, "rule_name": "사리 분류", "수동분류": "토핑",
  "category_large": "추가메뉴", "category_middle": "사리", "represent_menu": "사리",
  "exclude_check": "Y",
  "include_keywords": ["우동사리", "우동 사리", "사리"],
  "ignore_words": ["맛있는", "추가", "곱빼기", "선택"],
  "examples": ["맛있는 우동사리", "우동사리 추가", "우동 사리"]
}
```
- 엔진 출력 카테고리는 `수동분류`(10종). `category_large/middle`은 OKPOS 대/중메뉴 참고용(정보성).

**함수:**
- `build_rules_from_manual(rows: pd.DataFrame) -> list[dict]` — **규칙 자동 채굴**
  - 입력 컬럼 정규화(별칭 허용): item_name↔상품명, 수동분류↔수동분류_edit, 표준명↔표준_메뉴명_edit↔대표메뉴, exclude_check, 중메뉴, 대메뉴
  - 라벨별로 item_name을 `normalize_item_key` 정규화 후 토큰/부분문자열 추출
  - **해당 라벨에 편중된 고정밀 키워드**만 include_keywords로 채택(다른 라벨 등장 비율이 낮은 것). 짧고 모호한 토큰(1글자 등) 배제
  - `represent_menu` = 키워드 클러스터의 최빈 표준명/대표메뉴, `exclude_check` = 매칭 행 다수결, `examples` = 실제 item_name 상위 N(예: 5)
  - `priority` = 키워드 특이도(긴 키워드·단일 라벨 편중 높을수록 상위 = 낮은 숫자)
- `load_rules() -> list[dict]` — `FIN_PRODUCT_RULES_JSON_PATH` 로드. `FIN_PRODUCT_RULES_MANUAL_JSON_PATH` 존재 시 병합하여 수기 규칙을 상위 priority로 취급. 파일 없으면 `[]`
- `save_rules(rules: list[dict]) -> None` — tmp write → `os.replace` 원자적 저장
- `classify_by_rules(item: dict, rules: list[dict]) -> dict | None`
  - item_name(또는 상품명)에서 ignore_words 제거 → priority 오름차순으로 include_keyword 포함 검사
  - 첫 매칭 시 `{"수동분류": ..., "represent_menu": ..., "exclude_check": ..., "rule_name": ...}` 반환, 없으면 `None`
- `rules_to_prompt_block(rules: list[dict]) -> str` — 규칙을 "키워드→분류 + 예시" 텍스트로 포맷(LLM 프롬프트 주입용)
- `reconcile(rule_result: dict | None, llm_result: dict) -> dict` — **병행 검증**
  - 규칙·LLM `수동분류` 일치 → `classified_by="rule+llm"`
  - 불일치 → LLM 값 채택 + `검수사유="규칙/LLM 불일치(규칙=X, LLM=Y)"`
  - 규칙만 매칭 → `classified_by="rule"`, LLM만 → `classified_by="llm"`
  - 항상 검수유무=0(pending)

### 3. `DB_FinProduct_Map.py` 수정
- `_build_map_train_payload`(1018행): `label_rules[label]`에 `include_keywords/ignore_words/represent_menu` 추가
- `build_fin_product_map_train_json`(1058행): 동시에 `DB_FinProduct_Rules.build_rules_from_manual` 호출 →
  `save_rules`로 `fin_product_rules.json` 생성/갱신. **채굴 소스 = map 승인행 + `fin_product_grp_input.csv` 확정행 통합**(A/B 통합)
- `classify_unmapped`(1113행)/`_classify_batch`(1082행): 각 item에 `classify_by_rules` 후보 → `call_llm` 후보 → `reconcile`
- `build_prompt`(889행): `rules_to_prompt_block(load_rules())` 주입
- `build_examples`(1000행): `head(20)` 상한 제거 → 라벨 균형 샘플로 확대

### 4. `DB_FinProduct.py` 수정
- `_rule_based_classification`(709행): 하드코딩 튜플(alcohol_tokens 등) 제거 → `DB_FinProduct_Rules.classify_by_rules(item, load_rules())` 로 교체
- `_classify_one`(781행): 규칙 결과가 LLM을 무조건 덮지 않도록, 규칙 후보 + LLM 후보를 `reconcile`
- `_build_few_shot`(670행): `rules_to_prompt_block` 규칙 요약 추가, `_FEW_SHOT_PER_LABEL`(134행) 상향 또는 제거
- 시스템 프롬프트(791행)의 하드코딩 분류 지침 문장을 규칙 블록 참조로 대체
- 규칙 파일 없을 때 폴백: `classify_with_llm` 내에서 확정행으로 즉석 채굴

### 5. `DB_FinProduct_Map_Dags.py`
- `t1_5`(`build_fin_product_map_train_json`)가 규칙 JSON도 생성하도록 Step 3 함수 확장으로 자동 반영 → **태스크 그래프 변경 불필요**. 로그/태스크 설명만 규칙 생성 포함으로 갱신

## Reference Code

### DB_FinProduct_Map.py (핵심: 프롬프트/LLM/train JSON)
```python
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
MODEL_CANDIDATES = ["qwen2.5:14b", "qwen2.5:7b", "qwen2.5:latest", "gpt-oss:20b", "gpt-oss:latest", "gpt-oss"]
OLLAMA_HOSTS = [os.getenv("OLLAMA_HOST", "").strip(), "http://host.docker.internal:11434", "http://localhost:11434"]
BATCH_SIZE = 5
_TRAIN_EXAMPLES_PER_LABEL = 10

def build_prompt(batch: list[dict], examples: list[dict]) -> str:
    example_text = "\n".join(
        f'- "{r["item_name"]}" => 표준명 "{r["표준_메뉴명_edit"]}", 분류 "{r["수동분류_edit"]}"'
        for r in examples) or "(예시 없음)"
    # ... 허용 수동분류/규칙/기존 승인 예시/분류 대상 → JSON 배열 응답 요구

def _load_map_examples() -> list[dict]:
    # FIN_PRODUCT_MAP_TRAIN_JSON_PATH의 label_rules[*].examples 를 평탄화하여 로드

def call_llm(prompt: str) -> list[dict]:
    client, model = _get_ollama_client()
    response = client.chat(model=model, messages=[...], format="json", think=False,
                           options={"num_predict": 4096}, stream=False)
    # ... JSON 배열 파싱

def _build_map_train_payload(map_df: pd.DataFrame) -> dict:
    # approved 행 → label_rules[label] = {"examples": [...]}  (현재 examples만; 여기에 keyword 규칙 추가)

def build_fin_product_map_train_json(dry_run: bool = False, **context) -> dict: ...
def classify_unmapped(unmapped, map_df, limit, dry_run) -> list[dict]:
    examples = _load_map_examples() or build_examples(map_df)  # build_examples는 head(20) 상한
```

### DB_FinProduct.py (핵심: 하드코딩 규칙/few-shot/단건 분류)
```python
_GPT_MODEL_CANDIDATES = ["gpt-oss:20b", "gpt-oss:latest", "gpt-oss", "qwen2.5:7b", "qwen2.5:latest"]
_OLLAMA_HOST = "http://host.docker.internal:11434"
_CLASSIFY_LABELS = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
_FEW_SHOT_PER_LABEL = 8

def _rule_based_classification(item: dict) -> dict | None:
    # alcohol_tokens/drink_tokens/topping_tokens/option_tokens/... 하드코딩 튜플 매칭 → 제거 대상

def _build_few_shot(master: pd.DataFrame) -> str:
    # review_input.csv 검수완료 + master 확정행에서 라벨당 _FEW_SHOT_PER_LABEL개 예시 텍스트 생성

def _classify_one(item: dict, few_shot: str) -> dict:
    rule_result = _rule_based_classification(item)   # LLM보다 먼저 적용 → 병행 검증으로 변경
    if rule_result is not None: return rule_result
    # ... gpt-oss 호출, system_prompt에 few_shot 주입, {"수동분류","is_main_candidate"} 반환
```

### DB_ItemIdAllocator.py (재사용할 정규화 유틸 — both가 import)
```python
_ITEM_KEY_RE = re.compile(r"[^0-9A-Za-z가-힣一-龥]+")
def normalize_item_key(name: object) -> str: ...   # 상품명 → 키 정규화(공백/특수문자 제거)
def canonical_source(source: object) -> str: ...    # source 표준화
def _safe_replace(src: Path, dst: Path) -> None: ...  # 원자적 파일 교체
```

### paths.py (앵커)
```python
FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp_input.csv"
FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"
FIN_PRODUCT_MAP_REVIEW_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_review_input.csv"
FIN_PRODUCT_MAP_TRAIN_JSON_PATH = MART_DB / "fin_product" / "fin_product_map_train.json"
```

## Test Cases
1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_FinProduct_Rules import build_rules_from_manual, load_rules, save_rules, classify_by_rules, rules_to_prompt_block, reconcile"` → 기대: ImportError 없음
2. [규칙 채굴] 스크래치 스크립트로 confirmed 샘플 DataFrame(예: `상품명="맛있는 우동사리 추가", 수동분류="토핑", 대표메뉴="사리", exclude_check="Y"` 여러 건) → `build_rules_from_manual` → 기대: `include_keywords`에 `"사리"` 포함, `수동분류="토핑"`, `exclude_check="Y"` 규칙 생성
3. [규칙 적용] `classify_by_rules({"상품명": "맛있는 우동사리 추가"}, rules)` → 기대: `{"수동분류": "토핑", ...}` (ignore_words "맛있는/추가" 제거 후 "사리" 매칭)
4. [병행 검증] `reconcile({"수동분류":"토핑",...}, {"수동분류":"사이드"})` → 기대: `검수사유`에 "규칙/LLM 불일치" 포함, LLM 값(사이드) 채택
5. [paths] `python -c "from modules.transform.utility.paths import FIN_PRODUCT_RULES_JSON_PATH; print(FIN_PRODUCT_RULES_JSON_PATH)"` → 기대: `.../fin_product/fin_product_rules.json`
6. [DAG import] `python -c "from dags.db.DB_FinProduct_Map_Dags import dag"` 및 `python -c "from dags.db.DB_FinProduct_Dags import dag"` → 기대: ImportError 없음
7. [dry-run] `DB_FinProduct_Map` DAG conf `{"dry_run": true, "limit": 20}` 실행 → 기대: 규칙 채굴/병행 검증 로그 출력, CSV/JSON 미기록

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 사람의 작업 흐름 유지: 재분류는 오직 `*_input.csv`에서만. 별도 규칙 JSON 수기 관리 요구 금지(자동 채굴이 기본)
- 규칙이 LLM을 **무조건 대체 금지** — 반드시 `reconcile` 병행 검증. 불일치는 검수 대상으로 노출
- `classify_by_rules` 결과·LLM 결과 모두 검수유무=0(pending)으로 저장 (자동 승인 금지)
- 카테고리는 10종 고정. 규칙의 `수동분류`는 반드시 이 10종 중 하나
- 원자적 파일 저장(`os.replace`) 패턴 준수 — 부분 기록 방지
- 기존 KEY_COLUMNS/컬럼 스키마(MAP_COLUMNS, REVIEW_COLUMNS 등) 깨지 않기
- `/ollama-connect` 스킬로 모델명(`qwen2.5:14b`, `gpt-oss:20b`)·호스트·JSON 응답 형식 검증 후 프롬프트 변경

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (있음)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 규칙 채굴 임계값(최소 빈도/편중 비율): 합리적 기본값 선택(예: 최소 3건, 라벨 편중 ≥0.8)
