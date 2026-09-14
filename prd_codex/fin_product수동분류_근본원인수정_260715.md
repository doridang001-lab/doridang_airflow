# fin_product 신규 메뉴 수동분류 — 근본 원인 수정

## Task
`fin_product_grp_input.csv`(상품 마스터)에 posfeed/배달/unipos/okpos(sales 등록분)이 `수동분류` 빈칸 + `llm_check=N`으로 등록되는데, `DB_FinProduct.py`의 분류 태스크는 OKPOS/EASYPOS xlsx 변경분과 `unionpos` pending만 처리해서 나머지는 **영영 분류되지 않는 고아 행**이 된다(현재 `is_latest=Y` 빈칸 16,497건). 등록↔분류 루프를 닫는 **소스-무관 분류 태스크**를 추가하고, LLM 호출을 host 하드코딩에서 `qwen_client`로 교체한다. 일회성이 아니라 DAG에 상시 배치해 재발을 막는다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- LLM 호출: **반드시 `modules/transform/utility/qwen_client.py` 재사용** (신규 ollama 클라이언트 금지)
- CSV 저장: `_safe_replace(tmp, target)`로 원자적 저장, `encoding="utf-8-sig"`

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_FinProduct.py`
  - 신규 함수 `classify_pending_master(...)` 추가
  - `_classify_one`의 LLM 호출부(`_get_gpt_client()` + `client.chat`)를 `qwen_client`로 교체
- **수정** `dags/db/DB_FinProduct_Dags.py` — `classify_pending_master` 태스크(t6b) 배선
- **(선택)** `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` — `_make_posfeed_master_row`의 `llm_check="N"`→`"Y"` (필수 아님)

## Implementation Steps

### 1) `DB_FinProduct.py` — `_classify_one` LLM 호출을 qwen_client로 교체
목적: `_OLLAMA_HOST = "http://host.docker.internal:11434"` 단일 하드코딩 제거 → 호스트/컨테이너 양쪽 동작 + skill 규칙 준수.
- 파일 상단에 import 추가:
  ```python
  from modules.transform.utility.qwen_client import get_ollama_client_with_candidates, query_qwen_json
  ```
- `_classify_one` 안의 `client, model = _get_gpt_client()` + `response = client.chat(...)` + 수동 JSON 블록 추출 부분을 아래로 교체(프롬프트 텍스트·`reconcile`·폴백 로직은 **그대로 유지**):
  ```python
  result = query_qwen_json(prompt, system_prompt=system_prompt)  # dict 반환, 실패 시 {"raw_response":..,"parse_error":..}
  if result.get("parse_error"):
      raise ValueError(result.get("parse_error"))
  ```
  - `system_prompt`/`prompt`는 기존 `_classify_one`의 문자열을 그대로 사용.
  - `분류 = result.get("수동분류", "기타")` 이후 검증/`is_main` 로직 유지.
- `_get_gpt_client`는 다른 곳에서 안 쓰이면 제거, 쓰이면 유지(무해). `finalize_unionpos_pending`도 `_classify_one`을 쓰므로 자동으로 qwen_client 경유가 된다.

### 2) `DB_FinProduct.py` — 신규 태스크 `classify_pending_master`
```python
def classify_pending_master(
    limit: int | None = 1000,
    since: str | None = None,
    sources: list[str] | None = None,
    enable_llm: bool = True,
    dry_run: bool = False,
) -> str:
```
로직:
1. `df = _read_master()`; 비었으면 스킵 반환.
2. 대상 선정(**llm_check 무관, "빈칸" 기준**):
   ```python
   lab = df["수동분류"].fillna("").astype(str).str.strip()
   latest = df["is_latest"].fillna("").astype(str).str.strip().str.upper() == "Y"
   name_ok = df["상품명"].fillna("").astype(str).str.strip() != ""
   mask = (lab == "") & latest & name_ok
   if since:
       mask &= df["updated_at"].fillna("").astype(str).str[:10] >= since
   if sources:
       mask &= df["source"].fillna("").astype(str).str.strip().str.lower().isin([s.lower() for s in sources])
   ```
3. 대상 인덱스를 `updated_at desc` 정렬 후 `limit` 적용(최신 메뉴 우선).
4. `if not enable_llm:` → 스킵 반환. `rules = _available_rules(df)`, `few_shot = _build_few_shot(df, rules=rules)`.
5. 대상 행별:
   ```python
   item = row.to_dict()
   result = _classify_one(item, few_shot, rules=rules)   # 기존 rule+LLM+reconcile 재사용
   df.at[idx, "수동분류"] = result.get("수동분류", "기타")
   df.at[idx, "is_main_candidate"] = result.get("is_main_candidate", "N")
   df.at[idx, "llm_check"] = "Y"      # 초안=검수대상 (few-shot/rules에서 제외됨 → 학습 오염 없음)
   df.at[idx, "updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
   ```
6. 저장 전: `df = ensure_allocator_columns(df)`, `df = _mark_is_latest(df)`, `validate_fin_product_codes(df, allow_legacy_blank=True)`.
7. `dry_run`이면 저장 생략 로그. 아니면 **원본 백업 후** 저장:
   ```python
   backup = FIN_PRODUCT_CSV_PATH.with_name(f"fin_product_grp_input.backup_{datetime.now():%Y%m%d_%H%M%S}.csv")
   backup.write_bytes(FIN_PRODUCT_CSV_PATH.read_bytes())
   # tmp → _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)  (기존 update_product_master와 동일 패턴)
   ```
8. 반환: `f"pending 분류: 대상 {n}건, 분류 {done}건, 실패 {fail}건 (limit={limit}, since={since})"`. 로그로 라벨 분포 남김.

### 3) `dags/db/DB_FinProduct_Dags.py` — 태스크 배선
- import에 `classify_pending_master` 추가.
- t6b 태스크 추가, `op_kwargs={"limit": 1000, "enable_llm": ENABLE_LLM}`.
- 의존성: `t6 >> t6b >> t7` (즉, `finalize_unionpos_pending >> classify_pending_master >> apply_review_approvals`).

## Reference Code

### DB_FinProduct.py (기존 — 재사용 대상)
```python
from datetime import datetime
import pandas as pd
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, existing_fin_product_csv_path
from modules.transform.pipelines.db.DB_FinProduct_Rules import (
    build_rules_from_manual, classify_by_rules, load_rules, reconcile, rules_to_prompt_block)
from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    ensure_allocator_columns, normalize_item_key, validate_fin_product_codes)

_CLASSIFY_LABELS = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
_OLLAMA_HOST = "http://host.docker.internal:11434"   # ← 교체 대상(하드코딩)

def _read_master() -> pd.DataFrame: ...              # grp_input 로드 + ensure_allocator_columns + 플래그 정규화
def _available_rules(master: pd.DataFrame | None = None) -> list[dict]: ...
def _build_few_shot(master, rules=None) -> str: ...
def _rule_based_classification(item: dict) -> dict | None: ...
def _mark_is_latest(df: pd.DataFrame) -> pd.DataFrame: ...  # source+상품코드별 최신행 is_latest=Y
def _safe_replace(src, dst) -> None: ...             # os.replace + OneDrive fallback

def _classify_one(item: dict, few_shot: str, rules: list[dict] | None = None) -> dict:
    # rule_result = _rule_based_classification({**item, "_rules": rules})
    # system_prompt/prompt 구성 후 LLM 호출 → 아래 블록을 qwen_client로 교체
    #   client, model = _get_gpt_client(); response = client.chat(model=..., messages=[system,user], stream=False)
    # resolved = reconcile(rule_result, llm_result); return {수동분류, is_main_candidate, llm_error, classified_by, 검수사유}

def finalize_unionpos_pending(enable_llm=True, **context) -> str:
    # pending = df[(source==unionpos) & (llm_check==Y)]; for row: result=_classify_one(item, few_shot, rules)
    # out[수동분류/is_main_candidate], llm_check="N", append → _mark_is_latest → validate → _safe_replace  (배선 템플릿)
```

### qwen_client.py (LLM 호출 — 반드시 이걸 사용)
```python
from modules.transform.utility.qwen_client import get_ollama_client_with_candidates, query_qwen_json
# get_ollama_client_with_candidates() -> (client, ["qwen2.5:14b","gpt-oss:20b"])  host: docker.internal→localhost→127.0.0.1 자동
# query_qwen_json(prompt, system_prompt=None, client=None, model_candidates=None) -> dict
#   실패 시 {"raw_response": <text>, "parse_error": <str>} 반환(예외 아님) → parse_error 체크 필요
```

### DB_UnifiedSales_posfeed.py (근본 원인 지점 — 참고)
```python
def _make_posfeed_master_row(columns, item_name, exclude_check, updated_at, unit_price="") -> dict:
    row = {col: "" for col in columns}
    row.update({"source": "posfeed", "구분": "배달", "상품코드": "", "상품명": item_name,
                "판매단가": unit_price, "llm_check": "N",   # ← 수동분류 미설정 + N → 고아 행 원인
                "exclude_check": exclude_check, "updated_at": updated_at})
    # is_latest="Y" 등 세팅
    return row
```

### DB_FinProduct_Dags.py (기존 배선)
```python
from modules.transform.pipelines.db.DB_FinProduct import (
    load_okpos_product_xlsx, detect_product_changes, classify_with_llm, update_product_master,
    send_alert_email, finalize_unionpos_pending, apply_review_approvals, build_fin_product_mart)
ENABLE_LLM = True
# t6 = finalize_unionpos_pending; t7 = apply_review_approvals; t8 = build_fin_product_mart
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
```

## Test Cases
1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_FinProduct import classify_pending_master; print('ok')"` → 기대: `ok`, 예외 없음
2. [qwen_client 연결] `python -c "from modules.transform.utility.qwen_client import get_ollama_client_with_candidates as g; c,m=g(); print(m)"` → 기대: `['qwen2.5:14b', 'gpt-oss:20b']` (또는 설치된 후보)
3. [dry_run 소량] `python -c "from modules.transform.pipelines.db.DB_FinProduct import classify_pending_master as f; print(f(limit=5, since='2026-07-08', dry_run=True))"` → 기대: `pending 분류: 대상 N건 ...`, CSV 미변경(백업/저장 없음)
4. [실분류 5건] `python -c "from modules.transform.pipelines.db.DB_FinProduct import classify_pending_master as f; print(f(limit=5, since='2026-07-08'))"` → 기대: 분류 5건 근처, 백업 파일 생성, 해당 5행 `수동분류` 채워지고 `llm_check=Y`
5. [라벨 검증] `python -c "import pandas as pd; from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH as P; df=pd.read_csv(P,dtype=str,encoding='utf-8-sig').fillna(''); lab=df['수동분류'].str.strip(); bad=lab[(lab!='') & ~lab.isin(['메인','1인','사이드','기타','주류','음료','토핑','옵션','세트','리뷰'])]; print('invalid_labels=', len(bad))"` → 기대: `invalid_labels= 0`
6. [DAG import] `python -c "from dags.db.DB_FinProduct_Dags import dag; print(len(dag.tasks))"` → 기대: `9` (기존 8 + classify_pending_master), ImportError 없음
7. [finalize 회귀] `python -c "from modules.transform.pipelines.db.DB_FinProduct import finalize_unionpos_pending as f; print(f(enable_llm=True))"` → 기대: qwen_client 교체 후에도 정상 실행(unionpos pending 없으면 스킵 메시지)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→7 순서 실행
  2. FAIL 항목 → 원인 분석 (import 경로 / 컬럼명 / qwen_client 반환형)
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```
전량 즉시분(사용자 pain)은 검증 PASS 후 실행: `classify_pending_master(since="2026-07-08")` (~2,498건).

## Constraints
- **CSV 스키마 변경 금지** — 사용자가 직접 편집하는 파일. `classified_by`/`검수사유` 등 신규 컬럼 추가하지 말 것(근거는 로그로).
- **학습 오염 방지** — LLM 초안은 반드시 `llm_check="Y"`. (few-shot/rules/confirmed_map은 `llm_check != Y`만 사용하므로 Y여야 제외됨)
- **원본 백업 필수** — 저장 직전 `fin_product_grp_input.backup_YYYYMMDD_HHMMSS.csv` 복사.
- **대상 기준은 "수동분류 빈칸"** — `llm_check` 값으로 거르지 말 것(posfeed가 N으로 넣어 놓치는 게 원인).
- **`limit`으로 1회 처리량 제한** — DAG 상시 실행 시 16k 폭주 방지(최신순 drain).
- LLM 호출은 `qwen_client`만 사용. 새 ollama 클라이언트/requests 금지.
- 기존 `_classify_one` 프롬프트/`reconcile` 로직 변경 금지(호출부만 qwen_client로 교체).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(덮어쓰기)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게(있음)
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- (선택) posfeed `llm_check="Y"` 정합성 수정은 여유 있으면 포함, 아니면 생략 가능(새 태스크가 커버)
