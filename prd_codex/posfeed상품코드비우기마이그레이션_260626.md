# posfeed 상품코드 비우기 마이그레이션 실행

## Task
`fin_product_grp.csv`의 posfeed 행은 설계상 `상품코드`를 비우고 `상품명`으로 식별해야 하는데, 레거시 데이터에서 **상품코드 = 상품명**으로 중복 채워져 있다(예: `0단계기본맵기,0단계기본맵기`). 이를 비우는 일회성 스크립트 `scripts/migrate_posfeed_grp.py`는 작성되어 있으나 **아직 실행되지 않았다.** 신규 행 생성 로직(`_make_posfeed_master_row`)은 이미 `상품코드: ""`로 올바르게 동작하므로, 남은 작업은 기존 레거시 행을 비우는 마이그레이션 실행 + 검증뿐이다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 일회성 스크립트 위치: `scripts/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 저장: `utf-8-sig` + `.tmp` 작성 후 atomic replace(`_safe_replace`)
- 작업 디렉터리: `C:\airflow` (또는 PYTHONPATH)

## Files to Create / Modify
**수정/생성 없음** — 기존 `scripts/migrate_posfeed_grp.py`를 그대로 실행한다.

검증 대상 데이터:
- `fin_product_grp.csv` (`FIN_PRODUCT_CSV_PATH`, OneDrive mart/fin_product)
- `fin_product_posfeed_whitelist.csv` (`POSFEED_WHITELIST_CSV_PATH`)

## Implementation Steps

1. **마이그레이션 실행**
   ```
   cd C:\airflow
   python scripts/migrate_posfeed_grp.py
   ```
   스크립트 동작:
   - whitelist의 `is_valid`로 grp `exclude_check` 복원 (`is_valid=N` → `exclude_check=Y`, `is_valid=Y` → `exclude_check=N`, **whitelist 우선·기존값 덮어씀**)
   - whitelist 미존재 posfeed 행 → 현재 `exclude_check` 유지
   - 모든 posfeed 행 `상품코드 = ""`
   - `updated_at` 현재 시각 갱신
   - `utf-8-sig` + atomic replace 저장
   - 로그: posfeed 행수 / whitelist 매칭·미매칭 / 덮어쓴 행수 / 상품코드 비운 행수 / 최종 exclude_check Y·N 분포

2. **결과 검증** (Test Cases 1~2)

## Reference Code

### scripts/migrate_posfeed_grp.py (실행 대상 — 변경 없음)
```python
from modules.transform.pipelines.db.DB_FinProduct import _safe_replace
from modules.transform.pipelines.db.DB_UnifiedSales_common import _normalize_item_key
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, POSFEED_WHITELIST_CSV_PATH

def main() -> dict[str, int]:
    whitelist = _read_csv(POSFEED_WHITELIST_CSV_PATH)
    grp = _read_csv(FIN_PRODUCT_CSV_PATH)
    # whitelist_map[_normalize_item_key(item_name)] = is_valid(Y/N)
    posfeed_mask = grp["source"].str.strip().str.lower() == "posfeed"
    for idx in grp.index[posfeed_mask]:
        key = _normalize_item_key(str(grp.at[idx, "상품명"]).strip())
        is_valid = whitelist_map.get(key)
        if is_valid in {"Y", "N"}:
            grp.at[idx, "exclude_check"] = "Y" if is_valid == "N" else "N"
        else:
            grp.at[idx, "exclude_check"] = "N"
        grp.at[idx, "상품코드"] = ""           # ← 핵심: 비움
        grp.at[idx, "updated_at"] = now
    # tmp 작성 후 _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
```

### modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py — 신규 행 생성 (이미 정상)
```python
def _make_posfeed_master_row(columns, item_name, exclude_check, updated_at):
    row = {col: "" for col in columns}
    row.update({
        "source": "posfeed", "구분": "배달",
        "상품코드": "",                       # ← 신규 행도 항상 공백
        "상품명": item_name,
        "llm_check": "Y", "exclude_check": exclude_check,
        "updated_at": updated_at,
    })
    # is_main_candidate=N, is_latest=Y, 중복_수동분류=N
    return row
```

## Test Cases
1. [마이그레이션 실행] `python scripts/migrate_posfeed_grp.py` → 기대: 에러 없음, 매칭/덮어쓴 행수·상품코드 비운 행수 로그 출력
2. [상품코드 공백 검증] `python -c "import pandas as pd; from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH as p; df=pd.read_csv(p,dtype=str).fillna(''); pf=df[df['source']=='posfeed']; print('blank_code', (pf['상품코드'].str.strip()=='').all(), 'excludeY', (pf['exclude_check'].str.upper()=='Y').sum())"` → 기대: `blank_code True`, `excludeY ≈ 2260`
3. [블랙리스트 grp 로드] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import _load_posfeed_blacklist as f; print(len(f()))"` → 기대: ImportError 없음, ~2260 항목

> 모든 명령은 `C:\airflow`를 작업 디렉터리(또는 PYTHONPATH)로 두고 실행.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행
  2. FAIL 항목 → 원인 분석 (whitelist 경로 / normalize 키 불일치 / 캐시 mtime)
  3. 필요 시 수정 후 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- whitelist `is_valid`가 grp `exclude_check`보다 **우선**(덮어쓰기). 반대 방향 금지.
- 모든 posfeed 행 `상품코드`를 비우고 `상품명`으로 식별(이후 신규 행도 동일).
- 비-posfeed 행은 손대지 않음.
- CSV 저장은 `utf-8-sig` + atomic replace, OneDrive PermissionError 폴백 유지.
- `migrate_posfeed_grp.py`는 일회성 — DAG runtime 로직 추가 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
