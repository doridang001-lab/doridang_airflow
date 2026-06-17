# COLLECT_RANGE 매장 절반 분할 수집

## Task
컴퓨터 2대로 배민 수집을 나눠 돌리기 위해, 매장명 가나다(한글)순 정렬 후
**A컴=상위 절반 / B컴=하위 절반(나머지)** 만 수집하도록 `COLLECT_RANGE` 상수를 추가한다.
각 컴퓨터의 DAG 파일에서 상수만 바꾸면 담당 범위가 정해진다. (100개→50/50, 93개→46/47)
대상 파일은 `C:\airflow\dags\db\DB_Beamin_Macro_Dags.py` **단일 파일**이다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `C:\airflow\dags\db\DB_Beamin_Macro_Dags.py` (상수 1개 + 헬퍼 1개 추가 + `load_accounts` 수정)
- 생성 파일 없음.

## Implementation Steps

### 1. 상수 추가 — `TARGET_STORES` 정의(77번 줄 부근) 바로 아래
```python
# ─── 매장 분할 수집 (컴퓨터 2대 분산) ──────────────────────────────────────
# 매장명 가나다순 정렬 후 절반으로 분할한다.
#   "상위" → A컴: 앞쪽 절반   /   "하위" → B컴: 뒤쪽 나머지   /   None → 전체
# 분할 기준: mid = n // 2  (예: 93개 → 상위 46 / 하위 47, 100개 → 50 / 50)
# DAG 실행 conf {"collect_range": "상위"|"하위"} 로 덮어쓸 수 있다.
COLLECT_RANGE: str | None = None
```

### 2. 분할 헬퍼 추가 — `load_accounts` 함수 정의 바로 위(139번 줄 부근)
```python
def _split_accounts_by_range(accounts: list[dict], collect_range: str | None) -> list[dict]:
    """매장명 가나다순 정렬 후 상위/하위 절반만 반환. None이면 전체.

    NOTE: 현재 데이터는 계정ID↔매장 1:1(배민 67행/고유 67계정)이라 행 단위 분할이 안전.
    향후 한 계정이 여러 매장을 담당하면 account_id 단위 그룹 분할로 바꿔야 한다.
    """
    if not collect_range or not accounts:
        return accounts
    ordered = sorted(accounts, key=lambda a: str(a.get("store_name", "")))
    mid = len(ordered) // 2
    if collect_range == "상위":
        selected = ordered[:mid]
    elif collect_range == "하위":
        selected = ordered[mid:]
    else:
        logger.warning("알 수 없는 COLLECT_RANGE=%r → 전체 수집", collect_range)
        return accounts
    logger.info(
        "매장 분할 [%s]: 전체 %d개 → %d개 (%s ~ %s)",
        collect_range, len(ordered), len(selected),
        selected[0]["store_name"] if selected else "-",
        selected[-1]["store_name"] if selected else "-",
    )
    return selected
```
- 파이썬 기본 정렬은 완성형 한글 음절을 유니코드 코드포인트(=가나다 순)로 정렬 → 별도 로케일 불필요.
- `mid = n // 2` → 상위 = 앞 `n//2`개, 하위 = 나머지. (93→46/47, 100→50/50, 67→33/34)

### 3. `load_accounts(**context)` 수정 (139~148번 줄)
`pipeline_load_accounts` 호출 후, **stores_override(테스트/재수집 conf)가 없을 때만** 분할 적용.
conf의 `collect_range`가 상수 `COLLECT_RANGE`보다 우선.
```python
def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")  # e.g. ["도리당 역삼점"] — 테스트·재수집 용
    target = stores_override if stores_override else TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)

    # 특정 매장(stores_override) 지정 시엔 분할 미적용
    if not stores_override:
        collect_range = conf.get("collect_range", COLLECT_RANGE)
        accounts = _split_accounts_by_range(accounts, collect_range)

    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개: {stores}"
```

## Reference Code
### modules/transform/pipelines/db/DB_Beamin_collect.py (계정 구조)
```python
CSV_PATH = ONEDRIVE_DB / "sales_employee.csv"

def load_accounts(target_stores: list[str], exact: bool = False) -> list[dict]:
    """sales_employee.csv → 배달의 민족 계정 목록.
    반환: [{"account_id": str, "password": str, "store_name": str}, ...]
    target_stores가 비어 있으면 전 매장 반환.
    exact=True 이면 매장명 완전 일치(isin), False 이면 부분 문자열 매칭.
    """
    df = pd.read_csv(CSV_PATH)
    df = df[df["플랫폼"] == "배달의 민족"].copy()
    if target_stores:
        if exact:
            df = df[df["매장명"].isin(target_stores)]
        else:
            pattern = "|".join(target_stores)
            df = df[df["매장명"].str.contains(pattern, na=False)]
    accounts = [
        {"account_id": str(r["계정ID"]), "password": str(r["계정PW"]), "store_name": str(r["매장명"])}
        for _, r in df.iterrows()
    ]
    return accounts
```

## Test Cases
1. [구문] `python -c "import ast; ast.parse(open(r'C:\airflow\dags\db\DB_Beamin_Macro_Dags.py', encoding='utf-8').read()); print('OK')"` → 기대: `OK`
2. [분할 로직] 아래 실행 → 기대: `33 34 True True` (상위33/하위34, 합집합=전체, 교집합 없음)
   ```
   python -c "import importlib.util as u; s=u.spec_from_file_location('m', r'C:\airflow\dags\db\DB_Beamin_Macro_Dags.py'); import sys; print('skip-import-heavy')"
   ```
   (DAG 직접 import는 airflow 의존성으로 무거우므로, 헬퍼만 떼어 검증해도 됨:)
   ```
   python -c "
   accts=[{'store_name':f'매장{i:02d}','account_id':str(i)} for i in range(67)]
   ordered=sorted(accts,key=lambda a:str(a.get('store_name','')))
   mid=len(ordered)//2
   up=ordered[:mid]; lo=ordered[mid:]
   ids=lambda x:{a['account_id'] for a in x}
   print(len(up),len(lo), ids(up)|ids(lo)==ids(accts), len(ids(up)&ids(lo))==0)
   "
   ```
   → 기대: `33 34 True True`
3. [DAG import] `python -c "from dags.db.DB_Beamin_Macro_Dags import dag, COLLECT_RANGE, _split_accounts_by_range; print(COLLECT_RANGE)"` → 기대: `None` (ImportError 없음)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→2→3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- **단일 파일만 수정**: `DB_Beamin_Macro_Dags.py`. pipeline/utility 파일 건드리지 말 것.
- `COLLECT_RANGE` 기본값은 반드시 `None` (전체 수집). 운영자가 컴퓨터별로 `"상위"`/`"하위"` 수동 설정.
- `stores_override`(conf `stores`) 지정 시엔 분할을 적용하지 말 것 (재수집·테스트 경로 보존).
- print 금지, `logger` 사용.
- 두 컴퓨터가 같은 `ONEDRIVE_DB/sales_employee.csv`를 읽어 동일 정렬·동일 mid로 분할 → 상위∪하위=전체, 교집합=∅ 보장 (로직 변형 금지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기 (이 파일은 수정 대상)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`str | None` 사용)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
