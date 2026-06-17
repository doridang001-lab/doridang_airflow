# 배민 수집 DAG — retry_failed 오탐 실패 개선

## Task
`DB_Beamin_Macro_Dags`가 부분 실패(1개 계정 재시도 실패)를 DAG 전체 실패로 격상시켜 오탐 알림을 보낸다. 원인은 `retry_once_failed`가 본 수집용 `collect_now_and_woori`를 재사용하는데, 그 함수가 "전 계정 실패면 무조건 raise"하기 때문이다. 재시도를 best-effort로 바꿔, 이미 다른 계정으로 수집된 매장의 계정 실패가 DAG를 죽이지 않게 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_Beamin_combined.py`
  - `collect_now_and_woori()` — raise 가드(현재 line 961-962)에 파라미터 게이트 추가
  - `retry_once_failed()` — 계정 재수집 호출(현재 line 648-649)을 best-effort로
- **수정(Phase 2, 선택)** `dags/db/DB_Beamin_Macro_Dags.py`
  - `retry_failed()` (현재 line 336-346) — `store_info_per_account` XCom을 함께 pull해 전달

## Implementation Steps

### Phase 1 (필수) — 재시도를 best-effort로

1. **`collect_now_and_woori` 시그니처에 `raise_on_all_fail: bool = True` 추가.**
   현재(line 773-777 부근):
   ```python
   def collect_now_and_woori(
       account_list: list[dict],
       target_date: str | None = None,
       stability_profile: str | None = None,
   ) -> dict:
   ```
   →
   ```python
   def collect_now_and_woori(
       account_list: list[dict],
       target_date: str | None = None,
       stability_profile: str | None = None,
       raise_on_all_fail: bool = True,
   ) -> dict:
   ```

2. **raise 가드(현재 line 961-962)를 파라미터로 게이트.**
   ```python
   if success == 0 and total > 0:
       raise RuntimeError(f"Baemin collect_all failed for all accounts ({summary})")
   ```
   →
   ```python
   if raise_on_all_fail and success == 0 and total > 0:
       raise RuntimeError(f"Baemin collect_all failed for all accounts ({summary})")
   ```
   → 본 수집(collect_all)은 `raise_on_all_fail` 미지정 → 기본 `True` → 동작 불변.

3. **`retry_once_failed`의 계정 재수집 호출(현재 line 648-649)을 best-effort로.**
   ```python
   if failed.get("accounts"):
       collect_now_and_woori(failed["accounts"], target_date=target_date)
   ```
   →
   ```python
   if failed.get("accounts"):
       try:
           collect_now_and_woori(
               failed["accounts"], target_date=target_date, raise_on_all_fail=False
           )
       except Exception as exc:
           logger.warning("계정 재시도 실패(무시): %s", exc)
   ```
   → 재시도에서 계정이 또 실패해도 `retry_failed` 태스크는 SUCCESS로 끝남.
   → DAG status는 `notify_collection_result`가 hard_failure 없음으로 보고 "부분성공" 표기.

### Phase 2 (권장, 사용자 확인 후) — 이미 수집된 매장이면 재시도 스킵

4. `dags/db/DB_Beamin_Macro_Dags.py`의 `retry_failed`에서 `store_info_per_account` XCom을 함께 pull:
   ```python
   store_info_per_account = context["ti"].xcom_pull(
       task_ids="collect_all", key="store_info_per_account"
   ) or []
   ```
   이미 성공 수집된 store 집합을 만들어 `pipeline_retry_failed(failed, target_date=..., already_collected_stores=...)`로 전달.

5. `retry_once_failed`에 `already_collected_stores: set[str] | None = None` 파라미터 추가.
   `failed["accounts"]` 중 계정의 `store_name`(시드 CSV 매장명)이 already_collected에 포함되면
   재시도 목록에서 제외하고 로그만:
   `logger.info("이미 다른 계정으로 수집됨, 재시도 스킵: %s", store_name)`.
   매장명 매칭은 `from modules.transform.utility.store_normalize import normalize, strip_brand` 재사용(브랜드 접두 차이 흡수).

## Reference Code

### modules/transform/pipelines/db/DB_Beamin_combined.py (retry_once_failed)
```python
def retry_once_failed(failed: dict, target_date: str | None = None) -> str:
    """실패한 계정/매장만 1회 재시도."""
    n_accounts = len(failed.get("accounts", []))
    n_stores = len(failed.get("stores", []))
    n_orders = len(failed.get("orders", []))
    logger.info("재시도 시작: accounts=%d stores=%d orders=%d", n_accounts, n_stores, n_orders)

    # 1. 계정 레벨 실패 → 해당 계정 전체 재수집
    if failed.get("accounts"):
        collect_now_and_woori(failed["accounts"], target_date=target_date)

    # 2. per-store 레벨 실패 → 해당 계정 로그인 후 해당 매장만
    for item in failed.get("stores", []):
        account = item["account"]
        store_info = item["store"]
        ...
```

### modules/transform/pipelines/db/DB_Beamin_combined.py (collect_now_and_woori 말미)
```python
    total = success + fail
    summary = f"성공 {success}/{total} 계정"
    metrics["ended_at"] = datetime.now(UTC).isoformat()
    logger.info(summary)

    if success == 0 and total > 0:
        raise RuntimeError(f"Baemin collect_all failed for all accounts ({summary})")

    return {
        "summary": summary,
        "failed": {
            "accounts": failed_accounts,
            "stores": failed_stores,
            "orders": failed_orders,
            "ads": failed_ads,
        },
        "validation": validation_results,
        "ad_stores": ad_store_infos,
        "store_info_per_account": store_info_per_account_list,
    }
```

### dags/db/DB_Beamin_Macro_Dags.py (retry_failed)
```python
def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")

    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or {}
    if not any(failed.get(k) for k in ("accounts", "stores", "orders", "ads")):
        logger.info("재시도 대상 없음")
        return "재시도 없음"

    return pipeline_retry_failed(failed, target_date=target_date)
```

## Test Cases
1. [구문] `python -c "import ast; ast.parse(open('modules/transform/pipelines/db/DB_Beamin_combined.py',encoding='utf-8').read()); print('OK')"` → 기대: `OK`
2. [시그니처] `python -c "import inspect; from modules.transform.pipelines.db.DB_Beamin_combined import collect_now_and_woori as f; assert 'raise_on_all_fail' in inspect.signature(f).parameters; print('OK')"` → 기대: `OK`
3. [DAG import] `python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print(dag.dag_id)"` → 기대: `DB_Beamin_Macro_Dags`, ImportError 없음
4. [동작] `retry_once_failed`가 계정 재수집 실패 시 raise를 흡수하는지 — 존재하지 않는 더미 계정으로 호출해 예외 없이 반환되는지 확인(브라우저 실행 실패가 try/except로 흡수되어 함수가 정상 종료).
5. [DAG import errors] `docker exec airflow-airflow-scheduler-1 airflow dags list-import-errors` → 기대: 본 DAG 관련 오류 없음

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
- `collect_now_and_woori`의 기본 동작(본 수집 collect_all 경로)은 **절대 변경 금지** — `raise_on_all_fail` 기본값 `True`로 기존 raise 유지.
- 실패 정보(`failed` dict, 알림 본문)는 그대로 보존 — 실패를 숨기는 게 아니라 "DAG 전체 실패 격상"만 막는 것.
- 대시보드 로드 타임아웃(60s)·재시도 횟수는 이번 변경에서 건드리지 않는다(사이트 지연성 원인이라 효과 제한적).
- Phase 2는 Phase 1 적용·검증 후 별도로 진행. 사용자 확인 전 자동 적용 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정 대상 기존 파일)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게(있음)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
