# DB_Beamin_Macro_Dags 반복 실패 수정 (어제만 수집 + 대시보드 task 제거)

## Task
배민 매크로 수집 DAG가 매일 `collect_all` 단계에서 300분(5h) `execution_timeout`에 걸려 실패하고 텔레그램 실패 알림을 보낸다. 원인은 `ORDERS_BACKFILL_DAYS=7`이 매일 7일치 주문을 재수집하며 Chrome을 수백 회 띄우다 후반부 `Read timed out`(30s) 다발로 시간 예산을 소진하기 때문. **항상 어제 1일만 수집하도록 백필을 제거하고, 불필요한 `start_dashboard` task도 삭제**한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG는 orchestration만, 비즈니스 로직은 pipelines로

## Files to Create / Modify
- 수정: `dags/db/DB_Beamin_Macro_Dags.py` (유일 변경 파일)

## Implementation Steps

### 1) 항상 "어제만" 수집 — 백필 비활성화 + 백필 코드 제거
1. 상수 변경: `ORDERS_BACKFILL_DAYS: int | None = 7` → **`ORDERS_BACKFILL_DAYS: int | None = None`** (약 L91).
2. 실제 바인딩되는 `collect_all`(L958~1033) 내부의 백필 관련 코드 블록 제거:
   - `backfill_dates` 계산 블록 (`if target_date is None and ORDERS_BACKFILL_DAYS is not None:` ~ `backfill_dates` 로깅까지)
   - 백필 루프 전체 (`backfill_summary = ""` ~ `logger.info("백필 완료: ...")` 구간, 약 L1001-1028)
   - 말미의 `if backfill_summary:` 로 summary에 덧붙이는 부분
   - 결과적으로 `collect_all`은 `result = pipeline_collect_all(account_list, target_date=target_date, stability_profile=profile["name"])` 후 xcom push만 하고 `return result["summary"]` 로 끝나게 한다.
   - `conf["target_date"]` 가 있으면 그 1일을 수집하는 기존 동작은 유지 (`target_date = conf.get("target_date")`).
3. (정리) 파일 상단의 **미사용 legacy `collect_all`**(약 L326-364, `ORDERS_BACKFILL_DAYS` 기반 다중일 루프 버전)도 삭제 가능하면 삭제. 실제 task에는 L958 정의가 바인딩되어 동작엔 무영향이나 혼란 방지용. 삭제 시 import/참조 깨지지 않는지 확인.

### 2) `start_dashboard` task 및 함수 삭제
1. DAG 본문에서 `t_dash = PythonOperator(task_id="start_dashboard", ...)` 오퍼레이터 제거 (약 L1461-1465).
2. 의존성 체인 수정: `t_dash >> t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8` (L1524)
   → **`t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8`** (선두를 t0=precheck로).
3. `start_dashboard` 콜러블 함수 정의 제거 (약 L1394-1446). 다른 참조 없음(DAG 내부에서만 사용) — 제거 후 grep으로 잔존 참조 없는지 확인.

## Reference Code
### dags/db/DB_Beamin_Macro_Dags.py (현재 실 바인딩 collect_all 일부)
```python
ORDERS_BACKFILL_DAYS: int | None = 7  # → None 으로 변경

def collect_all(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    profile = resolve_stability_profile(conf.get("stability_profile") or "safe_memory")
    wait_sec = random.uniform(*profile["initial_stagger_range"])
    logger.info("수집 시작 전 계정 지터 대기 %.0f초", wait_sec)
    time.sleep(wait_sec)

    target_date = conf.get("target_date")
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    # ↓↓↓ 아래 backfill_dates 계산 + 백필 루프 + backfill_summary 전부 제거 ↓↓↓
    result = pipeline_collect_all(
        account_list, target_date=target_date, stability_profile=profile["name"],
    )
    context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
    store_info_per_account = result.get("store_info_per_account", [])
    context["ti"].xcom_push(key="store_info_per_account", value=store_info_per_account)
    metrics = result.get("metrics")
    if metrics and dag_run:
        write_runtime_metrics(run_id=dag_run.run_id, stage="collect_all", payload=metrics)
    return result["summary"]
```

### DAG 본문 의존성 (변경 후)
```python
    # t_dash (start_dashboard) 제거
    t0 = PythonOperator(task_id="precheck_manual_baemin_orders", ...)
    t1 = PythonOperator(task_id="load_accounts", ...)
    # ... t2~t8 동일 ...
    t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8
```

## Test Cases
1. [import 에러] `python -c "import importlib; importlib.import_module('dags.db.DB_Beamin_Macro_Dags')"` → 기대: 에러 없음
2. [task 목록] `airflow tasks list DB_Beamin_Macro_Dags` → 기대: `start_dashboard` 없음, `precheck_manual_baemin_orders`가 선두
3. [백필 상수] `python -c "import dags.db.DB_Beamin_Macro_Dags as m; assert m.ORDERS_BACKFILL_DAYS is None; print('OK')"` → 기대: `OK`
4. [잔존 참조] `grep -n "start_dashboard\|backfill_summary\|backfill_dates" dags/db/DB_Beamin_Macro_Dags.py` → 기대: 매칭 없음(또는 주석/문서 외 코드 참조 없음)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~4 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `collect_all` 외 다른 task(precheck/validate/notify/retry)의 로직은 건드리지 않는다.
- `conf["target_date"]` 로 특정일 1일 수집하는 기존 동작은 반드시 유지.
- `pipeline_collect_all` 시그니처/호출은 그대로 유지(백필 루프만 제거).
- DAG는 orchestration만 — 비즈니스 로직 추가 금지.
- 파일 내 다른 중복/legacy 함수(`_build_collection_notification_legacy_*` 등)는 이번 범위에서 손대지 않는다.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: 기존 파일 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
