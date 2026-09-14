# 배민 매크로 수집 실패 복구 + Retry DAG 미실행 수정

## Task

2026-07-09 `DB_Beamin_Macro_Dags` run에서 12계정 중 4계정이 수집 실패했는데 `DB_Beamin_Macro_Dags_Retry`가 한 번도 트리거되지 않아 해당 매장 데이터가 영구 누락됐다.
원인은 두 가지다. (1) Chrome renderer 타임아웃이 배민 파이프라인에서 "복구 가능한 드라이버 장애"로 분류되지 않아 세션 재생성 없이 계정을 통째로 버린다. (2) 재시도 단계에서 잔여 실패 신호가 소실되어 Retry DAG 트리거 게이트가 "재시도 불필요"로 오판한다.
실패를 복구하고, 복구 못 하면 반드시 Retry DAG로 넘어가게 만든다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG에는 오케스트레이션만, 비즈니스 로직은 `modules/transform/pipelines/`

## Files to Create / Modify

**Create**: 없음

**Modify**:
- `modules/extract/croling_beamin.py` — crash 키워드에 renderer 타임아웃 추가
- `modules/transform/pipelines/db/DB_Beamin_combined.py` — 세션 마커 추가 / 대시보드 로드 재시도 / `retry_once_failed` 반환 dict화
- `modules/transform/pipelines/db/DB_Beamin_05_ad_funnel.py` — `set_script_timeout` + 크래시 시 raise
- `modules/transform/pipelines/db/DB_Beamin_retry.py` — `retry_needed` 게이트 순서 수정
- `dags/db/DB_Beamin_Macro_Dags.py` — `retry_failed` residual push / `trigger_retry_if_needed` residual 소비 / `validate_toorder` blind 경고
- `dags/db/DB_Beamin_Macro_Dags_Retry.py` — residual 규약 정합성 확인 (변경 없을 수도 있음)
- `tests/test_beamin_retry_conf.py` — 회귀 테스트 추가

## Implementation Steps

### 1. renderer 타임아웃을 복구 가능 장애로 인식

`modules/extract/croling_beamin.py:1393` `_DRIVER_CRASH_KEYWORDS` 튜플에 `"Timed out receiving message from renderer"` 추가.
쿠팡 `modules/extract/croling_coupang.py:105-118` `_CRASH_KEYWORDS`에는 이미 존재하므로 문구를 그대로 맞춘다.

`is_driver_crash_error()` (croling_beamin.py:1410)가 소문자 정규화를 하는지 확인하고, 안 하면 대소문자 무관 비교로 맞출 것. 실제 로그 문구는 `"Message: timeout: Timed out receiving message from renderer: 32.167"`.

`modules/transform/pipelines/db/DB_Beamin_combined.py:140` `_SESSION_ISSUE_MARKERS`(전부 소문자 비교)에 `"timed out receiving message from renderer"` 추가.

이 변경만으로 `_build_dashboard_session`, `_recover_driver_for_stage`, `DB_Beamin_01_now._is_recoverable_driver_error`가 모두 세션 재생성 경로를 탄다.

### 2. 대시보드 매장목록 조회에 세션 재생성 재시도 추가

`DB_Beamin_combined.py:960-1010` 계정 루프. 현재 `wait_for_page(bootstrap_driver, "select[class*='ShopSelect']", timeout=60)`가 실패하면 즉시 `RuntimeError("main dashboard load failed")`를 던져 계정을 버린다 — **재시도가 아예 없다.**

드라이버를 닫고 `_build_dashboard_session(account, metrics, profile)`으로 1회 재생성 후 재시도한 뒤 실패 처리한다.
- 재시도 횟수는 `profile["max_session_recovery_per_account"]` 재사용
- 성공적으로 재생성하면 `metrics["session_recovery_count"] += 1`
- `metrics["page_timeout_count"]` 증가 규약 유지

### 3. ad_funnel 공유 드라이버 경로 강화

`DB_Beamin_05_ad_funnel.py:640-664` `collect_ad_funnel_for_driver()`:
- `driver.set_page_load_timeout(45)` 다음에 `driver.set_script_timeout(60)` 추가 (독립 세션 버전 `launch_browser`와 정렬)
- `except Exception as exc:` 안에서 `is_driver_crash_error(exc)`면 **`raise`**하여 상위 스테이지 복구 로직(`_recover_driver_for_stage`)이 드라이버를 재생성하게 한다. 크래시가 아닌 경우만 기존대로 `logger.warning` + `return False`.

참고: 독립 세션 버전 `collect_ad_funnel_for_account()` (line 665-748)는 이미 `max_attempts=3` + 드라이버 재생성을 갖고 있다. 공유 드라이버 버전만 비대칭적으로 취약하다.

### 4. 잔여 실패 신호를 끝까지 전파 — 핵심

실패 신호가 세 군데서 새어나간다. 순서대로 막는다.

#### 4-1. `retry_once_failed()`가 잔여 실패를 반환

`DB_Beamin_combined.py:737`
```python
def retry_once_failed(failed, target_date=None) -> dict:   # str → dict
```
- 현재 `collect_now_and_woori(failed["accounts"], target_date=target_date)` (line 753)의 반환값을 **버리고 있다**. `result["failed"]`를 받아 누적할 것.
- stores/orders 루프의 `except` 분기(line 787-788 등)에서도 실패 항목을 residual 리스트에 append.
- 반환: `{"summary": "<기존 문자열>", "residual_failed": {"accounts": [...], "stores": [...], "orders": [...], "ads": [...]}}`
- **호환 주의**: 반환 타입이 바뀌므로 `retry_once_failed` / `pipeline_retry_failed` 호출부를 전수 확인 (`dags/db/DB_Beamin_Macro_Dags.py`, `dags/db/DB_Beamin_Macro_Dags_Retry.py`, `modules/transform/pipelines/db/DB_Beamin_retry.py`, `tests/`).

#### 4-2. `retry_failed` task가 잔여 실패를 XCom push

`dags/db/DB_Beamin_Macro_Dags.py:337-366`
- `ti.xcom_push(key="residual_failed", value=<residual dict>)`
- 즉시 재시도 대상이 없던 조기 반환 분기에서는 `collect_all.failed`의 `stores`/`ads`를 그대로 residual로 넘긴다 (deferred 항목이 사라지지 않도록)
- 반환 문자열이 **잔여 실패를 반영**해야 한다: `"재시도 완료: 대상 accounts=4 → 잔여 accounts=3"` 형태.
  현재처럼 대상 개수만 찍으면(`"재시도 완료: accounts=4 stores=0 orders=0 ads=0"`) 알림에서 실패가 성공으로 읽힌다.

#### 4-3. `retry_needed()`의 실패계정 게이트를 무조건 평가

`modules/transform/pipelines/db/DB_Beamin_retry.py:278`. 현재 `count_failed_items(failed) > 0` 검사가 `toorder_result is None`일 때만 실행된다. 2026-07-09 run은 `validate_toorder`가 정상 종료(`비교 0개 매장 / 불일치 0개`)해 `toorder_result`가 non-None → 게이트를 못 타고 `False` 반환 → 트리거 스킵.

```python
def retry_needed(toorder_result, ad_funnel_result, failed=None) -> bool:
    if count_failed_items(failed) > 0:          # ← toorder_result 여부와 무관하게 최우선 검사
        return True
    if ad_funnel_result and ad_funnel_result.get("still_empty"):
        return True
    if toorder_result is None:
        return False
    if toorder_result.get("missing_brand_stores"):
        return True
    mismatched = set(toorder_result.get("mismatched_stores") or [])
    if mismatched:
        retried = set(toorder_result.get("retried_stores") or [])
        return bool(mismatched - retried)
    return False
```

여기 `failed`에 넘기는 값은 **`collect_all.failed`가 아니라 `retry_failed.residual_failed`**여야 한다. 아니면 t3가 성공적으로 복구한 항목까지 재트리거되어 무한 재시도 방향으로 흐른다. (`MAX_ATTEMPTS=3` + 결정론적 run_id가 상한을 잡아주지만 애초에 넣지 않는 게 맞다.)

#### 4-4. `trigger_retry_if_needed`가 residual 기준으로 판단

`dags/db/DB_Beamin_Macro_Dags.py:1136-1190`
- `failed = ti.xcom_pull(task_ids="retry_failed", key="residual_failed")`, 없으면 `ti.xcom_pull(task_ids="collect_all", key="failed")`로 fallback (구버전 run 호환)
- `count_failed_items(failed) == 0` 조기 반환과 `build_retry_conf(failed=...)` 모두 residual을 쓴다
- `dags/db/DB_Beamin_Macro_Dags_Retry.py`의 self-chain(line 232-255)도 동일 규약을 쓰는지 확인

### 5. 검증이 실패 매장을 못 보는 문제

`dags/db/DB_Beamin_Macro_Dags.py`의 `validate_toorder` (뒤쪽 정의, line 1079-).

실패한 4계정은 매장 목록 조회 자체가 실패해 `store_info_per_account`에 없다. 그래서 비교 대상 0개로 `"비교 0개 매장 / 불일치 0개"` = 이상 없음을 보고했다. **실패가 클수록 검증이 조용해지는 역방향 구조.**

- `load_accounts.account_list` 대비 비교 매장 수가 0이거나 현저히 적으면 `logger.warning`을 남기고 결과 dict에 `"blind": True`를 실어 알림 본문에 노출
- **주의**: 이걸 `retry_needed`의 트리거 조건으로 삼지 말 것. 4-3의 residual 게이트가 이미 커버하고, 중복 트리거는 `selenium_pool` 슬롯 경합을 만든다.

### 6. (별도 커밋) `비고` 컬럼 유실 — P2

`modules/transform/utility/account.py:100`에서 `sales_employee.csv`에 `비고` 컬럼이 없다고 경고한다. `dags/sales/Sales_Employee_Extract_Dags.py:201` 근처에서 원본 시트에 `비고`가 없으면 조용히 drop되기 때문.

현재는 fallback(account.py:101-110)이 동작해 12계정 로드에 성공했으나, `target_stores`가 비면 **대상 0건**이 된다. 표준화 후 `AUTOMATION_NOTE_COL`이 없으면 `logger.error` + run 실패로 처리할지 결정 필요. **이번 P0 수정과 분리해 진행.**

## Reference Code

### modules/extract/croling_coupang.py (line 105-122) — 목표 패턴
```python
_CRASH_KEYWORDS = (
    "Remote end closed",
    "Connection aborted",
    "RemoteDisconnected",
    "Connection refused",
    "Max retries exceeded",
    "NewConnectionError",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "Timed out receiving message from renderer",   # ← 배민에 없는 항목
)


def is_driver_crash_error(exc: Exception) -> bool:
    return any(keyword in str(exc) for keyword in _CRASH_KEYWORDS)
```

### modules/extract/croling_beamin.py (line 1393-1413) — 현재 상태
```python
_DRIVER_CRASH_KEYWORDS = (
    "Remote end closed",
    "Connection aborted",
    "RemoteDisconnected",
    "Connection refused",
    "Max retries exceeded",
    "NewConnectionError",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "cannot connect to chrome",
    "session not created",
)   # ← renderer 타임아웃 없음


def is_driver_crash_error(exc: Exception) -> bool:
    """예외가 Chrome/chromedriver 프로세스 사망(연결 끊김) 계열인지 판별."""
    msg = str(exc)
    return any(keyword in msg for keyword in _DRIVER_CRASH_KEYWORDS)
```

### modules/transform/pipelines/db/DB_Beamin_combined.py (line 140-215)
```python
_SESSION_ISSUE_MARKERS = (
    "remote end closed",
    "connection aborted",
    "remotedisconnected",
    "connection refused",
    "max retries exceeded",
    "newconnectionerror",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "cannot connect to chrome",
    "session not created",
)   # ← renderer 타임아웃 없음


def _is_recoverable_session_issue(exc: Exception) -> bool:
    error_text = str(exc).lower()
    return is_driver_crash_error(exc) or any(
        marker in error_text for marker in _SESSION_ISSUE_MARKERS
    )


def _build_dashboard_session(account: dict, metrics: dict, profile: dict) -> Any | None:
    account_id = account["account_id"]
    max_recovery = int(profile["max_session_recovery_per_account"])
    for attempt in range(max_recovery + 1):
        driver = _build_account_session(account, metrics, profile)
        if driver is None:
            return None
        try:
            if not is_on_main_dashboard(driver.current_url):
                driver.set_page_load_timeout(45)
                driver.get("https://self.baemin.com/")
            return driver
        except Exception as exc:
            if _is_recoverable_session_issue(exc):
                _close_driver_safely(driver, account_id)
                if attempt < max_recovery:
                    metrics["session_recovery_count"] += 1
                    logger.info("초기 대시보드 세션 문제 감지, 재생성 시도: %s / %s", account_id, exc)
                    continue
                logger.warning("초기 대시보드 세션 문제 재생성 한도 초과: %s / %s", account_id, exc)
                return None
            logger.info("대시보드 이동 타임아웃 (계속 진행): %s", exc)
            return driver
    return None
```

### modules/transform/pipelines/db/DB_Beamin_combined.py (line 960-1010) — 재시도 없는 대시보드 로드
```python
        account_id = account["account_id"]
        requested_store_name = str(account.get("store_name") or "").strip()
        bootstrap_driver = _build_dashboard_session(account, metrics, profile)
        if bootstrap_driver is None:
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            continue

        store_list: list[dict] = []
        try:
            if not wait_for_page(bootstrap_driver, "select[class*='ShopSelect']", timeout=60):
                metrics["page_timeout_count"] += 1
                raise RuntimeError(f"main dashboard load failed: {account_id}")   # ← 재시도 없이 계정 폐기

            raw_options = get_store_options(bootstrap_driver)
            store_list = _build_store_list_from_options(raw_options)
            ...
        except Exception as exc:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, exc, exc_info=True)  # line 1001
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            store_list = []
        finally:
            if not store_list:
                try:
                    bootstrap_driver.quit()
                except Exception:
                    pass
```

### modules/transform/pipelines/db/DB_Beamin_combined.py (line 737-755) — 반환값 폐기 지점
```python
def retry_once_failed(failed: dict, target_date: str | None = None) -> str:
    """실패한 계정/매장만 1회 재시도."""
    n_accounts = len(failed.get("accounts", []))
    n_stores = len(failed.get("stores", []))
    n_orders = len(failed.get("orders", []))
    n_ads = len(failed.get("ads", []))
    logger.info("재시도 시작: accounts=%d stores=%d orders=%d ads=%d", n_accounts, n_stores, n_orders, n_ads)

    # 1. 계정 레벨 실패 → 해당 계정 전체 재수집
    if failed.get("accounts"):
        collect_now_and_woori(failed["accounts"], target_date=target_date)   # ← 반환 dict 폐기, 잔여 실패 소실
```

### modules/transform/pipelines/db/DB_Beamin_05_ad_funnel.py (line 640-664)
```python
def collect_ad_funnel_for_driver(driver, store_info: dict, target_date: str | None = None) -> bool:
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand = store_info["brand"]
    store = store_info["store"]
    logger.info("Ad funnel collection start: %s (%s) / %s", store, store_id, target_date)

    try:
        driver.set_page_load_timeout(45)
        # ← set_script_timeout 없음
        driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            logger.warning("Ad funnel page failed to load, skipping: %s", store)
            return False

        return _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date)
    except Exception as exc:
        logger.warning("Ad funnel collection failed (%s): %s", store, exc)   # line 663
        return False   # ← 크래시도 조용히 삼킴, 상위 복구 안 탐
```

### modules/transform/pipelines/db/DB_Beamin_retry.py (line 61-63, 278-293)
```python
def count_failed_items(failed: dict | None) -> int:
    data = failed or {}
    return sum(len(data.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))


def retry_needed(toorder_result: dict | None, ad_funnel_result: dict | None, failed: dict | None = None) -> bool:
    if ad_funnel_result and ad_funnel_result.get("still_empty"):
        return True

    if toorder_result is None:
        return count_failed_items(failed) > 0     # ← toorder가 돌면 이 줄에 도달 못 함

    if toorder_result.get("missing_brand_stores"):
        return True

    mismatched = set(toorder_result.get("mismatched_stores") or [])
    if mismatched:
        retried = set(toorder_result.get("retried_stores") or [])
        return bool(mismatched - retried)

    return False     # ← 실패계정 4개가 있어도 여기로 떨어짐
```

### dags/db/DB_Beamin_Macro_Dags.py (line 337-366) — retry_failed task
```python
def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")

    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or {}
    immediate_failed = {
        "accounts": failed.get("accounts") or [],
        "stores": [],
        "orders": failed.get("orders") or [],
        "ads": [],
    }
    deferred_counts = {"stores": len(failed.get("stores") or []), "ads": len(failed.get("ads") or [])}
    if not any(immediate_failed.get(k) for k in ("accounts", "orders")):
        logger.info("즉시 재시도 대상 없음: deferred stores=%d ads=%d", deferred_counts["stores"], deferred_counts["ads"])
        if deferred_counts["stores"] or deferred_counts["ads"]:
            return ("즉시 재시도 없음 "
                    f"(검증 단계로 이관: stores={deferred_counts['stores']} ads={deferred_counts['ads']})")
        return "재시도 없음"

    return pipeline_retry_failed(immediate_failed, target_date=target_date)   # ← residual XCom push 없음
```

### dags/db/DB_Beamin_Macro_Dags.py (line 1136-1160) — trigger_retry_if_needed
```python
def trigger_retry_if_needed(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}   # ← residual로 교체 대상
    failed_count = count_failed_items(failed)
    if failed_count == 0:
        logger.info("Retry DAG 트리거 스킵: 원본 실패 없음")
        return "Retry DAG 트리거 스킵: 원본 실패 없음"

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result")
    if not retry_needed(toorder_result, ad_funnel_result, failed):
        logger.info("Retry DAG 트리거 스킵: 검증상 추가 재시도 불필요")   # ← 2026-07-09 run이 여기서 종료됨
        return "Retry DAG 트리거 스킵: 추가 재시도 불필요"

    source_run_id = getattr(dag_run, "run_id", context.get("run_id", "manual"))
    retry_conf = build_retry_conf(
        failed=failed, target_date=target_date, source_dag_id=dag_id,
        source_run_id=source_run_id, attempt=1, max_attempts=int(conf.get("max_attempts", 3)),
    )
```

## Test Cases

1. **[회귀] 실패 계정이 남으면 retry_needed=True**
   ```
   pytest tests/test_beamin_retry_conf.py -v -k retry_needed
   ```
   테스트 내용:
   - `retry_needed({"missing_brand_stores": [], "mismatched_stores": []}, {"still_empty": []}, failed={"accounts": [{"account_id": "dbtjr3333"}]})` → 기대: `True`
     (수정 전 코드로 돌리면 `False` — 즉 **먼저 실패하는 테스트를 작성**한 뒤 코드를 고칠 것)
   - `retry_needed({"missing_brand_stores": [], "mismatched_stores": []}, {"still_empty": []}, failed={})` → 기대: `False`
   - `retry_needed(None, None, failed={"orders": [{"x": 1}]})` → 기대: `True`

2. **[크래시 판별] renderer 타임아웃 인식**
   ```
   python -c "from modules.extract.croling_beamin import is_driver_crash_error; assert is_driver_crash_error(Exception('Message: timeout: Timed out receiving message from renderer: 32.167')); print('PASS')"
   ```
   → 기대: `PASS`

3. **[세션 마커] combined 복구 판정**
   ```
   python -c "from modules.transform.pipelines.db.DB_Beamin_combined import _is_recoverable_session_issue; assert _is_recoverable_session_issue(Exception('timeout: Timed out receiving message from renderer: 44.618')); print('PASS')"
   ```
   → 기대: `PASS`

4. **[반환 스키마] retry_once_failed가 dict 반환**
   ```
   pytest tests/test_beamin_retry_conf.py -v -k residual
   ```
   → 기대: 반환 dict에 `summary`(str), `residual_failed`(dict, 키 4개: accounts/stores/orders/ads) 존재

5. **[DAG import] 파싱 회귀 없음**
   ```
   python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print('PASS')"
   python -c "from dags.db.DB_Beamin_Macro_Dags_Retry import dag; print('PASS')"
   ```
   → 기대: ImportError / NameError 없음

6. **[DAG 등록] import 에러 목록 비어 있음**
   ```
   airflow dags list-import-errors
   ```
   → 기대: 배민 관련 항목 없음

7. **[운영 확인 — 코드 밖 축] Retry DAG가 paused 상태인지 확인**
   ```
   airflow dags details DB_Beamin_Macro_Dags_Retry
   ```
   → 기대: `is_paused = False`.
   **True면 위 코드 수정과 무관하게 DagRun이 생성만 되고 task는 영원히 안 돈다. 가장 먼저 확인할 것.**

8. **[드라이런] Retry DAG 수동 트리거**
   ```
   airflow dags trigger DB_Beamin_Macro_Dags_Retry -c '{"attempt":1,"max_attempts":3,"target_date":"2026-07-09","failed_account_ids":["dbtjr3333"]}'
   ```
   → 기대: `retry_collect`가 계정을 복원해 실제 수집까지 진행. `selenium_pool` 슬롯 대기에 걸려 queued로 멈추지 않을 것.

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행 (7, 8은 컨테이너 기동 시에만)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

컨테이너 내 실행이 필요하면: `docker compose exec airflow-scheduler pytest tests/test_beamin_retry_conf.py -v`

## Constraints

- **`dags/db/DB_Beamin_Macro_Dags.py`에는 `validate_orders` / `validate_ad_funnel` / `validate_toorder`가 중복 정의되어 있다** (line 369/413/447 vs line 1018/1046/1079). 파이썬은 나중 정의가 이긴다. **반드시 뒤쪽(line 1018+) 정의를 수정할 것.** 앞쪽 `validate_orders`(line 394)는 미정의 변수 `missing_brand_stores`를 참조해 실행되면 `NameError`다. 죽은 앞쪽 정의 삭제는 **별도 커밋으로 분리**.
- `retry_once_failed` 반환 타입 변경(str → dict)은 호출부 전수 수정이 필요하다. 누락 시 알림 본문이 dict를 그대로 찍는다.
- `retry_needed`에 넘기는 `failed`는 반드시 **residual**(t3 이후 잔여)이어야 한다. `collect_all.failed`(원본)를 넘기면 이미 복구된 항목까지 재트리거된다.
- `validate_toorder`의 `blind` 플래그를 `retry_needed` 트리거 조건으로 삼지 말 것 — residual 게이트와 중복되어 `selenium_pool` 슬롯 경합을 만든다.
- 폴더 구조 / 파일명 변경 금지.
- `print` 금지, `logger` 사용.
- Step 6(`비고` 컬럼)은 이번 P0 범위 밖 — 별도 커밋.
- 기존 `metrics` 카운터 키(`page_timeout_count`, `session_recovery_count`, `failed_accounts`) 규약을 그대로 유지할 것.
- `MAX_ATTEMPTS=3`과 결정론적 run_id(`retry__{date}__attempt_N__{root}`)는 재트리거 폭주 방지 장치다. 건드리지 말 것 (`prd_codex/배민매크로재시도폭주수정_260707.md` 참조).

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게

## 2026-07-10 실행 보완 기록

### 실제 실패 run 확인

- 원본 DAG: `DB_Beamin_Macro_Dags`
- 원본 run_id: `scheduled__2026-07-08T18:10:00+00:00`
- 수집 대상일: `2026-07-09`
- 원본 run 상태: `success`
- Retry DAG 상태: `retry__20260709...` run 없음
- XCom 확인 결과:
  - `collect_all.failed.accounts`: 4건
  - `collect_all.failed.stores`: 1건
  - `retry_failed.return_value`: `재시도 완료: accounts=4 stores=0 orders=0 ads=0`
  - `retry_failed.residual_failed`: 없음
  - `validate_toorder.return_value`: `비교 0개 매장 / 불일치 0개`
  - `trigger_retry_if_needed.return_value`: `Retry DAG 트리거 스킵: 추가 재시도 불필요`

### 즉시 복구 실행 절차

수동 복구는 `DB_Beamin_Macro_Dags_Retry`를 직접 트리거한다. 비밀번호는 conf에 넣지 않고 Retry pipeline의 계정 복원 로직이 `account_id` 기준으로 로드하게 한다.

```powershell
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_Beamin_Macro_Dags_Retry -r manual_fix__20260709__failed_accounts_store__001 -c '{"attempt":1,"max_attempts":3,"target_date":"2026-07-09","source_dag_id":"DB_Beamin_Macro_Dags","source_run_id":"scheduled__2026-07-08T18:10:00+00:00","retry_wait_sec":0,"failed_account_ids":["dbtjr3333","now1223","m2329921","skmo1004","hjh7312"],"failed_accounts_ids_only":["dbtjr3333","now1223","m2329921","skmo1004"],"failed_stores":[{"account_id":"hjh7312","store":{"store_id":"14352139","brand":"도리당","store":"법흥리점"}}],"failed_orders":[],"failed_ads":[],"retry_history":{}}'
```

실행 후 확인:

```powershell
docker exec airflow-airflow-scheduler-1 airflow dags list-runs -d DB_Beamin_Macro_Dags_Retry -s 2026-07-09 -o table
docker exec airflow-airflow-scheduler-1 airflow tasks states-for-dag-run DB_Beamin_Macro_Dags_Retry manual_fix__20260709__failed_accounts_store__001
```

### 구현 보완

- renderer timeout을 배민 공통 crawler와 우가클 단계의 recoverable crash로 추가했다.
- `collect_now_and_woori`와 `collect_orders_only`의 대시보드 매장목록 로드 실패 시 세션 재생성 재시도를 추가했다.
- `retry_once_failed()`는 `summary`와 `residual_failed`를 포함한 dict를 반환한다.
- `DB_Beamin_retry.restore_failed_from_conf()`는 `비고` 컬럼 유실로 전 계정 로드가 빈 목록이 될 때, Retry conf의 `account_id` 기준으로 `sales_employee.csv`를 좁게 fallback 로드한다.
- Retry 계정 재수집이 전 계정 실패로 예외를 던져도 `retry_collect_from_conf()`가 task를 죽이지 않고 해당 계정들을 `residual_failed.accounts`로 보존한다.
- 메인 DAG의 `retry_failed`는 즉시 재시도 후 잔여 실패와 deferred store 실패를 `residual_failed` XCom으로 남긴다.
- `trigger_retry_if_needed`는 `retry_failed.residual_failed`를 우선 사용하고, 없을 때만 구버전 run 호환용으로 `collect_all.failed`를 사용한다.
- Retry DAG self-chain은 `retry_payload.residual_failed`를 `retry_needed`에 전달하고, 잔여 account 실패를 다음 attempt conf에 유지한다.
- Retry DAG는 `retry_payload`가 없으면 notify task도 `RuntimeError`를 발생시켜 leaf task 성공으로 DAG가 false success 처리되는 것을 막는다.
- `validate_toorder`는 수집 계정 대비 매장정보가 없거나 부족하면 `blind=True`를 결과에 남기고 로그/요약에 노출한다.

### 검증 결과

```powershell
python -X utf8 -m py_compile modules\extract\croling_beamin.py modules\transform\pipelines\db\DB_Beamin_02_woori_shop_click.py modules\transform\pipelines\db\DB_Beamin_combined.py modules\transform\pipelines\db\DB_Beamin_05_ad_funnel.py modules\transform\pipelines\db\DB_Beamin_retry.py dags\db\DB_Beamin_Macro_Dags.py dags\db\DB_Beamin_Macro_Dags_Retry.py dags\db\DB_Beamin_Macro_Pc2_Dags.py
python -X utf8 -m pytest tests/test_beamin_retry_conf.py -q
python -X utf8 -m pytest tests/test_baemin_macro_driver_recovery.py -q -k "renderer_timeout or collect_now_and_woori"
```

- 결과: py_compile 통과, `tests/test_beamin_retry_conf.py` 9 passed, driver recovery 핵심 3 passed.
- 참고: `tests/test_baemin_macro_driver_recovery.py` 전체 실행 시 `test_shop_change_save_csv_keeps_same_timestamp_multiple_rows`가 CSV 읽기와 parquet 출력 포맷 충돌로 실패한다. 이번 Retry DAG 수정 범위 밖의 기존 테스트/저장 포맷 불일치다.

### 수동 복구 실행 결과

- `manual_fix__20260709__failed_accounts_store__001`: 최초 JSON CLI 인용 실패 후 Python trigger로 생성. 당시 계정 복원 fallback 전이라 `retry_collect`가 `재시도 대상 없음`으로 종료.
- `manual_fix__20260709__failed_accounts_store__002`: 계정 복원 fallback 반영 후 5계정 실제 로그인/대시보드 로드까지 진행.
  - 모든 계정에서 `Timed out receiving message from renderer`가 반복되어 대시보드 매장목록 로드 실패.
  - 세션 재생성 재시도는 정상 동작했고, 계정별로 복구 한도 초과 후 다음 계정으로 진행.
  - 실행 중이던 task에는 예외→residual 보완 전 코드가 로드되어 `retry_collect`는 `배민 collect_all 전체 계정 수집 실패(성공 0/5 계정)`로 failed.
  - 후속 task의 `ALL_DONE` 때문에 DAG run이 success로 보이는 false success 구조를 확인했고, 이후 `retry_payload 없음`이면 notify task가 실패하도록 수정.
