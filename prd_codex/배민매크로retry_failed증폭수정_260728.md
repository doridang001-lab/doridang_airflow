# 배민매크로retry_failed증폭수정

## Task

배민 매크로 수집 DAG(`DB_Beamin_Macro_Dags`)에서 병렬 수집 배치 4개는 전부 성공하는데
후속 `retry_failed` 태스크가 5시간 `execution_timeout`을 넘겨 실패하고, `retries=1` 때문에
처음부터 다시 5시간을 더 도는 문제를 고친다.

근본 원인은 "무시 가능(fatal=False)"으로 분류된 스테이지 실패 1건이 **계정 전체**를
재시도 대상으로 등록하고, 재시도 로직이 그 계정의 **모든 스테이지를 다시 수집**하는 증폭 구조다.
(실측: 렌더러 타임아웃 몇 건 → 12계정 × 20분 × 2회 ≈ 10시간)

재시도 대상을 실제 실패한 **스테이지/매장 단위**로 좁히고, 타임아웃 시 이어받기(resume)를
지원하며, 5시간짜리 중복 재실행을 차단한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG에는 오케스트레이션만, 비즈니스 로직은 `modules/transform/pipelines/`

## Files to Create / Modify

수정만 하며, 신규 파일은 없다.

- `modules/transform/pipelines/db/DB_Beamin_combined.py` (핵심)
- `modules/transform/pipelines/db/DB_Beamin_retry.py`
- `modules/transform/pipelines/db/beamin_staging.py`
- `dags/db/DB_Beamin_Macro_Dags.py`

## Implementation Steps

### 1. `failed` 페이로드에 `stages` 버킷 신설

현재 `failed` dict는 `{"accounts", "stores", "orders", "ads"}` 4개 버킷이다.
여기에 5번째 버킷 `stages`를 추가한다. 항목 스키마:

```python
{"account": {...}, "store": {...}, "stage": "운영시간 수집"}
```

`stage` 값은 `_run_stage()`에 넘기는 `stage_label` 문자열 그대로다.
`track_account_failure=True`가 붙은 스테이지는 아래 3개뿐이다:
`"NOW 수집"`, `"우리가게 수집"`, `"운영시간 수집"` — 전부 `fatal=False`.

### 2. `DB_Beamin_combined.py` — 무시 가능 실패를 계정이 아닌 스테이지로 등록

`collect_now_and_woori()` 내부(1043행 부근)에 `failed_stages: list[dict] = []` 선언 추가.

기존 `_mark_partial_account_failure()`(1057행)를 아래로 교체한다.
**`failed_accounts`에는 절대 넣지 않는다** — 이것이 증폭의 원인이었다.

```python
def _mark_failed_stage(account: dict, account_id: str, store_info: dict, stage_label: str) -> None:
    store_id = str((store_info or {}).get("store_id") or "").strip()
    if any(
        str((item.get("account") or {}).get("account_id") or "") == account_id
        and str((item.get("store") or {}).get("store_id") or "") == store_id
        and item.get("stage") == stage_label
        for item in failed_stages
    ):
        return
    failed_stages.append({"account": account, "store": store_info, "stage": stage_label})
    logger.warning(
        "부분 실패 스테이지 retry 등록: account=%s store=%s stage=%s",
        account_id,
        (store_info or {}).get("store"),
        stage_label,
    )
```

호출부는 `_run_stage()` 안 두 곳(1445-1450행, 1461-1466행). 둘 다 `store_info`를 넘기도록 변경:

```python
if track_account_failure:
    _mark_failed_stage(account, account_id, store_info, stage_label)
```

**주의**: 로그인/드라이버 실패 경로인 `_mark_failed_account()`(1051행)는 **손대지 않는다**.
그건 실제 계정 단위 실패이고 계정 전체 재수집이 맞다.

`failed` dict를 조립하는 모든 지점(1178, 1239, 1297, 1641, 1721행 부근 — 전부 동일 패턴)에
`"stages": failed_stages` 를 추가한다. progress carry 복원부(1110행 부근)에도
`failed_stages.extend(carry_failed.get("stages") or [])` 를 추가한다.

### 3. `DB_Beamin_combined.py` — `collect_now_and_woori()`에 스테이지/매장 필터 추가

시그니처(1026행)에 키워드 인자 2개 추가:

```python
def collect_now_and_woori(
    account_list: list[dict],
    target_date: str | None = None,
    stability_profile: str | None = None,
    woori_only: bool = False,
    *,
    stage_filter: set[str] | None = None,
    store_id_filter: set[str] | None = None,
    progress_file: Path | None = None,
    progress_run_id: str | None = None,
    _allow_login_second_pass: bool = True,
    _raise_on_total_failure: bool = True,
) -> dict:
```

`_run_stage()`(1372행) 진입부에 스테이지 스킵 가드 추가:

```python
if stage_filter is not None and stage_label not in stage_filter:
    logger.info("stage_filter: %s 스킵 [%s]", stage_label, account_id)
    return
```

`stage_stores` 계산(1381-1385행)에 매장 필터 추가:

```python
stage_stores = [
    store_info
    for store_info in store_list
    if store_info["store_id"] in active_store_ids
    and (store_id_filter is None or str(store_info["store_id"]) in store_id_filter)
]
```

두 필터 모두 `None`이 기본값이므로 기존 호출부 동작은 그대로다.

### 4. `DB_Beamin_combined.py` — `retry_once_failed()`에 스테이지 재시도 블록 추가

`retry_once_failed()`(752행) 시그니처에 progress 인자 추가:

```python
def retry_once_failed(
    failed: dict,
    target_date: str | None = None,
    stability_profile: str | None = None,
    *,
    progress_file: Path | None = None,
    progress_run_id: str | None = None,
) -> dict:
```

`residual_failed` 초기값(759행)에 `"stages": []` 추가.
로그 문자열(766-773행)에 `stages=%d` 추가.

기존 "1. 계정 레벨 실패" 블록(776-782행)의 `collect_now_and_woori()` 호출에
`progress_file` / `progress_run_id` 를 전달한다.

그 **직후에** 스테이지 레벨 재시도 블록을 새로 넣는다.
계정별로 그룹핑해 계정당 로그인 1회로 처리하고, `woori_only=True`로 주문/광고 스테이지를 스킵한다:

```python
# 1-2. 스테이지 레벨 실패 → 해당 계정의 해당 매장/스테이지만 재수집
stage_groups: dict[str, dict] = {}
for item in failed.get("stages") or []:
    account = item.get("account") or {}
    store = item.get("store") or {}
    account_id = str(account.get("account_id") or "").strip()
    store_id = str(store.get("store_id") or "").strip()
    stage = str(item.get("stage") or "").strip()
    if not account_id or not stage:
        continue
    group = stage_groups.setdefault(
        account_id, {"account": account, "stages": set(), "store_ids": set()}
    )
    group["stages"].add(stage)
    if store_id:
        group["store_ids"].add(store_id)

for account_id, group in stage_groups.items():
    logger.info(
        "스테이지 재시도: %s / stages=%s / stores=%d",
        account_id,
        sorted(group["stages"]),
        len(group["store_ids"]),
    )
    kwargs = {"target_date": target_date}
    if stability_profile is not None:
        kwargs["stability_profile"] = stability_profile
    try:
        result = collect_now_and_woori(
            [group["account"]],
            woori_only=True,
            stage_filter=group["stages"],
            store_id_filter=group["store_ids"] or None,
            _raise_on_total_failure=False,
            **kwargs,
        )
        if isinstance(result, dict):
            extend_residual(result.get("failed"))
    except Exception as exc:
        logger.warning("스테이지 재시도 실패: %s / %s", account_id, exc)
        residual_failed["stages"].extend(
            item for item in (failed.get("stages") or [])
            if str((item.get("account") or {}).get("account_id") or "") == account_id
        )
```

`extend_residual()`(761-764행)의 키 튜플에도 `"stages"` 를 추가한다.

### 5. `DB_Beamin_retry.py` — `stages` 키 대응

- `count_failed_items()`(63행): 키 튜플 `("accounts", "stores", "orders", "ads")` →
  `("accounts", "stores", "orders", "ads", "stages")`
- `merge_failed_payloads()`(68행): `stages` 병합 추가.
  중복 제거 키는 `(account_id, store_id, stage)`.
  기존 규칙("계정 전체 실패가 있으면 같은 계정의 하위 실패는 제거")을 `stages`에도 동일 적용 —
  `accounts_by_id`에 있는 계정의 `stages` 항목은 결과에서 제외한다.
  반환 dict에 `"stages"` 키 포함.
- `restore_failed_from_conf()`(514행), `build_retry_conf()`(166행),
  `build_next_retry_conf()`(303행), `split_retry_conf_by_lane()`(381행),
  `retry_needed()`(416행): 기존 `stores` 처리 방식과 같은 형태로 `stages` 대응 추가.
  Retry DAG conf 키 이름은 `failed_stages` 로 통일.

### 6. `beamin_staging.py` — progress carry 스키마에 `stages` 추가

`init_progress()`(122행)의 carry 초기값:

```python
"failed": {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []},
```

### 7. `dags/db/DB_Beamin_Macro_Dags.py` — 버킷 반영 + resume + retries=0

**(a) `_filter_failed_to_loaded_accounts()`(206행)**
반환 dict에 `"stages"` 추가. `stores`와 동일하게 `item.get("account")` 기준으로 필터:

```python
"stages": [
    item
    for item in data.get("stages") or []
    if isinstance(item, dict) and account_allowed(item.get("account"))
],
```
`allowed_ids`가 비었을 때의 early-return dict에도 `"stages": data.get("stages") or []` 추가.

**(b) `retry_failed()`(532행)**
- `residual_failed` 초기값(550행)에 `"stages": []` 추가.
- `immediate_failed` 조립(555-564행)에서 `stages`를 **즉시 재시도** 대상에 포함한다
  (`accounts`, `orders`와 함께). `retry_all_types=False`일 때 `stores`/`ads`를
  deferred로 남기는 기존 규칙은 그대로 둔다:
  ```python
  immediate_failed = {
      "accounts": failed.get("accounts") or [],
      "stages": failed.get("stages") or [],
      "stores": [],
      "orders": failed.get("orders") or [],
      "ads": [],
  }
  ```
- resume 적용. `local_analytics` 를 얻은 뒤(582행) progress 경로를 만들고
  `pipeline_retry_failed()` 호출에 전달:
  ```python
  prog_path = progress_path(local_analytics, "retry")
  ...
  result = pipeline_retry_failed(
      immediate_failed,
      target_date=target_date,
      stability_profile=profile["name"],
      progress_file=prog_path,
      progress_run_id=_current_run_id(context),
  )
  ```
  `progress_path` 는 이미 import 되어 있다(63-75행 import 블록).

**(c) `_build_single_dag_parallel_lanes()`의 `retry_task`(1603행)**
`PythonOperator(...)` 인자에 `retries=0` 추가 — `default_args`의 `retries: 1` 오버라이드.
타임아웃 시 재시작 대신 곧바로 실패시킨다. 하위 태스크는 전부 `TriggerRule.ALL_DONE`이라
수집분 적재(`export_to_upload_inbox` → `trigger_upload_after_export`)는 그대로 진행된다.
`_build_legacy_batch_dag()`의 `t3`(1681행)에도 동일하게 `retries=0` 추가.

**(d) 알림 문자열**
`_build_collection_notification()`의 실패 카운트 라인(937-947행)과
HTML 테이블 행(1013-1014행)에 `stages={len(...get('stages') or [])}` 추가.

`execution_timeout=300min` 은 변경하지 않는다 — 스텝 2~4로 재시도 총량이 크게 줄기 때문.

## Reference Code

### modules/transform/pipelines/db/DB_Beamin_combined.py (현재 상태, 수정 대상 지점)

```python
def _mark_failed_account(account: dict, account_id: str, *, login_stage: bool) -> None:
    failed_accounts.append(account)
    metrics["failed_accounts"].append(account_id)
    if login_stage:
        login_lost_accounts.append(account)

def _mark_partial_account_failure(account: dict, account_id: str, stage_label: str) -> None:
    if not any(
        str(item.get("account_id") or "") == account_id
        for item in failed_accounts
    ):
        failed_accounts.append(account)          # ← 증폭 원인: 계정 전체 등록
    if account_id not in {str(value) for value in metrics["failed_accounts"]}:
        metrics["failed_accounts"].append(account_id)
    logger.warning(
        "부분 실패 계정 최종 retry 등록: account=%s stage=%s", account_id, stage_label,
    )

# _run_stage 내부 — 비-fatal 경로
                    if fatal:
                        logger.warning("%s 실패: %s / %s / %s", stage_label, account_id, store_name, exc)
                        active_store_ids.discard(store_info["store_id"])
                        failed_stores.append({"account": account, "store": store_info})
                        _record_failed_store(metrics, account_id, store_name, f"{stage_label}: {exc}")
                    else:
                        logger.info("%s 실패(무시): %s / %s / %s", stage_label, account_id, store_name, exc)
                        if track_account_failure:
                            _mark_partial_account_failure(account, account_id, stage_label)
                    break

# track_account_failure=True 인 스테이지는 3개뿐
_run_stage("NOW 수집", ..., require_dashboard=True, fatal=False, track_account_failure=True)
_run_stage("우리가게 수집", ..., require_dashboard=used_store_hint, fatal=False, track_account_failure=True)
_run_stage("운영시간 수집", ..., fatal=False, track_account_failure=True)

# retry_once_failed — 계정 전체 재수집 지점
    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": []}

    def extend_residual(result_failed: dict | None) -> None:
        data = result_failed or {}
        for key in ("accounts", "stores", "orders", "ads"):
            residual_failed[key].extend(data.get(key) or [])

    if failed.get("accounts"):
        kwargs = {"target_date": target_date}
        if stability_profile is not None:
            kwargs["stability_profile"] = stability_profile
        result = collect_now_and_woori(failed["accounts"], **kwargs)   # ← 전 스테이지 재수집
```

### modules/transform/pipelines/db/DB_Beamin_retry.py

```python
def count_failed_items(failed: dict | None) -> int:
    data = failed or {}
    return sum(len(data.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))


def merge_failed_payloads(*payloads: dict | None) -> dict:
    """배치별 실패를 계정/매장 단위로 중복 없이 합친다.

    계정 전체 실패가 있으면 같은 계정의 매장·주문·광고 실패는 전체 재수집에
    포함되므로 별도 재시도 대상에서 제거한다.
    """
    accounts_by_id: dict[str, dict] = {}
    stores_by_key: dict[tuple[str, str], dict] = {}
    grouped: dict[str, dict[str, dict[str, dict]]] = {"orders": {}, "ads": {}}
    extras: dict[str, dict[str, object]] = {
        "accounts": {}, "stores": {}, "orders": {}, "ads": {},
    }
    ...
```

### modules/transform/pipelines/db/beamin_staging.py

```python
def progress_path(local_analytics: Path, progress_key: str | None = None) -> Path:
    if not progress_key:
        return local_analytics / PROGRESS_FILENAME
    safe_key = safe_run_id_part(progress_key)
    return local_analytics / f"_collect_progress_{safe_key}.json"


def load_progress(path: Path | None, *, run_id: str, target_date: str) -> dict | None:
    if path is None or not path.exists():
        return None
    ...
    if progress.get("run_id") != run_id or progress.get("target_date") != target_date:
        logger.warning("배민 수집 진행 파일 불일치, 처음부터 시작: ...")
        return None
    return progress


def init_progress(path, *, run_id: str, target_date: str, total_accounts: int) -> dict:
    progress = {
        "run_id": run_id, "target_date": target_date,
        "total_accounts": total_accounts, "done_accounts": [], "success": 0, "fail": 0,
        "carry": {
            "failed": {"accounts": [], "stores": [], "orders": [], "ads": []},
            "validation": [], "ad_stores": [], "store_info_per_account": [], "metrics": {},
        },
    }
```

### dags/db/DB_Beamin_Macro_Dags.py

```python
default_args = {
    "retries": 1,                       # ← retry_failed 태스크에서 0으로 오버라이드 필요
    "retry_delay": timedelta(minutes=10),
    ...
}

def retry_failed(*, collect_task_ids=None, retry_all_types: bool = False, **context) -> str:
    ...
    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": []}
    if retry_all_types:
        immediate_failed = failed
        deferred_counts = {"stores": 0, "ads": 0}
    else:
        immediate_failed = {
            "accounts": failed.get("accounts") or [],
            "stores": [],
            "orders": failed.get("orders") or [],
            "ads": [],
        }
    ...
    local_analytics, _local_baemin = _main_stage_paths(context)
    analytics_original, patched_paths = patch_baemin_staging_paths(local_analytics)
    try:
        result = pipeline_retry_failed(
            immediate_failed, target_date=target_date, stability_profile=profile["name"],
        )
    finally:
        restore_baemin_staging_paths(analytics_original, patched_paths)

# retry_task — retries=0 추가 대상
retry_task = PythonOperator(
    task_id="retry_failed",
    python_callable=retry_failed,
    op_kwargs={"collect_task_ids": list(_BATCH_TASK_IDS), "retry_all_types": False},
    pool="selenium_pool",
    trigger_rule=TriggerRule.ALL_DONE,
    execution_timeout=timedelta(minutes=300),
)
```

## Test Cases

1. [stages 집계] 
   `python -c "from modules.transform.pipelines.db.DB_Beamin_retry import count_failed_items as c; f={'accounts':[],'stores':[],'orders':[],'ads':[],'stages':[{'account':{'account_id':'a'},'store':{'store_id':'1'},'stage':'운영시간 수집'}]}; print(c(f))"`
   → 기대: `1`

2. [stages 중복 병합] 
   `python -c "from modules.transform.pipelines.db.DB_Beamin_retry import merge_failed_payloads as m; f={'accounts':[],'stores':[],'orders':[],'ads':[],'stages':[{'account':{'account_id':'a'},'store':{'store_id':'1'},'stage':'운영시간 수집'}]}; print(len(m(f,f)['stages']))"`
   → 기대: `1` (동일 항목 2번 넣어도 1건)

3. [계정 전체 실패가 stages를 흡수] 
   `python -c "from modules.transform.pipelines.db.DB_Beamin_retry import merge_failed_payloads as m; a={'account_id':'a','password':'x'}; f1={'accounts':[a],'stores':[],'orders':[],'ads':[],'stages':[]}; f2={'accounts':[],'stores':[],'orders':[],'ads':[],'stages':[{'account':a,'store':{'store_id':'1'},'stage':'NOW 수집'}]}; r=m(f1,f2); print(len(r['accounts']), len(r['stages']))"`
   → 기대: `1 0`

4. [carry 스키마] 
   `python -c "import tempfile,pathlib; from modules.transform.pipelines.db.beamin_staging import init_progress; p=pathlib.Path(tempfile.mkdtemp())/'p.json'; g=init_progress(p, run_id='r', target_date='2026-07-27', total_accounts=1); print('stages' in g['carry']['failed'])"`
   → 기대: `True`

5. [combined 시그니처] 
   `python -c "import inspect; from modules.transform.pipelines.db.DB_Beamin_combined import collect_now_and_woori as f, retry_once_failed as g; s=inspect.signature(f).parameters; t=inspect.signature(g).parameters; print('stage_filter' in s, 'store_id_filter' in s, 'progress_file' in t)"`
   → 기대: `True True True`

6. [기존 호출 호환] 
   `python -c "import inspect; from modules.transform.pipelines.db.DB_Beamin_combined import collect_now_and_woori as f; p=inspect.signature(f).parameters; print(p['stage_filter'].default, p['store_id_filter'].default)"`
   → 기대: `None None` (기본값이 None이라 기존 호출부 동작 불변)

7. [DAG import] 
   `python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print(dag.dag_id)"`
   → 기대: `DB_Beamin_Macro_Dags`, ImportError 없음

8. [retries=0 반영] 
   `python -c "from dags.db.DB_Beamin_Macro_Dags import dag; t=dag.get_task('retry_failed'); print(t.retries, t.execution_timeout)"`
   → 기대: `0 5:00:00`

9. [DAG import 에러 전체] 
   `docker compose exec airflow-scheduler airflow dags list-import-errors`
   → 기대: 출력 없음(0건)

10. [스테이지 한정 재시도 실측 — 가장 중요]
    ```
    docker compose exec airflow-scheduler airflow dags trigger DB_Beamin_Macro_Dags \
      -c '{"stores":["대전장대점"],"target_date":"2026-07-27","run_all_batches":false}'
    ```
    → `retry_failed` 로그에서 확인:
    - `재시도 시작: accounts=0 stages=N ...` — accounts가 0으로 떨어질 것
    - `stage_filter: 주문내역 수집 스킵` 류 로그가 찍히고 실패했던 스테이지만 실행될 것
    - 계정당 소요가 16~20분이 아니라 2~3분대일 것

11. [resume 확인]
    10번 실행 중 `retry_failed`를 Mark Failed 후 Clear.
    → `Local_DB/analytics_stage_main/<run_id>/_collect_progress_retry.json` 생성 확인,
      재실행 로그에서 이미 끝난 계정을 건너뛰는지(`done_accounts` 스킵) 확인.

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~9 순서대로 실행 (10~11은 Airflow 컨테이너 필요, 가능할 때만)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~9 전체 PASS + Constraints 위반 없음
```

## Constraints

- `_mark_failed_account()`(로그인/드라이버 실패 경로)는 **수정 금지**. 계정 단위 재수집이 맞다.
- `stage_filter` / `store_id_filter` 기본값은 반드시 `None` — 기존 호출부 동작이 바뀌면 안 된다.
- `failed` dict를 조립하는 지점이 여러 곳(1178/1239/1297/1641/1721행)이므로
  `"stages"` 키 누락이 없는지 전부 확인할 것. 한 곳이라도 빠지면 KeyError가 아니라
  **조용히 재시도 누락**으로 이어진다.
- `execution_timeout=300min` 은 변경하지 않는다.
- `retry_all_types=True` 경로(`immediate_failed = failed`)는 그대로 두고,
  `stages`가 자동으로 포함되게 한다.
- 폴더 구조/파일명 변경 금지. 신규 파일 생성 금지.
- print 금지, logging만 사용.

## 별도 수동 조치 (코드 아님, 참고용)

현재 진행 중인 런 `scheduled__2026-07-26T15:15:00+00:00`은 코드 수정 대상이 아니다.
운영자가 아래를 수동 처리한다 — Codex는 이 항목을 구현하지 않는다.

1. Airflow UI에서 `retry_failed` 를 Mark as Failed
2. `export_to_upload_inbox` 이하는 `ALL_DONE`이라 자동 진행 (안 되면 Clear)
3. `DB_Beamin_Macro_Upload_Dags` 트리거 확인

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`list[dict]`, `str | None` 스타일)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 로그 메시지 언어: 한국어 (기존 파일과 동일)
