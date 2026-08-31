# 배민매크로 collect_all 배치 3분할 + 이어받기 + 진행률 로그

## Task
배민 매크로 수집 DAG(`DB_Beamin_Macro_Dags`)가 33계정 × 계정당 14.9분 ≈ 490분이 걸려 `execution_timeout=300분`에 걸리고, 재시도가 staging을 지우고 1번 계정부터 다시 돌아 **매일 하위 13개 계정이 영구 미수집**된다. 이를 (1) 배치 3분할로 run당 164분으로 낮추고, (2) 계정 단위 체크포인트로 중단 지점부터 이어받게 하고, (3) `[진행 8/11 73%]` 진행률 로그를 추가해 해결한다.

실측 근거 (`logs/dag_id=DB_Beamin_Macro_Dags/run_id=scheduled__2026-07-22T18:15:00+00:00/`):
```
attempt=1  18:15:03 → 23:12:14  AirflowTaskTimeout, 계정 20/33 완료 (297분)
attempt=2  23:22 재시작 → 1번 계정부터 다시, 40분에 3계정 (retries=1이라 마지막 시도)
```

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG는 orchestration만, 비즈니스 로직은 `modules/transform/pipelines/`

## Files to Create / Modify

**Modify**
- `modules/transform/utility/schedule.py` — BATCH2/3 시각 조정 (70-72행)
- `modules/transform/pipelines/db/beamin_staging.py` — 체크포인트 저장소 4함수 추가
- `modules/transform/pipelines/db/DB_Beamin_combined.py` — `collect_now_and_woori`(**1019행 정의**) 이어받기 + 진행 로그
- `dags/db/DB_Beamin_Macro_Dags.py` — `load_accounts` 2단계 분할, `collect_all` staging 보존, DAG 3개 생성

**Create**
- `tests/test_baemin_collect_resume.py`
- `tests/test_baemin_batch_split.py`

## Implementation Steps

### 1. `schedule.py` — 배치 간격 조정 (70-72행)
현재 03:15 / 04:30 / 05:45는 간격 75분인데 배치당 164분 소요 → 겹침. 3시간 간격으로:
```python
SMD_BAEMIN_COLLECT_BATCH1_TIME = "15 3 * * *"   # 03:15 → ~06:00
SMD_BAEMIN_COLLECT_BATCH2_TIME = "15 6 * * *"   # 06:15 → ~09:00  (04:30에서 변경)
SMD_BAEMIN_COLLECT_BATCH3_TIME = "15 9 * * *"   # 09:15 → ~12:00  (05:45에서 변경)
```
`SMD_BAEMIN_COLLECT_TIME`, `SMD_BAEMIN_UPLOAD_TIME`, `SMD_BAEMIN_UPLOAD_PC2_TIME`은 변경 금지.

### 2. `beamin_staging.py` — 체크포인트 저장소
**저장 위치는 반드시 `local_analytics/_collect_progress.json`** (= `baemin_macro/`의 한 단계 **위**).
`export_staging_to_inbox`는 `local_baemin`(=`local_analytics/baemin_macro`)만 복사하므로, 위에 두어야 계정 PW가 든 파일이 공유 inbox로 나가지 않는다. `cleanup_staging(local_analytics)`가 run 종료 시 함께 지운다.

```python
PROGRESS_FILENAME = "_collect_progress.json"

def progress_path(local_analytics: Path) -> Path:
    return local_analytics / PROGRESS_FILENAME

def load_progress(path: Path | None, *, run_id: str, target_date: str) -> dict | None:
    """없음/JSON 깨짐/run_id·target_date 불일치 → None(처음부터). 예외는 warning 후 삼킨다."""

def init_progress(path: Path | None, *, run_id: str, target_date: str, total_accounts: int) -> dict:
    ...

def save_progress(path: Path | None, progress: dict) -> None:
    """tmp write → os.replace 원자적 교체. path is None이면 no-op."""
```

진행 파일 스키마:
```json
{ "run_id": "scheduled__...", "target_date": "2026-07-23", "total_accounts": 11,
  "done_accounts": ["hosig6"], "success": 8, "fail": 0,
  "carry": { "failed": {"accounts": [], "stores": [], "orders": [], "ads": []},
             "validation": [], "ad_stores": [], "store_info_per_account": [], "metrics": {} } }
```
직렬화는 기존 export와 동일하게 `json.dumps(..., ensure_ascii=False, default=str)`.

### 3. `DB_Beamin_combined.py` — `collect_now_and_woori` (1019행 정의)
> 같은 이름 정의가 248행·494행에도 있으나 **셰도잉된 죽은 코드다. 건드리지 말 것.**

**3-1. 시그니처에 keyword-only 인자 추가** (기본 `None` → `DB_Beamin_retry.py:481`, `retry_once_failed`(780행), 기존 테스트 동작 변화 없음):
```python
def collect_now_and_woori(..., *, progress_file: Path | None = None,
                          _allow_login_second_pass: bool = True,
                          _raise_on_total_failure: bool = True) -> dict:
```

**3-2. 시작 시 이어받기** — `load_progress`가 dict를 주면:
- `done = set(progress["done_accounts"])`로 `account_list` 필터
- `success`/`fail`과 `failed_accounts / failed_stores / failed_orders / failed_ads / validation_results / ad_store_infos / store_info_per_account_list`를 `carry`에서 seed
- `metrics` 숫자 지표는 **기존 `_merge_numeric_metrics`(1047행) 재사용**
- `logger.info("이어받기: 완료 %d/%d, 남은 %d개 계정부터 재개", ...)`
- 남은 계정이 0이면 루프 없이 병합 결과 반환
- `total_accounts = len(done) + len(remaining)` → 로그가 항상 `/11`로 일관

**3-3. 체크포인트 기록 — 정확히 4곳, 함수 추출 금지**
> **`try/finally` 사용 금지.** `AirflowTaskTimeout`으로 중단된 계정까지 done으로 찍혀 그 계정이 영구 누락된다. 정상 종료 경로에서만 기록한다.

| 위치 | 상황 |
|---|---|
| 1078행 `continue` 직전 | 로그인/세션 실패 |
| 1103행 `continue` 직전 | 요청 매장 매칭 실패 |
| 1119행 `continue` 직전 | 매장 목록 비어서 continue |
| 1391행 `success += 1` 직후 | 정상 완료 |

`_checkpoint(account_id)` = `done_accounts` append → `carry` 갱신 → **즉시 `save_progress` flush**(SIGKILL로 죽어도 직전 계정까지 보존). 인덴트 변경 없는 순수 추가 diff.
계정 실패도 done 처리한다 — 재시도는 downstream `retry_failed` / Retry DAG 담당이라 `collect_all` 재시도에서 또 돌 필요 없다.

**3-4. second pass**(1393-1441행): 재귀 호출(1405행)에 `progress_file`을 넘기지 않는다. 2차 시도 종료 후 최종 상태를 한 번 `save_progress`.

**3-5. 진행률 로그**
```
[진행 8/11 73%] 계정 시작 hosig6 (도리당 광명철산점)
  === 우리가게 수집 [8/11] 매장 1/2 · 도리당 광명철산점 ===
[진행 8/11 73%] 계정 완료 ok · 누적 성공 7 실패 1 · 경과 119분 · 평균 14.9분/계정 · 예상잔여 45분
```
- 계정 태그 `f"{idx}/{total_accounts}"`를 `_run_stage` 단계 헤더(1178행), 주문내역 루프 헤더(1291행), 광고 funnel 루프 헤더(1341행)에 추가
- 매장 카운터는 각 루프에서 `active_store_ids`에 남은 것만 `enumerate` → `매장 i/n`
- 예상잔여 = `평균(계정 소요) × 남은 계정 수`. 표본 없으면 생략
- 최종 `summary`는 `f"성공 {success}/{total} 계정"` 유지

### 4. `DB_Beamin_Macro_Dags.py`

**4-1. ⚠️ `load_accounts` 분할을 2단계로 (216-222행) — 이걸 안 하면 상위/하위 PC 분담이 깨진다**
현재는 `conf > params > _MACRO_ROLE["range"] > COLLECT_RANGE` 중 **하나만** 골라 `_split_accounts_by_range`를 1회 호출한다. `batch:1/3`을 그대로 넣으면 `_MACRO_ROLE["range"]="상위"`가 무시되어 **67개를 3분할한 22개**가 대상이 된다.
```python
role_range  = conf.get("collect_range") or params.get("collect_range") or _MACRO_ROLE["range"] or COLLECT_RANGE
batch_range = params.get("collect_range")   # "batch:N/M" 형태만

accounts = _split_accounts_by_range(accounts, role_range)    # 67 → 상위 33
accounts = _split_accounts_by_range(accounts, batch_range)   # 33 → batch:1/3 → 11
```
- `conf`/`params`에 `상위`/`하위`가 오면 기존처럼 role 자리를 덮어쓰도록 유지(하위 호환)
- `batch:` 형태가 아닌 값은 batch 단계에서 무시
- 로그에 두 단계 결과 모두: `67개 → 상위 33개 → batch:1/3 11개`
- `_split_accounts_by_range`(150-197행)는 `batch:N/M` 파서를 이미 갖고 있으므로 **수정 불필요, 재사용만**

**4-2. `collect_all`(874행) staging 보존**
```python
local_analytics, local_baemin = _main_stage_paths(context)
prog_path = progress_path(local_analytics)
resume = (not conf.get("force_restart")) and load_progress(
    prog_path, run_id=_current_run_id(context),
    target_date=_target_date_from_context(context)) is not None

if resume:
    logger.info("배민 수집 이어받기: staging 유지 %s", local_baemin)
else:
    init_empty_staging(local_baemin)      # 기존 동작 (첫 시도 / force_restart)
...
result = pipeline_collect_all(..., progress_file=prog_path)
```
- `except Exception` 블록(911행)이 전체 계정을 failed로 push하는 부분 → **완료 계정을 제외한 나머지만** 기록 (`AirflowTaskTimeout`도 `Exception` 하위라 여기로 들어온다)
- `orders_only` 분기(`pipeline_collect_orders_only`)는 현행 유지
- import에 `progress_path`, `load_progress` 추가
- `retries` / `retry_delay` / `execution_timeout`은 **변경 금지** (배치 분할로 여유 확보됨)

**4-3. DAG 3개 생성 (1219행 단일 생성부 교체)**
```python
dag   = _build_baemin_macro_dag(dag_id_value=dag_id,          schedule=SMD_BAEMIN_COLLECT_BATCH1_TIME, collect_range="batch:1/3")
dag_2 = _build_baemin_macro_dag(dag_id_value=f"{dag_id}_B2",  schedule=SMD_BAEMIN_COLLECT_BATCH2_TIME, collect_range="batch:2/3")
dag_3 = _build_baemin_macro_dag(dag_id_value=f"{dag_id}_B3",  schedule=SMD_BAEMIN_COLLECT_BATCH3_TIME, collect_range="batch:3/3")
```
- 1번 dag_id는 기존 이름 그대로 (기존 로그/알림/Retry 트리거 이력이 붙어 있음)
- `trigger_retry_if_needed`는 `ti.dag_id` 사용(1120행)이라 배치별로 자동 동작
- `globals()`에 DAG 객체가 노출되도록 모듈 최상단 변수로 할당할 것

## Reference Code

### modules/transform/pipelines/db/beamin_staging.py
```python
from modules.transform.utility.paths import LOCAL_DB

def local_stage_paths(subdir: str, run_id: str | None = None) -> tuple[Path, Path]:
    local_analytics = LOCAL_DB / subdir
    if run_id:
        local_analytics = local_analytics / safe_run_id_part(run_id)
    return local_analytics, local_analytics / "baemin_macro"

def init_empty_staging(local_baemin: Path) -> None:
    local_root = LOCAL_DB.resolve()
    target = local_baemin.resolve()
    if local_baemin.exists():
        if not target.is_relative_to(local_root):
            raise RuntimeError(f"staging 삭제 범위 오류: {local_baemin}")
        shutil.rmtree(local_baemin)
    local_baemin.mkdir(parents=True, exist_ok=True)
    logger.info("배민 빈 staging 생성: %s", local_baemin)

def cleanup_staging(local_analytics: Path) -> None:
    ...  # LOCAL_DB 범위 검사 후 rmtree

def export_staging_to_inbox(local_baemin: Path, run_id: str | None, inbox_dir: Path,
                            meta: dict | None = None, *, replace_existing: bool = False,
                            folder_prefix: str = "") -> Path:
    ...
    dst_root = tmp_run / "baemin_macro"
    for src in local_baemin.rglob("*"):     # ← local_baemin 아래만 복사됨
        ...
```

### dags/db/DB_Beamin_Macro_Dags.py — 기존 batch 파서 (수정 불필요, 재사용)
```python
def _split_accounts_by_range(accounts: list[dict], collect_range: str | None) -> list[dict]:
    if isinstance(collect_range, str) and collect_range.strip().lower() in {"", "none", "null"}:
        collect_range = None
    if not collect_range or not accounts:
        return accounts

    ordered = sorted(accounts, key=lambda a: str(a.get("store_name", "")))
    normalized_range = str(collect_range).strip()

    batch_match = re.fullmatch(r"batch:(\d+)/(\d+)", normalized_range)
    if batch_match:
        batch_index = int(batch_match.group(1))
        batch_total = int(batch_match.group(2))
        if batch_total <= 0 or batch_index < 1 or batch_index > batch_total:
            logger.warning("알 수 없는 COLLECT_RANGE=%r", collect_range)
            return accounts
        start = len(ordered) * (batch_index - 1) // batch_total
        end = len(ordered) * batch_index // batch_total
        selected = ordered[start:end]
        logger.info("매장 분할 [%s]: 전체 %d개 -> %d개 (%s ~ %s)", ...)
        return selected

    mid = len(ordered) // 2
    if normalized_range == "상위":
        selected = ordered[:mid]
    elif normalized_range == "하위":
        selected = ordered[mid:]
    else:
        logger.warning("알 수 없는 COLLECT_RANGE=%r", collect_range)
        return accounts
    return selected
```

### dags/db/DB_Beamin_Macro_Dags.py — DAG 팩토리 (1156행)
```python
def _build_baemin_macro_dag(*, dag_id_value: str, schedule: str, collect_range: str | None) -> DAG:
    with DAG(
        dag_id=dag_id_value, schedule=schedule,
        start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
        catchup=False, concurrency=1, max_active_runs=1, max_active_tasks=1,
        default_args=default_args, params={"collect_range": collect_range},
        tags=["db", "baemin", "crawl"],
    ) as built_dag:
        t1 = PythonOperator(task_id="load_accounts", python_callable=load_accounts)
        t2 = PythonOperator(task_id="collect_all", python_callable=collect_all,
                            pool="selenium_pool", execution_timeout=timedelta(minutes=300))
        t3 = PythonOperator(task_id="retry_failed", python_callable=retry_failed,
                            pool="selenium_pool", trigger_rule="all_done",
                            execution_timeout=timedelta(minutes=120))
        t4 = PythonOperator(task_id="export_to_upload_inbox", python_callable=export_to_upload_inbox,
                            trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=30))
        t5 = PythonOperator(task_id="trigger_upload_after_export", python_callable=trigger_upload_after_export,
                            trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=5))
        t6 = PythonOperator(task_id=_NOTIFY_TASK_ID, python_callable=notify_collection_result,
                            trigger_rule=TriggerRule.ALL_DONE)
        t1 >> t2 >> t3 >> t4 >> t5 >> t6
    return built_dag
```

### modules/transform/pipelines/db/DB_Beamin_combined.py — 계정 루프 (1061행~)
```python
for account in account_list:                       # ← 여기에 enumerate + 진행 로그
    account_id = account["account_id"]
    store_list: list[dict] = _store_list_from_account_hint(account)
    used_store_hint = bool(store_list)
    bootstrap_driver = None
    if not store_list:
        bootstrap_driver = _build_dashboard_session(account, metrics, profile)
        if bootstrap_driver is None:
            fail += 1
            _mark_failed_account(account, account_id, login_stage=True)
            continue                               # ← 체크포인트 지점 (1078행)
        try:
            ...
            if requested_store_name:
                store_list = _filter_store_list_for_request(store_list, requested_store_name)
                if not store_list:
                    fail += 1
                    _mark_failed_account(account, account_id, login_stage=False)
                    continue                       # ← 체크포인트 지점 (1103행)
        ...
    if not store_list:
        continue                                   # ← 체크포인트 지점 (1119행)
    ...
    wait_sec = random.uniform(*profile["account_wait_range"])
    time.sleep(wait_sec)
    success += 1                                   # ← 체크포인트 지점 (1391행)
```

### modules/transform/utility/schedule.py (70-75행)
```python
SMD_BAEMIN_COLLECT_BATCH1_TIME = "15 3 * * *"  # 매일 KST 03:15 실행
SMD_BAEMIN_COLLECT_BATCH2_TIME = "30 4 * * *"  # 매일 KST 04:30 실행
SMD_BAEMIN_COLLECT_BATCH3_TIME = "45 5 * * *"  # 매일 KST 05:45 실행
SMD_BAEMIN_COLLECT_TIME = SMD_BAEMIN_COLLECT_BATCH1_TIME
SMD_BAEMIN_UPLOAD_TIME = "40 7 * * *"          # 중앙 PC 전용
SMD_BAEMIN_UPLOAD_PC2_TIME = "0,30 8-12 * * *" # 하위 PC 도착분 확인
```

### tests/test_baemin_macro_driver_recovery.py — 테스트 패턴 (그대로 따를 것)
```python
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_combined as combined


def test_collect_now_and_woori_bootstrap_recovery_failure_marks_account_failed():
    account = {"account_id": "acct1", "password": "pw"}
    with patch.object(combined, "_build_account_session", return_value=None):
        with pytest.raises(RuntimeError):
            combined.collect_now_and_woori([account])
```

## Test Cases

1. [배치 분할] `python -m pytest tests/test_baemin_batch_split.py -q` → 기대: PASS
   - 67계정 + role `"상위"` + `batch:1/3` → **11개** (22개 아님)
   - 세 배치 합집합 == 상위 33개, 중복 0
   - batch 미지정 시 기존 동작(상위 33개) 유지
2. [이어받기] `python -m pytest tests/test_baemin_collect_resume.py -q` → 기대: PASS
   - 계정 3개 중 2번째에서 예외 → `done_accounts == ["1번"]`, 중단된 2번은 **미기록**
   - 같은 진행 파일로 재호출 → 1번 수집 함수 미호출, 2·3번만 수집, `validation`/`store_info_per_account`에 1차 carry 병합
   - `run_id`/`target_date` 불일치 → `load_progress`가 `None`
   - `progress_file=None` 기본 호출은 기존과 동일 (회귀 방지)
   - `export_staging_to_inbox` 결과 폴더에 `_collect_progress.json` **없음**
3. [기존 회귀] `python -m pytest tests/test_baemin_macro_driver_recovery.py tests/test_baemin_shop_change_schedule.py tests/test_beamin_retry_conf.py tests/test_baemin_macro_validation.py -q` → 기대: 전체 PASS
4. [DAG import] `python -c "import dags.db.DB_Beamin_Macro_Dags as m; print(m.dag.dag_id, m.dag_2.dag_id, m.dag_3.dag_id)"` → 기대: `DB_Beamin_Macro_Dags DB_Beamin_Macro_Dags_B2 DB_Beamin_Macro_Dags_B3`, ImportError 없음
5. [스케줄 상수] `python -c "from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_BATCH2_TIME as b2, SMD_BAEMIN_COLLECT_BATCH3_TIME as b3; print(b2, '|', b3)"` → 기대: `15 6 * * * | 15 9 * * *`
6. [진행 파일 위치] `python -c "from pathlib import Path; from modules.transform.pipelines.db.beamin_staging import local_stage_paths, progress_path; a,b = local_stage_paths('x','r1'); print(progress_path(a).parent == a, b.parent == a)"` → 기대: `True True` (진행 파일이 `baemin_macro/` **밖**)

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
- `DB_Beamin_combined.py`의 **248행·494행 `collect_now_and_woori`는 셰도잉된 죽은 코드** — 절대 수정하지 말 것. 대상은 1019행 정의뿐.
- 계정 루프에 **`try/finally` 체크포인트 금지** — 타임아웃으로 중단된 계정이 done으로 기록되어 영구 누락된다. 정상 종료 경로 4곳에서만 기록.
- 진행 파일은 반드시 `local_analytics/` 직하 (= `baemin_macro/`의 **밖**). 안에 두면 계정 PW가 공유 inbox로 유출된다.
- `progress_file` 기본값은 `None`이어야 한다 — `DB_Beamin_retry.py:481`과 `retry_once_failed`(780행)는 이어받기 대상이 아니다.
- `retries`(1) / `retry_delay`(10분) / `execution_timeout`(300분) 변경 금지.
- `SMD_BAEMIN_COLLECT_BATCH1_TIME`, `SMD_BAEMIN_UPLOAD_TIME`, `SMD_BAEMIN_UPLOAD_PC2_TIME` 변경 금지 (BATCH2/3만 조정).
- `_split_accounts_by_range`는 이미 `batch:N/M`을 지원하므로 **수정하지 말고 두 번 호출**만 할 것.
- 폴더 구조/파일명 변경 금지, print 금지, 경로·스케줄 하드코딩 금지.
- second pass 재귀 호출(1405행)에 `progress_file`을 전달하지 말 것.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`Path | None` 스타일)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
