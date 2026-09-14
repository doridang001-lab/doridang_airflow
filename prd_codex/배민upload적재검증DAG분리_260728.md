# 배민 upload inbox 적체 해소 — 적재/검증 DAG 분리

## Task

`DB_Beamin_Macro_Upload_Dags`에서 `ingest`(2분)와 Selenium 검증(`validate_toorder`가 7시간 행 발생)이 한 DAG에 묶여 있고 `max_active_runs=1` + `max_active_tasks=1`이라, 검증 태스크 하나가 DAG 전체 큐를 막아 `_baemin_upload_inbox` 폴더가 19시간 넘게 적재되지 않고 쌓인다. 대기 중이던 런 2개는 태스크를 단 하나도 실행하지 못한 채(`start_date` NULL) 수동 실패 처리됐다.

적재(ingest)와 검증(validate)을 **별도 DAG으로 분리**하여, 검증이 몇 시간 걸려도 inbox는 항상 수 분 내에 비워지게 한다. 함께 Pc2 스윕 창을 종일로 넓히고, 스윕 간 캐스케이드 대기를 제거한다.

### 실측 근거 (참고용, 재확인 불필요)

- `pc2_bottom__20260727T013000`: `ingest` 2분 22초 완료 / `validate_toorder`(attempt 2) `2026-07-27 02:38 → 09:39 UTC` **7시간 1분** 행 후 실패. `execution_timeout=30분` 미작동, 로그는 03:08 chromedriver 호출에서 멈춤.
- `collect_export__scheduled__2026-07-25T18_15…` (queued 05:06 → failed 05:58, `start_date` NULL)
- `pc2_bottom__20260727T030000` (queued 06:00 → failed 07:51, `start_date` NULL)
- Pc2 스윕 창이 `0,30 8-12 * * *`뿐이라, 13:19에 도착한 bottom 폴더는 구조적으로 다음날 아침까지 대기.
- `_baemin_upload_inbox/_quarantine`에 `_tmp__` 잔해 4건 = 조용히 버려진 bottom 수집분.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- `dag_id = Path(__file__).stem`, `catchup=False`, `max_active_runs=1` 기본
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB` (하드코딩 금지)
- DAG에는 orchestration만. 비즈니스 로직은 `modules/transform/pipelines/db/`

## Files to Create / Modify

**Create**
- `dags/db/DB_Beamin_Macro_Upload_Validate_Dags.py` — 검증 전용 DAG (신규)

**Modify**
- `modules/transform/pipelines/db/DB_Beamin_Macro_upload.py` — 메타 핸드오프 + `_meta_pull` 도입, stale 알림 throttle
- `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py` — `_sweep_stale_tmp_folders` Telegram 알림
- `dags/db/DB_Beamin_Macro_Upload_Dags.py` — 적재 전용으로 축소
- `dags/db/DB_Beamin_Macro_Upload_Pc2_Dags.py` — `wait_for_completion` 해제
- `modules/transform/utility/schedule.py` — `SMD_BAEMIN_UPLOAD_PC2_TIME` 확대
- `tests/test_baemin_pc2_upload_trigger.py` — `_meta_pull` 폴백 테스트 추가

## Implementation Steps

### 1. 메타 핸드오프 도입 — `modules/transform/pipelines/db/DB_Beamin_Macro_upload.py`

검증 태스크들은 현재 `ti.xcom_pull(task_ids="ingest", key=…)`로 7개 meta 키
(`target_date`, `account_list`, `validation`, `ad_stores`, `store_info_per_account`, `failed`, `original_failed`)와 `ingest_stats`를 읽는다. DAG이 갈라지면 이 XCom 경로가 끊기므로 파일 핸드오프를 추가한다.

meta에는 계정 자격증명(`restore_meta_credentials` 복원본)이 들어가므로 **`dag_run.conf`에 직접 싣지 말 것** — conf는 Airflow 메타DB에 pickle로 저장된다. 파일 경로만 전달한다.

```python
# 파일 상단 import에 추가
import json
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB  # LOCAL_DB 추가

HANDOFF_DIR = LOCAL_DB / "baemin_upload_handoff"
_HANDOFF_KEYS = (
    "target_date",
    "account_list",
    "validation",
    "ad_stores",
    "store_info_per_account",
    "failed",
    "original_failed",
)


def _write_handoff(run_id: str, meta: dict, stats: dict) -> Path:
    HANDOFF_DIR.mkdir(parents=True, exist_ok=True)
    path = HANDOFF_DIR / f"{_safe_run_id_part(run_id)}.json"
    payload = {key: meta.get(key) for key in _HANDOFF_KEYS}
    payload["ingest_stats"] = stats
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)
    return path


def _load_handoff(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    raw = conf.get("handoff_path")
    if not raw:
        return {}
    path = Path(str(raw))
    if not path.exists():
        logger.warning("upload handoff 파일 없음: %s", path)
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("upload handoff 파싱 실패: %s / %s", path, exc)
        return {}


def _meta_pull(context, key: str):
    """같은 DAG의 ingest XCom을 우선 쓰고, 없으면 handoff 파일로 폴백한다."""
    value = context["ti"].xcom_pull(task_ids="ingest", key=key)
    if value is not None:
        return value
    return _load_handoff(context).get(key)
```

`ingest()`를 아래처럼 변경 (기존 XCom push는 단일 DAG 실행·기존 테스트 호환을 위해 그대로 유지):

```python
def ingest(folder_pattern: str = DEFAULT_FOLDER_PATTERN, **context) -> str:
    result = ingest_baemin_upload_inbox(folder_pattern=folder_pattern)
    meta = restore_meta_credentials(result.get("meta") or {})
    stats = result.get("stats") or {}
    for key, value in meta.items():
        context["ti"].xcom_push(key=key, value=value)
    context["ti"].xcom_push(key="ingest_stats", value=stats)
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or "manual"
    handoff_path = _write_handoff(str(run_id), meta, stats)
    context["ti"].xcom_push(key="handoff_path", value=str(handoff_path))
    return result.get("summary", "upload inbox 적재 완료")
```

그 다음 **`task_ids="ingest"` pull 전부를 `_meta_pull(context, …)`로 교체**한다. 교체 대상 (현재 라인 기준):

| 라인 | 함수 | 원본 |
|---|---|---|
| 152 | `_target_date` | `ti.xcom_pull(task_ids="ingest", key="target_date")` |
| 214 | `has_ingested_folders` | `...key="ingest_stats"` |
| 292 | `validate_orders` | `...key="validation"` |
| 321 | `validate_ad_funnel` | `...key="ad_stores"` |
| 348, 349 | `validate_toorder` | `...key="account_list"` / `"store_info_per_account"` |
| 445, 452, 453, 472, 480 | `build_upload_notification_context` | `"account_list"` / `"failed"` / `"validation"` / `"ingest_stats"` / `"ad_stores"` |
| 612 | `trigger_retry_if_needed` | `...key="failed"` |

`_target_date`는 시그니처가 `_target_date(context)`이므로 그대로 `_meta_pull(context, "target_date")` 사용. `has_ingested_folders`는 적재 DAG에 남으므로 교체해도 동작 동일(폴백은 안 타게 됨).

**교체하지 말 것** — 같은 DAG 안에 남는 pull:
`task_ids="precheck_manual_baemin_orders"` (350), `task_ids="validate_toorder"` (457, 621), `task_ids="validate_ad_funnel"` (458, 622), `task_ids="trigger_retry_if_needed"` (578, 580).

`build_upload_notification_context`의 `"source_dag_id"` 기본값은 `getattr(ti, "dag_id", "DB_Beamin_Macro_Upload_Dags")`인데, 이제 검증 DAG에서 호출되므로 handoff에 원 dag_id/run_id를 실어 보내도 되고, 현행 유지해도 무방(알림 문구용). **현행 유지**한다.

### 2. `dags/db/DB_Beamin_Macro_Upload_Dags.py` — 적재 전용으로 축소

남기는 태스크: `ingest` → `has_pending`(ShortCircuit) → `trigger_validate`.

```python
"""Central Baemin macro upload DAG (적재 전용).

이 DAG은 중앙 PC에서만 unpause한다. 수집 PC는 _baemin_upload_inbox까지만
내보내고, analytics/baemin_macro 적재는 이 DAG이 담당한다.
검증(validate_*)은 DB_Beamin_Macro_Upload_Validate_Dags로 분리되어 있다.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import has_ingested_folders, ingest
from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_TIME

dag_id = Path(__file__).stem
VALIDATE_DAG_ID = "DB_Beamin_Macro_Upload_Validate_Dags"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}

with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_UPLOAD_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "upload"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        op_kwargs={"folder_pattern": "{{ dag_run.conf.get('folder_pattern', 'manual__top__*') }}"},
        execution_timeout=timedelta(minutes=30),
    )

    t_gate = ShortCircuitOperator(
        task_id="has_pending",
        python_callable=has_ingested_folders,
    )

    t_trigger_validate = TriggerDagRunOperator(
        task_id="trigger_validate",
        trigger_dag_id=VALIDATE_DAG_ID,
        trigger_run_id="upload__{{ ti.xcom_pull(task_ids='ingest', key='handoff_path') and run_id | replace(':', '_') | replace('+', '_') | truncate(100, True, '') }}",
        conf={
            "handoff_path": "{{ ti.xcom_pull(task_ids='ingest', key='handoff_path') }}",
            "folder_pattern": "{{ dag_run.conf.get('folder_pattern', 'manual__top__*') }}",
            "source": "upload_ingest",
            "source_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    t_ingest >> t_gate >> t_trigger_validate
```

`trigger_run_id`의 Jinja가 복잡하면 **PythonOperator + `airflow.api.common.trigger_dag.trigger_dag`** 로 대체해도 된다. 그 경우 `dags/db/DB_Beamin_Macro_Dags.py:645-691`의 `trigger_upload_after_export` 패턴을 그대로 따르고, run_id는 `safe_run_id_part`(`modules/transform/pipelines/db/beamin_staging.py:61`)로 만든다. **이 방식을 권장한다** — Jinja 조합보다 안전하다.

- `max_active_tasks=1` 제거 (기본값 사용)
- 제거되는 태스크: `precheck_manual_baemin_orders`, `validate_orders`, `validate_ad_funnel`, `validate_toorder`, `trigger_retry_if_needed`, `notify_upload_result`
- 목표 런타임 2~3분

### 3. `dags/db/DB_Beamin_Macro_Upload_Validate_Dags.py` — 신규 (검증 전용)

기존 Upload DAG에서 검증 태스크를 그대로 옮긴다. `pool`, `trigger_rule`, `execution_timeout` 전부 원본 유지.

```python
"""Central Baemin macro validation DAG.

DB_Beamin_Macro_Upload_Dags(적재)가 handoff_path를 conf로 넘겨 트리거한다.
Selenium 검증이 길어져도 inbox 적재를 막지 않도록 분리되어 있다.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_Macro_upload import (
    notify_upload_result,
    precheck_manual,
    trigger_retry_if_needed,
    validate_ad_funnel,
    validate_orders,
    validate_toorder,
)
from modules.transform.utility.notifier import on_failure_callback_no_telegram

dag_id = Path(__file__).stem

default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}

with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "upload", "validate"],
) as dag:
    t_precheck = PythonOperator(
        task_id="precheck_manual_baemin_orders",
        python_callable=precheck_manual,
        execution_timeout=timedelta(minutes=15),
    )

    t_validate_orders = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_validate_ad_funnel = PythonOperator(
        task_id="validate_ad_funnel",
        python_callable=validate_ad_funnel,
        pool="selenium_pool",
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120),
    )

    t_validate_toorder = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )

    t_trigger_retry = PythonOperator(
        task_id="trigger_retry_if_needed",
        python_callable=trigger_retry_if_needed,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_notify = PythonOperator(
        task_id="notify_upload_result",
        python_callable=notify_upload_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_precheck >> [t_validate_orders, t_validate_ad_funnel, t_validate_toorder] >> t_trigger_retry >> t_notify
```

주의: 원본 Upload DAG의 `t_precheck`에는 `trigger_rule=ALL_DONE`이 있었으나, 이제 upstream이 없으므로 제거해도 무방하다. `retries=0`으로 두어(원본은 `retries=1`) 7시간 행이 재시도로 두 배 늘지 않게 한다.

### 4. `modules/transform/utility/schedule.py` — Pc2 스윕 창 확대

```python
SMD_BAEMIN_UPLOAD_PC2_TIME = "5,35 * * * *"  # 30분 간격 종일 — 하위 PC 도착분 확인
```

기존 `"0,30 8-12 * * *"`(08:00~12:30)를 대체한다. `5,35`로 오프셋을 줘 정각 스케줄 몰림을 피한다.

### 5. `dags/db/DB_Beamin_Macro_Upload_Pc2_Dags.py` — 캐스케이드 대기 제거

`t_trigger_upload`를 아래로 교체한다.

```python
    t_trigger_upload = TriggerDagRunOperator(
        task_id="trigger_upload",
        trigger_dag_id=TARGET_UPLOAD_DAG_ID,
        trigger_run_id="pc2_bottom__{{ logical_date | ts_nodash }}",
        conf={
            "folder_pattern": BOTTOM_FOLDER_PATTERN,
            "skip_if_empty": True,
            "source": "pc2_bottom_sweep",
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )
```

제거: `poke_interval`, `allowed_states`, `failed_states`, `deferrable`, `execution_timeout`.
이전 스윕이 다음 스윕을 막던 구조(07-27 12:00 스윕이 15:00에야 실행)를 없앤다.
중복 트리거는 `has_bottom_pending` ShortCircuit + `skip_when_already_exists`가 막고, `ingest`는 성공한 폴더를 삭제하므로 재실행이 안전하다.

### 6. 조용한 데이터 유실 알림

**`modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py`** — `_sweep_stale_tmp_folders`(533행)가 `_tmp__` 폴더를 quarantine할 때 `logger.warning`만 남긴다. 현재 `_quarantine`에 이미 4건이 쌓여 있다(= 버려진 bottom 수집분). 스윕 종료 시 Telegram 알림을 추가한다.

```python
    if moved:
        try:
            from modules.transform.utility.notifier import send_telegram

            send_telegram(
                f"[배민 inbox 잔해 회수] {inbox_dir.name}에서 중단된 export {moved}건을 "
                f"quarantine으로 옮김 (age>{max_age_hours}h). 해당 수집분은 적재되지 않음."
            )
        except Exception as exc:
            logger.warning("잔해 회수 알림 실패(무시): %s", exc)
    return moved
```

import는 순환참조를 피하기 위해 **함수 내부 지연 import**로 둔다 (`_warn_if_upload_inbox_stale`가 `DB_Beamin_pc2_distribute`를 지연 import하는 것과 대칭).

**`modules/transform/pipelines/db/DB_Beamin_Macro_upload.py`** — `_warn_if_upload_inbox_stale`(181행)은 스윕이 30분마다 돌면 같은 폴더로 반복 발송된다. 폴더당 6시간 1회로 throttle 한다.

```python
UPLOAD_STALE_ALERT_THROTTLE_HOURS = 6
STALE_ALERT_MARKER_DIR = LOCAL_DB / "baemin_upload_stale_alert"


def _stale_alert_due(names: list[str]) -> bool:
    STALE_ALERT_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    marker = STALE_ALERT_MARKER_DIR / (_safe_run_id_part("__".join(sorted(names))) + ".marker")
    cutoff = time.time() - UPLOAD_STALE_ALERT_THROTTLE_HOURS * 3600
    if marker.exists() and marker.stat().st_mtime > cutoff:
        return False
    marker.write_text(str(time.time()), encoding="utf-8")
    return True
```

`_warn_if_upload_inbox_stale`에서 `logger.warning(body)`는 항상 남기고, `send_telegram(body)` 호출만 `if _stale_alert_due(stale):` 로 감싼다.

## Reference Code

### modules/transform/pipelines/db/DB_Beamin_Macro_upload.py (현행 — 수정 대상)

```python
from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import (
    BOTTOM_FOLDER_PATTERN, DEFAULT_FOLDER_PATTERN,
    count_baemin_upload_inbox_folders, ingest_baemin_upload_inbox,
)
from modules.transform.pipelines.db.DB_Beamin_retry import (
    build_retry_conf, count_failed_items, restore_meta_credentials, retry_needed,
)
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")
UPLOAD_INBOX_STALE_HOURS = 12


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def _target_date(context) -> str:
    ti = context["ti"]
    target_date = ti.xcom_pull(task_ids="ingest", key="target_date")   # → _meta_pull
    if target_date:
        return target_date
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")


def ingest(folder_pattern: str = DEFAULT_FOLDER_PATTERN, **context) -> str:
    result = ingest_baemin_upload_inbox(folder_pattern=folder_pattern)
    meta = restore_meta_credentials(result.get("meta") or {})
    for key, value in meta.items():
        context["ti"].xcom_push(key=key, value=value)
    context["ti"].xcom_push(key="ingest_stats", value=result.get("stats") or {})
    return result.get("summary", "upload inbox 적재 완료")


def has_ingested_folders(**context) -> bool:
    stats = context["ti"].xcom_pull(task_ids="ingest", key="ingest_stats") or {}   # → _meta_pull
    folders = int(stats.get("folders") or 0)
    cleaned = int(stats.get("cleaned") or 0)
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    skip_if_empty = bool(conf.get("skip_if_empty", False))
    if folders and not cleaned:
        from airflow.exceptions import AirflowException
        raise AirflowException(f"upload 대상 {folders}개 중 정상 적재 폴더 없음: {stats}")
    if not folders and skip_if_empty:
        logger.info("PC2 upload 대상 폴더 없음 → downstream skip (stats=%s)", stats)
        return False
    return True
```

### modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py (현행 — 수정 대상)

```python
UPLOAD_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_upload_inbox"
DEFAULT_FOLDER_PATTERN = "manual__*"
TOP_FOLDER_PATTERN = "manual__top__*"
BOTTOM_FOLDER_PATTERN = "manual__bottom__*"
QUARANTINE_DIR_NAME = "_quarantine"
TMP_FOLDER_PREFIX = "_tmp__manual__"
STALE_TMP_MAX_AGE_HOURS = 24


def _sweep_stale_tmp_folders(inbox_dir: Path, max_age_hours: float = STALE_TMP_MAX_AGE_HOURS) -> int:
    """중단된 export 잔해(_tmp__manual__*)를 quarantine으로 회수한다."""
    if not inbox_dir.exists():
        return 0
    cutoff = time.time() - max_age_hours * 3600
    moved = 0
    for folder in sorted(inbox_dir.glob(f"{TMP_FOLDER_PREFIX}*")):
        if not folder.is_dir():
            continue
        try:
            if folder.stat().st_mtime > cutoff:
                continue
        except OSError as exc:
            logger.warning("stale tmp mtime 확인 실패: %s / %s", folder, exc)
            continue
        if _quarantine_empty_folder(folder, inbox_dir):
            moved += 1
            logger.warning("중단된 export 잔해 quarantine: %s (age>%sh)", folder.name, max_age_hours)
    return moved
```

### dags/db/DB_Beamin_Macro_Dags.py:645-691 — trigger_dag 패턴 (신규 trigger_validate에 재사용)

```python
def trigger_upload_after_export(**context) -> str:
    ti = context["ti"]
    exported = ti.xcom_pull(task_ids="export_to_upload_inbox", key="upload_inbox_run")
    if not exported:
        logger.warning("배민 upload 트리거 스킵: export 폴더 없음")
        return "upload 트리거 스킵: export 폴더 없음"
    folder_name = Path(str(exported)).name
    if not folder_name.startswith("manual__"):
        raise ValueError(f"허용되지 않은 upload 폴더명: {folder_name}")
    source_run_id = _current_run_id(context)
    upload_run_id = f"collect_export__{safe_run_id_part(source_run_id)}"
    trigger_conf = {"folder_pattern": folder_name, "skip_if_empty": True,
                    "source": "main_top_collect_export",
                    "target_date": _target_date_from_context(context)}

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    try:
        trigger_dag(dag_id=_UPLOAD_DAG_ID, run_id=upload_run_id, conf=trigger_conf)
    except DagRunAlreadyExists:
        logger.info("배민 upload DAG run 이미 존재: %s", upload_run_id)
        return f"upload DAG run 이미 존재: {upload_run_id}"
    logger.info("배민 upload DAG 트리거 완료: run_id=%s folder=%s", upload_run_id, folder_name)
    return f"upload DAG 트리거 완료: {upload_run_id}"
```

### modules/transform/pipelines/db/beamin_staging.py:61 — run_id 안전 변환

```python
def safe_run_id_part(value: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]
```

### modules/transform/utility/paths.py — 경로 상수

```python
COLLECT_DB = resolve_collect_db()     # 수집 데이터 (OneDrive/컨테이너 자동 감지)
LOCAL_DB = resolve_local_db()         # C:/Local_DB — 충돌 방지용 로컬
ANALYTICS_DB = resolve_analytics_db()
```

## Test Cases

1. [핸드오프 왕복] `python -c "import json,tempfile,pathlib; from modules.transform.pipelines.db import DB_Beamin_Macro_upload as u; p=u._write_handoff('run/1:2+3', {'target_date':'2026-07-27','account_list':[{'account_id':'a'}]}, {'folders':1,'cleaned':1}); d=json.loads(pathlib.Path(p).read_text(encoding='utf-8')); assert d['target_date']=='2026-07-27' and d['ingest_stats']['cleaned']==1; print('OK', p)"` → 기대: `OK <경로>`, 파일명에 `/`·`:`·`+` 없음
2. [XCom 우선순위] `_meta_pull`이 ingest XCom 값이 있으면 그것을, `None`이면 handoff 파일 값을 반환 → `tests/test_baemin_pc2_upload_trigger.py`에 신규 테스트 추가 후 `python -m pytest tests/test_baemin_pc2_upload_trigger.py -q` → 기대: 전부 PASS
3. [기존 단위 테스트] `python -m pytest tests/test_beamin_upload_inbox_distribute.py tests/test_beamin_stale_tmp_sweep.py tests/test_baemin_final_notification.py tests/test_baemin_macro_validation.py tests/test_beamin_retry_conf.py -q` → 기대: 전부 PASS (회귀 0건)
4. [DAG import] `python -c "from dags.db.DB_Beamin_Macro_Upload_Dags import dag; from dags.db.DB_Beamin_Macro_Upload_Validate_Dags import dag as d2; from dags.db.DB_Beamin_Macro_Upload_Pc2_Dags import dag as d3; print(sorted(t.task_id for t in dag.tasks)); print(sorted(t.task_id for t in d2.tasks))"` → 기대: 첫 줄 `['has_pending', 'ingest', 'trigger_validate']`, 둘째 줄에 `notify_upload_result`·`precheck_manual_baemin_orders`·`trigger_retry_if_needed`·`validate_ad_funnel`·`validate_orders`·`validate_toorder` 6개
5. [스케줄 상수] `python -c "from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_PC2_TIME as s; print(s); assert s=='5,35 * * * *'"` → 기대: `5,35 * * * *`
6. [파싱 에러 없음] `docker exec airflow-airflow-scheduler-1 airflow dags list-import-errors` → 기대: 출력에 `DB_Beamin_Macro_Upload` 관련 항목 없음
7. [태스크 그래프] `docker exec airflow-airflow-scheduler-1 airflow tasks list DB_Beamin_Macro_Upload_Validate_Dags --tree` → 기대: precheck → 3개 validate → trigger_retry → notify 구조
8. [실데이터 E2E] inbox에 남아 있는 `manual__bottom__manual__bottom_random4__20260726__codex`로 검증.
   `docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_Beamin_Macro_Upload_Dags -c '{"folder_pattern":"manual__bottom__*","skip_if_empty":true}'`
   → 기대: (a) 런이 **5분 안에** success, (b) `_baemin_upload_inbox`에서 해당 폴더 소멸, (c) `ANALYTICS_DB/baemin_macro/orders/...`에 반영, (d) `DB_Beamin_Macro_Upload_Validate_Dags` 런이 별도 생성돼 독립 진행
9. [기아 회귀 확인] `docker exec airflow-postgres-1 psql -U airflow -d airflow -c "select run_id,state,start_date,end_date from dag_run where dag_id='DB_Beamin_Macro_Upload_Dags' order by queued_at desc limit 10;"` → 기대: `start_date`가 NULL인(= 한 번도 시작 못 한) 런 없음

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1→9 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

Test 6~9는 Docker/Airflow가 떠 있어야 한다. 컨테이너에 접근할 수 없으면 1~5만 수행하고, 6~9는 **미실행으로 명시 보고**한다 — PASS로 처리하지 말 것.

## Constraints

- **meta를 `dag_run.conf`에 직접 싣지 말 것.** 계정 자격증명이 포함되며 conf는 메타DB에 pickle로 저장된다. 반드시 파일 경로만 전달한다.
- `ingest_baemin_upload_inbox` / `ingest_inbox` 적재 로직, ACL 복구 핸드셰이크(`_request_and_wait_acl_repair`), quarantine 규칙, `_cleanup_processed_folder` 가드는 **손대지 말 것**.
- `dags/db/DB_Beamin_Macro_Dags.py`의 `export_to_upload_inbox` / `trigger_upload_after_export`는 **변경 없음**. 트리거 대상은 여전히 `DB_Beamin_Macro_Upload_Dags`이며, 이제 즉시 끝난다.
- `validate_toorder` 내부 Selenium 로직은 **변경 없음**. 7시간 행 자체는 DAG 분리로 격리되며, 하드 watchdog은 별도 후속 과제다.
- 기존 `ingest()`의 XCom push는 유지할 것 — 단일 DAG 실행과 기존 테스트 호환에 필요하다.
- `_meta_pull` 교체 시 `task_ids="precheck_manual_baemin_orders"` / `"validate_toorder"` / `"validate_ad_funnel"` / `"trigger_retry_if_needed"` pull은 건드리지 말 것 (같은 DAG 내부).
- 폴더 구조·파일명 변경 금지. print 금지, `logger` 사용.
- `Path(__file__).stem`으로 dag_id를 만들므로 신규 DAG 파일명이 곧 dag_id다 — `DB_Beamin_Macro_Upload_Validate_Dags.py` 이름을 정확히 지킬 것.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`from __future__ import annotations` + 시그니처 힌트)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `trigger_validate`를 TriggerDagRunOperator로 할지 PythonOperator+`trigger_dag`로 할지: **PythonOperator + `trigger_dag`** (Reference Code의 `trigger_upload_after_export` 패턴)
