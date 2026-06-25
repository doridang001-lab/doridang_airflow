# 배민OneDrivesync충돌방지

## Task
두 컴퓨터(상위/하위)가 동시에 배민을 수집하면서 OneDrive sync 충돌로 parquet 파일이 이전 버전으로 덮어씌워지는 문제를 해결한다.
수집 중 계정마다 즉시 write하던 방식을 LOCAL_DB staging 후 완료 시 한 번에 OneDrive에 복사하는 방식으로 전환하고, 세컨드 컴퓨터 수집 시작을 30분 지연시킨다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- `paths._cache: dict = {}` 는 module-level global — `_cache.clear()`로 재해석 가능

## Files to Create / Modify
- **수정**: `dags/db/DB_Beamin_Macro_Dags.py`
  - DAG import에 `os`, `shutil` 추가
  - `LOCAL_DB` import 추가 (`ANALYTICS_DB`는 이미 import됨)
  - `BAEMIN_SCHEDULE` 환경변수 override 상수 추가
  - DAG 정의의 `schedule=` 파라미터 교체
  - `collect_all` 함수 **2곳** (line 326, line 958) 모두에 staging wrapper 추가

## Implementation Steps

### 1. import 블록에 `os`, `shutil`, `LOCAL_DB` 추가

현재 파일 상단 (line 32~68):
```python
import html
import logging
import random
import re
import time
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Any
```

여기에 `os`와 `shutil`을 추가:
```python
import os
import shutil
```

현재 (line 67):
```python
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB
```
→ 변경:
```python
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB
```

### 2. `BAEMIN_SCHEDULE` 상수 추가

현재 (line 65):
```python
from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_TIME
```
그 아래에 추가:
```python
BAEMIN_SCHEDULE = os.getenv("BAEMIN_COLLECT_SCHEDULE", SMD_BAEMIN_COLLECT_TIME)
```

### 3. DAG 정의 schedule 파라미터 교체

현재 (line ~1396):
```python
with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_COLLECT_TIME,
```
→ 변경:
```python
with DAG(
    dag_id=dag_id,
    schedule=BAEMIN_SCHEDULE,
```

### 4. `collect_all` 함수 staging wrapper 추가 — line 326 버전

현재 함수 시작 직후, `account_list` xcom_pull 이전에 staging 준비 코드 삽입:

```python
def collect_all(**context) -> str:
    import random
    import time

    wait_sec = random.uniform(0, 60)
    logger.info("수집 시작 전 랜덤 대기: %.0f초", wait_sec)
    time.sleep(wait_sec)

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    # ── staging 설정 ──────────────────────────────────────────────────
    import modules.transform.utility.paths as _paths
    analytics_original = os.environ.get("ANALYTICS_DB")
    onedrive_baemin = ANALYTICS_DB / "baemin_macro"
    local_analytics = LOCAL_DB / "analytics_stage"
    local_baemin = local_analytics / "baemin_macro"

    if onedrive_baemin.exists():
        shutil.copytree(str(onedrive_baemin), str(local_baemin), dirs_exist_ok=True)
    else:
        local_baemin.mkdir(parents=True, exist_ok=True)

    os.environ["ANALYTICS_DB"] = str(local_analytics)
    _paths._cache.clear()
    logger.info("staging 모드 전환: %s", local_analytics)
    # ─────────────────────────────────────────────────────────────────

    # 수집 날짜 목록 결정
    if conf.get("target_date"):
        dates = [conf["target_date"]]
    elif ORDERS_BACKFILL_DAYS:
        yesterday = pendulum.yesterday(KST)
        dates = [yesterday.subtract(days=i).format("YYYY-MM-DD") for i in range(ORDERS_BACKFILL_DAYS)]
        logger.info("백필 모드: %d일치 수집 %s ~ %s", len(dates), dates[-1], dates[0])
    else:
        dates = [pendulum.yesterday(KST).format("YYYY-MM-DD")]

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    summaries = []
    last_result: dict = {}
    try:
        for target_date in dates:
            logger.info("수집 날짜: %s", target_date)
            last_result = pipeline_collect_all(account_list, target_date=target_date)
            summaries.append(f"{target_date}: {last_result['summary']}")

        # LOCAL → OneDrive 복사
        if local_baemin.exists():
            shutil.copytree(str(local_baemin), str(onedrive_baemin), dirs_exist_ok=True)
            logger.info("OneDrive 업로드 완료: %s", onedrive_baemin)
    finally:
        if analytics_original is not None:
            os.environ["ANALYTICS_DB"] = analytics_original
        else:
            os.environ.pop("ANALYTICS_DB", None)
        _paths._cache.clear()

    context["ti"].xcom_push(key="failed", value=last_result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=last_result.get("validation", []))
    context["ti"].xcom_push(key="ad_stores", value=last_result.get("ad_stores", []))
    context["ti"].xcom_push(key="store_info_per_account", value=last_result.get("store_info_per_account", []))
    return " | ".join(summaries)
```

### 5. `collect_all` 함수 staging wrapper 추가 — line 958 버전

현재 함수 구조:
```python
def collect_all(**context) -> str:
    dag_run = context.get("dag_run")
    conf = ...
    profile = resolve_stability_profile(...)
    wait_sec = ...
    time.sleep(wait_sec)

    target_date = conf.get("target_date")
    account_list = context["ti"].xcom_pull(...)
    if not account_list: ...

    # 백필 날짜 목록 계산 ...

    result = pipeline_collect_all(...)
    context["ti"].xcom_push(...)
    ...
    return summary
```

`account_list` null 체크 이후, `pipeline_collect_all` 호출 이전에 staging 코드 삽입:

```python
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    # ── staging 설정 ──────────────────────────────────────────────────
    import modules.transform.utility.paths as _paths
    analytics_original = os.environ.get("ANALYTICS_DB")
    onedrive_baemin = ANALYTICS_DB / "baemin_macro"
    local_analytics = LOCAL_DB / "analytics_stage"
    local_baemin = local_analytics / "baemin_macro"

    if onedrive_baemin.exists():
        shutil.copytree(str(onedrive_baemin), str(local_baemin), dirs_exist_ok=True)
    else:
        local_baemin.mkdir(parents=True, exist_ok=True)

    os.environ["ANALYTICS_DB"] = str(local_analytics)
    _paths._cache.clear()
    logger.info("staging 모드 전환: %s", local_analytics)
    # ─────────────────────────────────────────────────────────────────

    try:
        # 백필 날짜 목록 계산 (기존 코드 그대로)
        backfill_dates: list[str] = []
        if target_date is None and ORDERS_BACKFILL_DAYS is not None:
            ...

        result = pipeline_collect_all(
            account_list,
            target_date=target_date,
            stability_profile=profile["name"],
        )
        context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
        context["ti"].xcom_push(key="validation", value=result.get("validation", []))
        context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
        store_info_per_account = result.get("store_info_per_account", [])
        context["ti"].xcom_push(key="store_info_per_account", value=store_info_per_account)
        metrics = result.get("metrics")
        if metrics and dag_run:
            write_runtime_metrics(run_id=dag_run.run_id, stage="collect_all", payload=metrics)

        # 백필 추가 날짜 처리 (기존 코드 그대로)
        backfill_summary = ""
        if backfill_dates and store_info_per_account:
            ...

        # LOCAL → OneDrive 복사
        if local_baemin.exists():
            shutil.copytree(str(local_baemin), str(onedrive_baemin), dirs_exist_ok=True)
            logger.info("OneDrive 업로드 완료: %s", onedrive_baemin)

    finally:
        if analytics_original is not None:
            os.environ["ANALYTICS_DB"] = analytics_original
        else:
            os.environ.pop("ANALYTICS_DB", None)
        _paths._cache.clear()

    return summary  # 기존 return 로직 그대로
```

## Reference Code

### dags/db/DB_Beamin_Macro_Dags.py (상단, line 1~70)
```python
import html
import logging
import random
import re
import time
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.utility.notifier import send_telegram
from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.beamin_stability import (
    resolve_stability_profile,
    write_runtime_metrics,
)
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori as pipeline_collect_all,
    retry_once_failed as pipeline_retry_failed,
)
from modules.transform.utility.schedule import SMD_BAEMIN_COLLECT_TIME
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

KST = pendulum.timezone("Asia/Seoul")
COLLECT_RANGE: str | None = '상위'
ORDERS_BACKFILL_DAYS: int | None = None
```

### modules/transform/utility/paths.py (핵심 부분)
```python
_cache: dict = {}

def _get(name: str):
    if name not in _cache:
        _cache[name] = _RESOLVERS[name]()
    return _cache[name]

def resolve_analytics_db() -> Path:
    env_path = os.getenv("ANALYTICS_DB")   # ← 환경변수 override 지원
    if env_path:
        return Path(env_path)
    container_mount = _container_mount("/opt/airflow/analytics")
    if container_mount.parent.exists():
        container_mount.mkdir(parents=True, exist_ok=True)
        return container_mount
    ...

_RESOLVERS = {
    "ANALYTICS_DB":            resolve_analytics_db,
    "LOCAL_DB":                resolve_local_db,
    "BAEMIN_ORDERS_DETAIL_DB": lambda: _get("ANALYTICS_DB") / "baemin_macro",
    "BAEMIN_ORDERS_DB":        lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "orders",
    ...
}
# os.environ["ANALYTICS_DB"] 변경 + _cache.clear() → 모든 BAEMIN_* 경로 즉시 재해석됨
```

## Test Cases

1. **import 확인**
   ```
   docker exec airflow-airflow-worker-1 python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print('OK')"
   ```
   → 기대: `OK` (ImportError 없음)

2. **BAEMIN_SCHEDULE 상수 확인**
   ```
   docker exec airflow-airflow-worker-1 python -c "
   from dags.db.DB_Beamin_Macro_Dags import BAEMIN_SCHEDULE, SMD_BAEMIN_COLLECT_TIME
   print('schedule:', BAEMIN_SCHEDULE)
   print('same as default?', BAEMIN_SCHEDULE == SMD_BAEMIN_COLLECT_TIME)
   "
   ```
   → 기대: `same as default? True` (env 미설정 시)

3. **staging 경로 생성 확인 (manual trigger 후)**
   ```
   ls /opt/airflow/Local_DB/analytics_stage/baemin_macro/orders/
   ```
   → 기대: parquet 파일들 존재

4. **OneDrive 복사 확인 (수집 완료 후)**
   수집 완료 로그에서:
   ```
   OneDrive 업로드 완료: /opt/airflow/analytics/baemin_macro
   ```
   → Airflow 로그에서 이 메시지 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~2 실행 (import, 상수)
  2. manual trigger 후 Test Cases 3~4 실행 (staging, 업로드)
  3. FAIL → 원인 분석 → 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS
```

## Constraints
- `collect_all` 함수가 **2곳** 있음 — 326번 줄(단순 버전)과 958번 줄(stability_profile 버전) **둘 다** 수정
- DAG에 with 블록은 1개(line ~1394) — `schedule=BAEMIN_SCHEDULE` 로만 변경
- `finally` 블록에서 환경변수 반드시 복원 — 다른 task에 영향 없도록
- `shutil.copytree` 시 `dirs_exist_ok=True` 필수 (Python 3.8+)
- 파이프라인 코드(`DB_Beamin_combined.py` 등) **변경 없음** — DAG 함수만 수정
- `validate_orders`, `retry_failed` 등 다른 함수는 OneDrive에서 읽어도 무방

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
