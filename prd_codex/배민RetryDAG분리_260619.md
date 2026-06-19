# 배민 Retry DAG 분리

## Task
메인 DAG(`DB_Beamin_Macro_Dags.py`)의 retry task를 아무리 늘려도 누락이 반복됨.
원인은 task 추가 방식이 "전체 계정을 다시 돌리는" 구조이기 때문.
해결: 메인 DAG 종료 후 **실패한 매장/항목만** 별도 DAG(`DB_Beamin_Macro_Dags_Retry.py`)에서 재수집.
최대 10회까지 자기 자신을 재트리거하며, 회당 3~15분 랜덤 딜레이를 둔다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- conf에 password 미포함 (Airflow DB 평문 저장 방지) — account_id만 전달 후 재로드

## Files to Create / Modify
- **수정**: `dags/db/DB_Beamin_Macro_Dags.py` — DAG 체인 끝에 `trigger_retry_if_needed` task 추가
- **신규**: `dags/db/DB_Beamin_Macro_Dags_Retry.py` — 실패 매장만 재수집하는 retry DAG

## Implementation Steps

### Step 1. `DB_Beamin_Macro_Dags.py` 수정

기존 DAG 체인 마지막(`t7` 이후)에 `t8` task를 추가한다.

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def trigger_retry_if_needed(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}

    # 실패 항목이 없으면 트리거 안 함
    has_failure = any(failed.get(k) for k in ("accounts", "stores", "orders", "ads"))
    if not has_failure:
        logger.info("재시도 불필요: 실패 없음")
        return "재시도 불필요"

    # password 제외하고 account_id만 추출
    failed_account_ids = list({
        item["account"]["account_id"]
        for key in ("stores", "orders", "ads")
        for item in failed.get(key, [])
        if "account" in item
    } | {
        a["account_id"] for a in failed.get("accounts", [])
    })

    # store/orders/ads 항목에서 account 필드 제거 (password 제거)
    def _strip_password(items: list[dict]) -> list[dict]:
        result = []
        for item in items:
            stripped = {k: v for k, v in item.items() if k != "account"}
            stripped["account_id"] = item.get("account", {}).get("account_id", "")
            result.append(stripped)
        return result

    retry_conf = {
        "attempt": 1,
        "max_attempts": 10,
        "target_date": target_date,
        "failed_account_ids": failed_account_ids,
        "failed_stores": _strip_password(failed.get("stores", [])),
        "failed_orders": _strip_password(failed.get("orders", [])),
        "failed_ads": _strip_password(failed.get("ads", [])),
        "failed_accounts_ids_only": [a["account_id"] for a in failed.get("accounts", [])],
    }
    logger.info(
        "Retry DAG 트리거: attempt=1 accounts=%d stores=%d orders=%d ads=%d",
        len(failed.get("accounts", [])),
        len(failed.get("stores", [])),
        len(failed.get("orders", [])),
        len(failed.get("ads", [])),
    )

    # TriggerDagRunOperator 대신 Airflow API 직접 호출 (PythonOperator 내에서)
    from airflow.api.common.trigger_dag import trigger_dag
    import uuid
    trigger_dag(
        dag_id="DB_Beamin_Macro_Dags_Retry",
        run_id=f"retry__{target_date}__attempt_1__{uuid.uuid4().hex[:8]}",
        conf=retry_conf,
        replace_microseconds=False,
    )
    return f"Retry DAG 트리거 완료: {len(failed_account_ids)}개 계정 / target_date={target_date}"
```

DAG 정의 블록 수정:
```python
# 기존
t_dash >> t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7

# 변경
t8 = PythonOperator(
    task_id="trigger_retry_if_needed",
    python_callable=trigger_retry_if_needed,
    trigger_rule=TriggerRule.ALL_DONE,
)
t_dash >> t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8
```

---

### Step 2. `DB_Beamin_Macro_Dags_Retry.py` 신규 생성

```
load_failed_and_accounts
        ↓
retry_collect  (3~15분 random sleep 포함)
        ↓
validate_toorder
        ↓
notify_and_trigger_next
```

**전체 파일 구조:**

```python
import json
import logging
import random
import time
import uuid
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts as pipeline_load_accounts
from modules.transform.pipelines.db.DB_Beamin_combined import retry_once_failed as pipeline_retry_failed
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders
from modules.transform.utility.notifier import send_telegram

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
MAX_ATTEMPTS = 10


def load_failed_and_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    attempt = int(conf.get("attempt", 1))
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    failed_account_ids = conf.get("failed_account_ids") or []

    # password 복원: account_id 기반으로 전체 계정 로드 후 필터링
    all_accounts = pipeline_load_accounts(target_stores=[])
    account_map = {a["account_id"]: a for a in all_accounts}
    filtered_accounts = [account_map[aid] for aid in failed_account_ids if aid in account_map]

    if not filtered_accounts:
        logger.warning("복원된 계정 없음: failed_account_ids=%s", failed_account_ids)

    # failed dict 재조립 (account 필드를 복원된 계정으로 채움)
    def _restore_account(items: list[dict]) -> list[dict]:
        result = []
        for item in items:
            aid = item.get("account_id", "")
            if aid not in account_map:
                continue
            restored = {k: v for k, v in item.items() if k != "account_id"}
            restored["account"] = account_map[aid]
            result.append(restored)
        return result

    failed = {
        "accounts": filtered_accounts if conf.get("failed_accounts_ids_only") else [],
        "stores": _restore_account(conf.get("failed_stores") or []),
        "orders": _restore_account(conf.get("failed_orders") or []),
        "ads": _restore_account(conf.get("failed_ads") or []),
    }

    context["ti"].xcom_push(key="failed", value=failed)
    context["ti"].xcom_push(key="attempt", value=attempt)
    context["ti"].xcom_push(key="target_date", value=target_date)
    context["ti"].xcom_push(key="filtered_accounts", value=filtered_accounts)

    total = sum(len(v) for v in failed.values())
    logger.info("Retry attempt=%d target_date=%s 재시도 대상=%d건", attempt, target_date, total)
    return f"attempt={attempt} target_date={target_date} 재시도={total}건"


def retry_collect(**context) -> str:
    wait_sec = random.uniform(180, 900)
    logger.info("재시도 전 랜덤 대기: %.0f초 (%.1f분)", wait_sec, wait_sec / 60)
    time.sleep(wait_sec)

    ti = context["ti"]
    failed = ti.xcom_pull(task_ids="load_failed_and_accounts", key="failed") or {}
    target_date = ti.xcom_pull(task_ids="load_failed_and_accounts", key="target_date")

    has_failure = any(failed.get(k) for k in ("accounts", "stores", "orders", "ads"))
    if not has_failure:
        logger.info("재시도 대상 없음")
        context["ti"].xcom_push(key="remaining_failed", value={})
        return "재시도 대상 없음"

    result_msg = pipeline_retry_failed(failed, target_date=target_date)
    logger.info("retry_once_failed 완료: %s", result_msg)

    # retry_once_failed는 str 반환 — 남은 실패는 추적 불가, 빈 dict로 표시
    # (pipeline이 내부적으로 2회 재시도 포함)
    context["ti"].xcom_push(key="remaining_failed", value={})
    context["ti"].xcom_push(key="retry_result", value=result_msg)
    return result_msg


def validate_toorder(**context) -> str:
    ti = context["ti"]
    target_date = ti.xcom_pull(task_ids="load_failed_and_accounts", key="target_date")
    filtered_accounts = ti.xcom_pull(task_ids="load_failed_and_accounts", key="filtered_accounts") or []

    if not filtered_accounts:
        return "검증 대상 없음"

    result = validate_toorder_orders(filtered_accounts, [], target_date)
    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    mismatched = result.get("mismatched_stores", [])
    gap_stores = result.get("toorder_gap_stores", [])

    summary = (
        f"[Retry 교차검증 {target_date}] 비교 {compared}개 / "
        f"{'완전일치' if matched else f'불일치 {len(mismatched)}개'}"
    )
    if gap_stores:
        summary += f"\nToOrder 갭: {', '.join(gap_stores)}"
    logger.info(summary)
    context["ti"].xcom_push(key="validate_summary", value=summary)
    return summary


def notify_and_trigger_next(**context) -> str:
    ti = context["ti"]
    attempt = ti.xcom_pull(task_ids="load_failed_and_accounts", key="attempt") or 1
    target_date = ti.xcom_pull(task_ids="load_failed_and_accounts", key="target_date")
    retry_result = ti.xcom_pull(task_ids="retry_collect", key="retry_result") or ""
    validate_summary = ti.xcom_pull(task_ids="validate_toorder", key="validate_summary") or ""

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    max_attempts = int(conf.get("max_attempts", MAX_ATTEMPTS))

    summary_lines = [
        f"[배민 Retry {attempt}/{max_attempts}회] {target_date}",
        retry_result,
        validate_summary,
    ]
    summary = "\n".join(l for l in summary_lines if l)

    if attempt >= max_attempts:
        final_msg = f"[배민 Retry 최종실패] {max_attempts}회 재시도 후에도 누락 잔존\n{summary}"
        logger.warning(final_msg)
        send_telegram(final_msg)
        return final_msg

    # 다음 attempt 트리거
    next_conf = dict(conf)
    next_conf["attempt"] = attempt + 1

    from airflow.api.common.trigger_dag import trigger_dag
    trigger_dag(
        dag_id="DB_Beamin_Macro_Dags_Retry",
        run_id=f"retry__{target_date}__attempt_{attempt + 1}__{uuid.uuid4().hex[:8]}",
        conf=next_conf,
        replace_microseconds=False,
    )
    msg = f"{summary}\n→ attempt {attempt + 1} 트리거"
    logger.info(msg)
    send_telegram(msg)
    return msg


with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["db", "baemin", "retry"],
) as dag:

    t1 = PythonOperator(
        task_id="load_failed_and_accounts",
        python_callable=load_failed_and_accounts,
        execution_timeout=timedelta(minutes=5),
    )
    t2 = PythonOperator(
        task_id="retry_collect",
        python_callable=retry_collect,
        execution_timeout=timedelta(minutes=180),
    )
    t3 = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )
    t4 = PythonOperator(
        task_id="notify_and_trigger_next",
        python_callable=notify_and_trigger_next,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4
```

## Reference Code

### DB_Beamin_collect.py
```python
from modules.transform.utility.paths import ONEDRIVE_DB
logger = logging.getLogger(__name__)
CSV_PATH = ONEDRIVE_DB / "sales_employee.csv"

def load_accounts(target_stores: list[str], exact: bool = False) -> list[dict]:
    """반환: [{"account_id": str, "password": str, "store_name": str}, ...]"""
    df = pd.read_csv(CSV_PATH)
    df = df[df["플랫폼"] == "배달의 민족"].copy()
    # target_stores 비어 있으면 전 매장 반환
    accounts = [{"account_id": str(r["계정ID"]), "password": str(r["계정PW"]), "store_name": str(r["매장명"])} ...]
    return accounts
```

### DB_Beamin_combined.py — retry_once_failed
```python
def retry_once_failed(failed: dict, target_date: str | None = None) -> str:
    """실패한 계정/매장만 1회 재시도. orders/ads는 내부적으로 2회 재시도 포함."""
    # failed = {"accounts": [...], "stores": [...], "orders": [...], "ads": [...]}
    # stores 항목: {"account": {account dict}, "store": {"store_id", "brand", "store"}}
    # orders/ads 항목: {"account": {account dict}, "stores": [store_info list]}
```

### DB_Beamin_Macro_validate.py — validate_toorder_orders
```python
def validate_toorder_orders(
    account_list: list,
    store_info_per_account: list,
    target_date: str,
) -> dict:
    # 반환: {"matched": bool, "compared_count": int, "mismatched_stores": [...],
    #         "toorder_gap_stores": [...], "missing_brand_stores": [...], "store_results": {...}}
```

## Test Cases

1. **Retry DAG import 확인**
   ```bash
   python -c "from dags.db.DB_Beamin_Macro_Dags_Retry import dag; print(dag.dag_id)"
   ```
   → 기대: `DB_Beamin_Macro_Dags_Retry`

2. **메인 DAG import 확인 (t8 추가 후)**
   ```bash
   python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print([t.task_id for t in dag.tasks])"
   ```
   → 기대: 리스트 마지막에 `trigger_retry_if_needed` 포함

3. **수동 Retry DAG 트리거 테스트**
   Airflow UI에서 `DB_Beamin_Macro_Dags_Retry` → Trigger w/ config:
   ```json
   {
     "attempt": 1,
     "max_attempts": 2,
     "target_date": "2026-06-18",
     "failed_account_ids": ["실제계정ID"],
     "failed_stores": [],
     "failed_orders": [{"account_id": "실제계정ID", "stores": []}],
     "failed_ads": [],
     "failed_accounts_ids_only": []
   }
   ```
   → 기대: `load_failed_and_accounts` → `retry_collect` (3~15분 대기 후) → `validate_toorder` → `notify_and_trigger_next`

4. **attempt=max 시 최종 실패 알림 확인**
   conf `{"attempt": 10, "max_attempts": 10, ...}` 으로 트리거
   → 기대: 텔레그램에 "[배민 Retry 최종실패] 10회 재시도 후에도 누락 잔존" 수신

5. **attempt < max 시 자기 자신 재트리거 확인**
   conf `{"attempt": 1, "max_attempts": 10, ...}` 으로 트리거
   → 기대: Airflow UI에 `retry__YYYY-MM-DD__attempt_2__XXXX` run 생성

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
- `retry_once_failed` 함수 시그니처 변경 금지 — 기존 함수 그대로 재사용
- conf에 password 절대 미포함 — account_id만 전달 후 `load_accounts`로 복원
- `DB_Beamin_Macro_Dags_Retry`의 `schedule=None` 유지 (트리거 전용)
- `max_active_runs=1` 유지 — 동시 retry run 방지
- `retry_collect`의 `execution_timeout=timedelta(minutes=180)` — 매장 수에 따라 조정 가능하나 최소 120분 이상
- `trigger_dag()` 호출은 `notify_and_trigger_next` task 내에서만 — 다른 곳에서 호출 금지
- 메인 DAG의 기존 task(`t1`~`t7`) 로직 변경 금지 — `t8`만 추가

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
