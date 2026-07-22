# DB_Beamin_Macro_Today_Dags 신설 — 당일 배민 orders 수집

## Task
배민 매크로 수집은 하루 1회(03:10) 로그인→NOW→우리가게→변경이력→orders→광고 funnel 전 단계를 돈다. OKPOS 등 POS는 이미 `*_Today_Dags.py`로 당일 여러 번(`13,15,17,19,21`시) 수집하는데 배민만 당일 수집이 없다. **로그인 → orders 만** 수집하는 경량 당일 DAG을 신설해 POS today와 같은 시간대로 돌린다. NOW/우리가게/변경이력/광고 funnel은 제외한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG에는 오케스트레이션만, 비즈니스 로직은 pipelines/에 배치

## Files to Create / Modify
- **수정** `modules/transform/utility/schedule.py` — 스케줄 상수 `SMD_BAEMIN_TODAY_TIME` 추가
- **수정** `modules/transform/pipelines/db/DB_Beamin_combined.py` — `collect_orders_only()` 공개 함수 추가
- **생성** `dags/db/DB_Beamin_Macro_Today_Dags.py` — 당일 orders 전용 DAG

## Implementation Steps

### 1. schedule.py — 상수 추가
today 섹션(`DB_OKPOS_SALES_TODAY_TIME` 등 49~53행 부근)에 추가:
```python
SMD_BAEMIN_TODAY_TIME = "20 13,15,17,19,21 * * *"  # 당일 배민 orders 수집 (POS today와 동일 시간대, 20분 스태거)
```
- POS today 창(13,15,17,19,21시)과 동일. 분은 20으로 두어 40분 `DB_UNIFIED_SALES_TODAY_TRIGGER_TIME` 통합 트리거 전에 끝나도록 배치.

### 2. DB_Beamin_combined.py — `collect_orders_only` 추가
기존 헬퍼(`_build_dashboard_session`, `_build_store_list_from_options`, `_filter_store_list_for_request`, `_store_collection_sort_key`, `_new_runtime_metrics`, `collect_orders_for_account`)를 재사용해 **orders 단계만** 수행하는 공개 함수를 파일 하단(다른 공개 함수 근처)에 추가한다.

```python
def collect_orders_only(
    account_list: list[dict],
    target_date: str | None = None,
    stability_profile: str | None = None,
) -> dict:
    """로그인 → 매장 목록 확인 → 주문내역(orders)만 수집한다. (당일 수집용)

    NOW/우리가게/변경이력/광고 funnel 단계를 제외하고 orders만 수집한다.
    target_date None이면 오늘(KST). collect_orders_for_account가 매장별
    login+orders+검증+CSV upsert+quit을 자체 처리한다.
    """
    profile = resolve_stability_profile(stability_profile)
    if target_date is None:
        target_date = pendulum.now(KST).format("YYYY-MM-DD")
    metrics = _new_runtime_metrics(profile["name"], account_list)
    success, fail = 0, 0
    failed_accounts: list[dict] = []
    failed_orders: list[dict] = []
    validation_results: list[dict] = []
    store_info_per_account_list: list[dict] = []

    for account in account_list:
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
                raise RuntimeError(f"main dashboard load failed: {account_id}")
            raw_options = get_store_options(bootstrap_driver)
            store_list = _build_store_list_from_options(raw_options)
            if requested_store_name:
                store_list = _filter_store_list_for_request(store_list, requested_store_name)
            store_list.sort(key=_store_collection_sort_key)
            logger.info(
                "orders-only 수집 매장/store_id: %s",
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )
        except Exception as exc:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, exc, exc_info=True)
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            store_list = []
        finally:
            try:
                bootstrap_driver.quit()
            except Exception:
                pass

        if not store_list:
            if account not in failed_accounts:
                fail += 1
                failed_accounts.append(account)
            continue

        metrics["requested_store_count"] += len(store_list)
        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

        result = collect_orders_for_account(
            account_id, account["password"], store_list, target_date=target_date
        )
        for store_info in (result.get("failed", []) if isinstance(result, dict) else []):
            failed_orders.append({"account": account, "stores": [store_info]})
        validation_results.extend(
            result.get("validation", []) if isinstance(result, dict) else []
        )
        success += 1

        wait_sec = random.uniform(*profile["account_wait_range"])
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)

    total = success + fail
    summary = f"orders-only 성공 {success}/{total} 계정 (target={target_date})"
    logger.info(summary)
    if success == 0 and total > 0:
        raise RuntimeError(f"배민 orders-only 전 계정 수집 실패({summary}) — 환경/네트워크 장애 의심")

    return {
        "summary": summary,
        "failed": {"orders": failed_orders, "accounts": failed_accounts},
        "validation": validation_results,
        "store_info_per_account": store_info_per_account_list,
    }
```
- 주의: 부트스트랩 실패로 이미 `failed_accounts`에 넣은 계정을 store_list 없음 분기에서 중복 카운트하지 말 것(위 `if account not in failed_accounts` 가드).

### 3. dags/db/DB_Beamin_Macro_Today_Dags.py — 신규 DAG
`DB_OKPOS_Sales_Today_Dags.py` 구조 + 배민 알림 패턴 참고. 경량 5-task 구성.

```python
"""배민 당일 주문(orders) 수집 DAG — 로그인 → orders만 수집."""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.DB_Beamin_combined import collect_orders_only
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.schedule import SMD_BAEMIN_TODAY_TIME

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
_ALERT_EMAILS = [MAIL_CMJ_PM]

# exact match; macro 원본의 누락 콤마 버그를 수정한 목록
TARGET_STORES = [
    "도리당 해운대중동점", "도리당 법흥리점", "도리당 송파삼전점",
    "도리당 동탄영천점", "도리당 중랑면목점", "도리당 시흥배곧점",
    "도리당 강원영월점", "도리당 평택비전점", "도리당 부산장림점",
    "도리당 경북상주점",
]


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
    except Exception as e:
        logger.error("알림 발송 실패: %s", e)


def _on_failure_callback(context):
    ti = context.get("task_instance")
    body = (
        f"DAG: {ti.dag_id}\nTask: {ti.task_id}\n"
        f"실행일시: {ti.execution_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n로그: {ti.log_url}"
    )
    _send_alert(subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}", body=body)
    send_telegram(body + "\n해결해라")


def resolve_today(**context) -> str:
    conf = (getattr(context.get("dag_run"), "conf", None) or {})
    target_date = conf.get("sale_date") or pendulum.now(KST).format("YYYY-MM-DD")
    context["ti"].xcom_push(key="target_date", value=target_date)
    return f"Today 배민 orders 수집 날짜: {target_date}"


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target = conf.get("stores") or TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)
    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개 {stores}"


def collect_orders(**context) -> str:
    target_date = context["ti"].xcom_pull(task_ids="resolve_today", key="target_date")
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    result = collect_orders_only(account_list, target_date=target_date)
    context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    return result["summary"]


def validate_orders(**context) -> str:
    validation = context["ti"].xcom_pull(task_ids="collect_orders", key="validation") or []
    if not validation:
        return "검증 없음"
    mismatches = [v for v in validation if v.get("matched") is False]
    matched = [v for v in validation if v.get("matched") is True]
    unknown = [v for v in validation if v.get("matched") is None]
    lines = [f"orders 검증 총 {len(validation)}건 (일치 {len(matched)}, 불일치 {len(mismatches)}, 미확인 {len(unknown)})"]
    for v in mismatches:
        lines.append(
            f"  - {v.get('store', '?')} [{v.get('status', '?')}] "
            f"수집={v.get('actual_count')}건/{v.get('actual_amount', 0):,}원 "
            f"기대={v.get('expected_count')}건/{v.get('expected_amount', 0):,}원"
        )
    summary = "\n".join(lines)
    logger.info(summary)
    if mismatches:
        send_telegram(summary)
    return summary


def notify_result(**context) -> str:
    target_date = context["ti"].xcom_pull(task_ids="resolve_today", key="target_date")
    summary = context["ti"].xcom_pull(task_ids="collect_orders", key="return_value")
    failed = context["ti"].xcom_pull(task_ids="collect_orders", key="failed") or {}
    body = (
        f"[배민 당일 orders 수집] {dag_id}\n"
        f"target_date: {target_date}\n"
        f"결과: {summary}\n"
        f"orders 실패: {len(failed.get('orders') or [])}건 / 계정 실패: {len(failed.get('accounts') or [])}건"
    )
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    return body


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_TODAY_TIME,
    start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["db", "baemin", "today", "selenium", "crawl"],
) as dag:
    t1 = PythonOperator(task_id="resolve_today", python_callable=resolve_today)
    t2 = PythonOperator(task_id="load_accounts", python_callable=load_accounts)
    t3 = PythonOperator(
        task_id="collect_orders",
        python_callable=collect_orders,
        pool="selenium_pool",
        execution_timeout=timedelta(minutes=120),
    )
    t4 = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule="all_done",
    )
    t5 = PythonOperator(
        task_id="notify_result",
        python_callable=notify_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4 >> t5
```

## Reference Code

### modules/transform/pipelines/db/DB_Beamin_04_orders.py (핵심 시그니처)
```python
from modules.transform.utility.paths import BAEMIN_ORDERS_DB
KST = pendulum.timezone("Asia/Seoul")
ORDERS_URL = "https://self.baemin.com/orders/history"

def collect_orders_for_account(account_id, password, store_list, target_date=None) -> dict:
    """매장별 독립 Chrome 세션으로 주문내역 수집(login+orders+검증+CSV upsert+quit).
       Returns {"failed": list[dict], "validation": list[dict]}. target_date None이면 어제."""

def _set_date(driver, target_date: str) -> bool:
    """target_date=='어제'면 프리셋, 그 외(=오늘 포함)는 _set_date_specific(달력 선택)."""
# 저장경로: analytics/baemin_macro/orders/brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM}.csv (upsert by 주문번호)
```

### modules/transform/pipelines/db/DB_Beamin_combined.py (재사용 헬퍼)
```python
from modules.extract.croling_beamin import (
    TIMING, get_store_options, is_on_main_dashboard, launch_browser,
    login_baemin, logout_baemin, wait_for_page,
)
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    collect_orders_for_account, collect_orders_for_driver,
)
from modules.transform.pipelines.db.beamin_stability import resolve_stability_profile
KST = pendulum.timezone("Asia/Seoul")

def _new_runtime_metrics(profile_name, account_list) -> dict: ...
def _build_dashboard_session(account, metrics, profile) -> Any | None: ...  # 로그인+대시보드 진입
def _build_store_list_from_options(raw_options: list[dict]) -> list[dict]: ...  # 중복제거 매장목록
def _filter_store_list_for_request(store_list, requested_store_name) -> list[dict]: ...
def _store_collection_sort_key(store_info: dict) -> tuple[int, str]: ...
# profile["account_wait_range"] 존재. import: random, time, pendulum 이미 상단에 있음
```

### dags/db/DB_OKPOS_Sales_Today_Dags.py (Today DAG 골격 참고)
```python
def resolve_today(**context) -> str:
    conf = (getattr(context.get("dag_run"), "conf", None) or {})
    sale_date = conf.get("sale_date") or pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    context["ti"].xcom_push(key="sale_date", value=sale_date)
    return f"Today OKPOS 수집 날짜: {sale_date}"

with DAG(dag_id=dag_id, schedule=DB_OKPOS_SALES_TODAY_TIME,
        start_date=pendulum.datetime(2026, 7, 6, tz="Asia/Seoul"),
        catchup=False, max_active_runs=1, default_args=default_args,
        tags=["db", "okpos", "today", "selenium"]) as dag:
    ...  # PythonOperator + pool="selenium_pool"
```

## Test Cases
1. [schedule import] `python -c "from modules.transform.utility.schedule import SMD_BAEMIN_TODAY_TIME; print(SMD_BAEMIN_TODAY_TIME)"` → 기대: `20 13,15,17,19,21 * * *` 출력, 에러 없음
2. [pipeline import] `python -c "from modules.transform.pipelines.db.DB_Beamin_combined import collect_orders_only; print('ok')"` → 기대: `ok`, ImportError 없음
3. [DAG import] `python -c "from dags.db.DB_Beamin_Macro_Today_Dags import dag; print(dag.dag_id, dag.schedule_interval)"` → 기대: `DB_Beamin_Macro_Today_Dags 20 13,15,17,19,21 * * *`
4. [DAG 파싱] `airflow dags list-import-errors` → 기대: `DB_Beamin_Macro_Today_Dags` 관련 에러 없음
5. [task 구성] `python -c "from dags.db.DB_Beamin_Macro_Today_Dags import dag; print(sorted(t.task_id for t in dag.tasks))"` → 기대: `['collect_orders', 'load_accounts', 'notify_result', 'resolve_today', 'validate_orders']`

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (import 경로/오타/누락 헬퍼)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- **범위 한정**: DAG/파이프라인 신설 + orders 수집까지만. `DB_UnifiedSales_Dags.py`의 `today_mode=true`는 현재 `reconcile_baemin`/`enforce_baemin_manual_only`를 스킵하므로 이 DAG이 수집한 당일 orders는 today 통합매출에 자동 반영되지 않는다. **unified 반영 로직은 이번 작업에 포함하지 않는다** (사용자 별도 결정).
- 기존 파이프라인 함수 시그니처(`collect_orders_for_account`, `_build_dashboard_session` 등) **수정 금지** — 재사용만.
- `_set_date`는 이미 오늘 날짜(달력 선택)를 지원하므로 orders 파이프라인 수정 불필요.
- print 금지, logging만. 스케줄/경로 하드코딩 금지.
- 폴더 구조/파일명 변경 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (있음)
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
