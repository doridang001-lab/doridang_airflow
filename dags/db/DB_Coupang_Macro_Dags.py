"""쿠팡이츠 매크로 DAG — 주문내역 자동 수집

수집 흐름:
  load_accounts
      ↓
  collect_all (매장별 독립 Chrome, 주문내역 수집)
      ↓ (all_done)
  retry_failed
      ↓ (all_done)
  validate_orders

저장 경로:
  analytics/coupang_macro/orders/
    brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM}.csv

conf 파라미터:
  {"target_date": "YYYY-MM-DD"} → 지정일 수집
  {"force": true} → 차단/쿨다운 안내가 있어도 수동 재실행 의도를 로그에 남김
  없음 → 어제 날짜 자동
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_Coupang_combined import (
    collect_all as pipeline_collect_all,
    load_coupang_accounts,
    retry_once_failed as pipeline_retry_failed,
)
from modules.transform.pipelines.db.DB_Coupang_02_validate import validate_coupang_toorder

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

KST = pendulum.timezone("Asia/Seoul")

TARGET_STORES = [
    "도리당 역삼점",
    # "나홀로 XXX점",  # TODO: 나홀로 쿠팡이츠 계정 등록 후 활성화
]  # 테스트 단계; 빈 리스트이면 전체 쿠팡 계정 수집


def _on_failure_callback(context):
    try:
        from modules.transform.utility.notifier import send_telegram
        ti = context.get("task_instance")
        logical_date = context.get("logical_date") or ti.execution_date
        exc_str = str(context.get("exception", "알 수 없음"))
        msg = (
            f"[Airflow 실패] {ti.dag_id} / {ti.task_id}\n"
            f"실행일시(KST): {logical_date.in_timezone(KST).strftime('%Y-%m-%d %H:%M')}\n"
            f"에러: {exc_str}\n"
            f"로그: {ti.log_url}"
        )
        send_telegram(msg)
    except Exception:
        pass


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def load_accounts(**context) -> str:
    accounts = load_coupang_accounts(target_stores=TARGET_STORES, exact=True)
    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 → %s", len(accounts), stores)
    return f"계정 {len(accounts)}개: {stores}"


def collect_all(**context) -> str:
    import random
    import time

    wait_sec = random.uniform(0, 30)
    logger.info("수집 시작 전 랜덤 대기: %.0f초", wait_sec)
    time.sleep(wait_sec)

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    force = bool(conf.get("force", False))

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    result = pipeline_collect_all(account_list, target_date=target_date, force=force)
    context["ti"].xcom_push(key="target_date", value=target_date)
    context["ti"].xcom_push(key="failed", value=result.get("failed", []))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    context["ti"].xcom_push(key="blocked", value=result.get("blocked", False))
    context["ti"].xcom_push(key="summary", value=result["summary"])

    failed = result.get("failed", [])
    blocked = bool(result.get("blocked", False))
    success_count = int(result.get("success_count", 0))
    total_count = int(result.get("total_count", len(account_list)))
    if blocked or failed or success_count == 0:
        raise RuntimeError(
            f"쿠팡 collect_all 실패: {result['summary']} "
            f"(success={success_count}/{total_count}, failed={len(failed)}, blocked={blocked})"
        )

    return result["summary"]


def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or context["ti"].xcom_pull(task_ids="collect_all", key="target_date")
    force = bool(conf.get("force", False))

    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or []
    if not failed:
        logger.info("재시도 대상 없음")
        return "재시도 없음"

    result = pipeline_retry_failed(failed, target_date=target_date, force=force, return_result=True)
    if result.get("blocked") or result.get("success_count", 0) < result.get("total_count", len(failed)):
        raise RuntimeError(
            f"쿠팡 retry_failed 실패: {result['summary']} "
            f"(success={result.get('success_count', 0)}/{result.get('total_count', len(failed))}, "
            f"blocked={result.get('blocked', False)})"
        )

    return result["summary"]


def validate_orders(**context) -> str:
    ti = context["ti"]
    validation = ti.xcom_pull(task_ids="collect_all", key="validation") or []
    target_date = ti.xcom_pull(task_ids="collect_all", key="target_date")

    lines = []

    # ── 1. DOM 수집건수 검증 ──────────────────────────────────
    if validation:
        mismatches = [v for v in validation if v.get("matched") is False]
        matched    = [v for v in validation if v.get("matched") is True]
        unknown    = [v for v in validation if v.get("matched") is None]
        lines.append(
            f"[DOM검증] 총 {len(validation)}건 "
            f"(일치 {len(matched)}, 불일치 {len(mismatches)}, 미확인 {len(unknown)})"
        )
        for v in mismatches:
            lines.append(
                f"  ❌ {v.get('store','?')} "
                f"수집={v.get('collected')}건 / 기대={v.get('expected')}건"
            )

    # ── 2. ToOrder 교차검증 ────────────────────────────────────
    vr: dict = {}
    if not target_date:
        lines.append("[ToOrder검증] target_date 없음 — 건너뜀")
    else:
        try:
            account_list = ti.xcom_pull(task_ids="load_accounts", key="account_list") or []
            allowed_stores = {a["store"] for a in account_list} if account_list else None
            vr = validate_coupang_toorder(target_date, allowed_stores=allowed_stores)
            compared = vr.get("compared_count", 0)
            store_res = vr.get("store_results", {})
            if compared == 0:
                lines.append(f"[ToOrder검증] 비교 대상 없음 (target_date={target_date})")
            else:
                all_matched = vr.get("matched", False)
                sr = store_res.get("__total__", {})
                t = sr.get("toorder", 0)
                c = sr.get("coupang", 0)
                gap = sr.get("toorder_gap")
                breakdown = sr.get("toorder_breakdown") or {}
                if gap:
                    lines.append(f"[ToOrder검증] 총액 비교 — ⚠️ ToOrder=0 / 쿠팡={c:,}원 (ToOrder갭)")
                elif all_matched:
                    lines.append(f"[ToOrder검증] 총액 비교 — ✅ 일치 {t:,}원")
                else:
                    lines.append(
                        f"[ToOrder검증] 총액 비교 — ❌ ToOrder={t:,}원 / 쿠팡={c:,}원 (차이={t-c:,}원)"
                    )
                if breakdown:
                    for sname, samount in sorted(breakdown.items()):
                        lines.append(f"  ToOrder {sname}: {samount:,}원")
        except Exception as exc:
            lines.append(f"[ToOrder검증] 오류: {exc}")
            logger.exception("ToOrder 교차검증 중 오류")

    summary = "\n".join(lines) if lines else "검증 결과 없음"
    logger.info(summary)

    return summary


with DAG(
    dag_id=dag_id,
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "coupang", "crawl"],
) as dag:

    t1 = PythonOperator(
        task_id="load_accounts",
        python_callable=load_accounts,
    )

    t2 = PythonOperator(
        task_id="collect_all",
        python_callable=collect_all,
        execution_timeout=timedelta(minutes=120),
    )

    t3 = PythonOperator(
        task_id="retry_failed",
        python_callable=retry_failed,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=60),
    )

    t4 = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4
