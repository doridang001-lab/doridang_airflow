'''
Baemin macro DAG — 배달의민족 계정별 자동 수집

=== 수집 흐름 (계정 단위, 단일 브라우저 세션) ===
  load_accounts
      ↓
  collect_all
    ├─ 로그인
    ├─ now 수집        (우리가게NOW 현황 지표)
    ├─ 우가클 수집     (우리가게 클릭 현황 - 이번달 + 저번달)
    ├─ 변경이력 수집   (매장 변경이력 - history/change/shop)
    ├─ 주문내역 수집   (orders/history - 어제)
    ├─ 광고 funnel 수집 (stat/advertisement - 어제)
    └─ 로그아웃

=== 저장 경로 ===
  now      : analytics/baemin_macro/metrics_now/
               brand={brand}/store={store}/ym={YYYY-MM}/baemin_now.csv
  우가클    : analytics/baemin_macro/metrics_our_store_clicks/
               brand={brand}/store={store}/ym={YYYY-MM}/woori_shop_click.csv
  변경이력  : analytics/baemin_macro/shop_change/
               brand={brand}/store={store}/ym={YYYY-MM}/shop_change.csv
  주문내역  : analytics/baemin_macro/orders/
  광고funnel: analytics/baemin_macro/ad_funnel/
               brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM-DD}.csv

=== 수집 월 ===
  우가클: 이번달 + 저번달 (덮어쓰기)
  주문내역: 어제 (upsert by 주문번호)
'''

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
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

warnings.filterwarnings(
    "ignore",
    message="This process .* is multi-threaded, use of fork\\(\\) may lead to deadlocks in the child\\.",
    category=DeprecationWarning,
)

KST = pendulum.timezone("Asia/Seoul")
_ALERT_EMAILS = ["a17019@kakao.com"]

TARGET_STORES = [
]  # exact match; empty list means all stores

# ─── 주문내역 백필 제어 ────────────────────────────────────────────────────────
# None  → 어제 1일만 수집 (기본)
# int N → 어제부터 N일 전까지 백필 (예: 7 → 어제 포함 최근 7일)
ORDERS_BACKFILL_DAYS: int | None = None


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html

    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info("알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("알림 발송 실패: %s", e)


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logical_date = context.get("logical_date") or ti.execution_date
    execution_date = logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시(KST): {execution_date}\n"
        f"에러: {context.get('exception', '알 수 없음')}\n"
        f"로그: {ti.log_url}"
    )
    _send_alert(subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}", body=body)
    send_telegram(body + "\n해결해라")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")  # e.g. ["도리당 역삼점"] — 테스트·재수집 용
    target = stores_override if stores_override else TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)
    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개: {stores}"


def collect_all(**context) -> str:
    import random
    import time

    wait_sec = random.uniform(0, 60)
    logger.info("수집 시작 전 랜덤 대기: %.0f초", wait_sec)
    time.sleep(wait_sec)

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")  # "YYYY-MM-DD" → orders CSV 날짜 라벨 override

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"
    result = pipeline_collect_all(account_list, target_date=target_date)
    context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
    context["ti"].xcom_push(key="store_info_per_account", value=result.get("store_info_per_account", []))
    return result["summary"]


def retry_failed(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date")

    failed = context["ti"].xcom_pull(task_ids="collect_all", key="failed") or {}
    if not any(failed.get(k) for k in ("accounts", "stores", "orders", "ads")):
        logger.info("재시도 대상 없음")
        return "재시도 없음"

    return pipeline_retry_failed(failed, target_date=target_date)


def validate_orders(**context) -> str:
    validation = context["ti"].xcom_pull(task_ids="collect_all", key="validation") or []
    if not validation:
        logger.info("orders 검증 결과 없음")
        return "검증 없음"

    mismatches = [v for v in validation if v.get("matched") is False]
    matched = [v for v in validation if v.get("matched") is True]
    unknown = [v for v in validation if v.get("matched") is None]

    lines = [
        f"orders 검증: 총 {len(validation)}건 "
        f"(일치 {len(matched)}, 불일치 {len(mismatches)}, 미확인 {len(unknown)})"
    ]
    for v in mismatches:
        lines.append(
            f"  ❌ {v.get('store', '?')} [{v.get('status', '?')}] "
            f"수집={v.get('actual_count')}건/{v.get('actual_amount', 0):,}원 "
            f"기대={v.get('expected_count')}건/{v.get('expected_amount', 0):,}원 "
            f"(재시도 {v.get('retried', 0)}회)"
        )
    summary = "\n".join(lines)
    logger.info(summary)

    if mismatches:
        _send_alert(subject=f"[배민 orders 불일치] {len(mismatches)}건", body=summary)
        send_telegram(summary)

    return summary


def validate_ad_funnel(**context) -> str:
    from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    ad_stores = context["ti"].xcom_pull(task_ids="collect_all", key="ad_stores") or []
    if not ad_stores:
        logger.info("ad_funnel 점검 대상 없음")
        return "점검 없음"

    result = _validate_and_retry_ad_funnel(ad_stores, target_date)
    empty = result["empty_stores"]
    still = result["still_empty"]

    lines = [
        f"ad_funnel 빈값 점검: 총 {len(ad_stores)}매장 / 빈값 {len(empty)}건 / "
        f"재수집 후 잔여 {len(still)}건"
    ]
    for s in still:
        lines.append(f"  ❌ {s.get('store', '?')} 재수집 후에도 빈값 잔존")
    summary = "\n".join(lines)
    logger.info(summary)

    if still:
        _send_alert(subject=f"[배민 ad_funnel 빈값] {len(still)}건 잔존", body=summary)
        send_telegram(summary)

    return summary


def validate_toorder(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list") or []
    store_info_per_account = (
        context["ti"].xcom_pull(task_ids="collect_all", key="store_info_per_account") or []
    )

    result = validate_toorder_orders(
        account_list, store_info_per_account, target_date
    )

    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    gap_stores = result.get("toorder_gap_stores", [])

    # 요약 헤더
    summary_lines = [
        f"토더 교차검증 [{target_date}]: "
        f"비교 {compared}개 매장 / "
        f"{'✅전체일치' if matched else f'❌불일치 {len(mismatched)}개'}"
    ]
    # 불일치 매장 상세
    for store, info in store_results.items():
        if not info.get("toorder_gap"):
            if not info["matched"]:
                retry_mark = " (재수집후)" if store in retried else ""
                summary_lines.append(
                    f"  [{store}] ToOrder={info['toorder']:,} / 배민={info['baemin']:,}{retry_mark}"
                )
    # ToOrder 갭 매장 (계정연결 문제) 별도 표기
    if gap_stores:
        summary_lines.append(f"⚠️ ToOrder 갭 의심(계정연결?): {', '.join(gap_stores)}")
        for store in gap_stores:
            info = store_results.get(store, {})
            summary_lines.append(
                f"  [{store}] ToOrder=0 / 배민={info.get('baemin', 0):,}"
            )
    summary = "\n".join(summary_lines)
    logger.info(summary)

    # 비교 매장이 0개면 검증 불가(예: ToOrder CSV 없음) → 허위 '불일치' 알림 보내지 않음
    if compared > 0 and (not matched or gap_stores):
        _send_alert(subject=f"[배민↔토더 불일치] {target_date}", body=summary)

    return summary


_NOTIFY_TASK_ID = "notify_collection_result"
_CORE_TASK_IDS = {
    "load_accounts",
    "collect_all",
    "retry_failed",
}
_VALIDATION_TASK_IDS = {
    "validate_orders",
    "validate_ad_funnel",
    "validate_toorder",
}
_PROBLEM_LOG_PATTERNS = (
    "ERROR",
    "WARNING",
    "Traceback",
    "Connection refused",
    "RemoteDisconnected",
    "Remote end closed",
    "Max retries exceeded",
    "invalid session",
    "chrome not reachable",
    "tab crashed",
    "store_fail=",
)
_EMPTY_SIGNAL_PATTERNS = (
    'DOM={"tr":0',
    "데이터 없음",
    "테이블 빠른 빈값",
    "테이블 로드 타임아웃",
)


_BENIGN_WARNING_PATTERNS = (
    "DeprecationWarning: This process",
    "ToOrder CSV 없음",
)

_RECOVERED_WARNING_PATTERNS = (
    "dashboard not ready",
    "지표 추출 오류",
)


def _safe_text(value: Any, limit: int = 500) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", "").strip()
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _count_failed_items(failed: dict) -> int:
    total = 0
    for key in ("accounts", "stores", "orders", "ads"):
        value = failed.get(key) or []
        try:
            total += len(value)
        except TypeError:
            total += 1
    return total


def _task_duration_seconds(task_instance) -> float | None:
    start = getattr(task_instance, "start_date", None)
    end = getattr(task_instance, "end_date", None)
    if not start or not end:
        return None
    try:
        return round((end - start).total_seconds(), 1)
    except Exception:
        return None


def _task_log_candidates(task_instance) -> list[Path]:
    base = Path("logs") / f"dag_id={task_instance.dag_id}"
    task_id = task_instance.task_id
    candidates = []
    try:
        run_id = getattr(task_instance, "run_id", None)
        try_number = getattr(task_instance, "try_number", None)
        if run_id and try_number:
            candidates.append(base / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={try_number}.log")
        candidates.extend(base.glob(f"run_id=*/task_id={task_id}/attempt=*.log"))
    except Exception:
        return []
    return sorted({p for p in candidates if p.exists()}, key=lambda p: p.stat().st_mtime, reverse=True)


def _is_empty_signal_line(line: str) -> bool:
    return any(pattern in line for pattern in _EMPTY_SIGNAL_PATTERNS) or "ToOrder CSV 없음" in line


def _is_recovered_warning_line(line: str) -> bool:
    return any(pattern in line for pattern in _RECOVERED_WARNING_PATTERNS) or any(
        pattern in line
        for pattern in (
            "metric extraction failed",
            "ad funnel extraction incomplete",
            "Metric extraction failed, retrying browser",
            "Metric extraction failed after retry, skipping",
        )
    )


def _is_zero_store_fail_line(line: str) -> bool:
    return re.search(r"\bstore_fail\s*=\s*0\b", line) is not None


def _is_benign_problem_line(line: str) -> bool:
    return _is_zero_store_fail_line(line) or _is_empty_signal_line(line) or _is_recovered_warning_line(line) or any(
        pattern in line for pattern in _BENIGN_WARNING_PATTERNS
    )


def _has_validation_issue(validation_texts: list[str]) -> bool:
    text = "\n".join((value or "").lower() for value in validation_texts)
    if any(token in text for token in ("mismatch", "still_empty")):
        return True

    nonzero_patterns = (
        r"불일치\s*[1-9]\d*",
        r"미확인\s*[1-9]\d*",
        r"잔존\s*[1-9]\d*",
        r"빈값\s*[1-9]\d*건",
    )
    return any(re.search(pattern, text) for pattern in nonzero_patterns)


def _extract_log_signals(task_instance, max_lines: int = 8) -> tuple[list[str], int, list[str]]:
    problem_lines: list[str] = []
    recovered_lines: list[str] = []
    empty_signal_count = 0
    for path in _task_log_candidates(task_instance)[:1]:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for line in lines[-500:]:
            if _is_empty_signal_line(line):
                empty_signal_count += 1
            if any(pattern in line for pattern in _PROBLEM_LOG_PATTERNS):
                compact = line.strip()
                if compact and not _is_benign_problem_line(compact):
                    problem_lines.append(compact[-260:])
                elif compact and _is_recovered_warning_line(compact):
                    recovered_lines.append(compact[-260:])
        break
    return problem_lines[-max_lines:], empty_signal_count, recovered_lines[-max_lines:]


def _build_collection_notification_legacy_v1(context) -> tuple[str, str, bool]:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date") or getattr(ti, "execution_date", None)
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    run_id = getattr(dag_run, "run_id", getattr(ti, "run_id", "?"))
    execution_time = (
        logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
        if hasattr(logical_date, "in_timezone")
        else str(logical_date or "?")
    )

    task_instances = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        task_instances = [
            t for t in dag_run.get_task_instances()
            if getattr(t, "task_id", None) != _NOTIFY_TASK_ID
        ]
    task_by_id = {t.task_id: t for t in task_instances}

    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}
    validation = ti.xcom_pull(task_ids="collect_all", key="validation") or []
    returns = {
        task_id: ti.xcom_pull(task_ids=task_id, key="return_value")
        for task_id in [
            "load_accounts",
            "collect_all",
            "retry_failed",
            "validate_orders",
            "validate_ad_funnel",
            "validate_toorder",
        ]
    }

    hard_failures = [
        t for t in task_instances
        if getattr(t, "state", None) in {"failed", "upstream_failed"}
        and getattr(t, "task_id", None) in (_CORE_TASK_IDS | _VALIDATION_TASK_IDS)
    ]
    mismatches = [v for v in validation if isinstance(v, dict) and v.get("matched") is False]

    validation_text = "\n".join(
        _safe_text(returns.get(task_id), 300)
        for task_id in ("validate_orders", "validate_ad_funnel", "validate_toorder")
        if returns.get(task_id)
    )
    validation_issue = any(word in validation_text for word in ("불일치", "잔존", "갭", "mismatch", "still_empty"))

    if hard_failures:
        status = "실패"
    elif retry_count or mismatches or validation_issue:
        status = "부분실패"
    else:
        status = "성공"

    lines = [
        f"[배민 수집 결과] {status}",
        f"DAG: {dag_id}",
        f"Run: {run_id}",
        f"실행시각(KST): {execution_time}",
        f"target_date: {target_date}",
        "",
        "[수집 요약]",
    ]
    for task_id in ("load_accounts", "collect_all", "retry_failed"):
        task = task_by_id.get(task_id)
        state = getattr(task, "state", "?") if task else "?"
        duration = _task_duration_seconds(task) if task else None
        duration_text = f", {duration}s" if duration is not None else ""
        lines.append(f"- {task_id}: {state}{duration_text} | {_safe_text(returns.get(task_id), 220)}")

    if failed:
        lines.append(
            "retry 대상: "
            f"accounts={len(failed.get('accounts') or [])}, "
            f"stores={len(failed.get('stores') or [])}, "
            f"orders={len(failed.get('orders') or [])}, "
            f"ads={len(failed.get('ads') or [])}"
        )
    if mismatches:
        lines.append(f"orders 검증 불일치: {len(mismatches)}건")

    if validation_text:
        lines.extend(["", "[검증 요약]", validation_text[:1200]])

    problem_lines: list[str] = []
    empty_signal_count = 0
    for task in task_instances:
        task_problems, task_empty_count = _extract_log_signals(task)
        empty_signal_count += task_empty_count
        if task_problems:
            problem_lines.append(f"- {task.task_id}:")
            problem_lines.extend(f"  {line}" for line in task_problems)

    if empty_signal_count:
        lines.extend(["", f"[정상 빈값 신호] 우가클/빈 테이블 로그 {empty_signal_count}건"])
    if problem_lines:
        lines.extend(["", "[문제 로그]", *problem_lines[:24]])

    if hard_failures:
        lines.extend(["", "[실패 task 로그 URL]"])
        for task in hard_failures:
            lines.append(f"- {task.task_id}: {getattr(task, 'log_url', '?')}")

    body = "\n".join(lines)
    subject = f"[배민 수집 결과] {status} / {target_date}"
    return subject, body[:6000], status != "성공"


def _build_collection_notification(context) -> tuple[str, str, str, bool]:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date") or getattr(ti, "execution_date", None)
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    run_id = getattr(dag_run, "run_id", getattr(ti, "run_id", "?"))
    execution_time = (
        logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
        if hasattr(logical_date, "in_timezone")
        else str(logical_date or "?")
    )

    task_instances = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        task_instances = [t for t in dag_run.get_task_instances() if getattr(t, "task_id", None) != _NOTIFY_TASK_ID]
    task_by_id = {t.task_id: t for t in task_instances}

    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}
    validation = ti.xcom_pull(task_ids="collect_all", key="validation") or []
    returns = {
        task_id: ti.xcom_pull(task_ids=task_id, key="return_value")
        for task_id in (
            "load_accounts",
            "collect_all",
            "retry_failed",
            "validate_orders",
            "validate_ad_funnel",
            "validate_toorder",
        )
    }

    hard_failures = [
        t for t in task_instances
        if getattr(t, "state", None) in {"failed", "upstream_failed"}
        and getattr(t, "task_id", None) in (_CORE_TASK_IDS | _VALIDATION_TASK_IDS)
    ]
    mismatches = [v for v in validation if isinstance(v, dict) and v.get("matched") is False]
    validation_texts = [
        _safe_text(returns.get(task_id), 500)
        for task_id in ("validate_orders", "validate_ad_funnel", "validate_toorder")
        if returns.get(task_id)
    ]
    validation_issue = _has_validation_issue(validation_texts)

    task_rows: list[tuple[str, str, str, str]] = []
    lines = [
        "[배민 수집 결과] {status}",
        f"DAG: {dag_id}",
        f"Run: {run_id}",
        f"실행시각(KST): {execution_time}",
        f"target_date: {target_date}",
        "",
        "[수집 요약]",
    ]
    for task_id in (
        "load_accounts",
        "collect_all",
        "retry_failed",
        "validate_orders",
        "validate_ad_funnel",
        "validate_toorder",
    ):
        task = task_by_id.get(task_id)
        state = getattr(task, "state", "?") if task else "?"
        duration = _task_duration_seconds(task) if task else None
        duration_text = f"{duration}s" if duration is not None else "-"
        summary = _safe_text(returns.get(task_id), 240)
        lines.append(f"- {task_id}: {state}, {duration_text} | {summary}")
        task_rows.append((task_id, state, duration_text, summary))

    if failed:
        lines.append(
            "retry 대상 "
            f"accounts={len(failed.get('accounts') or [])}, "
            f"stores={len(failed.get('stores') or [])}, "
            f"orders={len(failed.get('orders') or [])}, "
            f"ads={len(failed.get('ads') or [])}"
        )
    if mismatches:
        lines.append(f"orders 검증 불일치 {len(mismatches)}건")

    problem_lines: list[str] = []
    recovered_lines: list[str] = []
    empty_signal_count = 0
    for task in task_instances:
        task_problems, task_empty_count, task_recovered = _extract_log_signals(task)
        empty_signal_count += task_empty_count
        if task_problems:
            problem_lines.append(f"- {task.task_id}:")
            problem_lines.extend(f"  {line}" for line in task_problems)
        if task_recovered:
            recovered_lines.append(f"- {task.task_id}:")
            recovered_lines.extend(f"  {line}" for line in task_recovered)

    if hard_failures:
        status = "실패"
    elif problem_lines or mismatches or validation_issue:
        status = "부분성공"
    else:
        status = "성공"
    lines[0] = f"[배민 수집 결과] {status}"

    if validation_texts:
        lines.extend(["", "[검증 요약]", *validation_texts])
    if empty_signal_count:
        lines.extend(["", f"[정상 빈값 신호] 우가클/빈 테이블 로그 {empty_signal_count}건"])
    if recovered_lines:
        lines.extend(["", "[복구된 경고]", *recovered_lines[:24]])
    if problem_lines:
        lines.extend(["", "[문제 로그]", *problem_lines[:24]])
    if hard_failures:
        lines.extend(["", "[실패 task 로그 URL]"])
        for task in hard_failures:
            lines.append(f"- {task.task_id}: {getattr(task, 'log_url', '?')}")

    body = "\n".join(lines)[:6000]
    subject = f"[배민 수집 결과] {status} / {target_date}"

    badge_color = "#1f8b4c" if status == "성공" else "#d97706" if status == "부분성공" else "#c0392b"
    problem_html = "".join(f"<li>{html.escape(line)}</li>" for line in problem_lines[:24]) or "<li>없음</li>"
    recovered_html = "".join(f"<li>{html.escape(line)}</li>" for line in recovered_lines[:24]) or "<li>없음</li>"
    validation_html = "".join(f"<li>{html.escape(text)}</li>" for text in validation_texts) or "<li>없음</li>"
    html_rows = "".join(
        f"<tr><td>{html.escape(task_id)}</td><td>{html.escape(state)}</td><td>{html.escape(duration)}</td><td>{html.escape(summary)}</td></tr>"
        for task_id, state, duration, summary in task_rows
    )
    html_body = f"""
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family:'Malgun Gothic',Arial,sans-serif;background:#f4f6f8;padding:24px;color:#1f2937;">
      <div style="max-width:960px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:16px;overflow:hidden;">
        <div style="background:{badge_color};color:#fff;padding:20px 24px;">
          <h2 style="margin:0 0 6px 0;">배민 수집 결과: {html.escape(status)}</h2>
          <div style="font-size:14px;opacity:0.95;">{html.escape(target_date)} / {html.escape(run_id)}</div>
        </div>
        <div style="padding:24px;">
          <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
            <tr><td style="padding:8px 0;font-weight:700;">DAG</td><td>{html.escape(dag_id)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">실행시각</td><td>{html.escape(execution_time)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">Retry 대상</td><td>accounts={len(failed.get('accounts') or [])}, stores={len(failed.get('stores') or [])}, orders={len(failed.get('orders') or [])}, ads={len(failed.get('ads') or [])}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">정상 빈값 신호</td><td>{empty_signal_count}건</td></tr>
          </table>
          <h3 style="margin:20px 0 8px 0;">Task 요약</h3>
          <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
              <tr style="background:#111827;color:#fff;">
                <th style="padding:10px;text-align:left;">Task</th>
                <th style="padding:10px;text-align:left;">상태</th>
                <th style="padding:10px;text-align:left;">소요</th>
                <th style="padding:10px;text-align:left;">요약</th>
              </tr>
            </thead>
            <tbody>{html_rows}</tbody>
          </table>
          <h3 style="margin:20px 0 8px 0;">검증 요약</h3>
          <ul>{validation_html}</ul>
          <h3 style="margin:20px 0 8px 0;">복구된 경고</h3>
          <ul>{recovered_html}</ul>
          <h3 style="margin:20px 0 8px 0;">문제 로그</h3>
          <ul>{problem_html}</ul>
        </div>
      </div>
    </body>
    </html>
    """
    return subject, body, html_body, status != "성공"


def notify_collection_result(**context) -> str:
    subject, body, html_body, should_email = _build_collection_notification(context)
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    if should_email:
        try:
            _send_alert(subject=subject, body=body, html_content=html_body)
        except Exception as exc:
            logger.warning("Email 결과 알림 실패(무시): %s", exc)
    return subject


def collect_all(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    profile = resolve_stability_profile(conf.get("stability_profile"))
    wait_sec = random.uniform(*profile["initial_stagger_range"])
    logger.info("수집 시작 전 계정 지터 대기 %.0f초", wait_sec)
    time.sleep(wait_sec)

    target_date = conf.get("target_date")
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    # 백필 날짜 목록 계산 (ORDERS_BACKFILL_DAYS 설정 & conf 미지정 시)
    backfill_dates: list[str] = []
    if target_date is None and ORDERS_BACKFILL_DAYS is not None:
        yesterday = pendulum.yesterday(KST)
        backfill_dates = [
            yesterday.subtract(days=i).format("YYYY-MM-DD")
            for i in range(ORDERS_BACKFILL_DAYS)
        ]
        logger.info(
            "주문 백필 모드: %d일치 (%s ~ %s)",
            len(backfill_dates),
            backfill_dates[-1],
            backfill_dates[0],
        )

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

    # 백필: 추가 날짜들에 대해 orders만 재수집
    backfill_summary = ""
    if backfill_dates and store_info_per_account:
        from modules.transform.pipelines.db.DB_Beamin_04_orders import (
            collect_orders_for_account as _orders_fn,
        )
        pw_map = {a["account_id"]: a["password"] for a in account_list}
        lines = []
        for bdate in backfill_dates:
            ok, fail = 0, 0
            for item in store_info_per_account:
                acc_id = item["account_id"]
                stores = item.get("stores", [])
                pw = pw_map.get(acc_id, "")
                if not pw or not stores:
                    continue
                try:
                    res = _orders_fn(acc_id, pw, stores, target_date=bdate)
                    if res.get("failed"):
                        fail += len(res["failed"])
                    else:
                        ok += 1
                except Exception as exc:
                    logger.warning("백필 orders 실패 [%s / %s]: %s", acc_id, bdate, exc)
                    fail += 1
            lines.append(f"{bdate}: 성공 {ok} / 실패 {fail}")
        backfill_summary = " | ".join(lines)
        logger.info("백필 완료: %s", backfill_summary)

    summary = result["summary"]
    if backfill_summary:
        summary += f"\n[백필] {backfill_summary}"
    return summary


def _send_alert(subject: str, body: str, html_content: str | None = None) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html

    try:
        send_email(
            subject=subject,
            html_content=html_content or text_to_html(body),
            to_emails=_ALERT_EMAILS,
        )
        logger.info("알림 메일 발송 완료: %s", _ALERT_EMAILS)
    except Exception as exc:
        logger.error("알림 메일 발송 실패: %s", exc)


def validate_orders(**context) -> str:
    validation = context["ti"].xcom_pull(task_ids="collect_all", key="validation") or []
    if not validation:
        logger.info("orders 검증 결과 없음")
        return "검증 없음"

    mismatches = [v for v in validation if v.get("matched") is False]
    matched = [v for v in validation if v.get("matched") is True]
    unknown = [v for v in validation if v.get("matched") is None]

    lines = [
        f"orders 검증 총 {len(validation)}건"
        f"(일치 {len(matched)}, 불일치 {len(mismatches)}, 미확인 {len(unknown)})"
    ]
    for v in mismatches:
        lines.append(
            f"  - {v.get('store', '?')} [{v.get('status', '?')}] "
            f"수집={v.get('actual_count')}건/{v.get('actual_amount', 0):,}원 "
            f"기대={v.get('expected_count')}건/{v.get('expected_amount', 0):,}원 "
            f"(재시도 {v.get('retried', 0)}회)"
        )
    summary = "\n".join(lines)
    logger.info(summary)
    if mismatches:
        send_telegram(summary)
    return summary


def validate_ad_funnel(**context) -> str:
    from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    ad_stores = context["ti"].xcom_pull(task_ids="collect_all", key="ad_stores") or []
    if not ad_stores:
        logger.info("ad_funnel 대상 없음")
        return "대상 없음"

    result = _validate_and_retry_ad_funnel(ad_stores, target_date)
    empty = result["empty_stores"]
    still = result["still_empty"]

    lines = [
        f"ad_funnel 빈값 검증: 총 {len(ad_stores)}매장 / 빈값 {len(empty)}건 / 재수집 후 잔존 {len(still)}건"
    ]
    for item in still:
        lines.append(f"  - {item.get('store', '?')} 재수집 후에도 빈값")
    summary = "\n".join(lines)
    logger.info(summary)
    if still:
        send_telegram(summary)
    return summary


def validate_toorder(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list") or []
    store_info_per_account = context["ti"].xcom_pull(task_ids="collect_all", key="store_info_per_account") or []

    result = validate_toorder_orders(account_list, store_info_per_account, target_date)

    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    gap_stores = result.get("toorder_gap_stores", [])

    lines = [
        f"토더 교차검증[{target_date}]: 비교 {compared}개 매장 / "
        f"{'완전일치' if matched else f'불일치 {len(mismatched)}개'}"
    ]
    for store, info in store_results.items():
        if not info.get("toorder_gap") and not info.get("matched"):
            retry_mark = " (재수집후)" if store in retried else ""
            lines.append(f"  - {store} ToOrder={info['toorder']:,} / 배민={info['baemin']:,}{retry_mark}")
    if gap_stores:
        lines.append(f"ToOrder 값 미수집(계정연결?): {', '.join(gap_stores)}")
    summary = "\n".join(lines)
    logger.info(summary)
    if compared > 0 and (not matched or gap_stores):
        send_telegram(summary)
    if mismatched:
        _send_alert(
            subject=f"[배민 교차검증 불일치] {target_date} {len(mismatched)}개 매장",
            body=summary,
        )
    return summary


def _build_collection_notification_legacy_v4(context) -> tuple[str, str, str, bool]:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date") or getattr(ti, "execution_date", None)
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    run_id = getattr(dag_run, "run_id", getattr(ti, "run_id", "?"))
    execution_time = (
        logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
        if hasattr(logical_date, "in_timezone")
        else str(logical_date or "?")
    )

    task_instances = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        task_instances = [t for t in dag_run.get_task_instances() if getattr(t, "task_id", None) != _NOTIFY_TASK_ID]
    task_by_id = {t.task_id: t for t in task_instances}

    failed = ti.xcom_pull(task_ids="collect_all", key="failed") or {}
    validation = ti.xcom_pull(task_ids="collect_all", key="validation") or []
    returns = {
        task_id: ti.xcom_pull(task_ids=task_id, key="return_value")
        for task_id in [
            "load_accounts",
            "collect_all",
            "retry_failed",
            "validate_orders",
            "validate_ad_funnel",
            "validate_toorder",
        ]
    }

    hard_failures = [
        t for t in task_instances
        if getattr(t, "state", None) in {"failed", "upstream_failed"}
        and getattr(t, "task_id", None) in (_CORE_TASK_IDS | _VALIDATION_TASK_IDS)
    ]
    retry_count = _count_failed_items(failed)
    mismatches = [v for v in validation if isinstance(v, dict) and v.get("matched") is False]
    validation_texts = [
        _safe_text(returns.get(task_id), 500)
        for task_id in ("validate_orders", "validate_ad_funnel", "validate_toorder")
        if returns.get(task_id)
    ]
    validation_issue = _has_validation_issue(validation_texts)

    if hard_failures:
        status = "실패"
    elif retry_count or mismatches or validation_issue:
        status = "부분성공"
    else:
        status = "성공"

    lines = [
        f"[배민 수집 결과] {status}",
        f"DAG: {dag_id}",
        f"Run: {run_id}",
        f"실행시각(KST): {execution_time}",
        f"target_date: {target_date}",
        "",
        "[수집 요약]",
    ]

    task_rows: list[tuple[str, str, str, str]] = []
    for task_id in ("load_accounts", "collect_all", "retry_failed", "validate_orders", "validate_ad_funnel", "validate_toorder"):
        task = task_by_id.get(task_id)
        state = getattr(task, "state", "?") if task else "?"
        duration = _task_duration_seconds(task) if task else None
        duration_text = f"{duration}s" if duration is not None else "-"
        summary = _safe_text(returns.get(task_id), 240)
        lines.append(f"- {task_id}: {state}, {duration_text} | {summary}")
        task_rows.append((task_id, state, duration_text, summary))

    if failed:
        lines.append(
            "retry 대상 "
            f"accounts={len(failed.get('accounts') or [])}, "
            f"stores={len(failed.get('stores') or [])}, "
            f"orders={len(failed.get('orders') or [])}, "
            f"ads={len(failed.get('ads') or [])}"
        )
    if mismatches:
        lines.append(f"orders 검증 불일치 {len(mismatches)}건")

    problem_lines: list[str] = []
    empty_signal_count = 0
    for task in task_instances:
        task_problems, task_empty_count = _extract_log_signals(task)
        empty_signal_count += task_empty_count
        if task_problems:
            problem_lines.append(f"- {task.task_id}:")
            problem_lines.extend(f"  {line}" for line in task_problems)

    if validation_texts:
        lines.extend(["", "[검증 요약]", *validation_texts])
    if empty_signal_count:
        lines.extend(["", f"[정상 빈값 신호] 우가클/빈 테이블 로그 {empty_signal_count}건"])
    if problem_lines:
        lines.extend(["", "[문제 로그]", *problem_lines[:24]])
    if hard_failures:
        lines.extend(["", "[실패 task 로그 URL]"])
        for task in hard_failures:
            lines.append(f"- {task.task_id}: {getattr(task, 'log_url', '?')}")

    body = "\n".join(lines)[:6000]
    subject = f"[배민 수집 결과] {status} / {target_date}"

    badge_color = "#1f8b4c" if status == "성공" else "#d97706" if status == "부분성공" else "#c0392b"
    problem_html = "".join(f"<li>{html.escape(line)}</li>" for line in problem_lines[:24]) or "<li>없음</li>"
    validation_html = "".join(f"<li>{html.escape(text)}</li>" for text in validation_texts) or "<li>없음</li>"
    html_rows = "".join(
        f"<tr><td>{html.escape(task_id)}</td><td>{html.escape(state)}</td><td>{html.escape(duration)}</td><td>{html.escape(summary)}</td></tr>"
        for task_id, state, duration, summary in task_rows
    )
    html_body = f"""
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family:'Malgun Gothic',Arial,sans-serif;background:#f4f6f8;padding:24px;color:#1f2937;">
      <div style="max-width:960px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:16px;overflow:hidden;">
        <div style="background:{badge_color};color:#fff;padding:20px 24px;">
          <h2 style="margin:0 0 6px 0;">배민 수집 결과: {html.escape(status)}</h2>
          <div style="font-size:14px;opacity:0.95;">{html.escape(target_date)} · {html.escape(run_id)}</div>
        </div>
        <div style="padding:24px;">
          <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
            <tr><td style="padding:8px 0;font-weight:700;">DAG</td><td>{html.escape(dag_id)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">실행시각</td><td>{html.escape(execution_time)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">Retry 대상</td><td>accounts={len(failed.get('accounts') or [])}, stores={len(failed.get('stores') or [])}, orders={len(failed.get('orders') or [])}, ads={len(failed.get('ads') or [])}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">정상 빈값 신호</td><td>{empty_signal_count}건</td></tr>
          </table>
          <h3 style="margin:20px 0 8px 0;">Task 요약</h3>
          <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
              <tr style="background:#111827;color:#fff;">
                <th style="padding:10px;text-align:left;">Task</th>
                <th style="padding:10px;text-align:left;">상태</th>
                <th style="padding:10px;text-align:left;">소요</th>
                <th style="padding:10px;text-align:left;">요약</th>
              </tr>
            </thead>
            <tbody>{html_rows}</tbody>
          </table>
          <h3 style="margin:20px 0 8px 0;">검증 요약</h3>
          <ul>{validation_html}</ul>
          <h3 style="margin:20px 0 8px 0;">문제 로그</h3>
          <ul>{problem_html}</ul>
        </div>
      </div>
    </body>
    </html>
    """
    return subject, body, html_body, status != "성공"


def notify_collection_result_legacy_v4(**context) -> str:
    subject, body, html_body, should_email = _build_collection_notification(context)
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    if should_email:
        try:
            _send_alert(subject=subject, body=body, html_content=html_body)
        except Exception as exc:
            logger.warning("Email 결과 알림 실패(무시): %s", exc)
    return subject


def start_dashboard(**context) -> str:
    import subprocess
    import time
    import urllib.request

    dashboard_url = "http://localhost:8787/db-beamin-macro"
    health_url = "http://localhost:8787/health"

    # 실행 여부 확인
    running = False
    try:
        urllib.request.urlopen(health_url, timeout=3)
        running = True
        logger.info("대시보드 서버 이미 실행 중")
    except Exception:
        pass

    if not running:
        logger.info("대시보드 서버 시작 중...")
        try:
            subprocess.Popen(
                ["python", "-m", "modules.transform.dashboard.server"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(3)
        except Exception as e:
            logger.warning("대시보드 서버 시작 실패(무시): %s", e)

    # 브라우저 열기 (best-effort — WSL2/Docker 환경에서 cmd.exe 경유 시도)
    opened = False
    for cmd in (
        ["cmd.exe", "/c", "start", dashboard_url],
        ["/mnt/c/Windows/System32/cmd.exe", "/c", "start", dashboard_url],
    ):
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logger.info("브라우저 열기 시도: %s", dashboard_url)
            opened = True
            break
        except Exception:
            continue
    if not opened:
        try:
            import webbrowser
            webbrowser.open(dashboard_url)
            opened = True
        except Exception:
            pass
    if not opened:
        logger.info("브라우저 자동 열기 불가 — 수동 접속: %s", dashboard_url)

    return dashboard_url


with DAG(
    dag_id=dag_id,
    schedule=SMD_BAEMIN_COLLECT_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "baemin", "crawl"],
) as dag:

    t_dash = PythonOperator(
        task_id="start_dashboard",
        python_callable=start_dashboard,
        execution_timeout=timedelta(minutes=2),
    )

    t1 = PythonOperator(
        task_id="load_accounts",
        python_callable=load_accounts,
    )

    t2 = PythonOperator(
        task_id="collect_all",
        python_callable=collect_all,
        execution_timeout=timedelta(minutes=300),
    )

    t3 = PythonOperator(
        task_id="retry_failed",
        python_callable=retry_failed,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=120),
    )

    t4 = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        trigger_rule="all_done",
    )

    t5 = PythonOperator(
        task_id="validate_ad_funnel",
        python_callable=validate_ad_funnel,
        trigger_rule="all_done",
    )

    t6 = PythonOperator(
        task_id="validate_toorder",
        python_callable=validate_toorder,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
    )

    t7 = PythonOperator(
        task_id=_NOTIFY_TASK_ID,
        python_callable=notify_collection_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_dash >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7
