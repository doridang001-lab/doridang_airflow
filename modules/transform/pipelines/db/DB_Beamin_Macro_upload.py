"""Central Baemin macro upload and validation tasks."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import ingest_baemin_upload_inbox
from modules.transform.pipelines.db.DB_Beamin_retry import build_retry_conf, count_failed_items, retry_needed
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
SCHEDULED_DEFAULT_STABILITY_PROFILE = "safe_daily"
_ALERT_EMAILS = [MAIL_CMJ_PM]
MANUAL_BAEMIN_ORDERS_DIR = COLLECT_DB / "영업관리부_수집"


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def _save_validate_log(target_date: str, section: str, text: str) -> None:
    try:
        log_dir = ANALYTICS_DB / "baemin_validate_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        ymd = target_date.replace("-", "")
        log_path = log_dir / f"validate_{ymd}.md"
        header = f"# 배민 검증 리포트 — {target_date}\n\n" if not log_path.exists() else ""
        with log_path.open("a", encoding="utf-8") as f:
            f.write(header)
            f.write(f"## {section}\n")
            f.write(text.strip())
            f.write("\n\n")
    except Exception as exc:
        logger.warning("검증 로그 저장 실패: %s", exc)


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


def _manual_baemin_store_key(raw_store_name: str, fallback_name: str = "") -> str:
    text = str(raw_store_name or fallback_name or "").strip()
    if not text:
        return ""
    text = re.sub(r"\[.*?\]\s*", "", text).strip()
    brand = "나홀로" if "나홀로" in text else ("도리당" if "도리당" in text else "")
    matches = re.findall(r"[가-힣A-Za-z0-9]+(?:점|지점|분점|직영점)", text)
    branch = matches[-1] if matches else (text.split()[-1] if text.split() else text)
    normalized = f"{brand} {branch}".strip() if brand else branch
    normalized_series = normalize_store_names(pd.Series([normalized]))
    branch_series = strip_brand(normalized_series)
    return str(branch_series.iloc[0]).strip()


def _manual_baemin_filename_fallback(csv_path: Path) -> str:
    stem = csv_path.stem
    stem = re.sub(r"^baemin_orders_", "", stem)
    stem = re.sub(r"_unknown_\d{8}$", "", stem)
    return stem.replace("_", " ").strip()


def _collect_manual_baemin_orders(target_date: str, base_dir: Path) -> tuple[dict[str, dict], list[str]]:
    date_prefix = target_date.replace("-", ". ") + "."
    csv_paths = sorted(base_dir.glob("baemin_orders_*.csv"))
    store_frames: dict[str, list[pd.DataFrame]] = {}
    used_files: list[str] = []

    for csv_path in csv_paths:
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        except Exception as exc:
            logger.warning("manual baemin CSV read failed: %s / %s", csv_path, exc)
            continue
        if df.empty:
            continue
        fallback_name = _manual_baemin_filename_fallback(csv_path)
        raw_store_name = ""
        if "store_name" in df.columns and not df["store_name"].dropna().empty:
            raw_store_name = str(df["store_name"].dropna().astype(str).iloc[0])
        store_key = _manual_baemin_store_key(raw_store_name, fallback_name)
        if not store_key:
            logger.warning("manual baemin store parse failed: %s / raw=%s", csv_path.name, raw_store_name)
            continue
        required = {"주문상태", "주문번호", "주문시각", "결제금액"}
        if not required.issubset(df.columns):
            logger.warning("manual baemin CSV missing required columns: %s", csv_path.name)
            continue
        filtered = df[
            (df["주문상태"].astype(str) == "배달완료")
            & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
            & df["결제금액"].astype(str).str.strip().ne("")
        ][["주문번호", "결제금액"]].copy()
        if filtered.empty:
            continue
        store_frames.setdefault(store_key, []).append(filtered)
        used_files.append(csv_path.name)

    result: dict[str, dict] = {}
    for store_key, frames in store_frames.items():
        combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["주문번호"])
        amount = int(
            combined["결제금액"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum()
        )
        result[store_key] = {
            "amount": amount,
            "orders": int(combined["주문번호"].nunique()),
        }
    return result, used_files


def _target_date(context) -> str:
    ti = context["ti"]
    target_date = ti.xcom_pull(task_ids="ingest", key="target_date")
    if target_date:
        return target_date
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")


def ingest(**context) -> str:
    result = ingest_baemin_upload_inbox(**context)
    meta = result.get("meta") or {}
    for key, value in meta.items():
        context["ti"].xcom_push(key=key, value=value)
    context["ti"].xcom_push(key="ingest_stats", value=result.get("stats") or {})
    return result.get("summary", "upload inbox 적재 완료")


def precheck_manual(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = conf.get("target_date") or _target_date(context)
    manual_dir = Path(str(conf.get("manual_baemin_dir") or MANUAL_BAEMIN_ORDERS_DIR))

    if not manual_dir.exists():
        msg = f"manual baemin precheck skip: dir missing ({manual_dir})"
        logger.info(msg)
        context["ti"].xcom_push(key="manual_precheck_summary", value={"used": False, "reason": msg})
        return msg

    from modules.transform.pipelines.db.DB_Beamin_Macro_validate import _toorder_baemin_by_store

    manual_by_store, used_files = _collect_manual_baemin_orders(target_date, manual_dir)
    if not manual_by_store:
        msg = f"manual baemin precheck skip: no usable files for {target_date} in {manual_dir}"
        logger.info(msg)
        context["ti"].xcom_push(
            key="manual_precheck_summary",
            value={"used": False, "reason": msg, "files": used_files},
        )
        return msg

    toorder_by_store = _toorder_baemin_by_store(target_date)
    compare_stores = sorted(set(manual_by_store) & set(toorder_by_store))
    lines = [
        f"manual baemin precheck [{target_date}] dir={manual_dir}",
        f"files={len(used_files)} stores={len(manual_by_store)} compare={len(compare_stores)}",
    ]
    for store in sorted(manual_by_store):
        manual_amount = manual_by_store[store]["amount"]
        manual_orders = manual_by_store[store]["orders"]
        if store in toorder_by_store:
            toorder_amount = int(toorder_by_store[store])
            diff = toorder_amount - manual_amount
            lines.append(
                f"  - {store} manual={manual_amount:,} ({manual_orders} orders) / "
                f"ToOrder={toorder_amount:,} / diff={diff:,}"
            )
        else:
            lines.append(f"  - {store} manual={manual_amount:,} ({manual_orders} orders) / ToOrder=<missing>")
    summary = "\n".join(lines)
    logger.info(summary)
    context["ti"].xcom_push(
        key="manual_precheck_summary",
        value={
            "used": True,
            "target_date": target_date,
            "dir": str(manual_dir),
            "files": used_files,
            "stores": manual_by_store,
            "compared_stores": compare_stores,
            "summary": summary,
        },
    )
    return summary


def validate_orders(**context) -> str:
    target_date = _target_date(context)
    validation = context["ti"].xcom_pull(task_ids="ingest", key="validation") or []
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
        _send_alert(subject=f"[배민 orders 불일치] {len(mismatches)}건", body=summary)
    _save_validate_log(target_date, "orders 검증", summary)
    return summary


def validate_ad_funnel(**context) -> str:
    target_date = _target_date(context)
    ad_stores = context["ti"].xcom_pull(task_ids="ingest", key="ad_stores") or []
    if not ad_stores:
        logger.info("ad_funnel 대상 없음")
        context["ti"].xcom_push(
            key="ad_funnel_result",
            value={"empty_stores": [], "retried": [], "still_empty": []},
        )
        return "대상 없음"

    result = _validate_and_retry_ad_funnel(ad_stores, target_date)
    context["ti"].xcom_push(key="ad_funnel_result", value=result)
    empty = result["empty_stores"]
    still = result["still_empty"]
    lines = [f"ad_funnel 빈값 검증: 총 {len(ad_stores)}매장 / 빈값 {len(empty)}건 / 재수집 후 잔존 {len(still)}건"]
    for item in still:
        lines.append(f"  - {item.get('store', '?')} 재수집 후에도 빈값")
    summary = "\n".join(lines)
    logger.info(summary)
    if still:
        _send_alert(subject=f"[배민 ad_funnel 빈값] {len(still)}건 잔존", body=summary)
    _save_validate_log(target_date, "ad_funnel 빈값 점검", summary)
    return summary


def validate_toorder(**context) -> str:
    target_date = _target_date(context)
    ti = context["ti"]
    account_list = ti.xcom_pull(task_ids="ingest", key="account_list") or []
    store_info_per_account = ti.xcom_pull(task_ids="ingest", key="store_info_per_account") or []
    manual_precheck = ti.xcom_pull(task_ids="precheck_manual_baemin_orders", key="manual_precheck_summary") or {}

    result = validate_toorder_orders(account_list, store_info_per_account, target_date)
    expected_accounts = len(account_list)
    observed_accounts = len(store_info_per_account)
    blind = compared_blind = False
    if expected_accounts and observed_accounts == 0:
        blind = compared_blind = True
    elif expected_accounts and observed_accounts < expected_accounts:
        blind = True
    if blind:
        result["blind"] = True
        result["expected_accounts"] = expected_accounts
        result["observed_accounts"] = observed_accounts
    ti.xcom_push(key="toorder_result", value=result)

    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    gap_stores = result.get("toorder_gap_stores", [])
    missing_brand_stores = result.get("missing_brand_stores", [])
    lines = [
        f"ToOrder 교차검증: 비교 {compared}건 / 일치={matched} / 재시도={len(retried)} / 불일치={len(mismatched)}"
    ]
    if blind:
        lines.append(f"수집 관측 누락: expected_accounts={expected_accounts}, observed_accounts={observed_accounts}")
    if compared_blind:
        lines.append("수집 결과가 0건이라 ToOrder 비교 결과를 신뢰할 수 없음")
    for store, item in store_results.items():
        lines.append(
            f"  - {store}: baemin={item.get('baemin', 0):,} / "
            f"toorder={item.get('toorder', 0):,} / "
            f"diff={item.get('toorder', 0) - item.get('baemin', 0):,}"
        )
    for store in missing_brand_stores:
        lines.append(f"  - {store}: brand 매핑 누락")
    if manual_precheck.get("used"):
        lines.append(
            f"수동사전검증: files={len(manual_precheck.get('files', []))} "
            f"stores={len((manual_precheck.get('stores') or {}))} "
            f"compare={len(manual_precheck.get('compared_stores', []))}"
        )
    summary = "\n".join(lines)
    logger.info(summary)
    _save_validate_log(target_date, "ToOrder 교차검증", summary)
    return summary


def _failed_account_ids(failed: dict | None) -> set[str]:
    account_ids: set[str] = set()
    for key in ("accounts", "stores", "orders", "ads"):
        for item in (failed or {}).get(key) or []:
            if not isinstance(item, dict):
                continue
            account = item.get("account") if isinstance(item.get("account"), dict) else item
            account_id = str((account or {}).get("account_id") or item.get("account_id") or "").strip()
            if account_id:
                account_ids.add(account_id)
    return account_ids


def _toorder_snapshot(result: dict | None) -> dict:
    result = result or {}
    store_results: dict[str, dict] = {}
    for store, item in (result.get("store_results") or {}).items():
        item = item or {}
        baemin = int(item.get("baemin") or 0)
        toorder = int(item.get("toorder") or 0)
        store_results[str(store)] = {
            "baemin": baemin,
            "toorder": toorder,
            "matched": bool(item.get("matched")),
            "toorder_gap": bool(item.get("toorder_gap")),
            "brand_issue": item.get("brand_issue"),
        }
    return {
        "compared": int(result.get("compared_count") or 0),
        "store_results": store_results,
        "mismatched_stores": sorted(set(result.get("mismatched_stores") or [])),
        "gap_stores": sorted(set(result.get("toorder_gap_stores") or [])),
        "missing_brand_stores": sorted(set(result.get("missing_brand_stores") or [])),
    }


def build_upload_notification_context(context) -> dict:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    account_list = ti.xcom_pull(task_ids="ingest", key="account_list") or []
    account_ids = {
        str(account.get("account_id") or "").strip()
        for account in account_list
        if isinstance(account, dict) and str(account.get("account_id") or "").strip()
    }
    total_accounts = len(account_ids) if account_ids else len(account_list)
    failed = ti.xcom_pull(task_ids="ingest", key="failed") or {}
    validation = ti.xcom_pull(task_ids="ingest", key="validation") or []
    order_matched = sum(1 for item in validation if isinstance(item, dict) and item.get("matched") is True)
    order_mismatched = sum(1 for item in validation if isinstance(item, dict) and item.get("matched") is False)
    order_unknown = len(validation) - order_matched - order_mismatched
    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result") or {}
    ad_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result") or {}
    hard_failures: list[str] = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        hard_failures = [
            task.task_id
            for task in dag_run.get_task_instances()
            if task.task_id != "notify_upload_result"
            and getattr(task, "state", None) in {"failed", "upstream_failed"}
        ]
    return {
        "source_dag_id": getattr(ti, "dag_id", "DB_Beamin_Macro_Upload_Dags"),
        "source_run_id": getattr(dag_run, "run_id", getattr(ti, "run_id", "?")),
        "target_date": _target_date(context),
        "total_accounts": total_accounts,
        "ingest_stats": ti.xcom_pull(task_ids="ingest", key="ingest_stats") or {},
        "orders": {
            "total": len(validation),
            "matched": order_matched,
            "mismatched": order_mismatched,
            "unknown": order_unknown,
        },
        "ad_funnel": {
            "total": len(ti.xcom_pull(task_ids="ingest", key="ad_stores") or []),
            "still_empty": len(ad_result.get("still_empty") or []),
        },
        "toorder": _toorder_snapshot(toorder_result),
        "residual_failed": failed,
        "hard_failures": hard_failures,
    }


def build_final_notification_message(
    notification_context: dict,
    *,
    final_toorder_result: dict | None = None,
    final_ad_funnel_result: dict | None = None,
    residual_failed: dict | None = None,
    attempt: int = 0,
    max_attempts: int = 3,
    hard_failure: str | None = None,
) -> str:
    root_toorder = notification_context.get("toorder") or {}
    merged_store_results = dict(root_toorder.get("store_results") or {})
    final_toorder = _toorder_snapshot(final_toorder_result) if final_toorder_result is not None else root_toorder
    merged_store_results.update(final_toorder.get("store_results") or {})
    unresolved_stores = {
        store
        for store, item in merged_store_results.items()
        if not (item or {}).get("matched")
    }
    if final_toorder_result is not None:
        unresolved_stores.update(final_toorder.get("gap_stores") or [])
        unresolved_stores.update(final_toorder.get("missing_brand_stores") or [])
    else:
        unresolved_stores.update(root_toorder.get("gap_stores") or [])
        unresolved_stores.update(root_toorder.get("missing_brand_stores") or [])
    compared = max(int(root_toorder.get("compared") or 0), int(final_toorder.get("compared") or 0))
    toorder_matched = max(compared - len(unresolved_stores), 0)

    final_failed = residual_failed if residual_failed is not None else notification_context.get("residual_failed") or {}
    residual_count = count_failed_items(final_failed)
    failed_account_count = len(_failed_account_ids(final_failed))
    total_accounts = int(notification_context.get("total_accounts") or 0)
    unresolved_account_count = failed_account_count or min(residual_count, total_accounts)
    completed_accounts = max(total_accounts - unresolved_account_count, 0)

    root_ad = notification_context.get("ad_funnel") or {}
    final_ad = final_ad_funnel_result or {}
    ad_total = int(root_ad.get("total") or 0)
    ad_still = (
        len(final_ad.get("still_empty") or [])
        if final_ad_funnel_result is not None
        else int(root_ad.get("still_empty") or 0)
    )
    orders = notification_context.get("orders") or {}
    ingest_stats = notification_context.get("ingest_stats") or {}
    hard_failures = list(notification_context.get("hard_failures") or [])
    if hard_failure:
        hard_failures.append(hard_failure)

    has_partial = bool(
        residual_count
        or int(orders.get("mismatched") or 0)
        or int(orders.get("unknown") or 0)
        or ad_still
        or unresolved_stores
        or int(ingest_stats.get("failed") or 0)
    )
    status = "실패" if hard_failures else "부분완료" if has_partial else "완료"
    lines = [
        f"[배민 최종 결과] {status}",
        f"target_date: {notification_context.get('target_date') or '?'}",
        f"대상 계정 {total_accounts} / 수집 완료 {completed_accounts} / 잔여 실패 {residual_count}",
        (
            f"orders 검증 {int(orders.get('total') or 0)} / "
            f"일치 {int(orders.get('matched') or 0)} / "
            f"불일치 {int(orders.get('mismatched') or 0)}"
        ),
        f"ad_funnel {ad_total} / 정상 {max(ad_total - ad_still, 0)} / 잔존 {ad_still}",
        f"ToOrder 비교 {compared} / 일치 {toorder_matched} / 불일치 {len(unresolved_stores)}",
        f"Retry {attempt}/{max_attempts}회" if attempt else "Retry 없음",
    ]
    problems: list[str] = []
    for store in sorted(unresolved_stores):
        item = merged_store_results.get(store) or {}
        baemin = int(item.get("baemin") or 0)
        toorder = int(item.get("toorder") or 0)
        problems.append(
            f"- {store}: 배민={baemin:,} / ToOrder={toorder:,} / diff={toorder - baemin:,}"
        )
    if hard_failures:
        problems.append(f"- 실패 task: {', '.join(sorted(set(hard_failures)))}")
    if problems:
        lines.extend(["", "[미해결]", *problems])
    return "\n".join(lines)[:4000]


def notify_upload_result(**context) -> str:
    ti = context["ti"]
    retry_triggered = bool(ti.xcom_pull(task_ids="trigger_retry_if_needed", key="retry_triggered"))
    notification_context = (
        ti.xcom_pull(task_ids="trigger_retry_if_needed", key="notification_context")
        or build_upload_notification_context(context)
    )
    notification_context = dict(notification_context)
    dag_run = context.get("dag_run")
    if dag_run and hasattr(dag_run, "get_task_instances"):
        current_failures = {
            task.task_id
            for task in dag_run.get_task_instances()
            if task.task_id != "notify_upload_result"
            and getattr(task, "state", None) in {"failed", "upstream_failed"}
        }
        notification_context["hard_failures"] = sorted(
            set(notification_context.get("hard_failures") or []) | current_failures
        )
    if retry_triggered:
        logger.info("최종 Telegram 보류: Retry DAG가 최종 결과를 발송함")
        return "최종 Telegram 보류: Retry 진행 중"
    body = build_final_notification_message(notification_context)
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    return body.splitlines()[0]


def trigger_retry_if_needed(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = _target_date(context)
    failed = ti.xcom_pull(task_ids="ingest", key="failed") or {}
    failed_count = count_failed_items(failed)
    notification_context = build_upload_notification_context(context)
    ti.xcom_push(key="notification_context", value=notification_context)
    if failed_count == 0:
        ti.xcom_push(key="retry_triggered", value=False)
        logger.info("Retry DAG 트리거 스킵: 잔여 실패 없음")
        return "Retry DAG 트리거 스킵: 잔여 실패 없음"

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result")
    if not retry_needed(toorder_result, ad_funnel_result, failed):
        ti.xcom_push(key="retry_triggered", value=False)
        logger.info("Retry DAG 트리거 스킵: 검증상 추가 재시도 불필요")
        return "Retry DAG 트리거 스킵: 추가 재시도 불필요"

    source_run_id = getattr(dag_run, "run_id", context.get("run_id", "manual"))
    retry_conf = build_retry_conf(
        failed=failed,
        target_date=target_date,
        source_dag_id=ti.dag_id,
        source_run_id=source_run_id,
        attempt=1,
        max_attempts=int(conf.get("max_attempts", 3)),
        stability_profile=conf.get("stability_profile") or SCHEDULED_DEFAULT_STABILITY_PROFILE,
    )
    retry_conf["notification_context"] = notification_context
    run_id = f"retry__{target_date.replace('-', '')}__attempt_1__{_safe_run_id_part(str(source_run_id))}"

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    try:
        trigger_dag(
            dag_id="DB_Beamin_Macro_Dags_Retry",
            run_id=run_id,
            conf=retry_conf,
        )
    except DagRunAlreadyExists:
        ti.xcom_push(key="retry_triggered", value=True)
        logger.info("Retry DAG run 이미 존재: %s", run_id)
        return f"Retry DAG run 이미 존재: {run_id}"

    ti.xcom_push(key="retry_triggered", value=True)
    logger.info("Retry DAG 트리거 완료: run_id=%s failed_count=%d", run_id, failed_count)
    return f"Retry DAG 트리거 완료: {run_id}"
