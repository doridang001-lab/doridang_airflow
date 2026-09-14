"""Central Baemin macro upload and validation tasks."""

from __future__ import annotations

from modules.transform.utility.workload import retry_dag_id, route_trigger

import logging
import json
import re
import time
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import (
    _validate_and_retry_ad_funnel,
    filter_ad_funnel_zero_sales_failures,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import (
    _merge_order_amounts,
    _order_amounts,
    store_info_from_account_list,
    validate_toorder_orders,
)
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import (
    BOTTOM_FOLDER_PATTERN,
    DEFAULT_FOLDER_PATTERN,
    count_baemin_upload_inbox_folders,
    ingest_baemin_upload_inbox,
)
from modules.transform.pipelines.db.DB_Beamin_retry import (
    build_retry_conf,
    count_failed_items,
    merge_failed_payloads,
    merge_toorder_notification_context,
    restore_meta_credentials,
    retry_needed,
)
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    count_partial_manual_baemin_order_files,
    count_pending_manual_baemin_order_files,
)
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
SCHEDULED_DEFAULT_STABILITY_PROFILE = "safe_daily"
UPLOAD_INBOX_STALE_HOURS = 12
UPLOAD_STALE_ALERT_THROTTLE_HOURS = 6
_ALERT_EMAILS = [MAIL_CMJ_PM]
MANUAL_BAEMIN_ORDERS_DIR = COLLECT_DB / "영업관리부_수집"
HANDOFF_DIR = LOCAL_DB / "baemin_upload_handoff"
STALE_ALERT_MARKER_DIR = LOCAL_DB / "baemin_upload_stale_alert"
_HANDOFF_KEYS = (
    "target_date",
    "target_dates",
    "orders_only",
    "account_list",
    "validation",
    "ad_stores",
    "store_info_per_account",
    "failed",
    "original_failed",
    "residual_failed",
)


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


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


def _handoff_path_from_context(context) -> Path | None:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    raw = conf.get("handoff_path")
    if not raw and context.get("ti"):
        raw = context["ti"].xcom_pull(task_ids="ingest", key="handoff_path")
    if not raw:
        return None
    return Path(str(raw))


def _cleanup_handoff(context) -> None:
    path = _handoff_path_from_context(context)
    if not path:
        return
    try:
        path.unlink(missing_ok=True)
        logger.info("upload handoff 삭제 완료: %s", path)
    except Exception as exc:
        logger.warning("upload handoff 삭제 실패(무시): %s / %s", path, exc)


def _meta_pull(context, key: str):
    value = context["ti"].xcom_pull(task_ids="ingest", key=key)
    if value is not None:
        return value
    return _load_handoff(context).get(key)


def _orders_only_context(context) -> bool:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if conf.get("orders_only") is True:
        return True
    return bool(_meta_pull(context, "orders_only"))


def _residual_failed_from_meta(context) -> dict:
    residual_failed = _meta_pull(context, "residual_failed")
    if residual_failed is not None:
        return residual_failed or {}
    return _meta_pull(context, "failed") or {}


def _orders_only_failed(failed: dict | None) -> dict:
    data = failed or {}
    return {
        "accounts": data.get("accounts") or [],
        "stores": data.get("stores") or [],
        "orders": data.get("orders") or [],
        "ads": [],
        "stages": [],
    }


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
        required = {"주문상태", "주문번호", "주문시각"}
        if not required.issubset(df.columns):
            logger.warning("manual baemin CSV missing required columns: %s", csv_path.name)
            continue
        filtered = df[
            (df["주문상태"].astype(str) == "배달완료")
            & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
        ]
        # ToOrder와 같은 기준(총결제금액, 없으면 결제금액)으로 집약한다.
        amounts = _order_amounts(filtered)
        if amounts.empty:
            continue
        store_frames.setdefault(store_key, []).append(amounts)
        used_files.append(csv_path.name)

    result: dict[str, dict] = {}
    for store_key, frames in store_frames.items():
        combined = _merge_order_amounts(frames)
        result[store_key] = {
            "amount": int(combined["amount"].sum()),
            "orders": int(len(combined)),
        }
    return result, used_files


def _target_date(context) -> str:
    target_date = _meta_pull(context, "target_date")
    if target_date:
        return target_date
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")


def _as_date_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    out: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            out.add(text)
    return sorted(out)


def _manual_ingest_order_dates(context) -> list[str]:
    ti = context.get("ti")
    if not ti:
        return []
    raw = ti.xcom_pull(task_ids="ingest_manual_baemin_orders", key="return_value")
    if not raw:
        return []
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        logger.warning("수동 배민 ingest XCom 파싱 실패: %s", raw)
        return []
    return _as_date_list((payload or {}).get("order_dates"))


def _target_dates(context) -> list[str]:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    explicit = _as_date_list(conf.get("target_date"))
    if explicit:
        return explicit
    configured = _as_date_list(conf.get("target_dates") or _meta_pull(context, "target_dates"))
    if configured:
        return configured
    manual_dates = _manual_ingest_order_dates(context)
    if manual_dates:
        return manual_dates
    meta_date = _as_date_list(_meta_pull(context, "target_date"))
    if meta_date:
        return meta_date
    return [_target_date(context)]


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


def has_upload_inbox_folders(
    folder_pattern: str = BOTTOM_FOLDER_PATTERN,
    **context,
) -> bool:
    """지정 패턴의 완성된 upload inbox 폴더가 있는지 확인한다."""
    count = count_baemin_upload_inbox_folders(folder_pattern)
    logger.info("upload inbox 감지: pattern=%s folders=%d", folder_pattern, count)
    if count:
        _warn_if_upload_inbox_stale(folder_pattern)
    return bool(count)


def _warn_if_upload_inbox_stale(folder_pattern: str) -> None:
    """완성 폴더가 임계 시간 넘게 적재되지 않고 남아 있으면 경고한다."""
    from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import (
        QUARANTINE_DIR_NAME,
        UPLOAD_INBOX_DIR,
    )

    cutoff = time.time() - UPLOAD_INBOX_STALE_HOURS * 3600
    stale = []
    for path in UPLOAD_INBOX_DIR.glob(folder_pattern):
        if not path.is_dir() or path.name == QUARANTINE_DIR_NAME:
            continue
        try:
            if path.stat().st_mtime < cutoff:
                stale.append(path.name)
        except OSError as exc:
            logger.warning("upload inbox mtime 확인 실패: %s / %s", path, exc)
    if not stale:
        return
    body = (
        f"[배민 upload inbox 적체] pattern={folder_pattern} "
        f"{len(stale)}개 폴더가 {UPLOAD_INBOX_STALE_HOURS}시간 넘게 미적재\n"
        + "\n".join(f"- {name}" for name in sorted(stale)[:10])
    )
    logger.warning(body)
    if not _stale_alert_due(stale):
        return
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("적체 알림 실패(무시): %s", exc)


def _stale_alert_due(names: list[str]) -> bool:
    STALE_ALERT_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    marker = STALE_ALERT_MARKER_DIR / (_safe_run_id_part("__".join(sorted(names))) + ".marker")
    cutoff = time.time() - UPLOAD_STALE_ALERT_THROTTLE_HOURS * 3600
    if marker.exists() and marker.stat().st_mtime > cutoff:
        return False
    marker.write_text(str(time.time()), encoding="utf-8")
    return True


def has_ingested_folders(**context) -> bool:
    """무대상 PC2 run만 skip하고 전부 실패한 적재는 실패로 노출한다."""
    stats = _meta_pull(context, "ingest_stats") or {}
    folders = int(stats.get("folders") or 0)
    cleaned = int(stats.get("cleaned") or 0)
    failed = int(stats.get("failed") or 0)
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    skip_if_empty = bool(conf.get("skip_if_empty", False))

    if folders and not cleaned and failed:
        from airflow.exceptions import AirflowException

        raise AirflowException(f"upload 대상 {folders}개 중 정상 적재 폴더 없음: {stats}")
    if not cleaned and (skip_if_empty or folders):
        logger.info("PC2 upload 정상 적재 폴더 없음 → downstream skip (stats=%s)", stats)
        _cleanup_handoff(context)
        return False
    return True


def has_ingested_or_manual_files(**context) -> bool:
    """Continue validation when upload ingest cleaned folders or manual CSVs are waiting."""
    stats = _meta_pull(context, "ingest_stats") or {}
    folders = int(stats.get("folders") or 0)
    cleaned = int(stats.get("cleaned") or 0)
    failed = int(stats.get("failed") or 0)
    manual_files = count_pending_manual_baemin_order_files()
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    skip_if_empty = bool(conf.get("skip_if_empty", False))

    if folders and not cleaned and failed:
        from airflow.exceptions import AirflowException

        raise AirflowException(f"upload 대상 {folders}개 중 정상 적재 폴더 없음: {stats}")
    if manual_files:
        logger.info("수동 배민 orders CSV 감지: files=%d stats=%s", manual_files, stats)
        return True
    partial_files = count_partial_manual_baemin_order_files()
    if partial_files:
        logger.warning("partial 수동 배민 orders CSV만 감지되어 validate 진행 제외: files=%d stats=%s", partial_files, stats)
        if not cleaned:
            _cleanup_handoff(context)
            return False
    if not cleaned and (skip_if_empty or folders):
        logger.info("PC2 upload 정상 적재 폴더 및 수동 CSV 없음 → downstream skip (stats=%s)", stats)
        _cleanup_handoff(context)
        return False
    return True


def precheck_manual(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_dates = _target_dates(context)
    manual_dir = Path(str(conf.get("manual_baemin_dir") or MANUAL_BAEMIN_ORDERS_DIR))

    if not manual_dir.exists():
        msg = f"manual baemin precheck skip: dir missing ({manual_dir})"
        logger.info(msg)
        context["ti"].xcom_push(key="manual_precheck_summary", value={"used": False, "reason": msg})
        return msg

    from modules.transform.pipelines.db.DB_Beamin_Macro_validate import _toorder_baemin_by_store

    by_date: dict[str, dict] = {}
    all_files: set[str] = set()
    lines = [f"manual baemin precheck dates={','.join(target_dates)} dir={manual_dir}"]
    for target_date in target_dates:
        manual_by_store, used_files = _collect_manual_baemin_orders(target_date, manual_dir)
        all_files.update(used_files)
        if not manual_by_store:
            lines.append(f"[{target_date}] usable files 없음")
            by_date[target_date] = {"used": False, "files": used_files, "stores": {}, "compared_stores": []}
            continue

        toorder_by_store = _toorder_baemin_by_store(target_date)
        compare_stores = sorted(set(manual_by_store) & set(toorder_by_store))
        lines.append(
            f"[{target_date}] files={len(used_files)} stores={len(manual_by_store)} compare={len(compare_stores)}"
        )
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
        by_date[target_date] = {
            "used": True,
            "target_date": target_date,
            "dir": str(manual_dir),
            "files": used_files,
            "stores": manual_by_store,
            "compared_stores": compare_stores,
        }

    used_dates = [date for date, item in by_date.items() if item.get("used")]
    if not used_dates:
        msg = f"manual baemin precheck skip: no usable files for {','.join(target_dates)} in {manual_dir}"
        logger.info(msg)
        context["ti"].xcom_push(
            key="manual_precheck_summary",
            value={"used": False, "reason": msg, "files": sorted(all_files), "by_date": by_date},
        )
        return msg

    summary = "\n".join(lines)
    logger.info(summary)
    first_used = by_date[used_dates[0]]
    stores = {}
    compared_stores: set[str] = set()
    for item in by_date.values():
        stores.update(item.get("stores") or {})
        compared_stores.update(item.get("compared_stores") or [])
    context["ti"].xcom_push(
        key="manual_precheck_summary",
        value={
            "used": True,
            "target_date": used_dates[0],
            "target_dates": target_dates,
            "dir": str(manual_dir),
            "files": sorted(all_files),
            "stores": stores if len(used_dates) > 1 else first_used.get("stores", {}),
            "compared_stores": sorted(compared_stores)
            if len(used_dates) > 1
            else first_used.get("compared_stores", []),
            "by_date": by_date,
            "summary": summary,
        },
    )
    return summary


def validate_orders(**context) -> str:
    target_date = _target_date(context)
    validation = _meta_pull(context, "validation") or []
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
            f"수집={v.get('actual_count')}건/{v.get('actual_amount') or 0:,}원 "
            f"기대={v.get('expected_count')}건/{v.get('expected_amount') or 0:,}원 "
            f"(재시도 {v.get('retried', 0)}회)"
        )
    summary = "\n".join(lines)
    logger.info(summary)
    if mismatches:
        _send_alert(subject=f"[배민 orders 불일치] {len(mismatches)}건", body=summary)
    _save_validate_log(target_date, "orders 검증", summary)
    return summary


def validate_ad_funnel(**context) -> str:
    if _orders_only_context(context):
        result = {"empty_stores": [], "retried": [], "still_empty": []}
        context["ti"].xcom_push(key="ad_funnel_result", value=result)
        logger.info("orders_only=true: ad_funnel 검증 스킵")
        return "orders_only: ad_funnel 검증 스킵"
    target_date = _target_date(context)
    ad_stores = _meta_pull(context, "ad_stores") or []
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
    target_dates = _target_dates(context)
    ti = context["ti"]
    account_list = _meta_pull(context, "account_list") or []
    store_info_per_account = _meta_pull(context, "store_info_per_account") or []
    if not store_info_per_account:
        store_info_per_account = store_info_from_account_list(account_list)
    manual_precheck = ti.xcom_pull(task_ids="precheck_manual_baemin_orders", key="manual_precheck_summary") or {}
    date_untrusted_stores = _date_untrusted_stores_from_validation(
        _meta_pull(context, "validation") or []
    )

    results_by_date: dict[str, dict] = {}
    summary_blocks: list[str] = []
    for target_date in target_dates:
        result = validate_toorder_orders(account_list, store_info_per_account, target_date)
        result, blind, compared_blind = _apply_toorder_context_flags(
            context,
            result,
            date_untrusted_stores,
        )
        results_by_date[target_date] = result
        summary_blocks.extend(
            _format_toorder_summary(
                target_date,
                result,
                blind=blind,
                compared_blind=compared_blind,
                date_untrusted_stores=date_untrusted_stores,
                manual_precheck=manual_precheck,
            )
        )

    ti.xcom_push(key="toorder_results_by_date", value=results_by_date)
    result = (
        next(iter(results_by_date.values()))
        if len(results_by_date) == 1
        else _aggregate_toorder_results(results_by_date)
    )
    ti.xcom_push(key="toorder_result", value=result)

    lines = summary_blocks
    summary = "\n".join(lines)
    logger.info(summary)
    for target_date in target_dates:
        _save_validate_log(target_date, "ToOrder 교차검증", "\n".join(_format_toorder_summary(
            target_date,
            results_by_date[target_date],
            blind=bool(results_by_date[target_date].get("blind")),
            compared_blind=bool(results_by_date[target_date].get("blind"))
            and int(results_by_date[target_date].get("observed_accounts") or 0) == 0,
            date_untrusted_stores=date_untrusted_stores,
            manual_precheck=manual_precheck,
        )))
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


def _store_match_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = normalize_store_names(pd.Series([text]))
    return str(strip_brand(normalized).iloc[0]).strip()


def _store_name_from_meta(store: object) -> str:
    if isinstance(store, dict):
        return str(
            store.get("store")
            or store.get("store_name")
            or store.get("name")
            or store.get("text")
            or ""
        ).strip()
    return str(store or "").strip()


def _date_untrusted_stores_from_validation(validation: list | None) -> list[str]:
    stores: set[str] = set()
    for item in validation or []:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason") or "").strip()
        amount_source = str(item.get("amount_source") or "").strip()
        if reason not in {"date_filter", "date_filtered_rows"} and amount_source != "date_filter":
            continue
        store_key = _store_match_key(_store_name_from_meta(item))
        if store_key:
            stores.add(store_key)
    return sorted(stores)


def _apply_toorder_context_flags(context, result: dict, date_untrusted_stores: list[str]) -> tuple[dict, bool, bool]:
    if date_untrusted_stores:
        result["date_untrusted_stores"] = date_untrusted_stores
        source_mismatch = set(result.get("source_mismatch_stores") or [])
        result["source_mismatch_stores"] = sorted(source_mismatch - set(date_untrusted_stores))

    account_list = _meta_pull(context, "account_list") or []
    store_info_per_account = _meta_pull(context, "store_info_per_account") or []
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
        if store_info_per_account:
            result["store_info_fallback_accounts"] = len(store_info_per_account)
    return result, blind, compared_blind


def _aggregate_toorder_results(results_by_date: dict[str, dict]) -> dict:
    aggregate = {
        "matched": True,
        "compared_count": 0,
        "retried_stores": [],
        "mismatched_stores": [],
        "retry_failed_stores": [],
        "retry_skipped_stores": [],
        "date_untrusted_stores": [],
        "toorder_gap_stores": [],
        "missing_brand_stores": [],
        "source_mismatch_stores": [],
        "amount_only_mismatch_stores": [],
        "restored_stores": [],
        "store_results": {},
        "results_by_date": results_by_date,
    }
    list_keys = [
        "retried_stores",
        "mismatched_stores",
        "retry_failed_stores",
        "retry_skipped_stores",
        "date_untrusted_stores",
        "toorder_gap_stores",
        "missing_brand_stores",
        "source_mismatch_stores",
        "amount_only_mismatch_stores",
        "restored_stores",
    ]
    for target_date, result in results_by_date.items():
        aggregate["matched"] = bool(aggregate["matched"]) and bool(result.get("matched"))
        aggregate["compared_count"] += int(result.get("compared_count") or 0)
        for key in list_keys:
            aggregate[key].extend(result.get(key) or [])
        for store, item in (result.get("store_results") or {}).items():
            aggregate["store_results"][f"{target_date} {store}"] = item
        for key in ("blind", "expected_accounts", "observed_accounts", "store_info_fallback_accounts"):
            if key in result:
                aggregate[key] = result.get(key)
    for key in list_keys:
        aggregate[key] = sorted({str(v) for v in aggregate[key] if str(v or "").strip()})
    return aggregate


def _format_toorder_summary(
    target_date: str,
    result: dict,
    *,
    blind: bool,
    compared_blind: bool,
    date_untrusted_stores: list[str],
    manual_precheck: dict,
) -> list[str]:
    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    missing_brand_stores = result.get("missing_brand_stores", [])
    lines = [
        f"ToOrder 교차검증[{target_date}]: 비교 {compared}건 / "
        f"일치={matched} / 재시도={len(retried)} / 불일치={len(mismatched)}"
    ]
    if blind:
        lines.append(
            f"수집 관측 누락: expected_accounts={result.get('expected_accounts', 0)}, "
            f"observed_accounts={result.get('observed_accounts', 0)}"
        )
    if compared_blind:
        lines.append("수집 결과가 0건이라 ToOrder 비교 결과를 신뢰할 수 없음")
    skipped_retry = result.get("retry_skipped_stores") or []
    if skipped_retry:
        lines.append(
            f"시간 예산으로 재수집 이월 {len(skipped_retry)}건 (기존 orders 보존): "
            f"{', '.join(skipped_retry)}"
        )
    if date_untrusted_stores:
        lines.append(
            f"날짜 필터 불신뢰 재시도 대상 {len(date_untrusted_stores)}건: "
            f"{', '.join(date_untrusted_stores)}"
        )
    for store, item in store_results.items():
        lines.append(
            f"  - {store}: baemin={item.get('baemin', 0):,} / "
            f"toorder={item.get('toorder', 0):,} / "
            f"diff={item.get('toorder', 0) - item.get('baemin', 0):,}"
        )
    for store in missing_brand_stores:
        lines.append(f"  - {store}: brand 매핑 누락")
    if manual_precheck.get("used"):
        by_date = manual_precheck.get("by_date") or {}
        precheck_item = by_date.get(target_date) or manual_precheck
        lines.append(
            f"수동사전검증: files={len(precheck_item.get('files', []))} "
            f"stores={len((precheck_item.get('stores') or {}))} "
            f"compare={len(precheck_item.get('compared_stores', []))}"
        )
    return lines


def _toorder_retry_failed_payload(context, toorder_result: dict | None) -> dict:
    result = toorder_result or {}
    target_names = {
        _store_match_key(store)
        for store in [
            *(result.get("retry_failed_stores") or []),
            *(result.get("retry_skipped_stores") or []),
            *(result.get("date_untrusted_stores") or []),
            *(result.get("mismatched_stores") or []),
        ]
    }
    target_names.discard("")
    target_names -= {
        _store_match_key(store)
        for store in (result.get("source_mismatch_stores") or [])
    }
    if not target_names:
        return {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}

    account_list = _meta_pull(context, "account_list") or []
    account_map = {
        str(account.get("account_id") or "").strip(): account
        for account in account_list
        if isinstance(account, dict) and str(account.get("account_id") or "").strip()
    }
    grouped: dict[str, dict] = {}
    store_info_per_account = _meta_pull(context, "store_info_per_account") or []
    if not store_info_per_account:
        store_info_per_account = store_info_from_account_list(account_list)
    for item in store_info_per_account:
        if not isinstance(item, dict):
            continue
        account_id = str(item.get("account_id") or "").strip()
        account = account_map.get(account_id)
        if not account:
            continue
        for store in item.get("stores") or []:
            if _store_match_key(_store_name_from_meta(store)) not in target_names:
                continue
            store_payload = store if isinstance(store, dict) else {"store": str(store)}
            grouped.setdefault(account_id, {"account": account, "stores": []})["stores"].append(store_payload)

    return {
        "accounts": [],
        "stores": [],
        "orders": list(grouped.values()),
        "ads": [],
        "stages": [],
    }


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
            "source_mismatch": bool(item.get("source_mismatch")),
            "source_mismatch_reason": item.get("source_mismatch_reason"),
            "amount_only": bool(item.get("amount_only")),
        }
    snapshot = {
        "compared": int(result.get("compared_count") or 0),
        "store_results": store_results,
        "mismatched_stores": sorted(set(result.get("mismatched_stores") or [])),
        "gap_stores": sorted(set(result.get("toorder_gap_stores") or [])),
        "missing_brand_stores": sorted(set(result.get("missing_brand_stores") or [])),
        "source_mismatch_stores": sorted(set(result.get("source_mismatch_stores") or [])),
        "amount_only_mismatch_stores": sorted(set(result.get("amount_only_mismatch_stores") or [])),
        "restored_stores": sorted(set(result.get("restored_stores") or [])),
    }
    for key in ("blind", "expected_accounts", "observed_accounts", "store_info_fallback_accounts"):
        if key in result:
            snapshot[key] = result.get(key)
    return snapshot


def build_upload_notification_context(context) -> dict:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    account_list = _meta_pull(context, "account_list") or []
    account_ids = {
        str(account.get("account_id") or "").strip()
        for account in account_list
        if isinstance(account, dict) and str(account.get("account_id") or "").strip()
    }
    total_accounts = len(account_ids) if account_ids else len(account_list)
    failed = _residual_failed_from_meta(context)
    validation = _meta_pull(context, "validation") or []
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
        "target_dates": _target_dates(context),
        "total_accounts": total_accounts,
        "ingest_stats": _meta_pull(context, "ingest_stats") or {},
        "orders": {
            "total": len(validation),
            "matched": order_matched,
            "mismatched": order_mismatched,
            "unknown": order_unknown,
        },
        "ad_funnel": {
            "total": len(_meta_pull(context, "ad_stores") or []),
            "still_empty": len(ad_result.get("still_empty") or []),
        },
        "toorder": _toorder_snapshot(toorder_result),
        "residual_failed": failed,
        "hard_failures": hard_failures,
        "metrics": _meta_pull(context, "metrics") or {},
        "orders_only": _orders_only_context(context),
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
    final_context = (
        merge_toorder_notification_context(notification_context, final_toorder_result)
        if final_toorder_result is not None
        else dict(notification_context or {})
    )
    root_toorder = final_context.get("toorder") or {}
    merged_store_results = dict(root_toorder.get("store_results") or {})
    final_toorder = root_toorder
    unresolved_stores = {
        store
        for store, item in merged_store_results.items()
        if not (item or {}).get("matched") and not (item or {}).get("source_mismatch")
    }
    unresolved_stores.update(root_toorder.get("gap_stores") or [])
    unresolved_stores.update(root_toorder.get("missing_brand_stores") or [])
    compared = int(root_toorder.get("compared") or 0)
    toorder_matched = max(compared - len(unresolved_stores), 0)
    source_mismatch_stores = {
        store
        for store, item in merged_store_results.items()
        if (item or {}).get("source_mismatch")
    }
    source_mismatch_stores.update(root_toorder.get("source_mismatch_stores") or [])
    source_mismatch_stores.update(final_toorder.get("source_mismatch_stores") or [])
    # 건수는 같고 금액만 다른 매장: 재수집 대상이 아니라 기준 차이로 따로 보고한다.
    amount_only_stores = {
        store
        for store, item in merged_store_results.items()
        if (item or {}).get("amount_only")
    }
    amount_only_stores.update(root_toorder.get("amount_only_mismatch_stores") or [])
    amount_only_stores.update(final_toorder.get("amount_only_mismatch_stores") or [])
    amount_only_stores &= unresolved_stores
    restored_partitions = sorted(
        set(root_toorder.get("restored_stores") or []) | set(final_toorder.get("restored_stores") or [])
    )

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

    orders_has_partial = final_toorder_result is None and bool(
        int(orders.get("mismatched") or 0) or int(orders.get("unknown") or 0)
    )
    has_partial = bool(
        residual_count
        or orders_has_partial
        or ad_still
        or unresolved_stores
        or int(ingest_stats.get("failed") or 0)
        or int(ingest_stats.get("skipped") or 0)
    )
    status = "실패" if hard_failures else "부분완료" if has_partial else "완료"
    lines = [
        f"[배민 최종 결과] {status}",
        f"target_date: {', '.join(notification_context.get('target_dates') or []) or notification_context.get('target_date') or '?'}",
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
    blind_info = root_toorder.get("blind") or final_toorder.get("blind")
    if blind_info:
        lines.append(
            "수집 관측 누락: "
            f"expected_accounts={root_toorder.get('expected_accounts') or final_toorder.get('expected_accounts') or 0}, "
            f"observed_accounts={root_toorder.get('observed_accounts') or final_toorder.get('observed_accounts') or 0}"
        )
    metrics = notification_context.get("metrics") or {}
    if metrics.get("orders_date_filter_abort"):
        lines.append("주문 날짜필터 연속 실패 감지: 수집 중단 후 잔여 실패로 이월")
    def _store_line(store: str, suffix: str = "") -> str:
        item = merged_store_results.get(store) or {}
        baemin = int(item.get("baemin") or 0)
        toorder = int(item.get("toorder") or 0)
        text = f"- {store}: 배민={baemin:,} / ToOrder={toorder:,} / diff={toorder - baemin:,}"
        issue = str(item.get("brand_issue") or "")
        if issue:
            text += f" / {issue}"
        return text + suffix

    problems: list[str] = [
        _store_line(store) for store in sorted(unresolved_stores - amount_only_stores)
    ]
    if hard_failures:
        problems.append(f"- 실패 task: {', '.join(sorted(set(hard_failures)))}")
    if metrics.get("orders_date_filter_abort"):
        problems.append("- 주문 날짜필터 UI 파손 의심: orders 재시도 대상 보존")
    if problems:
        lines.extend(["", "[재수집 필요]", *problems])
    if amount_only_stores:
        lines.extend(
            [
                "",
                "[금액기준 차이-재수집 제외]",
                *(_store_line(store, " / 건수 동일") for store in sorted(amount_only_stores)),
            ]
        )
    if restored_partitions:
        lines.extend(
            [
                "",
                "[재수집 0건 감지-삭제 전 데이터 복원]",
                *(f"- {item}" for item in restored_partitions),
            ]
        )
    if source_mismatch_stores:
        source_lines = []
        for store in sorted(source_mismatch_stores):
            item = merged_store_results.get(store) or {}
            baemin = int(item.get("baemin") or 0)
            toorder = int(item.get("toorder") or 0)
            reason = str(item.get("source_mismatch_reason") or "원천 금액 차이")
            source_lines.append(
                f"- {store}: 배민={baemin:,} / ToOrder={toorder:,} / diff={toorder - baemin:,} / {reason}"
            )
        lines.extend(["", "[원천차이-재수집 제외]", *source_lines])
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
        _cleanup_handoff(context)
        return "최종 Telegram 보류: Retry 진행 중"
    body = build_final_notification_message(notification_context)
    logger.info(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("Telegram 결과 알림 실패(무시): %s", exc)
    _cleanup_handoff(context)
    return body.splitlines()[0]


def trigger_retry_if_needed(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_dates = _target_dates(context)
    residual_failed = _residual_failed_from_meta(context)
    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    toorder_results_by_date = ti.xcom_pull(task_ids="validate_toorder", key="toorder_results_by_date") or {}
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result")
    orders_only = _orders_only_context(context)
    failed_by_date: dict[str, dict] = {}
    for target_date in target_dates:
        date_toorder_result = toorder_results_by_date.get(target_date) or toorder_result
        base_failed = residual_failed if len(target_dates) == 1 else {}
        failed = merge_failed_payloads(
            base_failed,
            _toorder_retry_failed_payload(context, date_toorder_result),
        )
        if orders_only:
            failed = _orders_only_failed(failed)
        failed_by_date[target_date] = filter_ad_funnel_zero_sales_failures(failed, target_date)

    failed = {}
    for item in failed_by_date.values():
        failed = merge_failed_payloads(failed, item)
    failed_count = count_failed_items(failed)
    notification_context = build_upload_notification_context(context)
    notification_context["residual_failed"] = failed
    notification_context["target_dates"] = target_dates
    ti.xcom_push(key="notification_context", value=notification_context)
    if failed_count == 0:
        ti.xcom_push(key="retry_triggered", value=False)
        logger.info("Retry DAG 트리거 스킵: 잔여 실패 없음")
        return "Retry DAG 트리거 스킵: 잔여 실패 없음"

    if not retry_needed(toorder_result, ad_funnel_result, failed):
        ti.xcom_push(key="retry_triggered", value=False)
        logger.info("Retry DAG 트리거 스킵: 검증상 추가 재시도 불필요")
        return "Retry DAG 트리거 스킵: 추가 재시도 불필요"

    source_run_id = getattr(dag_run, "run_id", context.get("run_id", "manual"))
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    deferred: list[str] = []
    triggered: list[str] = []
    existing: list[str] = []
    for target_date in target_dates:
        date_failed = failed_by_date.get(target_date) or {}
        if count_failed_items(date_failed) == 0:
            continue
        date_toorder_result = toorder_results_by_date.get(target_date) or toorder_result
        if not retry_needed(date_toorder_result, ad_funnel_result, date_failed):
            continue
        retry_conf = build_retry_conf(
            failed=date_failed,
            target_date=target_date,
            source_dag_id=ti.dag_id,
            source_run_id=source_run_id,
            attempt=1,
            max_attempts=int(conf.get("max_attempts", 3)),
            stability_profile=conf.get("stability_profile") or SCHEDULED_DEFAULT_STABILITY_PROFILE,
            orders_only=orders_only,
        )
        retry_conf["notification_context"] = {**notification_context, "target_date": target_date}
        if conf.get("manual_baemin_dir"):
            retry_conf["manual_baemin_dir"] = conf["manual_baemin_dir"]
        run_id = f"retry__{target_date.replace('-', '')}__attempt_1__{_safe_run_id_part(str(source_run_id))}"
        try:
            result = route_trigger(trigger_dag, history_context=context,
                dag_id=retry_dag_id(conf, ti.dag_id),
                run_id=run_id,
                conf=retry_conf,
            )
            if result == "cancelled":
                logger.info("사용자 취소로 Retry 요청 제외: %s", run_id)
            elif result == "existing":
                existing.append(run_id)
            elif result == "deferred":
                deferred.append(run_id)
            else:
                triggered.append(run_id)
        except DagRunAlreadyExists:
            existing.append(run_id)
            logger.info("Retry DAG run 이미 존재: %s", run_id)

    retry_triggered = bool(triggered or existing or deferred)
    ti.xcom_push(key="retry_triggered", value=retry_triggered)
    if not retry_triggered:
        logger.info("Retry DAG 트리거 스킵: 날짜별 재시도 대상 없음")
        return "Retry DAG 트리거 스킵: 날짜별 재시도 대상 없음"
    msg = f"Retry DAG 트리거 완료: 신규 {len(triggered)}개 / 기존 {len(existing)}개"
    if deferred:
        msg = f"Retry DAG 요청 접수: 신규 {len(triggered)}개 / 기존 {len(existing)}개 / 보류 {len(deferred)}개"
    logger.info("%s triggered=%s existing=%s failed_count=%d", msg, triggered, existing, failed_count)
    return msg
