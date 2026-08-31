'''
Baemin macro DAG — 배달의민족 계정별 자동 수집

=== 수집 흐름 (계정 단위, 단일 브라우저 세션) ===
  load_accounts
      ↓
  init_staging
    ├─ collect_batch_1 → collect_batch_2
    └─ collect_batch_3 → collect_batch_4
    ├─ 로그인
    ├─ 매장/store_id 확인
    ├─ 우가클 수집     (우리가게 클릭 현황 - 이번달 + 저번달)
    ├─ 운영시간 수집   (매장 운영시간 - manage/operation)
    ├─ 주문내역 수집   (orders/history - 어제 배달완료)
    ├─ 광고 funnel 수집 (stat/advertisement - 어제)
    └─ 로그아웃
      ↓
  retry_failed (네 배치 실패 통합 최종 재시도)

=== 저장 경로 ===
  우가클    : analytics/baemin_macro/metrics_our_store_clicks/
               brand={brand}/store={store}/ym={YYYY-MM}/woori_shop_click.csv
  주문내역  : analytics/baemin_macro/orders/
  광고funnel: analytics/baemin_macro/ad_funnel/
               brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM-DD}.csv

=== 수집 월 ===
  우가클: 이번달 + 저번달 (덮어쓰기)
  주문내역: 어제 (upsert by 주문번호)
'''

import html
import logging
import os
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
from airflow.exceptions import AirflowTaskTimeout
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.utility.notifier import on_failure_callback_no_telegram
from modules.transform.pipelines.db.DB_Beamin_collect import (
    load_accounts as pipeline_load_accounts,
)
from modules.transform.pipelines.db.beamin_stability import (
    resolve_stability_profile,
    write_runtime_metrics,
)
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori as pipeline_collect_all,
    collect_orders_only as pipeline_collect_orders_only,
    retry_once_failed as pipeline_retry_failed,
)
from modules.transform.pipelines.db.beamin_staging import (
    cleanup_staging,
    export_staging_to_inbox,
    init_empty_staging,
    init_progress,
    load_progress,
    local_stage_paths,
    patch_baemin_staging_paths,
    progress_path,
    resolve_macro_role,
    safe_run_id_part,
    restore_baemin_staging_paths,
)
from modules.transform.pipelines.db.DB_Beamin_retry import (
    build_retry_conf,
    count_failed_items,
    merge_failed_payloads,
    retry_needed,
)
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import UPLOAD_INBOX_DIR
from modules.transform.utility.schedule import (
    SMD_BAEMIN_COLLECT_BATCH1_TIME,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import (
    _merge_order_amounts,
    _order_amounts,
    validate_toorder_orders,
)
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM

logger = logging.getLogger(__name__)
dag_id = Path(__file__).stem

warnings.filterwarnings(
    "ignore",
    message="This process .* is multi-threaded, use of fork\\(\\) may lead to deadlocks in the child\\.",
    category=DeprecationWarning,
)

KST = pendulum.timezone("Asia/Seoul")
_ALERT_EMAILS = [MAIL_CMJ_PM]
_UPLOAD_DAG_ID = "DB_Beamin_Macro_Upload_Dags"
_BATCH_TASK_IDS = (
    "collect_batch_1",
    "collect_batch_2",
    "collect_batch_3",
    "collect_batch_4",
)
_LANES: tuple[tuple[str, ...], ...] = (
    ("collect_batch_1", "collect_batch_2"),
    ("collect_batch_3", "collect_batch_4"),
)
_LANE_OFFSET_RANGE: tuple[float, float] = (15.0, 20.0)
COLLECT_LANES: int = 2
BAEMIN_SELENIUM_POOL = "baemin_selenium_pool"
_RETRY_FAILED_TIMEOUT_MINUTES = 120
TARGET_STORES: list[str] = []  # empty list means all stores (전체매장 대상 → COLLECT_RANGE로 상/하위 분할)
# None: 전체 수집, "상위": 가나다순 상위 절반, "하위": 가나다순 하위 절반(나머지)
# ex ) COLLECT_RANGE="상위" → 가나다순 상위 절반만 수집
COLLECT_RANGE: str | None = "상위" # 상위, 하위, None
SCHEDULED_DEFAULT_STABILITY_PROFILE = "safe_daily"
_MACRO_ROLE_RAW = os.getenv("BAEMIN_MACRO_ROLE")
_MACRO_ROLE = resolve_macro_role(_MACRO_ROLE_RAW)

MANUAL_BAEMIN_ORDERS_DIR = COLLECT_DB / "영업관리부_수집"


def _current_run_id(context) -> str:
    dag_run = context.get("dag_run")
    return str(getattr(dag_run, "run_id", context.get("run_id", "manual")) if dag_run else context.get("run_id", "manual"))


def _main_stage_paths(context) -> tuple[Path, Path]:
    return local_stage_paths("analytics_stage_main", _current_run_id(context))


def _target_date_from_context(context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if conf.get("target_date"):
        return conf["target_date"]
    data_interval_end = context.get("data_interval_end")
    if data_interval_end is not None:
        return pendulum.instance(data_interval_end).in_timezone(KST).subtract(days=1).format("YYYY-MM-DD")
    return pendulum.now(KST).subtract(days=1).format("YYYY-MM-DD")


def _normalize_collect_task_ids(collect_task_ids=None) -> tuple[str, ...]:
    if not collect_task_ids:
        return ("collect_all",)
    return tuple(str(task_id) for task_id in collect_task_ids)


def _pull_batch_values(ti, collect_task_ids, key: str) -> list:
    values: list = []
    for task_id in _normalize_collect_task_ids(collect_task_ids):
        value = ti.xcom_pull(task_ids=task_id, key=key)
        if isinstance(value, list):
            values.extend(value)
    return values


def _pull_batch_failures(ti, collect_task_ids) -> dict:
    return merge_failed_payloads(
        *(
            ti.xcom_pull(task_ids=task_id, key="failed") or {}
            for task_id in _normalize_collect_task_ids(collect_task_ids)
        )
    )


def _loaded_account_ids(context) -> set[str]:
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list") or []
    return {
        str((account or {}).get("account_id") or "").strip()
        for account in account_list
        if str((account or {}).get("account_id") or "").strip()
    }


def _account_id_from_failed_account(account: Any) -> str:
    if not isinstance(account, dict):
        return ""
    return str(account.get("account_id") or "").strip()


def _account_id_from_failed_item(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    account = item.get("account") or {}
    if not isinstance(account, dict):
        return ""
    return str(account.get("account_id") or "").strip()


def _unique_failed_account_ids(failed: dict | None) -> set[str]:
    data = failed or {}
    account_ids = {
        _account_id_from_failed_account(account)
        for account in data.get("accounts") or []
    }
    for key in ("stores", "orders", "ads", "stages"):
        account_ids.update(
            _account_id_from_failed_item(item)
            for item in data.get(key) or []
        )
    account_ids.discard("")
    return account_ids


def _collect_scope_label(context) -> str | None:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    params = context.get("params") or {}
    for candidate in (
        conf.get("collect_range"),
        params.get("collect_range"),
        _MACRO_ROLE["range"],
        COLLECT_RANGE,
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _filter_failed_to_loaded_accounts(failed: dict | None, allowed_ids: set[str]) -> dict:
    data = failed or {}
    if not allowed_ids:
        return {
            "accounts": data.get("accounts") or [],
            "stores": data.get("stores") or [],
            "orders": data.get("orders") or [],
            "ads": data.get("ads") or [],
            "stages": data.get("stages") or [],
        }

    def account_allowed(account: dict | None) -> bool:
        account_id = str((account or {}).get("account_id") or "").strip()
        return bool(account_id and account_id in allowed_ids)

    filtered = {
        "accounts": [
            account
            for account in data.get("accounts") or []
            if account_allowed(account if isinstance(account, dict) else None)
        ],
        "stores": [
            item
            for item in data.get("stores") or []
            if isinstance(item, dict) and account_allowed(item.get("account"))
        ],
        "orders": [
            item
            for item in data.get("orders") or []
            if isinstance(item, dict) and account_allowed(item.get("account"))
        ],
        "ads": [
            item
            for item in data.get("ads") or []
            if isinstance(item, dict) and account_allowed(item.get("account"))
        ],
        "stages": [
            item
            for item in data.get("stages") or []
            if isinstance(item, dict) and account_allowed(item.get("account"))
        ],
    }
    dropped = count_failed_items(data) - count_failed_items(filtered)
    if dropped:
        logger.warning(
            "상위 수집 범위 밖 실패 제외: dropped=%d allowed_accounts=%d",
            dropped,
            len(allowed_ids),
        )
    return filtered


def _save_validate_log(target_date: str, section: str, text: str) -> None:
    """검증 결과를 일자별 .md 파일에 append한다."""
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


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback_no_telegram,
}


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
        groups: dict[str, list[dict]] = {}
        for account in ordered:
            groups.setdefault(str(account.get("account_id") or ""), []).append(account)
        group_keys = list(groups)
        start = len(group_keys) * (batch_index - 1) // batch_total
        end = len(group_keys) * batch_index // batch_total
        selected = [account for key in group_keys[start:end] for account in groups[key]]
        logger.info(
            "매장 분할 [%s]: 전체 %d개 -> %d개 (%s ~ %s)",
            collect_range,
            len(ordered),
            len(selected),
            selected[0]["store_name"] if selected else "-",
            selected[-1]["store_name"] if selected else "-",
        )
        return selected

    mid = len(ordered) // 2

    if normalized_range == "상위":
        selected = ordered[:mid]
    elif normalized_range == "하위":
        selected = ordered[mid:]
    else:
        logger.warning("알 수 없는 COLLECT_RANGE=%r", collect_range)
        return accounts

    logger.info(
        "매장 분할 [%s]: 전체 %d개 -> %d개 (%s ~ %s)",
        collect_range,
        len(ordered),
        len(selected),
        selected[0]["store_name"] if selected else "-",
        selected[-1]["store_name"] if selected else "-",
    )
    return selected


def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    params = context.get("params") or {}
    stores_override = conf.get("stores")
    target = stores_override if stores_override else TARGET_STORES
    logger.info(
        "계정 로드 target 확인: stores_override=%r collect_range_conf=%r collect_range_param=%r target=%r",
        stores_override,
        conf.get("collect_range"),
        params.get("collect_range"),
        target,
    )
    accounts = pipeline_load_accounts(target_stores=target, exact=False)

    if not stores_override:
        conf_range = conf.get("collect_range")
        param_range = params.get("collect_range")
        role_range = _MACRO_ROLE["range"] or COLLECT_RANGE
        for candidate in (conf_range, param_range):
            if str(candidate or "").strip() in {"상위", "하위"}:
                role_range = str(candidate).strip()
                break

        batch_range = None
        if params.get("batch_orchestration") != "single_dag":
            for candidate in (conf_range, param_range):
                normalized = str(candidate or "").strip()
                if re.fullmatch(r"batch:\d+/\d+", normalized):
                    batch_range = normalized
                    break

        all_count = len(accounts)
        accounts = _split_accounts_by_range(accounts, role_range)
        role_count = len(accounts)
        accounts = _split_accounts_by_range(accounts, batch_range)
        logger.info(
            "계정 2단계 분할: 전체 %d개 -> %s %d개 -> %s %d개",
            all_count,
            role_range or "전체",
            role_count,
            batch_range or "배치 없음",
            len(accounts),
        )

    if _MACRO_ROLE["slug"]:
        logger.info(
            "배민 매크로 PC 역할: range=%s slug=%s",
            _MACRO_ROLE["range"],
            _MACRO_ROLE["slug"],
        )
    else:
        logger.warning(
            "BAEMIN_MACRO_ROLE 미설정 또는 미지원 값: %r (기존 전량 수집/폴더명 사용)",
            _MACRO_ROLE_RAW,
        )

    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
    return f"계정 {len(accounts)}개 {stores}"


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


def precheck_manual_baemin_orders(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = _target_date_from_context(context)
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
            lines.append(
                f"  - {store} manual={manual_amount:,} ({manual_orders} orders) / ToOrder=<missing>"
            )
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


def retry_failed(
    *,
    collect_task_ids=None,
    retry_all_types: bool = False,
    **context,
) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = _target_date_from_context(context)
    profile = resolve_stability_profile(conf.get("stability_profile") or SCHEDULED_DEFAULT_STABILITY_PROFILE)
    logger.info("재시도 안정성 프로필: %s", profile["name"])

    allowed_account_ids = _loaded_account_ids(context)
    failed = _filter_failed_to_loaded_accounts(
        _pull_batch_failures(context["ti"], collect_task_ids),
        allowed_account_ids,
    )
    context["ti"].xcom_push(key="original_failed", value=failed)
    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    if retry_all_types:
        immediate_failed = failed
        deferred_counts = {"stores": 0, "ads": 0}
    else:
        immediate_failed = {
            "accounts": failed.get("accounts") or [],
            "stages": failed.get("stages") or [],
            "stores": [],
            "orders": failed.get("orders") or [],
            "ads": [],
        }
        deferred_counts = {
            "stores": len(failed.get("stores") or []),
            "ads": len(failed.get("ads") or []),
        }
    if count_failed_items(immediate_failed) == 0:
        logger.info(
            "즉시 재시도 대상 없음: deferred stores=%d ads=%d",
            deferred_counts["stores"],
            deferred_counts["ads"],
        )
        if deferred_counts["stores"] or deferred_counts["ads"]:
            residual_failed["stores"] = failed.get("stores") or []
            residual_failed["ads"] = failed.get("ads") or []
            context["ti"].xcom_push(key="residual_failed", value=residual_failed)
            return (
                "즉시 재시도 없음 "
                f"(검증 단계로 이관: stores={deferred_counts['stores']} ads={deferred_counts['ads']})"
            )
        context["ti"].xcom_push(key="residual_failed", value=residual_failed)
        return "재시도 없음"

    local_analytics, _local_baemin = _main_stage_paths(context)
    prog_path = progress_path(local_analytics, "retry")
    analytics_original, patched_paths = patch_baemin_staging_paths(local_analytics)
    try:
        result = pipeline_retry_failed(
            immediate_failed,
            target_date=target_date,
            stability_profile=profile["name"],
            progress_file=prog_path,
            progress_run_id=_current_run_id(context),
        )
    finally:
        restore_baemin_staging_paths(analytics_original, patched_paths)
    if isinstance(result, dict):
        residual_failed = result.get("residual_failed") or residual_failed
        if not retry_all_types:
            residual_failed["stores"].extend(failed.get("stores") or [])
            residual_failed["ads"].extend(failed.get("ads") or [])
        context["ti"].xcom_push(key="residual_failed", value=residual_failed)
        return str(result.get("summary") or "재시도 완료")
    context["ti"].xcom_push(key="residual_failed", value=residual_failed)
    return str(result)


def export_to_upload_inbox(*, collect_task_ids=None, **context) -> str:
    ti = context["ti"]
    local_analytics, local_baemin = _main_stage_paths(context)
    if not local_baemin.exists():
        logger.warning("배민 upload export 스킵: staging 경로 없음 %s", local_baemin)
        return "upload inbox export 스킵: staging 없음"

    account_list = ti.xcom_pull(task_ids="load_accounts", key="account_list") or []
    validation = _pull_batch_values(ti, collect_task_ids, "validation")
    ad_stores = _pull_batch_values(ti, collect_task_ids, "ad_stores")
    store_info_per_account = _pull_batch_values(
        ti,
        collect_task_ids,
        "store_info_per_account",
    )
    original_failed = ti.xcom_pull(task_ids="retry_failed", key="original_failed")
    if original_failed is None:
        original_failed = _pull_batch_failures(ti, collect_task_ids)
    residual_failed = ti.xcom_pull(task_ids="retry_failed", key="residual_failed")
    failed = residual_failed if residual_failed is not None else original_failed
    if residual_failed is None:
        logger.warning("retry_failed residual_failed XCom 없음: 원본 실패 payload를 upload meta failed로 사용")
    meta = {
        "target_date": _target_date_from_context(context),
        "account_list": account_list,
        "validation": validation,
        "ad_stores": ad_stores,
        "store_info_per_account": store_info_per_account,
        "original_failed": original_failed,
        "failed": failed,
        "residual_failed": failed,
    }
    final_run = export_staging_to_inbox(
        local_baemin,
        _current_run_id(context),
        inbox_dir=UPLOAD_INBOX_DIR,
        meta=meta,
        replace_existing=False,
        folder_prefix=str(_MACRO_ROLE["slug"] or ""),
    )
    cleanup_staging(local_analytics)
    ti.xcom_push(key="upload_inbox_run", value=str(final_run))
    return f"upload inbox export 완료: {final_run}"


def trigger_upload_after_export(**context) -> str:
    """수집 종료 시각과 무관하게 이번 top 폴더만 중앙 적재 큐에 넣는다."""
    if _MACRO_ROLE["slug"] != "top":
        logger.info(
            "배민 upload 직접 트리거 스킵: role=%s (중앙 top 전용)",
            _MACRO_ROLE["slug"] or _MACRO_ROLE_RAW,
        )
        return f"upload 직접 트리거 스킵: role={_MACRO_ROLE['slug'] or _MACRO_ROLE_RAW}"

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
    trigger_conf = {
        "folder_pattern": folder_name,
        "skip_if_empty": True,
        "source": "main_top_collect_export",
        "target_date": _target_date_from_context(context),
    }
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if conf.get("manual_baemin_dir"):
        trigger_conf["manual_baemin_dir"] = conf["manual_baemin_dir"]

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    try:
        trigger_dag(
            dag_id=_UPLOAD_DAG_ID,
            run_id=upload_run_id,
            conf=trigger_conf,
        )
    except DagRunAlreadyExists:
        logger.info("배민 upload DAG run 이미 존재: %s", upload_run_id)
        return f"upload DAG run 이미 존재: {upload_run_id}"

    logger.info(
        "배민 upload DAG 트리거 완료: run_id=%s folder=%s",
        upload_run_id,
        folder_name,
    )
    return f"upload DAG 트리거 완료: {upload_run_id}"


_NOTIFY_TASK_ID = "notify_collection_result"
_CORE_TASK_IDS = {
    "load_accounts",
    "init_staging",
    "collect_all",
    *_BATCH_TASK_IDS,
    "retry_failed",
    "export_to_upload_inbox",
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
    "데이터 없음(정상)",
    "우가클 데이터없음(정상)",
    "NOW 데이터없음(정상)",
    "로그아웃 생략",
    "날짜 '어제' 배지 미확인, 주문 날짜 검증 단계로 계속 진행",
)

_RECOVERED_WARNING_PATTERNS = (
    "dashboard not ready",
    "지표 추출 오류",
    "세션 문제 감지, 재생성 시도",
    "로드 지연",
    "이동 지연",
    "페이지 이동 지연",
    "페이지 로드 지연",
    "메트릭 데이터 로드 지연",
    "새로고침 지연",
    "F5 재시도",
    "계속 진행",
    "실패(무시)",
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


def _build_collection_notification(
    context,
    *,
    collect_task_ids=None,
) -> tuple[str, str, str, bool]:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date") or getattr(ti, "execution_date", None)
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = _target_date_from_context(context)
    run_id = getattr(dag_run, "run_id", getattr(ti, "run_id", "?"))
    current_dag_id = getattr(ti, "dag_id", dag_id)
    execution_time = (
        logical_date.in_timezone(KST).strftime("%Y-%m-%d %H:%M")
        if hasattr(logical_date, "in_timezone")
        else str(logical_date or "?")
    )

    task_instances = []
    if dag_run and hasattr(dag_run, "get_task_instances"):
        task_instances = [t for t in dag_run.get_task_instances() if getattr(t, "task_id", None) != _NOTIFY_TASK_ID]
    task_by_id = {t.task_id: t for t in task_instances}

    normalized_collect_task_ids = _normalize_collect_task_ids(collect_task_ids)
    original_failed = ti.xcom_pull(task_ids="retry_failed", key="original_failed")
    if original_failed is None:
        original_failed = _pull_batch_failures(ti, normalized_collect_task_ids)
    residual_failed = ti.xcom_pull(task_ids="retry_failed", key="residual_failed")
    final_failed = residual_failed if residual_failed is not None else original_failed
    validation = _pull_batch_values(ti, normalized_collect_task_ids, "validation")
    summary_task_ids = (
        "load_accounts",
        *normalized_collect_task_ids,
        "retry_failed",
        "export_to_upload_inbox",
    )
    returns = {
        task_id: ti.xcom_pull(task_ids=task_id, key="return_value")
        for task_id in summary_task_ids
    }

    hard_failures = [
        t for t in task_instances
        if getattr(t, "state", None) in {"failed", "upstream_failed"}
        and getattr(t, "task_id", None) in _CORE_TASK_IDS
    ]
    mismatches = [v for v in validation if isinstance(v, dict) and v.get("matched") is False]
    settle_suspects = [
        v
        for v in validation
        if isinstance(v, dict)
        and (
            v.get("settlement_suspect")
            or (
                v.get("settle_rate") is not None
                and float(v.get("settle_rate") or 0) < 0.9
            )
        )
    ]

    task_rows: list[tuple[str, str, str, str]] = []
    lines = [
        "[배민 수집 결과] {status}",
        f"DAG: {current_dag_id}",
        f"Run: {run_id}",
        f"실행시각(KST): {execution_time}",
        f"target_date: {target_date}",
        "",
        "[수집 요약]",
    ]
    for task_id in summary_task_ids:
        task = task_by_id.get(task_id)
        state = getattr(task, "state", "?") if task else "?"
        duration = _task_duration_seconds(task) if task else None
        duration_text = f"{duration}s" if duration is not None else "-"
        summary = _safe_text(returns.get(task_id), 240)
        lines.append(f"- {task_id}: {state}, {duration_text} | {summary}")
        task_rows.append((task_id, state, duration_text, summary))

    original_retry_count = _count_failed_items(original_failed)
    final_retry_count = _count_failed_items(final_failed)
    if original_retry_count:
        lines.append(
            "원본 수집 실패 신호 "
            f"accounts={len(original_failed.get('accounts') or [])}, "
            f"stores={len(original_failed.get('stores') or [])}, "
            f"orders={len(original_failed.get('orders') or [])}, "
            f"ads={len(original_failed.get('ads') or [])}, "
            f"stages={len(original_failed.get('stages') or [])}"
        )
        lines.append(
            "최종 잔여 실패 "
            f"accounts={len(final_failed.get('accounts') or [])}, "
            f"stores={len(final_failed.get('stores') or [])}, "
            f"orders={len(final_failed.get('orders') or [])}, "
            f"ads={len(final_failed.get('ads') or [])}, "
            f"stages={len(final_failed.get('stages') or [])}"
        )
    if mismatches:
        lines.append(f"orders 검증 불일치 {len(mismatches)}건")
    if settle_suspects:
        lines.append(f"정산정보 수집 의심 {len(settle_suspects)}건")
        for item in settle_suspects[:12]:
            rate = item.get("settle_rate")
            rate_text = "-" if rate is None else f"{float(rate) * 100:.1f}%"
            lines.append(
                f"  - {item.get('store', '?')} [{item.get('status', '?')}] "
                f"rate={rate_text} 정산={item.get('settle_count')}/{item.get('settle_denominator')} "
                f"재시도={item.get('retried', 0)}"
            )

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

    has_final_issue = bool(mismatches or settle_suspects or final_retry_count)
    if hard_failures:
        status = "실패"
    elif has_final_issue:
        status = "부분성공"
    else:
        status = "성공"
    lines[0] = f"[배민 수집 결과] {status}"

    if empty_signal_count:
        lines.extend(["", f"[정상 빈값 신호] 우가클/빈 테이블 로그 {empty_signal_count}건"])
    if recovered_lines:
        lines.extend(["", "[복구된 경고]", *recovered_lines[:24]])
    if problem_lines and status == "성공":
        lines.extend(["", "[참고 로그: 복구/재시도 후 최종 성공]", *problem_lines[:24]])
    elif problem_lines:
        lines.extend(["", "[문제 로그]", *problem_lines[:24]])
    if hard_failures:
        lines.extend(["", "[실패 task 로그 URL]"])
        for task in hard_failures:
            lines.append(f"- {task.task_id}: {getattr(task, 'log_url', '?')}")

    body = "\n".join(lines)[:6000]
    subject = f"[배민 수집 결과] {status} / {target_date}"

    badge_color = "#1f8b4c" if status == "성공" else "#d97706" if status == "부분성공" else "#c0392b"
    problem_title = "참고 로그: 복구/재시도 후 최종 성공" if status == "성공" else "문제 로그"
    problem_html = "".join(f"<li>{html.escape(line)}</li>" for line in problem_lines[:24]) or "<li>없음</li>"
    recovered_html = "".join(f"<li>{html.escape(line)}</li>" for line in recovered_lines[:24]) or "<li>없음</li>"
    settle_html = "".join(
        "<li>"
        + html.escape(
            f"{item.get('store', '?')} [{item.get('status', '?')}] "
            f"rate={float(item.get('settle_rate') or 0) * 100:.1f}% "
            f"정산={item.get('settle_count')}/{item.get('settle_denominator')} "
            f"재시도={item.get('retried', 0)}"
        )
        + "</li>"
        for item in settle_suspects[:24]
    ) or "<li>없음</li>"
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
            <tr><td style="padding:8px 0;font-weight:700;">DAG</td><td>{html.escape(current_dag_id)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">실행시각</td><td>{html.escape(execution_time)}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">원본 수집 실패 신호</td><td>accounts={len(original_failed.get('accounts') or [])}, stores={len(original_failed.get('stores') or [])}, orders={len(original_failed.get('orders') or [])}, ads={len(original_failed.get('ads') or [])}, stages={len(original_failed.get('stages') or [])}</td></tr>
            <tr><td style="padding:8px 0;font-weight:700;">최종 잔여 실패</td><td>accounts={len(final_failed.get('accounts') or [])}, stores={len(final_failed.get('stores') or [])}, orders={len(final_failed.get('orders') or [])}, ads={len(final_failed.get('ads') or [])}, stages={len(final_failed.get('stages') or [])}</td></tr>
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
          <h3 style="margin:20px 0 8px 0;">복구된 경고</h3>
          <ul>{recovered_html}</ul>
          <h3 style="margin:20px 0 8px 0;">정산정보 수집 의심</h3>
          <ul>{settle_html}</ul>
          <h3 style="margin:20px 0 8px 0;">{html.escape(problem_title)}</h3>
          <ul>{problem_html}</ul>
        </div>
      </div>
    </body>
    </html>
    """
    return subject, body, html_body, status != "성공"


def notify_collection_result(*, collect_task_ids=None, **context) -> str:
    subject, body, html_body, should_email = _build_collection_notification(
        context,
        collect_task_ids=collect_task_ids,
    )
    logger.info(body)
    if should_email:
        try:
            _send_alert(subject=subject, body=body, html_content=html_body)
        except Exception as exc:
            logger.warning("Email 결과 알림 실패(무시): %s", exc)
    return subject


def init_staging(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    _local_analytics, local_baemin = _main_stage_paths(context)
    if conf.get("force_restart") or not local_baemin.exists():
        init_empty_staging(local_baemin)
        return f"staging 초기화: {local_baemin}"
    local_baemin.mkdir(parents=True, exist_ok=True)
    logger.info("배민 staging 누적 유지(태스크 재시도): %s", local_baemin)
    return f"staging 유지: {local_baemin}"


def collect_batch(
    *,
    batch_range: str | None = None,
    lane_offset: bool = False,
    **context,
) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    profile = resolve_stability_profile(conf.get("stability_profile") or SCHEDULED_DEFAULT_STABILITY_PROFILE)
    logger.info(
        "수집 안정성 프로필: name=%s driver_restart_every_stores=%s max_session_recovery_per_account=%s",
        profile["name"],
        profile.get("driver_restart_every_stores"),
        profile.get("max_session_recovery_per_account"),
    )
    if lane_offset:
        offset_range = conf.get("lane_offset_range") or _LANE_OFFSET_RANGE
        offset_sec = random.uniform(*offset_range)
        logger.info("레인 B 시작 오프셋 %.0f초", offset_sec)
        time.sleep(offset_sec)
    wait_sec = random.uniform(*profile["initial_stagger_range"])
    logger.info("수집 시작 전 계정 지터 대기 %.0f초", wait_sec)
    time.sleep(wait_sec)

    target_date = _target_date_from_context(context)
    context["ti"].xcom_push(key="target_date", value=target_date)
    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list")
    if not account_list:
        logger.warning("수집 대상 계정 없음")
        return "수집 대상 없음"

    requested_batch = str(conf.get("collect_range") or "").strip()
    if not re.fullmatch(r"batch:\d+/\d+", requested_batch):
        requested_batch = ""
    if batch_range:
        first_batch = "batch:1/4"
        selected_batch = requested_batch or first_batch
        partial_run = bool(conf.get("stores")) or not _batch_chain_enabled(
            conf.get("run_all_batches", True)
        )
        if (requested_batch and batch_range != requested_batch) or (
            partial_run and not requested_batch and batch_range != first_batch
        ):
            logger.info(
                "배치 수집 스킵: task=%s batch=%s requested=%s stores=%s run_all_batches=%r",
                context["ti"].task_id,
                batch_range,
                selected_batch,
                bool(conf.get("stores")),
                conf.get("run_all_batches", True),
            )
            for key in ("failed", "validation", "ad_stores", "store_info_per_account"):
                context["ti"].xcom_push(
                    key=key,
                    value={"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
                    if key == "failed"
                    else [],
                )
            return f"배치 스킵: {batch_range}"
        if not conf.get("stores"):
            account_list = _split_accounts_by_range(account_list, batch_range)
        logger.info(
            "단일 DAG 배치 수집 대상: task=%s range=%s accounts=%d",
            context["ti"].task_id,
            batch_range,
            len(account_list),
        )

    local_analytics, local_baemin = _main_stage_paths(context)
    run_id = _current_run_id(context)
    progress_key = batch_range.replace(":", "_").replace("/", "_") if batch_range else None
    prog_path = progress_path(local_analytics, progress_key)
    orders_only = bool(conf.get("orders_only"))
    resume_progress = None
    if not orders_only and not conf.get("force_restart"):
        resume_progress = load_progress(
            prog_path,
            run_id=run_id,
            target_date=target_date,
        )

    if resume_progress is not None:
        logger.info("배민 수집 이어받기: staging 유지 %s", local_baemin)
    else:
        local_baemin.mkdir(parents=True, exist_ok=True)
        logger.info("배민 배치 staging 누적 유지: %s", local_baemin)
        if not orders_only:
            init_progress(
                prog_path,
                run_id=run_id,
                target_date=target_date,
                total_accounts=len(account_list),
            )

    def _push_interrupted_state() -> None:
        progress = None
        if not orders_only:
            progress = load_progress(
                prog_path,
                run_id=run_id,
                target_date=target_date,
            )
        done_ids = {str(value) for value in (progress or {}).get("done_accounts") or []}
        remaining_accounts = [
            account
            for account in account_list
            if str(account.get("account_id") or "") not in done_ids
        ]
        carry = (progress or {}).get("carry") or {}
        carry_failed = carry.get("failed") or {}
        failed_accounts = list(carry_failed.get("accounts") or [])
        failed_ids = {str(account.get("account_id") or "") for account in failed_accounts}
        failed_accounts.extend(
            account
            for account in remaining_accounts
            if str(account.get("account_id") or "") not in failed_ids
        )
        failed = {
            "accounts": failed_accounts,
            "stores": list(carry_failed.get("stores") or []),
            "orders": list(carry_failed.get("orders") or []),
            "ads": list(carry_failed.get("ads") or []),
            "stages": list(carry_failed.get("stages") or []),
        }
        context["ti"].xcom_push(key="failed", value=failed)
        context["ti"].xcom_push(key="validation", value=list(carry.get("validation") or []))
        context["ti"].xcom_push(key="ad_stores", value=list(carry.get("ad_stores") or []))
        context["ti"].xcom_push(
            key="store_info_per_account",
            value=list(carry.get("store_info_per_account") or []),
        )

    analytics_original, patched_paths = patch_baemin_staging_paths(local_analytics)
    try:
        if orders_only:
            result = pipeline_collect_orders_only(
                account_list,
                target_date=target_date,
                stability_profile=profile["name"],
            )
        else:
            result = pipeline_collect_all(
                account_list,
                target_date=target_date,
                stability_profile=profile["name"],
                woori_only=bool(conf.get("woori_only")),
                progress_file=prog_path,
                progress_run_id=run_id,
            )
    except AirflowTaskTimeout:
        _push_interrupted_state()
        logger.exception(
            "%s 타임아웃 - 미완료 계정만 재시도 대상으로 XCom 저장",
            context["ti"].task_id,
        )
        raise
    except Exception:
        _push_interrupted_state()
        logger.exception(
            "%s 실패 - 미완료 계정만 재시도 대상으로 XCom 저장",
            context["ti"].task_id,
        )
        raise
    finally:
        restore_baemin_staging_paths(analytics_original, patched_paths)

    context["ti"].xcom_push(key="failed", value=result.get("failed", {}))
    context["ti"].xcom_push(key="validation", value=result.get("validation", []))
    context["ti"].xcom_push(key="ad_stores", value=result.get("ad_stores", []))
    store_info_per_account = result.get("store_info_per_account", [])
    context["ti"].xcom_push(key="store_info_per_account", value=store_info_per_account)
    metrics = result.get("metrics")
    if metrics and dag_run:
        write_runtime_metrics(
            run_id=dag_run.run_id,
            stage=context["ti"].task_id,
            payload=metrics,
        )
    return result["summary"]


def collect_all(**context) -> str:
    """legacy B2/B3/B4와 기존 수동 실행 호환용 단일 배치 수집."""
    _local_analytics, local_baemin = _main_stage_paths(context)
    init_empty_staging(local_baemin)
    return collect_batch(**context)


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
    target_date = _target_date_from_context(context)

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
        _send_alert(subject=f"[배민 orders 불일치] {len(mismatches)}건", body=summary)
    _save_validate_log(target_date, "orders 검증", summary)
    return summary


def validate_ad_funnel(**context) -> str:
    from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel

    target_date = _target_date_from_context(context)

    ad_stores = context["ti"].xcom_pull(task_ids="collect_all", key="ad_stores") or []
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

    lines = [
        f"ad_funnel 빈값 검증: 총 {len(ad_stores)}매장 / 빈값 {len(empty)}건 / 재수집 후 잔존 {len(still)}건"
    ]
    for item in still:
        lines.append(f"  - {item.get('store', '?')} 재수집 후에도 빈값")
    summary = "\n".join(lines)
    logger.info(summary)
    if still:
        _send_alert(subject=f"[배민 ad_funnel 빈값] {len(still)}건 잔존", body=summary)
    _save_validate_log(target_date, "ad_funnel 빈값 점검", summary)
    return summary


def validate_toorder(**context) -> str:
    target_date = _target_date_from_context(context)

    account_list = context["ti"].xcom_pull(task_ids="load_accounts", key="account_list") or []
    store_info_per_account = context["ti"].xcom_pull(task_ids="collect_all", key="store_info_per_account") or []
    manual_precheck = context["ti"].xcom_pull(task_ids="precheck_manual_baemin_orders", key="manual_precheck_summary") or {}

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
    context["ti"].xcom_push(key="toorder_result", value=result)

    matched = result.get("matched", False)
    compared = result.get("compared_count", 0)
    retried = result.get("retried_stores", [])
    mismatched = result.get("mismatched_stores", [])
    store_results = result.get("store_results", {})
    gap_stores = result.get("toorder_gap_stores", [])
    missing_brand_stores = result.get("missing_brand_stores", [])
    source_mismatch_stores = result.get("source_mismatch_stores", [])

    lines = [
        f"토더 교차검증[{target_date}]: 비교 {compared}개 매장 / "
        f"{'완전일치' if matched else f'불일치 {len(mismatched)}개'}"
    ]
    for store, info in store_results.items():
        if not info.get("toorder_gap") and not info.get("matched"):
            retry_mark = " (재수집후)" if store in retried else ""
            source_mark = " (원천차이/재수집제외)" if store in source_mismatch_stores else ""
            lines.append(
                f"  - {store} ToOrder={info['toorder']:,} / 배민={info['baemin']:,}"
                f"{retry_mark}{source_mark}"
            )
            if info.get("brand_issue"):
                lines.append(
                    f"    brand_issue={info.get('brand_issue')} "
                    f"expected={info.get('expected_brands', [])} "
                    f"active={info.get('active_brands', [])} "
                    f"missing={info.get('missing_brands', [])} "
                    f"stale={info.get('stale_brands', [])}"
                )
    if gap_stores:
        lines.append(f"ToOrder 값 미수집(계정연결?): {', '.join(gap_stores)}")
    if missing_brand_stores:
        lines.append(f"누락브랜드 의심 매장: {', '.join(sorted(set(missing_brand_stores)))}")
    if source_mismatch_stores:
        lines.append(f"원천 금액 차이(자동재수집 제외): {', '.join(sorted(set(source_mismatch_stores)))}")
    if blind:
        lines.append(f"검증 사각지대: 계정 {expected_accounts}개 중 수집 매장정보 {observed_accounts}개")
        logger.warning(
            "ToOrder 검증 사각지대 감지: expected_accounts=%d observed_accounts=%d compared=%d zero=%s",
            expected_accounts,
            observed_accounts,
            compared,
            compared_blind,
        )
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


def _safe_run_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def trigger_retry_if_needed(**context) -> str:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    target_date = _target_date_from_context(context)

    allowed_account_ids = _loaded_account_ids(context)
    residual_failed = ti.xcom_pull(task_ids="retry_failed", key="residual_failed")
    failed = residual_failed if residual_failed is not None else (ti.xcom_pull(task_ids="collect_all", key="failed") or {})
    failed = _filter_failed_to_loaded_accounts(failed, allowed_account_ids)
    failed_count = count_failed_items(failed)
    if failed_count == 0:
        logger.info("Retry DAG 트리거 스킵: 원본 실패 없음")
        return "Retry DAG 트리거 스킵: 원본 실패 없음"

    failed_account_ids = _unique_failed_account_ids(failed)
    loaded_account_count = len(allowed_account_ids)
    failed_account_count = len(failed_account_ids)
    retry_account_ratio = (
        failed_account_count / loaded_account_count
        if loaded_account_count
        else 0.0
    )
    if loaded_account_count and failed_account_count:
        logger.warning(
            "Retry 대상 계정 비율: failed_accounts=%d/%d ratio=%.1f%% failed_items=%d",
            failed_account_count,
            loaded_account_count,
            retry_account_ratio * 100,
            failed_count,
        )

    toorder_result = ti.xcom_pull(task_ids="validate_toorder", key="toorder_result")
    ad_funnel_result = ti.xcom_pull(task_ids="validate_ad_funnel", key="ad_funnel_result")
    if not retry_needed(toorder_result, ad_funnel_result, failed):
        logger.info("Retry DAG 트리거 스킵: 검증상 추가 재시도 불필요")
        return "Retry DAG 트리거 스킵: 추가 재시도 불필요"

    source_run_id = getattr(dag_run, "run_id", context.get("run_id", "manual"))
    scope_label = _collect_scope_label(context)
    retry_conf = build_retry_conf(
        failed=failed,
        target_date=target_date,
        source_dag_id=ti.dag_id,
        source_run_id=source_run_id,
        attempt=1,
        max_attempts=int(conf.get("max_attempts", 3)),
        stability_profile=conf.get("stability_profile") or SCHEDULED_DEFAULT_STABILITY_PROFILE,
        allowed_account_ids=sorted(allowed_account_ids),
        collect_range=scope_label,
    )
    run_id = (
        f"retry__{target_date.replace('-', '')}__attempt_1__"
        f"{_safe_run_id_part(str(source_run_id))}"
    )

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    try:
        trigger_dag(
            dag_id="DB_Beamin_Macro_Dags_Retry",
            run_id=run_id,
            conf=retry_conf,
        )
    except DagRunAlreadyExists:
        logger.info("Retry DAG run 이미 존재: %s", run_id)
        return f"Retry DAG run 이미 존재: {run_id}"

    logger.info(
        "Retry DAG 트리거 완료: run_id=%s range=%s allowed_accounts=%d failed_count=%d accounts=%d stores=%d orders=%d ads=%d stages=%d",
        run_id,
        retry_conf.get("collect_range") or "-",
        len(retry_conf.get("allowed_account_ids") or []),
        failed_count,
        len(retry_conf.get("failed_account_ids") or []),
        len(retry_conf.get("failed_stores") or []),
        len(retry_conf.get("failed_orders") or []),
        len(retry_conf.get("failed_ads") or []),
        len(retry_conf.get("failed_stages") or []),
    )
    return f"Retry DAG 트리거 완료: {run_id}"


def _batch_chain_enabled(value: Any) -> bool:
    if value is False:
        return False
    if isinstance(value, str) and value.strip().lower() in {"0", "false", "no", "off"}:
        return False
    return True


def _build_single_dag_parallel_lanes() -> DAG:
    parallel_enabled = COLLECT_LANES == 2
    with DAG(
        dag_id=dag_id,
        schedule=SMD_BAEMIN_COLLECT_BATCH1_TIME,
        start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
        catchup=False,
        concurrency=2 if parallel_enabled else 1,
        max_active_runs=1,
        max_active_tasks=2 if parallel_enabled else 1,
        default_args=default_args,
        params={
            "collect_range": None,
            "batch_orchestration": "single_dag",
        },
        is_paused_upon_creation=False,
        tags=["db", "baemin", "crawl"],
    ) as built_dag:
        load_task = PythonOperator(
            task_id="load_accounts",
            python_callable=load_accounts,
        )
        init_staging_task = PythonOperator(
            task_id="init_staging",
            python_callable=init_staging,
            execution_timeout=timedelta(minutes=5),
        )
        batch_tasks = [
            PythonOperator(
                task_id=task_id,
                python_callable=collect_batch,
                op_kwargs={
                    "batch_range": f"batch:{index}/4",
                    "lane_offset": parallel_enabled and task_id == "collect_batch_3",
                },
                pool=BAEMIN_SELENIUM_POOL,
                trigger_rule=(
                    TriggerRule.ALL_SUCCESS
                    if task_id in {lane[0] for lane in _LANES}
                    else TriggerRule.ALL_DONE
                ),
                execution_timeout=timedelta(minutes=300),
            )
            for index, task_id in enumerate(_BATCH_TASK_IDS, start=1)
        ]
        retry_task = PythonOperator(
            task_id="retry_failed",
            python_callable=retry_failed,
            op_kwargs={
                "collect_task_ids": list(_BATCH_TASK_IDS),
                "retry_all_types": False,
            },
            pool=BAEMIN_SELENIUM_POOL,
            trigger_rule=TriggerRule.ALL_DONE,
            retries=0,
            execution_timeout=timedelta(minutes=_RETRY_FAILED_TIMEOUT_MINUTES),
        )
        export_task = PythonOperator(
            task_id="export_to_upload_inbox",
            python_callable=export_to_upload_inbox,
            op_kwargs={"collect_task_ids": list(_BATCH_TASK_IDS)},
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(minutes=30),
        )
        upload_task = PythonOperator(
            task_id="trigger_upload_after_export",
            python_callable=trigger_upload_after_export,
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(minutes=5),
        )
        notify_task = PythonOperator(
            task_id=_NOTIFY_TASK_ID,
            python_callable=notify_collection_result,
            op_kwargs={"collect_task_ids": list(_BATCH_TASK_IDS)},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        load_task >> init_staging_task
        if parallel_enabled:
            init_staging_task >> batch_tasks[0] >> batch_tasks[1]
            init_staging_task >> batch_tasks[2] >> batch_tasks[3]
            [batch_tasks[1], batch_tasks[3]] >> retry_task
        else:
            init_staging_task >> batch_tasks[0]
            batch_tasks[0] >> batch_tasks[1] >> batch_tasks[2] >> batch_tasks[3]
            batch_tasks[3] >> retry_task
        retry_task >> export_task >> upload_task >> notify_task

    return built_dag


dag = _build_single_dag_parallel_lanes()
