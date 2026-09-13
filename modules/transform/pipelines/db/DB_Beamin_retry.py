import logging
import time
from copy import deepcopy
from collections.abc import Iterable

import pendulum

from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel
from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts as pipeline_load_accounts
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori,
    collect_orders_only,
    retry_once_failed,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders
from modules.transform.utility.account import _read_sales_employee_csv

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")
PER_STORE_MAX_RETRY = 3
MAX_RETRY_ATTEMPTS = 3
MIN_ACCOUNT_RETRY_SEC = 20 * 60
MIN_ITEM_RETRY_SEC = 10 * 60


def _unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _account_id(account: dict | None) -> str:
    return str((account or {}).get("account_id") or "").strip()


def _store_key(account_id, store_id) -> str:
    return f"{str(account_id).strip()}::{str(store_id).strip()}"


def _stores_from_payload(value) -> list[dict]:
    if isinstance(value, list):
        return [store for store in value if isinstance(store, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _strip_account(items: list[dict], payload_key: str) -> list[dict]:
    stripped_items: list[dict] = []
    for item in items or []:
        account_id = _account_id(item.get("account"))
        if not account_id:
            continue
        stripped = {"account_id": account_id, payload_key: item.get(payload_key)}
        if "stage" in item:
            stripped["stage"] = item.get("stage")
        stripped_items.append(stripped)
    return stripped_items


def count_failed_items(failed: dict | None) -> int:
    data = failed or {}
    return sum(len(data.get(key) or []) for key in ("accounts", "stores", "orders", "ads", "stages"))


def clamp_retry_attempts(value, *, default: int = MAX_RETRY_ATTEMPTS) -> int:
    try:
        requested = int(value)
    except (TypeError, ValueError):
        requested = int(default)
    if requested < 1:
        return 1
    if requested > MAX_RETRY_ATTEMPTS:
        logger.warning("Retry max_attempts 상한 적용: requested=%s capped=%s", requested, MAX_RETRY_ATTEMPTS)
        return MAX_RETRY_ATTEMPTS
    return requested


def merge_failed_payloads(*payloads: dict | None) -> dict:
    """배치별 실패를 계정/매장 단위로 중복 없이 합친다.

    계정 전체 실패가 있으면 같은 계정의 매장·주문·광고·스테이지 실패는 전체 재수집에
    포함되므로 별도 재시도 대상에서 제거한다.
    """
    accounts_by_id: dict[str, dict] = {}
    stores_by_key: dict[tuple[str, str], dict] = {}
    grouped: dict[str, dict[str, dict[str, dict]]] = {
        "orders": {},
        "ads": {},
    }
    stages_by_key: dict[tuple[str, str, str], dict] = {}
    extras: dict[str, dict[str, object]] = {
        "accounts": {},
        "stores": {},
        "orders": {},
        "ads": {},
        "stages": {},
    }

    for payload in payloads:
        failed = payload or {}
        for account in failed.get("accounts") or []:
            if not isinstance(account, dict):
                extras["accounts"].setdefault(repr(account), account)
                continue
            account_id = _account_id(account)
            if account_id:
                accounts_by_id.setdefault(account_id, account)

        for item in failed.get("stores") or []:
            if not isinstance(item, dict):
                extras["stores"].setdefault(repr(item), item)
                continue
            account = item.get("account") or {}
            store = item.get("store") or {}
            account_id = _account_id(account)
            store_id = str(store.get("store_id") or store.get("store") or "").strip()
            if account_id and store_id:
                stores_by_key.setdefault(
                    (account_id, store_id),
                    {"account": account, "store": store},
                )

        for item in failed.get("stages") or []:
            if not isinstance(item, dict):
                extras["stages"].setdefault(repr(item), item)
                continue
            account = item.get("account") or {}
            store = item.get("store") or {}
            account_id = _account_id(account)
            store_id = str(store.get("store_id") or store.get("store") or "").strip()
            stage = str(item.get("stage") or "").strip()
            if account_id and store_id and stage:
                stages_by_key.setdefault(
                    (account_id, store_id, stage),
                    {"account": account, "store": store, "stage": stage},
                )

        for category in ("orders", "ads"):
            for item in failed.get(category) or []:
                if not isinstance(item, dict):
                    extras[category].setdefault(repr(item), item)
                    continue
                account = item.get("account") or {}
                account_id = _account_id(account)
                if not account_id:
                    continue
                bucket = grouped[category].setdefault(
                    account_id,
                    {"account": account, "stores": {}},
                )
                for store in _stores_from_payload(item.get("stores")):
                    store_id = str(store.get("store_id") or store.get("store") or "").strip()
                    if store_id:
                        bucket["stores"].setdefault(store_id, store)

    full_account_ids = set(accounts_by_id)
    merged_stores = [
        item
        for (account_id, _store_id), item in stores_by_key.items()
        if account_id not in full_account_ids
    ]
    merged_stages = [
        item
        for (account_id, _store_id, _stage), item in stages_by_key.items()
        if account_id not in full_account_ids
    ]
    merged_grouped: dict[str, list[dict]] = {"orders": [], "ads": []}
    for category in ("orders", "ads"):
        for account_id, bucket in grouped[category].items():
            if account_id in full_account_ids:
                continue
            stores = list(bucket["stores"].values())
            if stores:
                merged_grouped[category].append(
                    {"account": bucket["account"], "stores": stores}
                )

    return {
        "accounts": [*accounts_by_id.values(), *extras["accounts"].values()],
        "stores": [*merged_stores, *extras["stores"].values()],
        "orders": [*merged_grouped["orders"], *extras["orders"].values()],
        "ads": [*merged_grouped["ads"], *extras["ads"].values()],
        "stages": [*merged_stages, *extras["stages"].values()],
    }


def _retry_history_from_conf_items(*items_by_payload_key: tuple[list[dict], str]) -> dict[str, int]:
    history: dict[str, int] = {}
    for items, payload_key in items_by_payload_key:
        for item in items or []:
            account_id = str(item.get("account_id") or "").strip()
            for store in _stores_from_payload(item.get(payload_key)):
                store_id = str(store.get("store_id") or "").strip()
                if account_id and store_id:
                    history.setdefault(_store_key(account_id, store_id), 1)
    return history


def build_retry_conf(
    failed: dict,
    target_date: str,
    source_dag_id: str | None = None,
    source_run_id: str | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
    stability_profile: str | None = None,
    allowed_account_ids: list[str] | None = None,
    collect_range: str | None = None,
    orders_only: bool = False,
) -> dict:
    failed_accounts = failed.get("accounts") or []
    failed_stores = _strip_account(failed.get("stores") or [], "store")
    failed_orders = _strip_account(failed.get("orders") or [], "stores")
    failed_ads = _strip_account(failed.get("ads") or [], "stores")
    failed_stages = _strip_account(failed.get("stages") or [], "store")
    retry_history = _retry_history_from_conf_items(
        (failed_stores, "store"),
        (failed_orders, "stores"),
        (failed_ads, "stores"),
        (failed_stages, "store"),
    )

    failed_account_ids = _unique(
        [
            *(_account_id(account) for account in failed_accounts),
            *(item.get("account_id") for item in failed_stores),
            *(item.get("account_id") for item in failed_orders),
            *(item.get("account_id") for item in failed_ads),
            *(item.get("account_id") for item in failed_stages),
        ]
    )

    conf = {
        "attempt": int(attempt),
        "max_attempts": clamp_retry_attempts(max_attempts),
        "target_date": target_date,
        "source_dag_id": source_dag_id,
        "source_run_id": source_run_id,
        "stability_profile": stability_profile,
        "orders_only": bool(orders_only),
        "failed_account_ids": failed_account_ids,
        "failed_accounts_ids_only": _unique(_account_id(account) for account in failed_accounts),
        "failed_stores": failed_stores,
        "failed_orders": failed_orders,
        "failed_ads": failed_ads,
        "failed_stages": failed_stages,
        "retry_history": retry_history,
    }
    allowed_ids = _unique(allowed_account_ids or [])
    if allowed_ids:
        conf["allowed_account_ids"] = allowed_ids
    if str(collect_range or "").strip():
        conf["collect_range"] = str(collect_range).strip()
    return conf


def _strip_store_runtime_fields(store: dict | None) -> dict:
    return {
        key: value
        for key, value in (store or {}).items()
        if key not in {"account_id", "password"}
    }


def _group_retry_stores(items: list[dict], payload_key: str) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    seen: dict[str, set[str]] = {}
    for item in items or []:
        account_id = str(item.get("account_id") or "").strip()
        nested_store = item.get("store")
        store_source = nested_store if isinstance(nested_store, dict) else item
        store = _strip_store_runtime_fields(store_source)
        store_id = str(store.get("store_id") or "").strip()
        if not account_id or not store_id:
            continue
        bucket_seen = seen.setdefault(account_id, set())
        if store_id in bucket_seen:
            continue
        bucket_seen.add(store_id)
        grouped.setdefault(account_id, []).append(store)
    return [
        {"account_id": account_id, payload_key: stores if payload_key == "stores" else stores[0]}
        for account_id, stores in grouped.items()
        if stores
    ]


def _seed_retry_history(previous_conf: dict) -> dict[str, int]:
    if "retry_history" in previous_conf:
        return dict(previous_conf.get("retry_history") or {})
    return _retry_history_from_conf_items(
        (previous_conf.get("failed_stores") or [], "store"),
        (previous_conf.get("failed_orders") or [], "stores"),
        (previous_conf.get("failed_ads") or [], "stores"),
        (previous_conf.get("failed_stages") or [], "store"),
    )


def _filter_retry_group_by_history(
    items: list[dict],
    *,
    prev_history: dict[str, int],
    new_history: dict[str, int],
    incremented: set[str],
    exhausted: list[dict] | None = None,
) -> list[dict]:
    filtered: list[dict] = []
    for item in items or []:
        account_id = str(item.get("account_id") or "").strip()
        kept_stores: list[dict] = []
        for store in _stores_from_payload(item.get("stores")):
            store_id = str(store.get("store_id") or "").strip()
            if not account_id or not store_id:
                continue
            key = _store_key(account_id, store_id)
            prev = int(prev_history.get(key) or 0)
            if prev >= PER_STORE_MAX_RETRY:
                if exhausted is not None:
                    exhausted.append(
                        {
                            "account_id": account_id,
                            "store_id": store_id,
                            "store": str(store.get("store") or "").strip(),
                            "attempts": prev,
                        }
                    )
                continue
            kept_stores.append(store)
            if key not in incremented:
                new_history[key] = prev + 1
                incremented.add(key)
        if kept_stores:
            filtered.append({"account_id": account_id, "stores": kept_stores})
    return filtered


def _store_lookup_from_retry_payload(retry_payload: dict) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for item in retry_payload.get("store_info_per_account") or []:
        account_id = str(item.get("account_id") or "").strip()
        for store in item.get("stores") or []:
            store_name = str(store.get("store") or "").strip()
            if account_id and store_name:
                lookup[store_name] = {"account_id": account_id, "store": store}
    for item in retry_payload.get("ad_store_infos") or []:
        store_name = str(item.get("store") or "").strip()
        account_id = str(item.get("account_id") or "").strip()
        if account_id and store_name:
            lookup[store_name] = {"account_id": account_id, "store": item}
    return lookup


def _toorder_notification_snapshot(result: dict | None) -> dict:
    result = result or {}
    store_results: dict[str, dict] = {}
    for store, item in (result.get("store_results") or {}).items():
        item = item or {}
        store_results[str(store)] = {
            "baemin": int(item.get("baemin") or 0),
            "toorder": int(item.get("toorder") or 0),
            "matched": bool(item.get("matched")),
            "toorder_gap": bool(item.get("toorder_gap")),
            "brand_issue": item.get("brand_issue"),
            "source_mismatch": bool(item.get("source_mismatch")),
            "source_mismatch_reason": item.get("source_mismatch_reason"),
            "amount_only": bool(item.get("amount_only")),
        }
    return {
        "compared": int(result.get("compared_count") or result.get("compared") or 0),
        "store_results": store_results,
        "mismatched_stores": sorted(set(result.get("mismatched_stores") or [])),
        "gap_stores": sorted(set(result.get("toorder_gap_stores") or result.get("gap_stores") or [])),
        "missing_brand_stores": sorted(set(result.get("missing_brand_stores") or [])),
        "source_mismatch_stores": sorted(set(result.get("source_mismatch_stores") or [])),
        "amount_only_mismatch_stores": sorted(set(result.get("amount_only_mismatch_stores") or [])),
        "restored_stores": sorted(set(result.get("restored_stores") or [])),
    }


def merge_toorder_notification_snapshot(root_toorder: dict | None, latest_result: dict | None) -> dict:
    root = deepcopy(root_toorder or {})
    if latest_result is None:
        return root

    latest = _toorder_notification_snapshot(latest_result)
    latest_stores = set(latest.get("store_results") or {})
    for key in (
        "mismatched_stores",
        "gap_stores",
        "missing_brand_stores",
        "source_mismatch_stores",
        "amount_only_mismatch_stores",
        "restored_stores",
    ):
        latest_stores.update(latest.get(key) or [])

    merged_store_results = dict(root.get("store_results") or {})
    merged_store_results.update(latest.get("store_results") or {})
    root["store_results"] = merged_store_results
    root["compared"] = max(int(root.get("compared") or 0), int(latest.get("compared") or 0))

    for key in (
        "mismatched_stores",
        "gap_stores",
        "missing_brand_stores",
        "source_mismatch_stores",
        "amount_only_mismatch_stores",
        "restored_stores",
    ):
        previous = set(root.get(key) or [])
        previous -= latest_stores
        previous.update(latest.get(key) or [])
        root[key] = sorted(previous)

    for key in ("blind", "expected_accounts", "observed_accounts", "store_info_fallback_accounts"):
        if key in latest_result:
            root[key] = latest_result.get(key)

    return root


def merge_toorder_notification_context(notification_context: dict | None, latest_result: dict | None) -> dict:
    context = deepcopy(notification_context or {})
    context["toorder"] = merge_toorder_notification_snapshot(context.get("toorder") or {}, latest_result)
    return context


def build_next_retry_conf(
    *,
    previous_conf: dict,
    retry_payload: dict,
    toorder_result: dict | None,
    ad_funnel_result: dict | None,
    attempt: int,
    max_attempts: int,
) -> dict:
    target_date = retry_payload.get("target_date") or previous_conf.get("target_date")
    source_dag_id = previous_conf.get("source_dag_id")
    source_run_id = previous_conf.get("source_run_id")
    prev_history = _seed_retry_history(previous_conf)

    ad_items = [
        item
        for item in (ad_funnel_result or {}).get("still_empty") or []
        if item.get("account_id") and item.get("store_id")
    ]

    store_lookup = _store_lookup_from_retry_payload(retry_payload)
    toorder_items: list[dict] = []
    store_results = (toorder_result or {}).get("store_results") or {}
    problem_stores = set((toorder_result or {}).get("mismatched_stores") or [])
    problem_stores.update((toorder_result or {}).get("missing_brand_stores") or [])
    problem_stores -= set((toorder_result or {}).get("source_mismatch_stores") or [])
    for store_name in problem_stores:
        info = store_results.get(store_name) or {}
        if info.get("toorder_gap"):
            continue
        source = store_lookup.get(store_name)
        if source:
            toorder_items.append({"account_id": source["account_id"], "store": source["store"]})

    failed_ads = _group_retry_stores(ad_items, "stores")
    # ToOrder 불일치는 배민 orders 파티션을 다시 채워야 한다.
    # failed_stores는 now/우가클 매장 재시도 경로라 orders 수집을 수행하지 않는다.
    failed_orders = _group_retry_stores(toorder_items, "stores")
    new_history = dict(prev_history)
    incremented: set[str] = set()
    failed_ads = _filter_retry_group_by_history(
        failed_ads,
        prev_history=prev_history,
        new_history=new_history,
        incremented=incremented,
    )
    toorder_retry_exhausted: list[dict] = []
    failed_orders = _filter_retry_group_by_history(
        failed_orders,
        prev_history=prev_history,
        new_history=new_history,
        incremented=incremented,
        exhausted=toorder_retry_exhausted,
    )
    failed_account_ids = _unique(
        [
            *(item.get("account_id") for item in failed_ads),
            *(item.get("account_id") for item in failed_orders),
        ]
    )

    next_notification_context = merge_toorder_notification_context(
        previous_conf.get("notification_context") or {},
        toorder_result,
    )

    return {
        "attempt": int(attempt),
        "max_attempts": clamp_retry_attempts(max_attempts),
        "target_date": target_date,
        "source_dag_id": source_dag_id,
        "source_run_id": source_run_id,
        "stability_profile": previous_conf.get("stability_profile"),
        "orders_only": bool(previous_conf.get("orders_only")),
        "retry_wait_sec": previous_conf.get("retry_wait_sec"),
        "allowed_account_ids": _unique(previous_conf.get("allowed_account_ids") or []),
        "collect_range": previous_conf.get("collect_range"),
        "failed_account_ids": failed_account_ids,
        "failed_accounts_ids_only": [],
        "failed_stores": [],
        "failed_orders": failed_orders,
        "failed_ads": failed_ads,
        "failed_stages": [],
        "retry_history": new_history,
        "toorder_retry_exhausted": toorder_retry_exhausted,
        "toorder_possible_mismatch_stores": sorted(
            {
                item.get("store")
                for item in toorder_retry_exhausted
                if item.get("store")
            }
        ),
        "notification_context": next_notification_context,
    }


def split_retry_conf_by_lane(conf: dict, lanes: int = 2) -> list[dict]:
    lanes = max(int(lanes or 1), 1)
    source = deepcopy(conf or {})
    account_ids = sorted(_unique(source.get("failed_account_ids") or []))
    base_size, extra = divmod(len(account_ids), lanes)
    lane_account_ids: list[set[str]] = []
    start = 0
    for index in range(lanes):
        end = start + base_size + (1 if index < extra else 0)
        lane_account_ids.append(set(account_ids[start:end]))
        start = end

    def filter_items(items: list[dict], lane_ids: set[str]) -> list[dict]:
        return [
            item
            for item in items or []
            if str((item or {}).get("account_id") or "").strip() in lane_ids
        ]

    result: list[dict] = []
    for lane_ids in lane_account_ids:
        lane_conf = deepcopy(source)
        lane_conf["failed_account_ids"] = [account_id for account_id in account_ids if account_id in lane_ids]
        lane_conf["failed_accounts_ids_only"] = [
            account_id
            for account_id in _unique(source.get("failed_accounts_ids_only") or [])
            if account_id in lane_ids
        ]
        lane_conf["failed_stores"] = filter_items(source.get("failed_stores") or [], lane_ids)
        lane_conf["failed_orders"] = filter_items(source.get("failed_orders") or [], lane_ids)
        lane_conf["failed_ads"] = filter_items(source.get("failed_ads") or [], lane_ids)
        lane_conf["failed_stages"] = filter_items(source.get("failed_stages") or [], lane_ids)
        result.append(lane_conf)
    return result


def retry_needed(toorder_result: dict | None, ad_funnel_result: dict | None, failed: dict | None = None) -> bool:
    if count_failed_items(failed) > 0:
        return True

    if ad_funnel_result and ad_funnel_result.get("still_empty"):
        return True

    if toorder_result is None:
        return False

    if toorder_result.get("missing_brand_stores"):
        return True

    mismatched = set(toorder_result.get("mismatched_stores") or [])
    mismatched -= set(toorder_result.get("source_mismatch_stores") or [])
    # 건수 동일·금액만 차이는 재수집해도 좁혀지지 않으므로 재시도 사유가 아니다.
    mismatched -= set(toorder_result.get("amount_only_mismatch_stores") or [])
    if mismatched:
        store_results = toorder_result.get("store_results") or {}
        return any(not (store_results.get(store) or {}).get("toorder_gap") for store in mismatched)

    return False


def _load_account_map(account_ids: list[str] | None = None) -> dict[str, dict]:
    accounts = pipeline_load_accounts(target_stores=[])
    account_map = {_account_id(account): account for account in accounts if _account_id(account)}
    if account_map or not account_ids:
        return account_map

    target_ids = {str(account_id).strip() for account_id in account_ids if str(account_id).strip()}
    df = _read_sales_employee_csv()
    required = {"플랫폼", "계정ID", "계정PW", "매장명"}
    if df.empty or not required.issubset(df.columns):
        return {}

    platform_mask = df["플랫폼"].astype(str).str.strip().eq("배달의 민족")
    account_mask = df["계정ID"].astype(str).str.strip().isin(target_ids)
    filtered = df[platform_mask & account_mask].copy()
    if not filtered.empty:
        logger.warning("Retry 계정 복원 fallback 사용: account_ids=%s", sorted(target_ids))

    return {
        str(row["계정ID"]).strip(): {
            "account_id": str(row["계정ID"]).strip(),
            "password": str(row["계정PW"]).strip(),
            "store_name": str(row["매장명"]).strip(),
        }
        for _, row in filtered.iterrows()
        if str(row.get("계정ID", "")).strip() and str(row.get("계정PW", "")).strip()
    }


def restore_meta_credentials(meta: dict) -> dict:
    """공유 inbox에서 제거한 비밀번호를 중앙 계정 원본으로 다시 결합한다."""
    restored = deepcopy(meta or {})
    account_ids: list[str] = []

    def remember(account_id: object) -> None:
        text = str(account_id or "").strip()
        if text and text not in account_ids:
            account_ids.append(text)

    for account in restored.get("account_list") or []:
        remember((account or {}).get("account_id"))
    for store in restored.get("ad_stores") or []:
        remember((store or {}).get("account_id"))
    for failed_key in ("original_failed", "failed"):
        failed = restored.get(failed_key) or {}
        for category in ("accounts", "stores", "orders", "ads", "stages"):
            for item in failed.get(category) or []:
                account = item if category == "accounts" else (item or {}).get("account")
                remember((account or {}).get("account_id"))

    account_map = _load_account_map(account_ids)

    def add_password(item: dict | None) -> None:
        if not isinstance(item, dict):
            return
        account_id = str(item.get("account_id") or "").strip()
        password = (account_map.get(account_id) or {}).get("password")
        if password:
            item["password"] = password

    for account in restored.get("account_list") or []:
        add_password(account)
    for store in restored.get("ad_stores") or []:
        add_password(store)
    for failed_key in ("original_failed", "failed"):
        failed = restored.get(failed_key) or {}
        for category in ("accounts", "stores", "orders", "ads", "stages"):
            for item in failed.get(category) or []:
                add_password(item if category == "accounts" else (item or {}).get("account"))

    missing = [account_id for account_id in account_ids if account_id not in account_map]
    if missing:
        logger.warning("upload meta 계정 인증정보 복원 실패: %s", missing)
    return restored


def restore_failed_from_conf(conf: dict) -> tuple[dict, list[dict]]:
    failed_account_ids = _unique(conf.get("failed_account_ids") or [])
    allowed_account_ids = set(_unique(conf.get("allowed_account_ids") or []))
    if allowed_account_ids:
        failed_account_ids = [
            account_id
            for account_id in failed_account_ids
            if account_id in allowed_account_ids
        ]
    account_map = _load_account_map(failed_account_ids)
    failed_accounts_ids_only = set(_unique(conf.get("failed_accounts_ids_only") or []))
    if allowed_account_ids:
        failed_accounts_ids_only &= allowed_account_ids
    retry_accounts = [account_map[account_id] for account_id in failed_account_ids if account_id in account_map]

    missing_ids = [account_id for account_id in failed_account_ids if account_id not in account_map]
    if missing_ids:
        logger.warning("Retry 계정 복원 실패: %s", missing_ids)

    def restore_items(items: list[dict], payload_key: str) -> list[dict]:
        restored: list[dict] = []
        for item in items or []:
            account_id = str(item.get("account_id") or "").strip()
            if allowed_account_ids and account_id not in allowed_account_ids:
                continue
            account = account_map.get(account_id)
            if not account:
                continue
            restored_item = {"account": account, payload_key: item.get(payload_key)}
            if "stage" in item:
                restored_item["stage"] = item.get("stage")
            restored.append(restored_item)
        return restored

    failed = {
        "accounts": [account_map[account_id] for account_id in failed_accounts_ids_only if account_id in account_map],
        "stores": restore_items(conf.get("failed_stores") or [], "store"),
        "orders": restore_items(conf.get("failed_orders") or [], "stores"),
        "ads": restore_items(conf.get("failed_ads") or [], "stores"),
        "stages": restore_items(conf.get("failed_stages") or [], "store"),
    }
    return failed, retry_accounts


def _store_info_per_account_from_failed(failed: dict) -> list[dict]:
    by_account: dict[str, list[dict]] = {}

    def add_stores(account_id: str, stores: list[dict]) -> None:
        bucket = by_account.setdefault(account_id, [])
        seen = {str(store.get("store_id") or "") for store in bucket}
        for store in stores:
            store_id = str((store or {}).get("store_id") or "")
            if store_id and store_id in seen:
                continue
            if store:
                bucket.append(store)
                if store_id:
                    seen.add(store_id)

    for item in failed.get("stores") or []:
        account_id = _account_id(item.get("account"))
        add_stores(account_id, [item.get("store") or {}])

    for item in failed.get("stages") or []:
        account_id = _account_id(item.get("account"))
        add_stores(account_id, [item.get("store") or {}])

    for key in ("orders", "ads"):
        for item in failed.get(key) or []:
            account_id = _account_id(item.get("account"))
            add_stores(account_id, item.get("stores") or [])

    return [
        {"account_id": account_id, "stores": stores}
        for account_id, stores in by_account.items()
        if account_id and stores
    ]


def _ad_store_infos_from_failed(failed: dict) -> list[dict]:
    infos: list[dict] = []
    for item in failed.get("ads") or []:
        account = item.get("account") or {}
        account_id = _account_id(account)
        password = account.get("password")
        for store in item.get("stores") or []:
            infos.append({**store, "account_id": account_id, "password": password})
    return infos


def _empty_failed_payload() -> dict:
    return {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}


def _extend_failed_payload(target: dict, source: dict | None) -> None:
    data = source or {}
    for key in ("accounts", "stores", "orders", "ads", "stages"):
        target.setdefault(key, []).extend(data.get(key) or [])


def _remaining_sec(deadline_at: float | None) -> float | None:
    if deadline_at is None:
        return None
    return float(deadline_at) - time.monotonic()


def _has_retry_budget(deadline_at: float | None, minimum_sec: int) -> bool:
    remaining = _remaining_sec(deadline_at)
    return remaining is None or remaining >= minimum_sec


def _split_item_failed_units(item_failed: dict) -> list[dict]:
    units: list[dict] = []
    for item in item_failed.get("stores") or []:
        units.append({"accounts": [], "stores": [item], "orders": [], "ads": [], "stages": []})
    for item in item_failed.get("stages") or []:
        units.append({"accounts": [], "stores": [], "orders": [], "ads": [], "stages": [item]})
    for category in ("orders", "ads"):
        for item in item_failed.get(category) or []:
            account = item.get("account")
            for store in item.get("stores") or []:
                unit = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
                unit[category].append({"account": account, "stores": [store]})
                units.append(unit)
    return units


def _sanitize_account(account: dict | None) -> dict:
    return {
        key: value
        for key, value in (account or {}).items()
        if key != "password"
    }


def _sanitize_failed_payload(failed: dict | None) -> dict:
    data = failed or {}
    sanitized = _empty_failed_payload()
    sanitized["accounts"] = [_sanitize_account(account) for account in data.get("accounts") or []]
    sanitized["stores"] = [
        {
            "account": _sanitize_account((item or {}).get("account")),
            "store": _strip_store_runtime_fields((item or {}).get("store")),
        }
        for item in data.get("stores") or []
        if isinstance(item, dict)
    ]
    for category in ("orders", "ads"):
        sanitized[category] = [
            {
                "account": _sanitize_account((item or {}).get("account")),
                "stores": [
                    _strip_store_runtime_fields(store)
                    for store in (item or {}).get("stores") or []
                    if isinstance(store, dict)
                ],
            }
            for item in data.get(category) or []
            if isinstance(item, dict)
        ]
    sanitized["stages"] = [
        {
            "account": _sanitize_account((item or {}).get("account")),
            "store": _strip_store_runtime_fields((item or {}).get("store")),
            "stage": (item or {}).get("stage"),
        }
        for item in data.get("stages") or []
        if isinstance(item, dict)
    ]
    return sanitized


def sanitize_retry_payload(payload: dict) -> dict:
    def strip_password(items: list[dict]) -> list[dict]:
        return [{key: value for key, value in item.items() if key != "password"} for item in items or []]

    return {
        "target_date": payload.get("target_date"),
        "orders_only": bool(payload.get("orders_only")),
        "retry_result": payload.get("retry_result"),
        "store_info_per_account": payload.get("store_info_per_account") or [],
        "ad_store_infos": strip_password(payload.get("ad_store_infos") or []),
        "residual_failed": _sanitize_failed_payload(payload.get("residual_failed") or {}),
    }


def merge_retry_payloads(*payloads: dict | None) -> dict:
    target_date = None
    orders_only = False
    retry_results: list[str] = []
    store_info_by_account: dict[str, dict] = {}
    ad_infos: dict[tuple[str, str], dict] = {}

    for payload in payloads:
        if not payload:
            continue
        if target_date is None and payload.get("target_date"):
            target_date = payload.get("target_date")
        orders_only = orders_only or bool(payload.get("orders_only"))
        retry_result = str(payload.get("retry_result") or "").strip()
        if retry_result:
            retry_results.append(retry_result)
        for item in payload.get("store_info_per_account") or []:
            account_id = str((item or {}).get("account_id") or "").strip()
            if not account_id:
                continue
            bucket = store_info_by_account.setdefault(account_id, {"account_id": account_id, "stores": []})
            seen = {
                str((store or {}).get("store_id") or (store or {}).get("store") or "").strip()
                for store in bucket["stores"]
            }
            for store in (item or {}).get("stores") or []:
                marker = str((store or {}).get("store_id") or (store or {}).get("store") or "").strip()
                if marker and marker in seen:
                    continue
                bucket["stores"].append(store)
                if marker:
                    seen.add(marker)
        for item in payload.get("ad_store_infos") or []:
            account_id = str((item or {}).get("account_id") or "").strip()
            store_id = str((item or {}).get("store_id") or (item or {}).get("store") or "").strip()
            if account_id and store_id:
                ad_infos.setdefault((account_id, store_id), item)

    return {
        "target_date": target_date,
        "orders_only": orders_only,
        "retry_result": "\n".join(retry_results),
        "store_info_per_account": list(store_info_by_account.values()),
        "ad_store_infos": list(ad_infos.values()),
        "residual_failed": merge_failed_payloads(
            *((payload or {}).get("residual_failed") or {} for payload in payloads)
        ),
    }


def retry_collect_from_conf(
    conf: dict,
    *,
    deadline_at: float | None = None,
    partial_result_callback=None,
) -> dict:
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    orders_only = bool(conf.get("orders_only"))
    failed, retry_accounts = restore_failed_from_conf(conf)

    account_failed = {"accounts": failed.get("accounts") or [], "stores": [], "orders": [], "ads": [], "stages": []}
    item_failed = {
        "accounts": [],
        "stores": failed.get("stores") or [],
        "orders": failed.get("orders") or [],
        "ads": failed.get("ads") or [],
        "stages": failed.get("stages") or [],
    }

    account_result: dict = {
        "summary": "",
        "failed": _empty_failed_payload(),
        "store_info_per_account": [],
        "ad_stores": [],
    }
    residual_failed = _empty_failed_payload()
    retry_result_parts: list[str] = []

    store_info_per_account = _store_info_per_account_from_failed(failed)
    ad_store_infos = _ad_store_infos_from_failed(failed)

    def current_payload() -> dict:
        retry_result = "\n".join(part for part in retry_result_parts if part).strip()
        if not retry_result:
            retry_result = "재시도 대상 없음"
        return {
            "target_date": target_date,
            "orders_only": orders_only,
            "failed": failed,
            "retry_accounts": retry_accounts,
            "retry_result": retry_result,
            "account_result": account_result,
            "store_info_per_account": store_info_per_account,
            "ad_store_infos": ad_store_infos,
            "residual_failed": residual_failed,
        }

    def emit_partial() -> None:
        if partial_result_callback is not None:
            partial_result_callback(current_payload())

    accounts = account_failed["accounts"]
    for index, account in enumerate(accounts):
        if not _has_retry_budget(deadline_at, MIN_ACCOUNT_RETRY_SEC):
            remaining_accounts = accounts[index:]
            residual_failed["accounts"].extend(remaining_accounts)
            logger.warning(
                "Retry 계정 재수집 deadline 임박으로 잔여 이월: remaining=%s accounts=%d",
                _remaining_sec(deadline_at),
                len(remaining_accounts),
            )
            emit_partial()
            break
        try:
            if orders_only:
                result = collect_orders_only(
                    [account],
                    target_date=target_date,
                    stability_profile=conf.get("stability_profile"),
                )
            else:
                result = collect_now_and_woori(
                    [account],
                    target_date=target_date,
                    stability_profile=conf.get("stability_profile"),
                    _raise_on_total_failure=False,
                )
        except Exception as exc:
            logger.exception("Retry 계정 재수집 실패 - 잔여 실패로 전파: %s", exc)
            result = {
                "summary": f"계정 재수집 실패: {exc}",
                "failed": {"accounts": [account], "stores": [], "orders": [], "ads": [], "stages": []},
                "store_info_per_account": [],
                "ad_stores": [],
            }
        if isinstance(result, dict):
            summary = str(result.get("summary") or "")
            if summary:
                retry_result_parts.append(summary)
                account_result["summary"] = "\n".join(
                    part
                    for part in (account_result.get("summary"), summary)
                    if part
                )
            _extend_failed_payload(account_result["failed"], result.get("failed"))
            _extend_failed_payload(residual_failed, result.get("failed"))
            account_result["store_info_per_account"].extend(result.get("store_info_per_account") or [])
            account_result["ad_stores"].extend(result.get("ad_stores") or [])
            store_info_per_account.extend(result.get("store_info_per_account") or [])
            ad_store_infos.extend(result.get("ad_stores") or [])
        else:
            retry_result_parts.append(str(result))
        emit_partial()

    item_units = _split_item_failed_units(item_failed)
    processed_item = False
    for index, unit_failed in enumerate(item_units):
        if not _has_retry_budget(deadline_at, MIN_ITEM_RETRY_SEC):
            for remaining_unit in item_units[index:]:
                _extend_failed_payload(residual_failed, remaining_unit)
            logger.warning(
                "Retry 매장 재수집 deadline 임박으로 잔여 이월: remaining=%s units=%d",
                _remaining_sec(deadline_at),
                len(item_units) - index,
            )
            emit_partial()
            break
        processed_item = True
        item_result = retry_once_failed(
            unit_failed,
            target_date=target_date,
            stability_profile=conf.get("stability_profile"),
            orders_only=orders_only,
        )
        if isinstance(item_result, dict):
            summary = str(item_result.get("summary") or "")
            if summary:
                retry_result_parts.append(summary)
            _extend_failed_payload(residual_failed, item_result.get("residual_failed"))
        else:
            retry_result_parts.append(str(item_result))
        emit_partial()

    if not accounts and not processed_item and not item_units:
        retry_result_parts.append("재시도 대상 없음")

    return current_payload()


def _accounts_for_store_info(store_info_per_account: list[dict]) -> list[dict]:
    account_ids = _unique(item.get("account_id") for item in store_info_per_account or [])
    account_map = _load_account_map(account_ids)
    return [account_map[account_id] for account_id in account_ids if account_id in account_map]


def _restore_ad_passwords(ad_store_infos: list[dict]) -> list[dict]:
    account_map = _load_account_map()
    restored: list[dict] = []
    for item in ad_store_infos or []:
        account_id = str(item.get("account_id") or "").strip()
        password = (account_map.get(account_id) or {}).get("password")
        if not account_id or not password:
            continue
        restored.append({**item, "password": password})
    return restored


def validate_retry_toorder(retry_payload: dict) -> dict:
    target_date = retry_payload["target_date"]
    store_info_per_account = retry_payload.get("store_info_per_account") or []
    retry_accounts = _accounts_for_store_info(store_info_per_account)

    return validate_toorder_orders(retry_accounts, store_info_per_account, target_date)


def validate_retry_ad_funnel(
    retry_payload: dict,
    *,
    deadline_at: float | None = None,
) -> dict:
    if retry_payload.get("orders_only"):
        return {"empty_stores": [], "retried": [], "still_empty": []}
    target_date = retry_payload["target_date"]
    ad_store_infos = _restore_ad_passwords(retry_payload.get("ad_store_infos") or [])
    if not ad_store_infos:
        return {"empty_stores": [], "retried": [], "still_empty": []}
    return _validate_and_retry_ad_funnel(
        ad_store_infos,
        target_date,
        deadline_at=deadline_at,
    )


def validate_retry_collection(retry_payload: dict) -> dict:
    toorder_result = validate_retry_toorder(retry_payload)
    ad_funnel_result = validate_retry_ad_funnel(retry_payload)
    return {
        "toorder_result": toorder_result,
        "ad_funnel_result": ad_funnel_result,
        "needs_next": retry_needed(toorder_result, ad_funnel_result, None),
    }
