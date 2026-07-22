import logging
from collections.abc import Iterable

import pendulum

from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import _validate_and_retry_ad_funnel
from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts as pipeline_load_accounts
from modules.transform.pipelines.db.DB_Beamin_combined import (
    collect_now_and_woori,
    retry_once_failed,
)
from modules.transform.pipelines.db.DB_Beamin_Macro_validate import validate_toorder_orders
from modules.transform.utility.account import _read_sales_employee_csv

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")
PER_STORE_MAX_RETRY = 2


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
        stripped_items.append(
            {
                "account_id": account_id,
                payload_key: item.get(payload_key),
            }
        )
    return stripped_items


def count_failed_items(failed: dict | None) -> int:
    data = failed or {}
    return sum(len(data.get(key) or []) for key in ("accounts", "stores", "orders", "ads"))


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
) -> dict:
    failed_accounts = failed.get("accounts") or []
    failed_stores = _strip_account(failed.get("stores") or [], "store")
    failed_orders = _strip_account(failed.get("orders") or [], "stores")
    failed_ads = _strip_account(failed.get("ads") or [], "stores")
    retry_history = _retry_history_from_conf_items(
        (failed_stores, "store"),
        (failed_orders, "stores"),
        (failed_ads, "stores"),
    )

    failed_account_ids = _unique(
        [
            *(_account_id(account) for account in failed_accounts),
            *(item.get("account_id") for item in failed_stores),
            *(item.get("account_id") for item in failed_orders),
            *(item.get("account_id") for item in failed_ads),
        ]
    )

    return {
        "attempt": int(attempt),
        "max_attempts": int(max_attempts),
        "target_date": target_date,
        "source_dag_id": source_dag_id,
        "source_run_id": source_run_id,
        "stability_profile": stability_profile,
        "failed_account_ids": failed_account_ids,
        "failed_accounts_ids_only": _unique(_account_id(account) for account in failed_accounts),
        "failed_stores": failed_stores,
        "failed_orders": failed_orders,
        "failed_ads": failed_ads,
        "retry_history": retry_history,
    }


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
    )


def _filter_retry_group_by_history(
    items: list[dict],
    *,
    prev_history: dict[str, int],
    new_history: dict[str, int],
    incremented: set[str],
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
    failed_orders = _filter_retry_group_by_history(
        failed_orders,
        prev_history=prev_history,
        new_history=new_history,
        incremented=incremented,
    )
    failed_account_ids = _unique(
        [
            *(item.get("account_id") for item in failed_ads),
            *(item.get("account_id") for item in failed_orders),
        ]
    )

    return {
        "attempt": int(attempt),
        "max_attempts": int(max_attempts),
        "target_date": target_date,
        "source_dag_id": source_dag_id,
        "source_run_id": source_run_id,
        "stability_profile": previous_conf.get("stability_profile"),
        "failed_account_ids": failed_account_ids,
        "failed_accounts_ids_only": [],
        "failed_stores": [],
        "failed_orders": failed_orders,
        "failed_ads": failed_ads,
        "retry_history": new_history,
        "notification_context": previous_conf.get("notification_context") or {},
    }


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
    if mismatched:
        retried = set(toorder_result.get("retried_stores") or [])
        return bool(mismatched - retried)

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


def restore_failed_from_conf(conf: dict) -> tuple[dict, list[dict]]:
    failed_account_ids = _unique(conf.get("failed_account_ids") or [])
    account_map = _load_account_map(failed_account_ids)
    failed_accounts_ids_only = set(_unique(conf.get("failed_accounts_ids_only") or []))
    retry_accounts = [account_map[account_id] for account_id in failed_account_ids if account_id in account_map]

    missing_ids = [account_id for account_id in failed_account_ids if account_id not in account_map]
    if missing_ids:
        logger.warning("Retry 계정 복원 실패: %s", missing_ids)

    def restore_items(items: list[dict], payload_key: str) -> list[dict]:
        restored: list[dict] = []
        for item in items or []:
            account_id = str(item.get("account_id") or "").strip()
            account = account_map.get(account_id)
            if not account:
                continue
            restored.append({"account": account, payload_key: item.get(payload_key)})
        return restored

    failed = {
        "accounts": [account_map[account_id] for account_id in failed_accounts_ids_only if account_id in account_map],
        "stores": restore_items(conf.get("failed_stores") or [], "store"),
        "orders": restore_items(conf.get("failed_orders") or [], "stores"),
        "ads": restore_items(conf.get("failed_ads") or [], "stores"),
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


def sanitize_retry_payload(payload: dict) -> dict:
    def strip_password(items: list[dict]) -> list[dict]:
        return [{key: value for key, value in item.items() if key != "password"} for item in items or []]

    return {
        "target_date": payload.get("target_date"),
        "retry_result": payload.get("retry_result"),
        "store_info_per_account": payload.get("store_info_per_account") or [],
        "ad_store_infos": strip_password(payload.get("ad_store_infos") or []),
    }


def retry_collect_from_conf(conf: dict) -> dict:
    target_date = conf.get("target_date") or pendulum.yesterday(KST).format("YYYY-MM-DD")
    failed, retry_accounts = restore_failed_from_conf(conf)

    account_failed = {"accounts": failed.get("accounts") or [], "stores": [], "orders": [], "ads": []}
    item_failed = {
        "accounts": [],
        "stores": failed.get("stores") or [],
        "orders": failed.get("orders") or [],
        "ads": failed.get("ads") or [],
    }

    account_result: dict = {}
    if account_failed["accounts"]:
        try:
            account_result = collect_now_and_woori(
                account_failed["accounts"],
                target_date=target_date,
                stability_profile=conf.get("stability_profile"),
            )
        except Exception as exc:
            logger.exception("Retry 계정 재수집 실패 - 잔여 실패로 전파: %s", exc)
            account_result = {
                "summary": f"계정 재수집 실패: {exc}",
                "failed": account_failed,
                "store_info_per_account": [],
                "ad_stores": [],
            }

    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": []}
    for key in ("accounts", "stores", "orders", "ads"):
        residual_failed[key].extend((account_result.get("failed") or {}).get(key) or [])

    retry_result = ""
    if count_failed_items(item_failed):
        item_result = retry_once_failed(item_failed, target_date=target_date)
        if isinstance(item_result, dict):
            retry_result = str(item_result.get("summary") or "")
            for key in ("accounts", "stores", "orders", "ads"):
                residual_failed[key].extend((item_result.get("residual_failed") or {}).get(key) or [])
        else:
            retry_result = str(item_result)
    else:
        retry_result = "재시도 대상 없음"

    store_info_per_account = _store_info_per_account_from_failed(failed)
    store_info_per_account.extend(account_result.get("store_info_per_account") or [])

    ad_store_infos = _ad_store_infos_from_failed(failed)
    ad_store_infos.extend(account_result.get("ad_stores") or [])

    return {
        "target_date": target_date,
        "failed": failed,
        "retry_accounts": retry_accounts,
        "retry_result": retry_result,
        "account_result": account_result,
        "store_info_per_account": store_info_per_account,
        "ad_store_infos": ad_store_infos,
        "residual_failed": residual_failed,
    }


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


def validate_retry_ad_funnel(retry_payload: dict) -> dict:
    target_date = retry_payload["target_date"]
    ad_store_infos = _restore_ad_passwords(retry_payload.get("ad_store_infos") or [])
    if not ad_store_infos:
        return {"empty_stores": [], "retried": [], "still_empty": []}
    return _validate_and_retry_ad_funnel(ad_store_infos, target_date)


def validate_retry_collection(retry_payload: dict) -> dict:
    toorder_result = validate_retry_toorder(retry_payload)
    ad_funnel_result = validate_retry_ad_funnel(retry_payload)
    return {
        "toorder_result": toorder_result,
        "ad_funnel_result": ad_funnel_result,
        "needs_next": retry_needed(toorder_result, ad_funnel_result, None),
    }
