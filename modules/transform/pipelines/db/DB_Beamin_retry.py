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

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")


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


def build_retry_conf(
    failed: dict,
    target_date: str,
    source_dag_id: str | None = None,
    source_run_id: str | None = None,
    attempt: int = 1,
    max_attempts: int = 10,
) -> dict:
    failed_accounts = failed.get("accounts") or []
    failed_stores = _strip_account(failed.get("stores") or [], "store")
    failed_orders = _strip_account(failed.get("orders") or [], "stores")
    failed_ads = _strip_account(failed.get("ads") or [], "stores")

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
        "failed_account_ids": failed_account_ids,
        "failed_accounts_ids_only": _unique(_account_id(account) for account in failed_accounts),
        "failed_stores": failed_stores,
        "failed_orders": failed_orders,
        "failed_ads": failed_ads,
    }


def retry_needed(toorder_result: dict | None, ad_funnel_result: dict | None, failed: dict | None = None) -> bool:
    if ad_funnel_result and ad_funnel_result.get("still_empty"):
        return True

    if toorder_result is None:
        return count_failed_items(failed) > 0

    if toorder_result.get("mismatched_stores") or toorder_result.get("missing_brand_stores"):
        return True

    if int(toorder_result.get("compared_count") or 0) == 0 and count_failed_items(failed) > 0:
        return True

    return False


def _load_account_map() -> dict[str, dict]:
    accounts = pipeline_load_accounts(target_stores=[])
    return {_account_id(account): account for account in accounts if _account_id(account)}


def restore_failed_from_conf(conf: dict) -> tuple[dict, list[dict]]:
    account_map = _load_account_map()
    failed_account_ids = _unique(conf.get("failed_account_ids") or [])
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
        account_result = collect_now_and_woori(account_failed["accounts"], target_date=target_date)

    retry_result = ""
    if count_failed_items(item_failed):
        retry_result = retry_once_failed(item_failed, target_date=target_date)
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
    }


def _accounts_for_store_info(store_info_per_account: list[dict]) -> list[dict]:
    account_map = _load_account_map()
    account_ids = _unique(item.get("account_id") for item in store_info_per_account or [])
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
