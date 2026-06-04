import logging

from dags.db.DB_Coupang_Macro_Dags import TARGET_STORES
from modules.transform.pipelines.db.DB_Coupang_combined import (
    collect_orders_for_account,
    load_coupang_accounts,
)


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
)


def main() -> None:
    target_date = "2026-05-27"
    accounts = load_coupang_accounts(TARGET_STORES, exact=True)
    if not accounts:
        raise SystemExit(f"account not found: {TARGET_STORES!r}")

    account = accounts[0]
    print("TARGET_STORE:", account["store_name"])
    result = collect_orders_for_account(account, target_date)
    print("RESULT:", result)


if __name__ == "__main__":
    main()
