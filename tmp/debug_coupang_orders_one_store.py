import logging

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
    target_store = "도리당 송파삼전점"

    accounts = load_coupang_accounts([target_store], exact=True)
    if not accounts:
        raise SystemExit(f"account not found: {target_store}")

    result = collect_orders_for_account(accounts[0], target_date)
    print("RESULT:", result)


if __name__ == "__main__":
    main()
