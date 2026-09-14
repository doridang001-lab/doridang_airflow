"""배민 주문내역 날짜 필터 DOM 덤프 스크립트.

컨테이너에서 직접 실행한다:
  python /opt/airflow/modules/transform/pipelines/db/_debug_baemin_orders_date_filter.py
"""

import logging
import sys
import time

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/home/airflow/.local/lib/python3.12/site-packages")
import setuptools  # noqa: F401

from modules.extract.croling_beamin import (
    get_store_options,
    launch_browser,
    login_baemin,
    logout_baemin,
    wait_for_page,
)
from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    _date_popup_debug_state,
    _open_date_filter_popup,
    _open_orders_history,
    _select_order_store,
    _wait_for_orders_page_shell,
)


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _find_store_id(options: list[dict], target_store: str) -> str:
    for option in options:
        text = str(option.get("text") or "")
        if target_store in text or text in target_store:
            return str(option.get("store_id") or "").strip()
    raise SystemExit(f"store_id not found: {target_store} / options={options}")


def _dump_state(driver, label: str) -> None:
    logger.info("DATE_FILTER_DUMP[%s]=%s", label, _date_popup_debug_state(driver, html_limit=2000))


def _click_radio_by_index(driver, index: int) -> bool:
    return bool(
        driver.execute_script(
            """
            const radios = [...document.querySelectorAll('input[type="radio"]')];
            const radio = radios[arguments[0]];
            if (!radio) return false;
            radio.click();
            radio.dispatchEvent(new Event('change', { bubbles: true }));
            return true;
            """,
            index,
        )
    )


def main() -> None:
    target_store = "도리당 강동점"
    accounts = load_accounts(target_stores=[target_store], exact=True)
    if not accounts:
        raise SystemExit(f"account not found: {target_store}")

    account = accounts[0]
    account_id = account["account_id"]
    driver = None
    try:
        driver = launch_browser(account_id)
        if not login_baemin(driver, account_id, account["password"]):
            raise SystemExit(f"login failed: {account_id}")
        if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
            raise SystemExit("dashboard store select not found")

        options = get_store_options(driver)
        store_id = _find_store_id(options, target_store)
        logger.info("TARGET_STORE=%s store_id=%s", target_store, store_id)

        _open_orders_history(driver)
        if not _wait_for_orders_page_shell(driver):
            raise SystemExit("orders page shell not ready")
        if not _select_order_store(driver, store_id, target_store):
            raise SystemExit(f"order store filter failed: {target_store} / {store_id}")

        if not _open_date_filter_popup(driver):
            raise SystemExit("date filter popup open failed")
        _dump_state(driver, "initial")

        radio_count = int(
            driver.execute_script("return document.querySelectorAll('input[type=\"radio\"]').length;") or 0
        )
        for index in range(radio_count):
            if _click_radio_by_index(driver, index):
                time.sleep(0.5)
                _dump_state(driver, f"radio_{index}")
    finally:
        if driver is not None:
            try:
                logout_baemin(driver, account_id)
            except Exception:
                logger.exception("logout failed")
            try:
                driver.quit()
            except Exception:
                logger.exception("driver quit failed")


if __name__ == "__main__":
    main()
