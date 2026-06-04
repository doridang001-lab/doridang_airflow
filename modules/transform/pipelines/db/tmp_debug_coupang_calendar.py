import json
import sys
import time
from pathlib import Path

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/home/airflow/.local/lib/python3.12/site-packages")
import setuptools  # noqa: F401

from selenium.webdriver.common.by import By

from modules.extract.croling_coupang import (
    ORDERS_BASE_URL,
    _extract_store_id_from_orders_url,
    _find_first_visible_in_page,
    _get_order_date_label,
    _has_calendar_popup,
    close_error_popups,
    get_store_id_from_url,
    launch_browser,
    login_coupang,
)
from modules.transform.pipelines.db.DB_Coupang_combined import load_coupang_accounts


DEBUG_DIR = Path("/opt/airflow/tmp/coupang_calendar_debug")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def _write(name: str, content: str) -> None:
    (DEBUG_DIR / name).write_text(content, encoding="utf-8")


def _dump_candidates(driver, label: str) -> None:
    data = {
        "url": driver.current_url,
        "title": driver.title,
        "order_date_label": _get_order_date_label(driver),
        "calendar_popup_detected": _has_calendar_popup(driver),
        "inputs": [],
        "buttons": [],
        "date_like_nodes": [],
    }

    for element in driver.find_elements(By.CSS_SELECTOR, "input")[:20]:
        data["inputs"].append(
            {
                "id": element.get_attribute("id"),
                "name": element.get_attribute("name"),
                "type": element.get_attribute("type"),
                "class": element.get_attribute("class"),
                "placeholder": element.get_attribute("placeholder"),
                "value": element.get_attribute("value"),
                "displayed": element.is_displayed(),
            }
        )

    for element in driver.find_elements(By.CSS_SELECTOR, "button")[:30]:
        data["buttons"].append(
            {
                "text": (element.text or "").strip(),
                "class": element.get_attribute("class"),
                "aria_label": element.get_attribute("aria-label"),
                "displayed": element.is_displayed(),
            }
        )

    selectors = [
        "[data-testid]",
        "[class*='date']",
        "[class*='picker']",
        "[class*='calendar']",
        "[class*='DayPicker']",
        "[role='dialog']",
    ]
    seen = set()
    for selector in selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector)[:80]:
                key = (
                    element.tag_name,
                    element.get_attribute("class"),
                    element.get_attribute("data-testid"),
                    (element.text or "").strip(),
                )
                if key in seen:
                    continue
                seen.add(key)
                data["date_like_nodes"].append(
                    {
                        "selector": selector,
                        "tag": element.tag_name,
                        "class": element.get_attribute("class"),
                        "data_testid": element.get_attribute("data-testid"),
                        "role": element.get_attribute("role"),
                        "aria_label": element.get_attribute("aria-label"),
                        "text": (element.text or "").strip()[:300],
                        "displayed": element.is_displayed(),
                    }
                )
        except Exception as exc:
            data["date_like_nodes"].append({"selector": selector, "error": str(exc)})

    _write(f"{label}.json", json.dumps(data, ensure_ascii=False, indent=2))
    _write(f"{label}.html", driver.page_source)
    driver.save_screenshot(str(DEBUG_DIR / f"{label}.png"))


def main() -> None:
    target_store = "도리당 송파삼전점"

    accounts = load_coupang_accounts([target_store], exact=True)
    if not accounts:
        raise SystemExit(f"account not found: {target_store}")

    account = accounts[0]
    account_id = account["account_id"]
    password = account["password"]

    driver = None
    try:
        driver = launch_browser(account_id)
        if not login_coupang(driver, account_id, password):
            raise RuntimeError("login failed")

        store_id = get_store_id_from_url(driver, account_id)
        print("store_id=", store_id)
        print("current_url=", driver.current_url)

        driver.get(ORDERS_BASE_URL)
        time.sleep(4)
        actual_store_id = _extract_store_id_from_orders_url(driver.current_url)
        print("actual_store_id=", actual_store_id)
        close_error_popups(driver)
        _dump_candidates(driver, "before_click")

        order_date_container = _find_first_visible_in_page(
            driver,
            [
                (By.CSS_SELECTOR, "span.css-1595qa6.e4pgcj00 > span"),
                (By.CSS_SELECTOR, "[class*='e4pgcj00'] span"),
                (By.CSS_SELECTOR, "[class*='e4pgcj09']"),
                (By.XPATH, "//*[contains(text(), '주문일')]"),
            ],
            account_id,
            timeout=5,
        )
        if order_date_container is None:
            raise RuntimeError("order date container not found")

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", order_date_container)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", order_date_container)
        time.sleep(2)
        _dump_candidates(driver, "after_click")

        extra_targets = [
            (By.CSS_SELECTOR, "div[data-testid='input']"),
            (By.CSS_SELECTOR, "span.css-1595qa6.e4pgcj00"),
            (By.XPATH, "//*[contains(text(), '주문일')]"),
        ]
        for idx, locator in enumerate(extra_targets, start=1):
            element = _find_first_visible_in_page(driver, [locator], account_id, timeout=2)
            if element is None:
                continue
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(2)
            _dump_candidates(driver, f"after_extra_click_{idx}")

        print("debug_dir=", DEBUG_DIR)
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()
