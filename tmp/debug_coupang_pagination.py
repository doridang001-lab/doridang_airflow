import json
import time

from modules.extract.croling_coupang import (
    get_store_id_from_url,
    launch_browser,
    login_coupang,
    navigate_to_orders_with_date,
)
from modules.transform.pipelines.db.DB_Coupang_combined import load_coupang_accounts


TARGET_STORE = "도리당 송파삼전점"
TARGET_DATE = "2026-05-28"


def main() -> None:
    accounts = load_coupang_accounts([TARGET_STORE], exact=True)
    if not accounts:
        raise SystemExit(f"account not found: {TARGET_STORE}")

    account = accounts[0]
    account_id = account["account_id"]
    driver = launch_browser(account_id)

    try:
        ok = login_coupang(driver, account_id, account["password"])
        print("LOGIN", ok, driver.current_url)
        if not ok:
            return

        store_id = get_store_id_from_url(driver, account_id)
        print("STORE_ID", store_id)
        result = navigate_to_orders_with_date(driver, store_id, TARGET_DATE, account_id)
        print("DATE_FILTER", result)
        time.sleep(1.0)

        payload = driver.execute_script(
            """
            return {
              buttons: Array.from(document.querySelectorAll("button")).map((b, i) => ({
                i,
                txt: (b.textContent || "").trim(),
                cls: b.className || "",
                aria: b.getAttribute("aria-label") || "",
                dis: !!b.disabled
              })),
              links: Array.from(document.querySelectorAll("a")).map((a, i) => ({
                i,
                txt: (a.textContent || "").trim(),
                cls: a.className || "",
                href: a.getAttribute("href") || "",
                aria: a.getAttribute("aria-label") || ""
              })),
              navs: Array.from(document.querySelectorAll("nav, ul, ol, [role='navigation']")).map((el, i) => ({
                i,
                tag: el.tagName,
                cls: el.className || "",
                txt: (el.textContent || "").trim().slice(0, 1000)
              }))
            };
            """
        )
        print(json.dumps(payload, ensure_ascii=False))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
