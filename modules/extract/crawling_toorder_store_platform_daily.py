"""
toorder CEO channel sales API collection.

This module logs in with Selenium, extracts the Cognito access token,
then calls the underlying sales APIs directly. If the plain requests path
fails because the site expects more browser context, it falls back to
executing the same fetch call inside the logged-in browser.
"""

import json
import logging
import time

import requests

from modules.extract.crawling_toorder_sales_report import _do_login, _launch_browser
from modules.transform.utility.paths import DOWN_DIR

logger = logging.getLogger(__name__)

_API_BASE = "https://5yb80agx2l.execute-api.ap-northeast-2.amazonaws.com/main"
_COMPANY_ID = "10071"
_PATHNAME = "/dashboard/sales-report/orderkinds"
_API_HEADERS_COMMON = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "isCompany": "true",
    "pathname": _PATHNAME,
}


def fetch_store_platform_daily(
    toorder_id: str,
    toorder_pw: str,
    target_date: str,
    store_name: str = "해운대중동점",
) -> dict:
    """
    Collect channel-level daily sales data for a single store.

    Returns:
        {"store_name": str, "shop_id": str, "api_data": dict}
    """
    driver = _launch_browser(toorder_id, DOWN_DIR)
    try:
        if not _do_login(driver, toorder_id, toorder_pw):
            raise RuntimeError(f"toorder login failed (id={toorder_id})")

        access_token = _extract_access_token(driver)
        logger.info("[%s] accessToken extracted", toorder_id)

        headers = {**_API_HEADERS_COMMON, "Authorization": access_token}

        shop_id = _get_shop_id(driver, headers, toorder_id, store_name)
        logger.info("[%s] %s SHOP_ID=%s", toorder_id, store_name, shop_id)

        api_data = _call_sales_api(driver, headers, shop_id, target_date)
        logger.info("[%s] API collection complete (date=%s)", toorder_id, target_date)

        return {"store_name": store_name, "shop_id": shop_id, "api_data": api_data}
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _extract_access_token(driver) -> str:
    """Extract the Cognito access token from localStorage."""
    time.sleep(0.5)
    keys = driver.execute_script("return Object.keys(localStorage);") or []
    token_key = next(
        (key for key in keys if "Cognito" in key and "accessToken" in key),
        None,
    )
    if not token_key:
        raise RuntimeError(f"accessToken key not found in localStorage: {keys}")
    token = driver.execute_script(f"return localStorage.getItem('{token_key}');")
    if not token:
        raise RuntimeError(f"accessToken is empty: {token_key}")
    return token


def _get_shop_id(driver, headers: dict, toorder_id: str, store_name: str) -> str:
    """Resolve the store shop ID by store title."""
    data = _request_json(
        driver=driver,
        url=f"{_API_BASE}/user/getUserInfo",
        payload={"USER_ID": toorder_id},
        headers=headers,
        label="getUserInfo",
    )

    company_list = data.get("data", {}).get("companyList", [])
    for company in company_list:
        for shop in company.get("shopList", []):
            if shop.get("title") == store_name:
                return str(shop["id"])

    shops = [
        shop.get("title")
        for company in company_list
        for shop in company.get("shopList", [])
    ]
    raise ValueError(f"shop '{store_name}' not found. shops={shops[:10]}")


def _call_sales_api(driver, headers: dict, shop_id: str, target_date: str) -> dict:
    """Call the channel sales report API."""
    return _request_json(
        driver=driver,
        url=f"{_API_BASE}/sales/getReportSalesDateByOrderKinds",
        payload={
            "COMPANY_ID": _COMPANY_ID,
            "SHOP_IDS": [shop_id],
            "START_DATE": target_date,
            "END_DATE": target_date,
        },
        headers=headers,
        label="getReportSalesDateByOrderKinds",
    )


def _request_json(driver, url: str, payload: dict, headers: dict, label: str) -> dict:
    """
    Prefer direct requests for simplicity and speed.
    If the backend rejects the call despite a valid login, retry through the
    live browser session to preserve any hidden browser-side auth context.
    """
    try:
        return _requests_fetch_json(url, payload, headers, label)
    except Exception as exc:
        logger.warning("requests API failed (%s): %s", label, exc)
        return _browser_fetch_json(driver, url, payload, headers, label)


def _requests_fetch_json(url: str, payload: dict, headers: dict, label: str) -> dict:
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    if not resp.ok:
        snippet = resp.text[:300]
        raise RuntimeError(f"HTTP {resp.status_code}: {snippet}")
    try:
        return resp.json()
    except ValueError as exc:
        raise RuntimeError(f"invalid JSON response: {exc}") from exc


def _browser_fetch_json(driver, url: str, payload: dict, headers: dict, label: str) -> dict:
    driver.set_script_timeout(30)
    result = driver.execute_async_script(
        """
        const url = arguments[0];
        const payload = arguments[1];
        const headers = arguments[2];
        const callback = arguments[arguments.length - 1];

        fetch(url, {
            method: 'POST',
            headers,
            body: JSON.stringify(payload),
            credentials: 'include'
        })
        .then(async (resp) => {
            const text = await resp.text();
            let data = null;
            try {
                data = text ? JSON.parse(text) : null;
            } catch (err) {
                callback({
                    ok: false,
                    status: resp.status,
                    error: 'invalid_json',
                    text: text.slice(0, 500)
                });
                return;
            }
            callback({
                ok: resp.ok,
                status: resp.status,
                data
            });
        })
        .catch((err) => callback({
            ok: false,
            status: 0,
            error: String(err)
        }));
        """,
        url,
        payload,
        headers,
    )

    if not isinstance(result, dict):
        raise RuntimeError(f"browser fetch returned unexpected result: {result!r}")
    if not result.get("ok"):
        err = result.get("data")
        if err is None:
            err = result.get("text") or result.get("error")
        raise RuntimeError(f"HTTP {result.get('status')}: {err}")

    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"browser fetch returned non-dict JSON: {json.dumps(data)[:300]}")
    return data
