"""Baemin ad funnel metric collection pipeline.

Flow:
  account_id/password -> independent Chrome session per store
    -> open stat/advertisement?initialDateOption=MONTH
    -> apply DAILY/YESTERDAY filters
    -> extract impressions/clicks/orders/order amount
    -> upsert monthly CSV by target_date
    -> quit Chrome

Output:
  analytics/baemin_macro/ad_funnel/
    brand={brand}/store={store}/ym={YYYY-MM}/baemin_ad_funnel.csv
"""

import logging
import re
import random
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    human_click,
    launch_browser,
    login_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_AD_FUNNEL_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

_AD_URL_TEMPLATE = (
    "https://self.baemin.com/shops/{store_id}/stat/advertisement?initialDateOption=MONTH"
)
_FILTER_BTN_CSS   = "button.Filter-module__lRdH"
_METRIC_LABELS    = ["노출수", "클릭수", "주문수", "주문금액"]
_FILTER_LABEL_MAP: dict[int, list[str]] = {
    0: ["노출수", "클릭수"],
    1: ["주문수", "주문금액"],
}
_YESTERDAY_KO = "\uc5b4\uc81c"
_STATUS_COLUMN    = "collection_status"
_COLUMNS          = ["collected_at", "target_date", "store_name", _STATUS_COLUMN] + _METRIC_LABELS


def _snapshot_ad_metric_state(driver) -> dict:
    return driver.execute_script(
        """
        const body = document.body ? document.body.textContent : '';
        const tabs = [...document.querySelectorAll('button[class*="Tab_b_r4ax"]')]
          .map(b => b.textContent.trim())
          .filter(Boolean);
        const vals = [...document.querySelectorAll('span[style*="margin"]')]
          .map(s => s.textContent.trim())
          .filter(v => /^[\\d,]+$/.test(v));
        const busy = document.querySelectorAll('[aria-busy="true"], [class*="Loading"], [class*="Spinner"]').length;
        return {
          no_ads: body.includes('등록된 광고가 없') || body.includes('광고를 설정'),
          tabs,
          vals,
          busy,
          ready: document.readyState,
        };
        """
    ) or {}


def _is_blank_selenium_message(reason: str | None) -> bool:
    text = str(reason or "").strip()
    return not text or text == "Message:"


def _format_metric_state(state: dict | None) -> str:
    state = state or {}
    tabs = state.get("tabs") or []
    vals = state.get("vals") or []
    return (
        f"ready={state.get('ready')} "
        f"busy={state.get('busy')} "
        f"no_ads={state.get('no_ads')} "
        f"tabs={tabs[:4]} "
        f"vals={vals[:4]}"
    )




def _extract_filter_vals(driver, btn_idx: int, store_name: str) -> list[str] | None:
    vals: list[str] = driver.execute_script(
        """
        const btns = document.querySelectorAll('button.Filter-module__lRdH');
        const btn = btns[arguments[0]];
        if (!btn) return [];

        let el = btn.parentElement;
        for (let i = 0; i < 8; i++) {
            if (!el || el === document.body) break;
            const nums = [...el.querySelectorAll('span')]
                .map(s => s.textContent.trim())
                .filter(v => /^[\\d,]+$/.test(v));
            const filters = [...el.querySelectorAll('button.Filter-module__lRdH')];
            if (filters.length > 1 && nums.length >= (arguments[0] + 1) * 2) {
                return nums.slice(arguments[0] * 2, arguments[0] * 2 + 2);
            }
            if (filters.length <= 1 && nums.length >= 2) return nums.slice(0, 2);
            el = el.parentElement;
        }

        return [...document.querySelectorAll('span[style*="margin"]')]
            .map(s => s.textContent.trim())
            .filter(v => /^[\\d,]+$/.test(v))
            .slice(arguments[0] * 2, arguments[0] * 2 + 2);
        """,
        btn_idx,
    ) or []

    logger.info("Filter[%d] vals extracted: %s / %s", btn_idx, vals, store_name)
    if len(vals) == 0:
        logger.info("Filter[%d] no metrics in DOM, treating as zeroes: %s", btn_idx, store_name)
        return ["0", "0"]
    return vals


def _set_ad_filter_yesterday(driver, store_name: str) -> dict | None:
    """Apply both filters and return a four-metric dict."""
    try:
        btn_count = driver.execute_script(
            "return document.querySelectorAll('button.Filter-module__lRdH').length;"
        )
        if not btn_count:
            logger.warning("Filter buttons not found: %s", store_name)
            return None
        logger.info("Filter buttons found: %d / %s", btn_count, store_name)

        metrics: dict = {}
        for btn_idx in range(btn_count):
            vals = _apply_yesterday_to_filter(driver, btn_idx, store_name)
            if vals is None:
                logger.warning("Filter[%d] metric extraction failed: %s", btn_idx, store_name)
                return None
            for i, label in enumerate(_FILTER_LABEL_MAP.get(btn_idx, [])):
                if i < len(vals):
                    metrics[label] = vals[i]
            time.sleep(0.3)

        filter_texts = driver.execute_script(
            "return [...document.querySelectorAll('button.Filter-module__lRdH')]"
            ".map(b => b.textContent.trim());"
        )
        logger.info("Filter texts after apply: %s / %s", filter_texts, store_name)
        if not all(_YESTERDAY_KO in t for t in filter_texts):
            logger.warning("A Filter button is not on YESTERDAY: %s", filter_texts)
            return None

        if len(metrics) < 4:
            logger.warning("Metrics incomplete (%d/4): %s / %s", len(metrics), metrics, store_name)
            return None

        logger.info("Filter flow completed, metrics extracted: %s / %s", metrics, store_name)
        return metrics

    except (TimeoutException, Exception) as e:
        logger.warning("Ad filter setup error (%s): %s", store_name, e)
        return None


def _set_ad_filter_specific(
    driver,
    target_date: str,
    store_name: str,
) -> dict | None:
    """Apply both filters using DAY_MANUAL date picker."""
    try:
        btn_count = driver.execute_script(
            "return document.querySelectorAll('button.Filter-module__lRdH').length;"
        )
        if not btn_count:
            logger.warning("Filter buttons not found: %s", store_name)
            return None
        logger.info("Filter buttons found: %d / %s", btn_count, store_name)

        metrics: dict = {}
        for btn_idx in range(btn_count):
            vals = _apply_specific_date_to_filter(driver, btn_idx, target_date, store_name)
            if vals is None:
                logger.warning("Filter[%d] metric extraction failed: %s", btn_idx, store_name)
                return None
            for i, label in enumerate(_FILTER_LABEL_MAP.get(btn_idx, [])):
                if i < len(vals):
                    metrics[label] = vals[i]
            time.sleep(0.3)

        filter_texts = driver.execute_script(
            "return [...document.querySelectorAll('button.Filter-module__lRdH')]"
            ".map(b => b.textContent.trim());"
        )
        logger.info("Filter texts after apply: %s / %s", filter_texts, store_name)

        target_day = str(pendulum.parse(target_date, tz=KST).day)
        if not all(target_day in t for t in filter_texts):
            logger.warning("A Filter button is not on target date(%s): %s", target_date, filter_texts)
            return None

        if len(metrics) < 4:
            logger.warning("Metrics incomplete (%d/4): %s / %s", len(metrics), metrics, store_name)
            return None

        logger.info("Filter flow completed, metrics extracted: %s / %s", metrics, store_name)
        return metrics

    except (TimeoutException, Exception) as e:
        logger.warning("Ad filter setup error (%s): %s", store_name, e)
        return None


def _set_ad_filter(
    driver,
    target_date: str,
    store_name: str,
) -> dict | None:
    yesterday = pendulum.yesterday(KST).format("YYYY-MM-DD")
    if target_date == yesterday:
        return _set_ad_filter_yesterday(driver, store_name)
    return _set_ad_filter_specific(driver, target_date, store_name)


def _apply_yesterday_to_filter(driver, btn_idx: int, store_name: str) -> list[str] | None:
    """Apply DAILY/YESTERDAY on one filter and return its metric values."""
    clicked = driver.execute_script(
        """
        const btns = document.querySelectorAll('button.Filter-module__lRdH');
        const btn = btns[arguments[0]];
        if (!btn) return false;
        btn.click();
        return btn.textContent.trim().slice(0, 30);
        """,
        btn_idx,
    )
    if not clicked:
        logger.warning("Filter[%d] button not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] clicked: %r / %s", btn_idx, clicked, store_name)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'input[name="ad-stats-period-filter"][value="DAILY"]')
        )
    )

    daily = driver.execute_script(
        """
        const radio = document.querySelector('input[name="ad-stats-period-filter"][value="DAILY"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if daily == "not_found":
        logger.warning("Filter[%d] DAILY radio not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] DAILY: %s", btn_idx, daily)
    time.sleep(2.0)

    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script(
            'const r = document.querySelector(\'input[name="period"][value="YESTERDAY"]\');'
            'return r && r.offsetParent !== null;'
        )
    )
    yest = driver.execute_script(
        """
        const radio = document.querySelector('input[name="period"][value="YESTERDAY"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if yest == "not_found":
        logger.warning("Filter[%d] YESTERDAY radio not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] YESTERDAY: %s", btn_idx, yest)
    time.sleep(2.0)

    applied = driver.execute_script(
        """
        const btn = [...document.querySelectorAll('button')]
            .find(b => (b.innerText || b.textContent || '').trim().includes(String.fromCharCode(0xC801, 0xC6A9)));
        if (!btn) return false;
        btn.click();
        return true;
        """
    )
    if not applied:
        logger.warning("Filter[%d] apply button not found: %s", btn_idx, store_name)
        return None

    try:
        WebDriverWait(driver, 15).until(
            lambda d: _YESTERDAY_KO in (d.execute_script(
                "const btns = document.querySelectorAll('button.Filter-module__lRdH');"
                "return btns[arguments[0]]?.textContent || '';",
                btn_idx,
            ) or "")
        )
    except TimeoutException:
        logger.warning("Filter[%d] text not changed after apply (15s): %s", btn_idx, store_name)
        return None

    time.sleep(2.0)  # React data fetch completion is not tied to button text.
    return _extract_filter_vals(driver, btn_idx, store_name)


def _apply_specific_date_to_filter(
    driver,
    btn_idx: int,
    target_date: str,
    store_name: str,
) -> list[str] | None:
    """Apply DAILY/DAY_MANUAL on one filter and return its metric values."""
    clicked = driver.execute_script(
        """
        const btns = document.querySelectorAll('button.Filter-module__lRdH');
        const btn = btns[arguments[0]];
        if (!btn) return false;
        btn.click();
        return btn.textContent.trim().slice(0, 30);
        """,
        btn_idx,
    )
    if not clicked:
        logger.warning("Filter[%d] button not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] clicked: %r / %s", btn_idx, clicked, store_name)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'input[name="ad-stats-period-filter"][value="DAILY"]')
        )
    )

    daily = driver.execute_script(
        """
        const radio = document.querySelector('input[name="ad-stats-period-filter"][value="DAILY"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if daily == "not_found":
        logger.warning("Filter[%d] DAILY radio not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] DAILY: %s", btn_idx, daily)
    time.sleep(2.0)

    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script(
            'const r = document.querySelector(\'input[name="period"][value="DAY_MANUAL"]\');'
            'return r && r.offsetParent !== null;'
        )
    )
    manual = driver.execute_script(
        """
        const radio = document.querySelector('input[name="period"][value="DAY_MANUAL"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if manual == "not_found":
        logger.warning("Filter[%d] DAY_MANUAL radio not found: %s", btn_idx, store_name)
        return None
    logger.info("Filter[%d] DAY_MANUAL: %s", btn_idx, manual)
    time.sleep(0.5)

    dt = pendulum.parse(target_date, tz=KST)
    target_year = dt.year
    target_month = dt.month
    target_day = dt.day
    day_label = f"{target_day}일"

    clicked = driver.execute_script(
        """
        let btn = document.querySelector('[data-atelier-component="DatePicker.Trigger"]');
        if (!btn) {
            const wrap = document.querySelector('[class*="DefaultDateFilter"]');
            btn = wrap && wrap.querySelector('button');
        }
        if (btn) { btn.click(); return true; }
        return false;
        """
    )
    if not clicked:
        logger.warning("Filter[%d] datepicker trigger not found: %s", btn_idx, store_name)
        return None

    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script("return !!document.querySelector('[aria-label$=\"일\"]');")
    )

    for _ in range(24):
        text = driver.execute_script(
            """
            const h = document.querySelector('[class*="DatePicker"][class*="Header"], [class*="CalendarHeader"]');
            return h ? h.textContent : null;
            """
        )
        if not text:
            break
        match = re.search(r"(\\d{4})[^\\d]+(\\d{1,2})", text)
        if not match:
            break
        ym = (int(match.group(1)), int(match.group(2)))
        if ym == (target_year, target_month):
            break
        if ym > (target_year, target_month):
            driver.execute_script(
                """
                const btns = [...document.querySelectorAll('button')];
                const prev = btns.find(
                    b =>
                        b.getAttribute('aria-label') === '이전달' ||
                        b.textContent.trim() === '<' ||
                        b.className.includes('prev') ||
                        b.className.includes('Prev')
                );
                if (prev) prev.click();
                """
            )
            time.sleep(0.3)
        else:
            break

    clicked_start = driver.execute_script(
        """
        const targetYear = arguments[0];
        const targetMonth = arguments[1];
        const dayLabel = arguments[2];
        const buttons = [...document.querySelectorAll(`button[aria-label="${dayLabel}"]`)];
        for (const table of [...document.querySelectorAll('table')]) {
            const caption = table.querySelector('caption');
            if (!caption) continue;
            const txt = caption.textContent || '';
            const m = txt.match(/(\\d{4})[^\\d]+(\\d{1,2})/);
            if (!m) continue;
            const year = Number(m[1]);
            const month = Number(m[2]);
            if (year === targetYear && month === targetMonth) {
                const monthBtns = [...table.querySelectorAll(`button[aria-label="${dayLabel}"]`)]
                    .filter((b) => b.getAttribute('aria-disabled') !== 'true');
                if (monthBtns.length > 0) {
                    monthBtns[0].click();
                    return true;
                }
                return false;
            }
        }
        return false;
        """,
        target_year,
        target_month,
        day_label,
    )
    if not clicked_start:
        logger.warning("Filter[%d] start date button not found/disabled: %s", btn_idx, store_name)
        return None
    time.sleep(0.2)

    clicked_end = driver.execute_script(
        """
        const targetYear = arguments[0];
        const targetMonth = arguments[1];
        const dayLabel = arguments[2];
        for (const table of [...document.querySelectorAll('table')]) {
            const caption = table.querySelector('caption');
            if (!caption) continue;
            const txt = caption.textContent || '';
            const m = txt.match(/(\\d{4})[^\\d]+(\\d{1,2})/);
            if (!m) continue;
            const year = Number(m[1]);
            const month = Number(m[2]);
            if (year === targetYear && month === targetMonth) {
                const monthBtns = [...table.querySelectorAll(`button[aria-label="${dayLabel}"]`)]
                    .filter((b) => b.getAttribute('aria-disabled') !== 'true');
                if (monthBtns.length > 0) {
                    monthBtns[monthBtns.length - 1].click();
                    return true;
                }
                return false;
            }
        }
        return false;
        """,
        target_year,
        target_month,
        day_label,
    )
    if not clicked_end:
        logger.warning("Filter[%d] end date button not found/disabled: %s", btn_idx, store_name)
        return None

    applied = driver.execute_script(
        """
        const btn = [...document.querySelectorAll('button')]
            .find(b => (b.innerText || b.textContent || '').trim().includes(String.fromCharCode(0xC801, 0xC6A9)));
        if (!btn) return false;
        btn.click();
        return true;
        """
    )
    if not applied:
        logger.warning("Filter[%d] apply button not found: %s", btn_idx, store_name)
        return None

    try:
        WebDriverWait(driver, 15).until(
            lambda d: str(target_day) in (
                d.execute_script(
                    "const btns = document.querySelectorAll('button.Filter-module__lRdH');"
                    "return btns[arguments[0]]?.textContent || '';",
                    btn_idx,
                ) or ""
            )
        )
    except TimeoutException:
        logger.warning("Filter[%d] text not changed after apply (15s): %s", btn_idx, store_name)
        return None

    time.sleep(2.0)  # React data fetch completion is not tied to button text.
    return _extract_filter_vals(driver, btn_idx, store_name)

def _reload_and_retry_extract(driver, store_id: str, store_name: str) -> dict:
    logger.info("광고 페이지 새로고침 후 재시도: %s (%s)", store_name, store_id)
    try:
        driver.refresh()
        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            return {
                "status": "parse_error",
                "metrics": None,
                "reason": "refresh wait_for_page failed",
            }
        if not _set_ad_filter_yesterday(driver, store_name):
            return {
                "status": "parse_error",
                "metrics": None,
                "reason": "refresh filter apply failed",
            }
        time.sleep(2.0)  # 새로고침 후 React 렌더 안정화 대기
        return _extract_ad_metrics(driver, store_name)
    except Exception as exc:
        return {
            "status": "parse_error",
            "metrics": None,
            "reason": f"refresh retry failed: {exc}",
        }


def _reload_and_collect(
    driver,
    store_id: str,
    store_name: str,
    target_date: str,
) -> dict | None:
    """Refresh page and re-apply filter. Returns metrics dict or None on failure."""
    logger.info("Reloading ad funnel page for retry: %s (%s)", store_name, store_id)
    try:
        driver.refresh()
        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            return None
        return _set_ad_filter(driver, target_date, store_name)
    except Exception as exc:
        logger.warning("Reload retry failed (%s): %s", store_name, exc)
        return None


def _collect_ad_funnel_metrics(
    driver,
    store_id: str,
    brand: str,
    store: str,
    target_date: str,
) -> bool:
    if _snapshot_ad_metric_state(driver).get("no_ads"):
        saved = _save_ad_funnel_csv(None, brand, store, target_date, status="no_ads")
        logger.info("No ads (skip): %s / %s -> %s", store, target_date, saved)
        return True

    metrics = _set_ad_filter(driver, target_date, store)
    if metrics is None:
        metrics = _reload_and_collect(driver, store_id, store, target_date)
    if metrics is None:
        logger.warning("ad funnel extraction incomplete: %s / %s", store, target_date)
        return False

    saved = _save_ad_funnel_csv(metrics, brand, store, target_date, status="ok")
    logger.info("Saved ad funnel CSV: brand=%s store=%s %s", brand, store, saved)
    return True


def _collect_ad_funnel_for_driver_impl(
    driver,
    store_info: dict,
    target_date: str | None = None,
) -> bool:
    """Impl: navigate, apply filter, extract and save ad funnel metrics."""
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand = store_info["brand"]
    store = store_info["store"]
    logger.info("Ad funnel collection start: %s (%s) / %s", store, store_id, target_date)

    try:
        driver.set_page_load_timeout(45)
        driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            logger.warning("Ad funnel page failed to load, skipping: %s", store)
            return False

        return _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date)
    except Exception as exc:
        logger.warning("Ad funnel collection failed (%s): %s", store, exc)
        return False
# ---------------------------------------------------------------------------
# 공개 함수
# ---------------------------------------------------------------------------

def collect_ad_funnel_for_driver(
    driver,
    store_info: dict,
    target_date: str | None = None,
) -> bool:
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand = store_info["brand"]
    store = store_info["store"]
    logger.info("Ad funnel collection start: %s (%s) / %s", store, store_id, target_date)

    try:
        driver.set_page_load_timeout(45)
        driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            logger.warning("Ad funnel page failed to load, skipping: %s", store)
            return False

        return _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date)
    except Exception as exc:
        logger.warning("Ad funnel collection failed (%s): %s", store, exc)
        return False
def collect_ad_funnel_for_account(
    account_id: str,
    password: str,
    store_list: list[dict],
    target_date: str | None = None,
) -> list[dict]:
    """Collect ad funnel stats per store with independent Chrome sessions.

    Returns:
        Store info entries that failed collection.
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed_stores: list[dict] = []

    for store_info in store_list:
        store_id = store_info["store_id"]
        brand = store_info["brand"]
        store = store_info["store"]

        logger.info("Ad funnel collection start: %s (%s) / %s", store, store_id, target_date)

        succeeded = False
        for attempt in range(2):
            driver = None
            try:
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, password):
                    logger.warning("Login failed: %s / %s", account_id, store)
                    break

                driver.set_page_load_timeout(45)
                driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

                if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
                    logger.warning("Ad funnel page failed to load, skipping: %s", store)
                    if attempt == 0:
                        continue
                    break

                if not _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date):
                    if attempt == 0:
                        logger.warning("Metric extraction failed, retrying browser: %s", store)
                        continue
                    logger.warning("Metric extraction failed after retry, skipping: %s", store)
                    break

                succeeded = True
                break

            except Exception as e:
                logger.warning("Store collection failed (%s) attempt=%d: %s", store, attempt + 1, e)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            if attempt == 0:
                time.sleep(random.uniform(3.0, 5.0))

        if not succeeded:
            failed_stores.append(store_info)

        time.sleep(random.uniform(0.5, 1.5))

    return failed_stores
def _validate_and_retry_ad_funnel(
    store_infos: list[dict],
    target_date: str,
) -> dict:
    """저장된 ad_funnel CSV에서 빈값(주문수·주문금액) 있는 매장 찾아 재수집.

    store_infos 각 항목: {"account_id", "password", "store_id", "brand", "store"}

    Returns:
        {"empty_stores": [...], "retried": [...], "still_empty": [...]}
    """
    ym = target_date[:7]
    empty_stores: list[dict] = []

    for si in store_infos:
        brand, store = si["brand"], si["store"]
        csv_path = (
            BAEMIN_AD_FUNNEL_DB
            / f"brand={brand}"
            / f"store={store}"
            / f"ym={ym}"
            / "baemin_ad_funnel.csv"
        )
        if not csv_path.exists():
            empty_stores.append(si)
            logger.warning("ad_funnel CSV 미생성: %s / %s", store, target_date)
            continue
        df = pd.read_csv(csv_path, dtype=str)
        row = df[df["target_date"] == target_date]
        if row.empty:
            empty_stores.append(si)
            logger.warning("ad_funnel target_date 미생성: %s / %s", store, target_date)
            continue
        r = row.iloc[0]
        status = str(r.get(_STATUS_COLUMN, "")).strip().lower()
        if status == "no_ads":
            logger.info("ad_funnel 광고 없음(정상): %s / %s", store, target_date)
            continue
        if status and status != "ok":
            logger.warning("ad_funnel 수집 실패 상태: %s / %s / %s", store, target_date, status)
            empty_stores.append(si)
            continue
        if str(r.get("주문수", "")).strip() == "" or str(r.get("주문금액", "")).strip() == "":
            logger.warning("ad_funnel 빈값 발견: %s / %s", store, target_date)
            empty_stores.append(si)

    if not empty_stores:
        return {"empty_stores": [], "retried": [], "still_empty": []}

    logger.warning(
        "ad_funnel 빈값 재수집 대상: %s", [s["store"] for s in empty_stores]
    )

    # account_id 기준으로 그룹화해 재수집
    by_account: dict[tuple, list] = {}
    for s in empty_stores:
        key = (s["account_id"], s["password"])
        by_account.setdefault(key, []).append(s)

    for (account_id, password), stores in by_account.items():
        collect_ad_funnel_for_account(
            account_id, password, stores, target_date=target_date
        )

    # 재수집 후 재점검
    still_empty: list[dict] = []
    for si in empty_stores:
        brand, store = si["brand"], si["store"]
        csv_path = (
            BAEMIN_AD_FUNNEL_DB
            / f"brand={brand}"
            / f"store={store}"
            / f"ym={ym}"
            / "baemin_ad_funnel.csv"
        )
        if not csv_path.exists():
            still_empty.append(si)
            continue
        df2 = pd.read_csv(csv_path, dtype=str)
        row2 = df2[df2["target_date"] == target_date]
        if row2.empty:
            still_empty.append(si)
            logger.warning("재수집 후 target_date 미생성: %s / %s", store, target_date)
            continue
        status = str(row2.iloc[0].get(_STATUS_COLUMN, "")).strip().lower()
        if status == "no_ads":
            logger.info("재수집 성공(광고 없음): %s / %s", store, target_date)
            continue
        if status and status != "ok":
            still_empty.append(si)
            logger.warning("재수집 후 parse_error 잔존: %s / %s / %s", store, target_date, status)
            continue
        if str(row2.iloc[0].get("주문수", "")).strip() == "":
            still_empty.append(si)
            logger.warning("재수집 후에도 빈값 잔존: %s / %s", store, target_date)
        else:
            logger.info("재수집 성공: %s / %s", store, target_date)

    return {
        "empty_stores": empty_stores,
        "retried": empty_stores,
        "still_empty": still_empty,
    }


# ---------------------------------------------------------------------------
# CSV 저장
# ---------------------------------------------------------------------------

def _save_ad_funnel_csv(
    metrics: dict | None,
    brand: str,
    store: str,
    target_date: str,
    *,
    status: str = "ok",
) -> Path:
    """광고 funnel 지표를 월별 CSV에 upsert 저장한다."""
    ym = target_date[:7]  # "YYYY-MM"
    out_dir = BAEMIN_AD_FUNNEL_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "baemin_ad_funnel.csv"

    row = {
        "collected_at": pendulum.now(KST).isoformat(),
        "target_date":  target_date,
        "store_name":   store,
        _STATUS_COLUMN: status,
    }
    for label in _METRIC_LABELS:
        row[label] = (metrics or {}).get(label, "")

    new_df = pd.DataFrame([row], columns=_COLUMNS)

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # 같은 날짜 행 제거 후 append (upsert)
        existing = existing[existing["target_date"] != target_date]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


