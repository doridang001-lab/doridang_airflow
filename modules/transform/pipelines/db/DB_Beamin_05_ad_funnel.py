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
from modules.transform.pipelines.db.DB_Beamin_04_orders import has_orders_no_data_marker
from modules.transform.pipelines.db.beamin_store_io import find_tables, read_file
from modules.transform.utility.paths import BAEMIN_AD_FUNNEL_DB, BAEMIN_ORDERS_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

_AD_URL_TEMPLATE = (
    "https://self.baemin.com/shops/{store_id}/stat/advertisement?initialDateOption=MONTH"
)
_FILTER_BTN_CSS   = "button.Filter-module__lRdH"
_METRIC_LABELS    = ["노출수", "클릭수", "주문수", "주문금액"]
_YESTERDAY_KO = "\uc5b4\uc81c"
_STATUS_COLUMN    = "collection_status"
_COLUMNS          = ["collected_at", "target_date", "store_name", _STATUS_COLUMN] + _METRIC_LABELS
_DOM_MISSING_CIRCUIT_THRESHOLD = 3
_dom_metric_missing_streak = 0
_dom_metric_circuit_open = False
_dom_metric_circuit_warned = False
_CHART_TICK_ARTIFACTS = {
    "05101520",
    "0102030",
    "01020304050",
    "0246",
    "02468",
    "020406080100",
    "0255075100",
    "012345678910",
}


def _reset_ad_dom_circuit() -> None:
    _record_filter_extract_success()


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
def _dump_ad_dom_diagnostics(driver, btn_idx: int, store_name: str) -> None:
    try:
        for wait_sec in (0, 2, 5):
            if wait_sec:
                time.sleep(wait_sec)
            snapshot = driver.execute_script(
                """
                const labels = ['노출수', '클릭수', '주문수', '주문금액'];
                const trim = (v) => String(v || '').replace(/\\s+/g, ' ').trim();
                const limit = (v, n = 1500) => trim(v).slice(0, n);
                const btns = [...document.querySelectorAll('button.Filter-module__lRdH')];
                const btn = btns[arguments[0]];
                const ancestors = [];
                let el = btn;
                for (let i = 0; i < 6 && el && el !== document.body; i++) {
                    el = el.parentElement;
                    if (el) ancestors.push(limit(el.outerHTML));
                }
                const labelNodes = [];
                for (const node of [...document.querySelectorAll('body *')]) {
                    const text = trim(node.textContent);
                    if (!labels.some((label) => text.includes(label))) continue;
                    const parent = node.parentElement;
                    labelNodes.push({
                        tag: node.tagName,
                        className: String(node.className || ''),
                        text: limit(text, 500),
                        parentText: parent ? limit(parent.textContent, 500) : '',
                        siblingText: parent
                          ? [...parent.children].map((child) => limit(child.textContent, 200)).filter(Boolean).slice(0, 8)
                          : [],
                        childText: [...node.children].map((child) => limit(child.textContent, 200)).filter(Boolean).slice(0, 8),
                    });
                    if (labelNodes.length >= 40) break;
                }
                const numericNodes = [];
                for (const node of [...document.querySelectorAll('body *')]) {
                    const text = trim(node.textContent);
                    if (!/[\\d,]/.test(text)) continue;
                    if (text.length > 120) continue;
                    numericNodes.push({
                        tag: node.tagName,
                        className: String(node.className || ''),
                        text,
                    });
                    if (numericNodes.length >= 120) break;
                }
                return {
                    ready: document.readyState,
                    buttonText: btn ? trim(btn.textContent) : '',
                    ancestors,
                    labelNodes,
                    numericNodes,
                };
                """,
                btn_idx,
            ) or {}
            logger.warning(
                "ad funnel DOM diagnostics wait=%ss Filter[%d] / %s: %s",
                wait_sec,
                btn_idx,
                store_name,
                snapshot,
            )
    except Exception as exc:
        logger.debug("ad funnel DOM diagnostics failed: %s / Filter[%d] / %s", exc, btn_idx, store_name)


def _record_filter_extract_success() -> None:
    global _dom_metric_missing_streak, _dom_metric_circuit_open, _dom_metric_circuit_warned
    _dom_metric_missing_streak = 0
    _dom_metric_circuit_open = False
    _dom_metric_circuit_warned = False


def _record_filter_extract_missing() -> None:
    global _dom_metric_missing_streak, _dom_metric_circuit_open
    _dom_metric_missing_streak += 1
    if _dom_metric_missing_streak >= _DOM_MISSING_CIRCUIT_THRESHOLD:
        _dom_metric_circuit_open = True


def _record_filter_extract_failure() -> None:
    _record_filter_extract_missing()


def _ad_dom_circuit_open() -> bool:
    return _dom_metric_circuit_open


def _warn_ad_dom_circuit_once() -> None:
    global _dom_metric_circuit_warned
    if not _dom_metric_circuit_warned:
        logger.warning("DOM 구조 변경 의심 — ad_funnel 수집 중단")
        _dom_metric_circuit_warned = True


def _extract_filter_vals(driver, btn_idx: int, store_name: str) -> dict[str, str] | None:
    vals: dict[str, str] = driver.execute_script(
        """
        const btns = document.querySelectorAll('button.Filter-module__lRdH');
        const btn = btns[arguments[0]];
        if (!btn) return {};

        const metricLabels = ['노출수', '클릭수', '주문수', '주문금액'];
        const chartTickArtifacts = new Set(['05101520', '01020304050', '020406080100', '0255075100', '012345678910']);
        const normalize = (v) => String(v || '').replace(/\\s+/g, ' ').trim();
        const metricFromText = (text, label = '') => {
            const normalized = normalize(text);
            if (metricLabels.some((label) => normalized === label || normalized.includes(label))) {
                return null;
            }
            const compact = normalized.replace(/\\s+/g, '').replace(/,/g, '');
            if (chartTickArtifacts.has(compact)) return null;
            if (/^0\\d{3,}$/.test(compact)) return null;
            if (/^(?:0|5|10|15|20){4,}$/.test(compact)) return null;
            if (label !== '주문금액' && /^[0]\\d{2,}$/.test(compact)) return null;
            const exact = normalized.match(/^([\\d,]+)\\s*(?:회|원)?$/);
            if (exact) return exact[1];
            return null;
        };
        const elementIndex = (node) => {
            const all = [...document.querySelectorAll('body *')];
            return all.indexOf(node);
        };
        const closestLabelNode = (label) => {
            const nodes = [...document.querySelectorAll('body *')].filter((node) => {
                const text = normalize(node.textContent);
                if (!text.includes(label)) return false;
                if (metricFromText(text, label)) return false;
                return text.length <= 80 || node.children.length <= 3;
            });
            return nodes.sort((a, b) => normalize(a.textContent).length - normalize(b.textContent).length)[0] || null;
        };
        const candidateValue = (labelNode, label) => {
            const roots = [];
            let el = labelNode;
            for (let i = 0; i < 5 && el && el !== document.body; i++) {
                roots.push(el);
                el = el.parentElement;
            }
            const labelPos = elementIndex(labelNode);
            for (const root of roots) {
                const nodes = [...root.querySelectorAll('*')];
                for (const node of nodes) {
                    if (node === labelNode) continue;
                    if (elementIndex(node) < labelPos) continue;
                    const text = normalize(node.textContent);
                    if (!text || text.includes(label)) continue;
                    if (metricLabels.some((metricLabel) => text === metricLabel)) continue;
                    const value = metricFromText(text, label);
                    if (value) return value;
                }
            }
            return null;
        };

        const metrics = {};
        for (const label of metricLabels) {
            const node = closestLabelNode(label);
            const value = node ? candidateValue(node, label) : null;
            if (value) metrics[label] = value;
        }
        return metrics;
        """,
        btn_idx,
    ) or {}

    logger.info("Filter[%d] vals extracted: %s / %s", btn_idx, vals, store_name)
    if (
        btn_idx > 0
        and set(vals).issubset({"노출수", "클릭수"})
        and vals
        and all(_metric_int(value) == 0 for value in vals.values())
    ):
        zero_metrics = {"주문수": "0", "주문금액": "0"}
        logger.info(
            "Filter[%d] order-metric zero fallback: %s -> %s / %s",
            btn_idx,
            vals,
            zero_metrics,
            store_name,
        )
        return zero_metrics
    if not vals:
        no_period_metrics = driver.execute_script(
            """
            const text = document.body ? document.body.textContent : '';
            const noDataCount = (text.match(/선택한 기간의 정보가 없어요/g) || []).length;
            const btns = [...document.querySelectorAll('button.Filter-module__lRdH')]
              .map(b => (b.textContent || '').trim());
            return {noDataCount, btns};
            """
        ) or {}
        if int(no_period_metrics.get("noDataCount") or 0) > 0:
            zero_metrics = (
                {"노출수": "0", "클릭수": "0"}
                if btn_idx == 0
                else {"주문수": "0", "주문금액": "0"}
            )
            logger.info(
                "Filter[%d] no-period-data fallback to zero metrics: %s / %s state=%s",
                btn_idx,
                zero_metrics,
                store_name,
                no_period_metrics,
            )
            return zero_metrics
        logger.warning("Filter[%d] metrics missing in DOM: %s", btn_idx, store_name)
        _record_filter_extract_missing()
        _dump_ad_dom_diagnostics(driver, btn_idx, store_name)
        return None
    return vals


def _metric_int(value: object) -> int | None:
    text = str(value or "").replace(",", "").strip()
    if not text.isdigit():
        return None
    return int(text)


def _looks_like_chart_tick_artifact(value: object) -> bool:
    text = str(value or "").replace(",", "").replace(" ", "").strip()
    return text in _CHART_TICK_ARTIFACTS or (len(text) >= 4 and text.startswith("0") and text.isdigit())


def _metrics_are_plausible(metrics: dict) -> bool:
    if any(_looks_like_chart_tick_artifact(metrics.get(label)) for label in _METRIC_LABELS):
        return False
    values = {label: _metric_int(metrics.get(label)) for label in _METRIC_LABELS}
    if any(value is None for value in values.values()):
        return False
    impressions = int(values["노출수"])
    clicks = int(values["클릭수"])
    orders = int(values["주문수"])
    order_amount = int(values["주문금액"])
    if clicks > impressions:
        return False
    if orders > clicks:
        return False
    if orders > 0 and clicks == 0:
        return False
    if order_amount > 0 and orders == 0:
        return False
    return True


def _orders_zero_sales_for_store(brand: str, store: str, target_date: str) -> bool:
    """Return True when orders data proves this brand/store had zero sales."""
    if has_orders_no_data_marker(brand, store, target_date):
        return True

    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    paths = find_tables(
        BAEMIN_ORDERS_DB,
        f"brand={brand}/store={store}/ym={ym}/orders_{ym}",
    )
    if not paths:
        return False

    observed_orders_source = False
    total_rows = 0
    total_amount = 0
    for path in paths:
        try:
            df = read_file(path)
        except Exception as exc:
            logger.warning("orders zero-sales check read failed: %s / %s", path, exc)
            return False
        required = {"주문상태", "주문시각", "결제금액"}
        if df.empty or not required.issubset(df.columns):
            continue
        observed_orders_source = True
        rows = df[
            (df["주문상태"].astype(str) == "배달완료")
            & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
        ]
        total_rows += len(rows)
        total_amount += int(
            pd.to_numeric(
                rows["결제금액"].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

    return observed_orders_source and total_rows == 0 and total_amount == 0


def _mark_ad_funnel_zero_sales(store_info: dict, target_date: str) -> bool:
    brand = str(store_info.get("brand") or "").strip()
    store = str(store_info.get("store") or "").strip()
    if not brand or not store:
        return False
    if not _orders_zero_sales_for_store(brand, store, target_date):
        return False
    saved = _save_ad_funnel_csv(
        {"노출수": "0", "클릭수": "0", "주문수": "0", "주문금액": "0"},
        brand,
        store,
        target_date,
        status="zero_sales",
    )
    logger.info("ad_funnel 0원 정상 처리: %s / %s -> %s", store, target_date, saved)
    return True


def filter_ad_funnel_zero_sales_failures(failed: dict | None, target_date: str | None) -> dict:
    """Drop ad-funnel failures that are explained by confirmed zero Baemin orders."""
    data = failed or {}
    filtered = {
        "accounts": list(data.get("accounts") or []),
        "stores": list(data.get("stores") or []),
        "orders": list(data.get("orders") or []),
        "ads": [],
        "stages": list(data.get("stages") or []),
    }
    if not target_date:
        filtered["ads"] = list(data.get("ads") or [])
        return filtered

    dropped = 0
    for item in data.get("ads") or []:
        if not isinstance(item, dict):
            filtered["ads"].append(item)
            continue
        kept_stores = []
        for store_info in item.get("stores") or []:
            if isinstance(store_info, dict) and _mark_ad_funnel_zero_sales(store_info, target_date):
                dropped += 1
            else:
                kept_stores.append(store_info)
        if kept_stores:
            filtered["ads"].append({**item, "stores": kept_stores})

    if dropped:
        logger.info("ad_funnel 0원 정상 실패 제외: %d건 / %s", dropped, target_date)
    return filtered


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
            metrics.update(vals)
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
            _record_filter_extract_failure()
            return None
        if not _metrics_are_plausible(metrics):
            logger.warning("Metrics failed plausibility check: %s / %s", metrics, store_name)
            _record_filter_extract_failure()
            return None

        _record_filter_extract_success()
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
            metrics.update(vals)
            time.sleep(0.3)

        filter_texts = driver.execute_script(
            "return [...document.querySelectorAll('button.Filter-module__lRdH')]"
            ".map(b => b.textContent.trim());"
        )
        logger.info("Filter texts after apply: %s / %s", filter_texts, store_name)

        target_day = str(pendulum.parse(target_date, tz=KST).day)
        if not all(target_day in t or "날짜 직접 선택" in t for t in filter_texts):
            logger.warning("A Filter button is not on target date(%s): %s", target_date, filter_texts)
            return None

        if len(metrics) < 4:
            logger.warning("Metrics incomplete (%d/4): %s / %s", len(metrics), metrics, store_name)
            _record_filter_extract_failure()
            return None
        if not _metrics_are_plausible(metrics):
            logger.warning("Metrics failed plausibility check: %s / %s", metrics, store_name)
            _record_filter_extract_failure()
            return None

        _record_filter_extract_success()
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


def _apply_yesterday_to_filter(driver, btn_idx: int, store_name: str) -> dict[str, str] | None:
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


def _get_calendar_ym(driver) -> tuple[int, int] | None:
    """Return visible calendar year/month without relying on hashed Baemin classes."""
    text = driver.execute_script(
        """
        function hasCalendarMonth(text) {
            return /(\\d{4})\\s*[.년]\\s*(\\d{1,2})\\s*[.월]?/.test(text || '');
        }
        function cleanText(el) {
            return (el?.textContent || '').replace(/\\s+/g, ' ').trim();
        }
        const prev = [...document.querySelectorAll('button')]
            .find(b => /이전\\s*달/.test(b.getAttribute('aria-label') || ''));
        if (prev) {
            let node = prev.parentElement;
            for (let i = 0; i < 5 && node; i++, node = node.parentElement) {
                const t = cleanText(node);
                if (t.includes('~')) continue;
                if (hasCalendarMonth(t)) return t;
            }
        }
        const anchors = [
            '[data-atelier-component*="DatePicker"]',
            '[data-atelier-component*="Calendar"]',
            '[role="dialog"]',
            '[class*="DatePicker"][class*="Header"]',
            '[class*="CalendarHeader"]',
        ];
        for (const sel of anchors) {
            for (const el of document.querySelectorAll(sel)) {
                const t = cleanText(el);
                if (t.includes('~')) continue;
                if (hasCalendarMonth(t)) return t;
            }
        }
        return null;
        """
    )
    if not text:
        return None
    match = re.search(r"(\d{4})\s*[.년]\s*(\d{1,2})", text)
    return (int(match.group(1)), int(match.group(2))) if match else None


def _ad_filter_button_text(driver, btn_idx: int) -> str:
    return driver.execute_script(
        "const btns = document.querySelectorAll('button.Filter-module__lRdH');"
        "return btns[arguments[0]]?.textContent || '';",
        btn_idx,
    ) or ""


def _date_picker_trigger_has_target(driver, target_year: int, target_month: int, target_day: int) -> bool:
    """Baemin's range picker often keeps the exact selected date on DatePicker.Trigger."""
    return bool(
        driver.execute_script(
            """
            const y = String(arguments[0]);
            const m = String(arguments[1]);
            const d = String(arguments[2]);
            const norm = (v) => String(v || '').replace(/\\s+/g, ' ').trim();
            const triggers = [...document.querySelectorAll('[data-atelier-component="DatePicker.Trigger"]')]
                .filter((el) => {
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                })
                .map((el) => norm(el.textContent));
            return triggers.some((text) => {
                const compact = text.replace(/\\s+/g, '');
                return compact.includes(`${y}.${m}.${d}`)
                    || compact.includes(`${y}년${m}월${d}일`);
            });
            """,
            target_year,
            target_month,
            target_day,
        )
    )


def _ad_filter_debug_state(driver, btn_idx: int) -> dict:
    try:
        return driver.execute_script(
            """
            const trim = (v) => String(v || '').replace(/\\s+/g, ' ').trim();
            const visible = (el) => {
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
            };
            return {
                filterTexts: [...document.querySelectorAll('button.Filter-module__lRdH')]
                    .map((b) => trim(b.textContent)),
                buttons: [...document.querySelectorAll('button')]
                    .filter(visible)
                    .map((b) => ({
                        text: trim(b.innerText || b.textContent).slice(0, 80),
                        aria: b.getAttribute('aria-label') || '',
                        disabled: Boolean(b.disabled),
                        dialog: Boolean(b.closest('[role="dialog"]')),
                        atelier: b.closest('[data-atelier-component]')?.getAttribute('data-atelier-component') || '',
                    }))
                    .filter((b) => b.text || b.aria)
                    .slice(-80),
                checkedPeriods: [...document.querySelectorAll('input[type="radio"]:checked')]
                    .map((r) => ({name: r.name, value: r.value, id: r.id})),
                targetText: trim(document.querySelectorAll('button.Filter-module__lRdH')[arguments[0]]?.textContent || ''),
            };
            """,
            btn_idx,
        ) or {}
    except Exception as exc:
        return {"error": str(exc)}


def _click_visible_apply_button(driver, *, prefer_dialog: bool | None = None) -> bool:
    return bool(
        driver.execute_script(
            """
            const preferDialog = arguments[0];
            const buttonPattern = /(적용|확인|완료|조회)/;
            const buttons = [...document.querySelectorAll('button')].filter((b) => {
                const rect = b.getBoundingClientRect();
                const text = (b.innerText || b.textContent || '').trim();
                if (b.disabled || rect.width <= 0 || rect.height <= 0 || !buttonPattern.test(text)) return false;
                if (preferDialog === null) return true;
                return Boolean(b.closest('[role="dialog"]')) === preferDialog;
            });
            if (!buttons.length) return false;
            buttons[buttons.length - 1].click();
            return true;
            """,
            prefer_dialog,
        )
    )


def _apply_until_filter_text_changes(driver, btn_idx: int, target_day: int, store_name: str) -> bool:
    """DatePicker and filter popover can each have an apply button; click both if needed."""
    for attempt in range(4):
        if not _click_visible_apply_button(driver, prefer_dialog=None):
            logger.warning(
                "Filter[%d] apply button not found: %s state=%s",
                btn_idx,
                store_name,
                _ad_filter_debug_state(driver, btn_idx),
            )
            return False
        try:
            WebDriverWait(driver, 12).until(
                lambda d: (
                    str(target_day) in _ad_filter_button_text(d, btn_idx)
                    or "날짜 직접 선택" in _ad_filter_button_text(d, btn_idx)
                )
            )
            return True
        except TimeoutException:
            if attempt < 3:
                logger.warning(
                    "Filter[%d] text unchanged, retry apply click %d: %s state=%s",
                    btn_idx,
                    attempt + 1,
                    store_name,
                    _ad_filter_debug_state(driver, btn_idx),
                )
                time.sleep(0.5)
                continue
            logger.warning(
                "Filter[%d] text not changed after apply: %s state=%s",
                btn_idx,
                store_name,
                _ad_filter_debug_state(driver, btn_idx),
            )
            return False
    return False


def _apply_specific_date_to_filter(
    driver,
    btn_idx: int,
    target_date: str,
    store_name: str,
) -> dict[str, str] | None:
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

    if _date_picker_trigger_has_target(driver, target_year, target_month, target_day):
        if not _apply_until_filter_text_changes(driver, btn_idx, target_day, store_name):
            return None
        time.sleep(2.0)  # React data fetch completion is not tied to button text.
        return _extract_filter_vals(driver, btn_idx, store_name)

    target_month_index = target_year * 12 + target_month
    ym_unreadable = 0
    for _ in range(24):
        ym = _get_calendar_ym(driver)
        if ym == (target_year, target_month):
            break
        if not ym:
            ym_unreadable += 1
            if ym_unreadable >= 2:
                logger.warning("Filter[%d] calendar month unreadable: %s / %s", btn_idx, target_date, store_name)
                break
            time.sleep(0.4)
            continue
        direction = "prev" if (ym[0] * 12 + ym[1]) > target_month_index else "next"
        moved = driver.execute_script(
            """
            const direction = arguments[0];
            const buttons = [...document.querySelectorAll('button')].filter(b => {
                const rect = b.getBoundingClientRect();
                return !b.disabled && rect.width > 0 && rect.height > 0;
            });
            function textOf(button) {
                return [
                    button.getAttribute('aria-label') || '',
                    button.getAttribute('title') || '',
                    button.textContent || '',
                    button.className || '',
                ].join(' ').trim();
            }
            const patterns = direction === 'prev'
                ? [/이전\\s*달/, /이전/, /prev/i, /previous/i, /^<$/]
                : [/다음\\s*달/, /다음/, /next/i, /^>$/];
            const target = buttons.find(button => patterns.some(pattern => pattern.test(textOf(button))));
            if (!target) return false;
            target.click();
            return true;
            """,
            direction,
        )
        if not moved:
            logger.warning("Filter[%d] calendar month move button not found: %s / %s", btn_idx, target_date, store_name)
            break
        time.sleep(0.3)

    final_ym = _get_calendar_ym(driver)
    if final_ym != (target_year, target_month):
        if _date_picker_trigger_has_target(driver, target_year, target_month, target_day):
            logger.info(
                "Filter[%d] calendar month mismatch ignored because trigger has target date: target=%s final_ym=%s / %s",
                btn_idx,
                target_date,
                final_ym,
                store_name,
            )
            if not _apply_until_filter_text_changes(driver, btn_idx, target_day, store_name):
                return None
            time.sleep(2.0)  # React data fetch completion is not tied to button text.
            return _extract_filter_vals(driver, btn_idx, store_name)
        logger.warning(
            "Filter[%d] calendar target month failed: target=%s final_ym=%s / %s",
            btn_idx,
            target_date,
            final_ym,
            store_name,
        )
        return None

    clicked_start = driver.execute_script(
        """
        const targetDay = String(arguments[0]);
        const dayLabel = arguments[1];
        const buttons = [...document.querySelectorAll('button')].filter(b => {
            const rect = b.getBoundingClientRect();
            const aria = b.getAttribute('aria-label') || '';
            const text = (b.textContent || '').trim();
            return !b.disabled
                && rect.width > 0
                && rect.height > 0
                && (aria === dayLabel || aria.includes(dayLabel) || text === targetDay || text === dayLabel);
        });
        if (buttons.length > 0) {
            buttons[0].click();
            return true;
        }
        return false;
        """,
        target_day,
        day_label,
    )
    if not clicked_start:
        clicked_start = driver.execute_script(
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
        const targetDay = String(arguments[0]);
        const dayLabel = arguments[1];
        const buttons = [...document.querySelectorAll('button')].filter(b => {
            const rect = b.getBoundingClientRect();
            const aria = b.getAttribute('aria-label') || '';
            const text = (b.textContent || '').trim();
            return !b.disabled
                && rect.width > 0
                && rect.height > 0
                && (aria === dayLabel || aria.includes(dayLabel) || text === targetDay || text === dayLabel);
        });
        if (buttons.length > 0) {
            buttons[buttons.length - 1].click();
            return true;
        }
        return false;
        """,
        target_day,
        day_label,
    )
    if not clicked_end:
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

    if not _apply_until_filter_text_changes(driver, btn_idx, target_day, store_name):
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
    if _ad_dom_circuit_open():
        _warn_ad_dom_circuit_once()
        return False
    if _snapshot_ad_metric_state(driver).get("no_ads"):
        saved = _save_ad_funnel_csv(None, brand, store, target_date, status="no_ads")
        logger.info("No ads (skip): %s / %s -> %s", store, target_date, saved)
        return True

    metrics = _set_ad_filter(driver, target_date, store)
    if metrics is None:
        metrics = _reload_and_collect(driver, store_id, store, target_date)
    if metrics is None:
        saved = _save_ad_funnel_csv(None, brand, store, target_date, status="parse_error")
        logger.warning("ad funnel extraction incomplete: %s / %s", store, target_date)
        logger.info("Saved ad funnel parse_error CSV: brand=%s store=%s %s", brand, store, saved)
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
        if _ad_dom_circuit_open():
            _warn_ad_dom_circuit_once()
            return False
        driver.set_page_load_timeout(45)
        driver.set_script_timeout(60)
        driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            logger.warning("Ad funnel page failed to load, skipping: %s", store)
            return False

        return _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date)
    except Exception as exc:
        if is_driver_crash_error(exc):
            raise
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
        if _ad_dom_circuit_open():
            _warn_ad_dom_circuit_once()
            return False
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
    *,
    max_attempts: int = 3,
) -> list[dict]:
    """Collect ad funnel stats per store with independent Chrome sessions.

    Returns:
        Store info entries that failed collection.
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed_stores: list[dict] = []

    for store_index, store_info in enumerate(store_list):
        _reset_ad_dom_circuit()
        store_id = store_info["store_id"]
        brand = store_info["brand"]
        store = store_info["store"]

        logger.info("Ad funnel collection start: %s (%s) / %s", store, store_id, target_date)

        succeeded = False
        attempts = max(int(max_attempts or 1), 1)
        for attempt in range(attempts):
            driver = None
            try:
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, password):
                    logger.warning(
                        "Login failed: %s / %s attempt=%d/%d",
                        account_id,
                        store,
                        attempt + 1,
                        attempts,
                    )
                    continue

                driver.set_page_load_timeout(75)
                try:
                    driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))
                except TimeoutException as exc:
                    logger.info("Ad funnel page load delayed, continuing with DOM wait: %s / %s", store, exc)
                    try:
                        driver.execute_script("window.stop();")
                    except Exception:
                        pass

                if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
                    logger.warning("Ad funnel page failed to load, skipping: %s", store)
                    if attempt < attempts - 1:
                        continue
                    break

                if not _collect_ad_funnel_metrics(driver, store_id, brand, store, target_date):
                    if attempt < attempts - 1:
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

            if attempt < attempts - 1:
                time.sleep(random.uniform(3.0, 5.0))

        if not succeeded:
            failed_stores.append(store_info)

        time.sleep(random.uniform(0.5, 1.5))

    return failed_stores
def _validate_and_retry_ad_funnel(
    store_infos: list[dict],
    target_date: str,
    *,
    deadline_at: float | None = None,
    min_store_budget_sec: int = 240,
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
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            empty_stores.append(si)
            logger.warning("ad_funnel CSV 미생성: %s / %s", store, target_date)
            continue
        df = pd.read_csv(csv_path, dtype=str)
        row = df[df["target_date"] == target_date]
        if row.empty:
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            empty_stores.append(si)
            logger.warning("ad_funnel target_date 미생성: %s / %s", store, target_date)
            continue
        r = row.iloc[0]
        status = str(r.get(_STATUS_COLUMN, "")).strip().lower()
        if status == "no_ads":
            logger.info("ad_funnel 광고 없음(정상): %s / %s", store, target_date)
            continue
        if status == "zero_sales":
            logger.info("ad_funnel 0원 정상: %s / %s", store, target_date)
            continue
        if status and status != "ok":
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            logger.warning("ad_funnel 수집 실패 상태: %s / %s / %s", store, target_date, status)
            empty_stores.append(si)
            continue
        if str(r.get("주문수", "")).strip() == "" or str(r.get("주문금액", "")).strip() == "":
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            logger.warning("ad_funnel 빈값 발견: %s / %s", store, target_date)
            empty_stores.append(si)

    if not empty_stores:
        return {"empty_stores": [], "retried": [], "still_empty": []}

    logger.warning(
        "ad_funnel 빈값 재수집 대상: %s", [s["store"] for s in empty_stores]
    )

    retried_stores: list[dict] = []
    deferred_stores: list[dict] = []
    for index, store_info in enumerate(empty_stores):
        remaining = None if deadline_at is None else float(deadline_at) - time.monotonic()
        if remaining is not None and remaining < min_store_budget_sec:
            deferred_stores.extend(empty_stores[index:])
            logger.warning(
                "ad_funnel 재수집 deadline 임박으로 잔여 이월: remaining=%s stores=%d",
                remaining,
                len(empty_stores) - index,
            )
            break
        collect_ad_funnel_for_account(
            store_info["account_id"],
            store_info["password"],
            [store_info],
            target_date=target_date,
            max_attempts=1,
        )
        retried_stores.append(store_info)

    # 재수집 후 재점검
    still_empty: list[dict] = list(deferred_stores)
    for si in retried_stores:
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
        if status == "zero_sales":
            logger.info("재수집 성공(0원 정상): %s / %s", store, target_date)
            continue
        if status and status != "ok":
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            still_empty.append(si)
            logger.warning("재수집 후 parse_error 잔존: %s / %s / %s", store, target_date, status)
            continue
        if str(row2.iloc[0].get("주문수", "")).strip() == "":
            if _mark_ad_funnel_zero_sales(si, target_date):
                continue
            still_empty.append(si)
            logger.warning("재수집 후에도 빈값 잔존: %s / %s", store, target_date)
        else:
            logger.info("재수집 성공: %s / %s", store, target_date)

    return {
        "empty_stores": empty_stores,
        "retried": retried_stores,
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


