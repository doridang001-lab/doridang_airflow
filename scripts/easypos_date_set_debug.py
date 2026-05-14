import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.playwright_launcher import launch_chromium  # noqa: E402
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (  # noqa: E402
    _BTN_SEARCH_SELECTORS,
    _DATE_INPUT_SELECTORS,
    _click_handle,
    _get_main_frame,
    _login,
    _navigate_to_daily_sales,
)


def _read_value(el) -> str:
    try:
        return ((el.evaluate("el => el.value") or "")).strip()
    except Exception:
        return ""


def main() -> int:
    sale_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    target_digits = "".join([c for c in sale_date if c.isdigit()])
    print("sale_date:", sale_date, "target_digits:", target_digits)

    with sync_playwright() as p:
        browser = launch_chromium(
            p,
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        )
        bctx = browser.new_context(viewport={"width": 1920, "height": 1080})
        bctx.add_init_script(
            """(function(){
                var _origGetById = Document.prototype.getElementById;
                Document.prototype.getElementById = function(id) {
                    var el = _origGetById.call(this, id);
                    if (!el && id === 'loadingImg') {
                        return { style: {display: 'block'}, id: 'loadingImg',
                            setAttribute: function(){}, removeAttribute: function(){} };
                    }
                    return el;
                };
                Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
            })();"""
        )

        page = bctx.new_page()
        mf = _login(page)
        mf = _navigate_to_daily_sales(page, mf)

        mf = _get_main_frame(page)
        date_el = mf.query_selector("[id*='mskSales'][id$='_input']")
        print("shown_date(before):", _read_value(date_el) if date_el else None)

        candidates = []
        for sel in _DATE_INPUT_SELECTORS:
            try:
                for el in mf.query_selector_all(sel):
                    el_id = el.get_attribute("id")
                    candidates.append((sel, el_id, el))
            except Exception:
                pass
        print("date_candidates:", [(s, i) for (s, i, _) in candidates])
        if not candidates:
            browser.close()
            return 1

        sel, el_id, el = candidates[0]
        print("using:", sel, el_id)

        def snapshot(tag: str) -> None:
            out = Path(__file__).resolve().parents[1] / f"tmp_easypos_date_{tag}.png"
            try:
                page.screenshot(path=str(out), full_page=True)
                print("screenshot:", out.name)
            except Exception as e:
                print("screenshot_failed:", e)

        def try_method(name: str, fn) -> None:
            try:
                el.click()
            except Exception:
                pass
            time.sleep(0.2)
            fn()
            time.sleep(0.6)
            print(f"[{name}] value:", _read_value(el))

        snapshot("before")

        try_method("A_page_type_digits", lambda: (
            page.keyboard.press("Control+a"),
            time.sleep(0.05),
            page.keyboard.type(target_digits, delay=60),
            page.keyboard.press("Tab"),
        ))
        try_method("B_page_type_dash", lambda: (
            page.keyboard.press("Control+a"),
            time.sleep(0.05),
            page.keyboard.type(sale_date, delay=60),
            page.keyboard.press("Tab"),
        ))
        try_method("C_el_type_digits", lambda: (
            el.press("Control+a"),
            time.sleep(0.05),
            el.type(target_digits, delay=60),
            el.press("Tab"),
        ))
        try_method("D_backspace_digits", lambda: (
            [page.keyboard.press("Backspace") for _ in range(12)],
            page.keyboard.type(target_digits, delay=60),
            page.keyboard.press("Tab"),
        ))
        try_method("E_eval_set_value_digits", lambda: (
            el.evaluate(
                """(el, v) => {
                    el.value = v;
                    el.dispatchEvent(new Event('input', {bubbles:true}));
                    el.dispatchEvent(new Event('change', {bubbles:true}));
                }""",
                target_digits,
            ),
            page.keyboard.press("Tab"),
        ))

        # Click search to see if grid loads (only if button exists)
        mf = _get_main_frame(page)
        s_btn = None
        for s in _BTN_SEARCH_SELECTORS:
            try:
                s_btn = mf.query_selector(s)
                if s_btn is not None:
                    break
            except Exception:
                continue
        if s_btn is not None:
            _click_handle(page, s_btn, "조회", prefer_mouse=True)
            time.sleep(2.5)
            snapshot("after_search")

        browser.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

