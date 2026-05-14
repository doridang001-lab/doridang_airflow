import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright
import logging

# Ensure local imports work when run from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.playwright_launcher import launch_chromium  # noqa: E402
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (  # noqa: E402
    _BTN_SEARCH_SELECTORS,
    _POPUP_CLOSE_BTN,
    _POPUP_DETAIL_LIST,
    _click_handle,
    _get_main_frame,
    _get_row_indices_pw,
    _login,
    _navigate_to_daily_sales,
    _select_yesterday_and_search,
)


def _cell_text(mf, row_idx: int, col_idx: int) -> str:
    # Try a few likely Nexacro DOM ids/suffixes.
    base = f"mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_gridrow_{row_idx}_cell_{row_idx}_{col_idx}"
    for suffix in ("GridCellTextContainerElement", "GridCellTextSimpleContainerElement", ""):
        sel = f"#{base}{suffix}" if suffix else f"#{base}"
        try:
            el = mf.query_selector(sel)
            if el is None:
                continue
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if txt:
                return txt
        except Exception:
            continue
    return ""


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sale_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    print("sale_date:", sale_date)

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
        try:
            mf = _select_yesterday_and_search(page, mf, sale_date)
        except Exception as e:
            mf = _get_main_frame(page)
            try:
                date_el = mf.query_selector("[id*='mskSales'][id$='_input']")
                shown = ((date_el.evaluate("el => el.value") or "").strip() if date_el else "")
                print("shown_date(on_failure):", shown)
            except Exception:
                pass
            try:
                out = Path(__file__).resolve().parents[1] / f"tmp_easypos_click_test_FAIL_{sale_date}.png"
                page.screenshot(path=str(out), full_page=True)
                print("screenshot:", out.name)
            except Exception as se:
                print("screenshot_failed:", se)
            raise

        mf = _get_main_frame(page)
        try:
            date_el = mf.query_selector("[id*='mskSales'][id$='_input']")
            shown = ((date_el.evaluate("el => el.value") or "").strip() if date_el else "")
            print("shown_date:", shown)
        except Exception:
            pass
        try:
            out = Path(__file__).resolve().parents[1] / f"tmp_easypos_click_test_{sale_date}.png"
            page.screenshot(path=str(out), full_page=True)
            print("screenshot:", out.name)
        except Exception as e:
            print("screenshot_failed:", e)

        row_indices = []
        for _ in range(15):
            mf = _get_main_frame(page)
            row_indices = _get_row_indices_pw(mf, "grdSalePerDayList_body_gridrow_")
            if row_indices:
                break
            time.sleep(1)
        print("row_indices:", row_indices)
        if not row_indices:
            browser.close()
            return 0

        ok_count = 0
        fail = []

        for row_idx in row_indices:
            mf = _get_main_frame(page)
            receipt_btn = mf.query_selector(
                f"#mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_gridrow_{row_idx}_cell_{row_idx}_2_controlbutton"
            )
            receipt_no = ""
            try:
                tb = mf.query_selector(
                    f"#mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_gridrow_{row_idx}_cell_{row_idx}_2_controlbuttonTextBoxElement"
                )
                receipt_no = ((tb.inner_text() or "").strip() if tb else "")
            except Exception:
                pass

            table_name = _cell_text(mf, row_idx, 3)
            print(f"[row={row_idx}] receipt_no={receipt_no!r} table_name={table_name!r}")

            if receipt_btn is None:
                fail.append((row_idx, receipt_no, table_name, "no_button"))
                continue

            if not _click_handle(page, receipt_btn, f"receipt(row={row_idx})", prefer_mouse=True):
                fail.append((row_idx, receipt_no, table_name, "click_failed"))
                continue

            try:
                mf.wait_for_selector(f"#{_POPUP_DETAIL_LIST}", timeout=10_000)
            except Exception:
                fail.append((row_idx, receipt_no, table_name, "popup_timeout"))
                continue
            mf = _get_main_frame(page)
            detail_indices = _get_row_indices_pw(mf, "grdDetailList_body_gridrow_")
            if not detail_indices:
                fail.append((row_idx, receipt_no, table_name, "no_detail_rows"))
            else:
                ok_count += 1
            print(f"  detail_rows={len(detail_indices)}")

            # Close popup
            mf = _get_main_frame(page)
            close = mf.query_selector(f"#{_POPUP_CLOSE_BTN}")
            if close is not None:
                _click_handle(page, close, "popup_close", prefer_mouse=True)
            try:
                mf.wait_for_selector(f"#{_POPUP_DETAIL_LIST}", state="hidden", timeout=10_000)
            except Exception:
                # last resort: press ESC
                try:
                    page.keyboard.press("Escape")
                except Exception:
                    pass
            time.sleep(0.2)

        print("ok_count:", ok_count, "/", len(row_indices))
        if fail:
            print("failures:")
            for item in fail:
                print(" ", item)

        # Quick sanity: search button still exists
        mf = _get_main_frame(page)
        s_btn, _ = None, None
        for sel in _BTN_SEARCH_SELECTORS:
            try:
                s_btn = mf.query_selector(sel)
                if s_btn is not None:
                    break
            except Exception:
                continue
        print("search_btn_exists:", bool(s_btn))

        browser.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
