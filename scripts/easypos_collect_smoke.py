import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.playwright_launcher import launch_chromium  # noqa: E402
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (  # noqa: E402
    _collect_all_receipts,
    _get_main_frame,
    _login,
    _navigate_to_daily_sales,
    _select_yesterday_and_search,
)


def main() -> int:
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
        mf = _select_yesterday_and_search(page, mf, sale_date)

        # Give Nexacro a bit more time to finalize DOM.
        time.sleep(2)
        mf = _get_main_frame(page)

        rows = _collect_all_receipts(page, mf, sale_date)
        print("collected_rows:", len(rows))
        if rows:
            print("sample:", rows[0])

        browser.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

