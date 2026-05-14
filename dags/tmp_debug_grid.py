import sys, time
sys.path.insert(0, '/opt/airflow')

from playwright.sync_api import sync_playwright
from modules.transform.utility.playwright_launcher import launch_chromium
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (
    _login, _navigate_to_daily_sales, _select_yesterday_and_search,
    _get_main_frame, _get_row_indices_pw
)

with sync_playwright() as p:
    browser = launch_chromium(p, headless=True, args=[
        '--no-sandbox','--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled','--window-size=1920,1080'
    ])
    bctx = browser.new_context(viewport={'width':1920,'height':1080})
    bctx.add_init_script("""(function(){var _orig=Document.prototype.getElementById;Document.prototype.getElementById=function(id){var el=_orig.call(this,id);if(!el&&id==='loadingImg')return{style:{display:'block'},id:'loadingImg',setAttribute:function(){},removeAttribute:function(){}};return el;};})();""")
    page = bctx.new_page()

    mf = _login(page)
    mf = _navigate_to_daily_sales(page, mf)
    mf = _select_yesterday_and_search(page, mf, '2026-04-20')
    time.sleep(3)
    mf = _get_main_frame(page)

    els = mf.query_selector_all("[id*='grdSalePerDayList_body']")
    ids = sorted(set(el.get_attribute('id') for el in els if el.get_attribute('id')))
    print('=== grdSalePerDayList_body 요소 수:', len(ids))
    for i in ids[:60]:
        print(' ', i)

    row_indices = _get_row_indices_pw(mf, 'grdSalePerDayList_body_gridrow_')
    print('\nrow_indices:', row_indices)

    browser.close()
