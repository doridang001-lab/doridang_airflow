"""
EasyPOS 15차: grdLeftFavorite 더블클릭 + 메뉴 데이터셋에서 정확한 formurl 추출
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

all_reqs = []
all_console = []

def get_mf(page):
    for f in page.frames:
        if f.name == "main": return f
    return None

def wait_for_sales_page(page, seconds=20):
    for i in range(1, seconds+1):
        time.sleep(1)
        mf = get_mf(page)
        r = mf.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 당일매출내역 화면 로드 성공! {r}")
            return True
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")
    return False

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled","--window-size=1920,1080"],
    )
    ctx = browser.new_context(
        viewport={"width":1920,"height":1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )
    ctx.add_init_script("""
        (function() {
            var _origGetById = Document.prototype.getElementById;
            Document.prototype.getElementById = function(id) {
                var el = _origGetById.call(this, id);
                if (!el && id === 'loadingImg') {
                    return {style:{display:'block'},id:'loadingImg',setAttribute:function(){},removeAttribute:function(){}};
                }
                return el;
            };
            Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
        })();
    """)
    ctx.on("request", lambda r: all_reqs.append(r.url))

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:200]}"))

    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)

    mf = get_mf(page)
    mf.query_selector("#mainframe_childframe_form_divMain_edtId").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtId_input").fill(EASYPOS_ID)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw_input").fill(EASYPOS_PW)
    mf.query_selector("#mainframe_childframe_form_divMain_btnLogin").click()
    time.sleep(6)

    for pid in [
        "mainframe_childframe_popupChangePasswd_form_div_popup_bottom_btnClose",
        "mainframe_childframe_placeAdPopup_titlebar_closebutton",
    ]:
        mf = get_mf(page)
        el = mf and mf.query_selector(f"#{pid}")
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1)
    time.sleep(2)
    all_reqs.clear()
    all_console.clear()

    # --- 1. 메뉴 데이터셋에서 당일매출내역 formurl 추출 ---
    mf = get_mf(page)
    menu_url_info = mf.evaluate("""() => {
        try {
            var cf = window.application.mainframe.childframe;
            var form = cf.form;

            // dsLeftMenuParnas 데이터셋 접근 (NexacroN Dataset API)
            var ds = form.dsLeftMenuParnas;
            if (!ds) return {err: 'dsLeftMenuParnas not found'};

            var result = [];
            var rowCount = ds.rowcount || ds._rowcount || 0;
            for (var i = 0; i < Math.min(rowCount, 200); i++) {
                var menuNm = ds.getColumn(i, 'MENU_NM') || ds.getColumn(i, 'menu_nm') || '';
                var menuUrl = ds.getColumn(i, 'MENU_URL') || ds.getColumn(i, 'menu_url') || '';
                var menuId = ds.getColumn(i, 'MENU_ID') || ds.getColumn(i, 'menu_id') || '';
                if (menuNm && (menuNm.includes('당일') || menuNm.includes('일자별'))) {
                    result.push({idx: i, nm: menuNm, url: menuUrl, id: menuId});
                }
            }

            // 컬럼 이름 확인
            var colNames = [];
            var colCount = ds.colcount || ds._colcount || 0;
            for (var j = 0; j < Math.min(colCount, 20); j++) {
                colNames.push(ds.getColID(j));
            }

            return {
                rowCount: rowCount,
                colCount: colCount,
                colNames: colNames,
                matches: result,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("=== dsLeftMenuParnas 당일매출내역 검색 ===")
    print(json.dumps(menu_url_info, indent=2, ensure_ascii=False))

    # dsLeftMenuParnas00도 확인
    menu_url_info2 = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var ds = form.dsLeftMenuParnas00;
            if (!ds) return {err: 'dsLeftMenuParnas00 not found'};
            var colCount = ds.colcount || ds._colcount || 0;
            var colNames = [];
            for (var j = 0; j < Math.min(colCount, 20); j++) colNames.push(ds.getColID(j));
            var rowCount = ds.rowcount || ds._rowcount || 0;
            var matches = [];
            for (var i = 0; i < Math.min(rowCount, 200); i++) {
                var nm = ds.getColumn(i, 'MENU_NM') || '';
                var url = ds.getColumn(i, 'MENU_URL') || '';
                if (nm.includes('당일') || nm.includes('일자별') || nm.includes('매출')) {
                    matches.push({idx: i, nm: nm, url: url});
                }
            }
            return {rowCount: rowCount, colNames: colNames, matches: matches.slice(0, 10)};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("\n=== dsLeftMenuParnas00 ===")
    print(json.dumps(menu_url_info2, indent=2, ensure_ascii=False))

    # --- 2. grdLeftFavorite 더블클릭 시도 ---
    print("\n=== 방법 1: grdLeftFavorite 즐겨찾기 더블클릭 ===")
    favor_row = mf.query_selector("[id*='grdLeftFavorite_body_gridrow_0']")
    if favor_row:
        box = favor_row.bounding_box()
        print(f"  즐겨찾기 행 위치: {box}")
        if box and box['width'] > 0:
            cx = box['x'] + box['width']/2
            cy = box['y'] + box['height']/2
            page.mouse.click(cx, cy); time.sleep(0.3)
            page.mouse.click(cx, cy, click_count=2); time.sleep(1)
            print(f"  더블클릭 완료 @ ({cx:.0f}, {cy:.0f})")
    else:
        print("  [WARN] grdLeftFavorite_body_gridrow_0 없음")

    if wait_for_sales_page(page, 10):
        page.screenshot(path=f"{OUT_DIR}/dom15_success_fav.png", full_page=True)
    else:
        page.screenshot(path=f"{OUT_DIR}/dom15_fail_fav.png", full_page=True)
        print("  즐겨찾기 더블클릭 실패")

    # XFDL 요청 확인
    xfdl_reqs = [r for r in all_reqs if '.xfdl' in r]
    print(f"  XFDL 요청: {xfdl_reqs}")
    all_reqs.clear()

    # --- 3. gfnGetMenuUrl로 URL 추출 후 set_formurl 직접 호출 ---
    print("\n=== 방법 2: gfnGetMenuUrl + set_formurl ===")
    direct_nav = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var cf = window.application.mainframe.childframe;

            // grdLeftFavorite 데이터셋에서 URL 추출
            var grdFav = form.divLeftMenu && form.divLeftMenu.divLeftFavoriteList &&
                         form.divLeftMenu.divLeftFavoriteList.grdLeftFavorite;
            if (!grdFav) return {err: 'grdLeftFavorite not found'};

            var ds = grdFav._datasets && grdFav._datasets[0];
            if (!ds) {
                // 데이터셋 직접 접근 시도
                return {err: 'no dataset', grdFavKeys: Object.keys(grdFav).slice(0,20)};
            }

            var colCount = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));

            var row0 = {};
            for (var c of cols) {
                row0[c] = ds.getColumn(0, c);
            }
            return {cols: cols, row0: row0};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"  grdLeftFavorite 데이터: {json.dumps(direct_nav, ensure_ascii=False)}")

    # 현재 childframe formurl 확인
    cur_url = mf.evaluate("""() => {
        try { return {formurl: window.application.mainframe.childframe.formurl}; }
        catch(e) { return {err: e.toString()}; }
    }""")
    print(f"  현재 formurl: {cur_url}")
    print(f"  전체 콘솔: {all_console}")

    browser.close()
print("\n=== 15차 테스트 완료 ===")
