"""
EasyPOS: 각 메뉴 항목별 그리드 ID 탐색
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

def get_mf(page):
    for f in page.frames:
        if f.name == "main": return f
    return None

def find_grd_row_by_text(page, texts):
    mf = get_mf(page)
    for el in mf.query_selector_all("[id*='grdLeft_body_gridrow_']"):
        txt = (el.inner_text() or "").strip()
        if txt in texts:
            return el
    return None

def get_work_grids(mf):
    return mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="divWork"] [id*="_body_"]'))
            .filter(el => !el.id.includes('grdLeft') && /gridrow_\\d+$/.test(el.id))
            .map(el => ({
                grid: el.id.split('divWork_')[1].split('_body_')[0],
                row: el.id.split('_body_')[1],
                text: (el.textContent||'').trim().slice(0,40),
            }));
    }""")

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

    page = ctx.new_page()
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

    # 영업속보 탭 클릭
    mf = get_mf(page)
    tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    box = tab.bounding_box()
    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(1.5)
    try: mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8000)
    except: pass

    # 영업일보 클릭 후 메뉴 목록 얻기
    yil_el = find_grd_row_by_text(page, ["영업일보"])
    if yil_el:
        box = yil_el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(1.5)

    mf = get_mf(page)
    menu_items = mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="grdLeft_body_gridrow_"]'))
            .filter(el => /gridrow_\\d+$/.test(el.id))
            .map(el => ({
                idx: parseInt(el.id.split('gridrow_')[1]),
                text: (el.textContent||'').trim(),
                y: Math.round(el.getBoundingClientRect().y),
                x: Math.round(el.getBoundingClientRect().x),
                w: Math.round(el.getBoundingClientRect().width),
                h: Math.round(el.getBoundingClientRect().height),
            }))
            .filter(el => el.text && el.w > 0);
    }""")
    print(f"=== 메뉴 목록 ({len(menu_items)}개) ===")
    for m in menu_items:
        print(f"  row_{m['idx']}: {m['text']}")

    # 각 서브메뉴 더블클릭 후 그리드 확인
    # 영업일보(row_1) 제외하고 나머지 더블클릭
    sub_menus = [m for m in menu_items if m['idx'] >= 2]

    for menu in sub_menus:
        print(f"\n=== [{menu['text']}] 더블클릭 ===")
        cx = menu['x'] + menu['w'] / 2
        cy = menu['y'] + menu['h'] / 2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        time.sleep(3)

        # 조회 버튼이 있으면 전일+조회
        mf = get_mf(page)
        has_search = mf.evaluate("() => !!document.querySelector('[id*=\"btnCommSearch\"]')")
        if has_search:
            el = mf.query_selector("[id*='btnBeforeDay']")
            if el:
                box = el.bounding_box()
                if box:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(0.8)
            el = mf.query_selector("[id*='btnCommSearch']")
            if el:
                box = el.bounding_box()
                if box:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(5)

        mf = get_mf(page)
        grids = get_work_grids(mf)
        unique_grids = list(dict.fromkeys(r['grid'] for r in grids))
        print(f"  그리드: {unique_grids}")
        if grids:
            print(f"  첫 행: {grids[0]['text'][:50]}")
        if 'grdSalePerDayList' in unique_grids:
            print(f"  ★★★ grdSalePerDayList 발견!")
            # NexacroN dataset 확인
            ds = mf.evaluate("""() => {
                try {
                    var grd = window.application.mainframe.childframe.form.divMain.divWork.grdSalePerDayList;
                    var ds = grd._datasets && grd._datasets[0];
                    if (!ds) return {err: 'no ds'};
                    var rc = ds.rowcount || 0;
                    var cc = ds.colcount || 0;
                    var cols = [];
                    for (var j = 0; j < cc; j++) cols.push(ds.getColID(j));
                    var rows = [];
                    for (var i = 0; i < Math.min(rc, 5); i++) {
                        var row = {};
                        for (var k = 0; k < Math.min(cc, 10); k++) row[cols[k]] = ds.getColumn(i, k);
                        rows.push(row);
                    }
                    return {rowCount: rc, cols: cols, rows: rows};
                } catch(e) { return {err: e.toString()}; }
            }""")
            print(f"  grdSalePerDayList dataset: {json.dumps(ds, ensure_ascii=False)}")
            break

    browser.close()
    print("\n=== 완료 ===")
