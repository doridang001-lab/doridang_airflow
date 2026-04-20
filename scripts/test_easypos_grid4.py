"""
EasyPOS 전체 메뉴 트리 탐색 + grdTotalDay 클릭 시 grdSalePerDayList 나오는지 확인
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

def dump_menu(mf):
    items = mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="grdLeft_body_gridrow_"]'))
            .filter(el => el.id.match(/gridrow_\\d+$/))
            .map(el => ({
                id: el.id.split('gridrow_')[1],
                text: (el.textContent||'').trim().slice(0,30),
                x: Math.round(el.getBoundingClientRect().x),
                y: Math.round(el.getBoundingClientRect().y),
            }))
            .filter(el => el.text);
    }""")
    return items

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

    # 영업속보 클릭 후 초기 메뉴 덤프
    mf = get_mf(page)
    print("=== 초기 메뉴 트리 ===")
    for item in dump_menu(mf):
        print(f"  row_{item['id']}: {item['text']} @ y={item['y']}")

    # NexacroN dataset 전체 메뉴 읽기
    ds_menus = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var grd = form.divLeftMenu.divLeftMainList.grdLeft;
            var ds = grd._datasets && grd._datasets[0];
            if (!ds) return {err: 'no ds'};
            var rowCount = ds.rowcount || 0;
            var colCount = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));
            var rows = [];
            for (var i = 0; i < rowCount; i++) {
                var row = {};
                for (var k = 0; k < Math.min(colCount, 8); k++) row[cols[k]] = ds.getColumn(i, k);
                rows.push(row);
            }
            return {rowCount: rowCount, cols: cols, rows: rows};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== grdLeft dataset (rowCount={ds_menus.get('rowCount')}) ===")
    print(f"cols: {ds_menus.get('cols')}")
    for row in ds_menus.get('rows', []):
        print(f"  {row}")

    # 모든 카테고리 클릭해서 메뉴 확장
    print("\n=== 각 카테고리 클릭 후 메뉴 목록 ===")
    mf = get_mf(page)
    rows = mf.query_selector_all("[id*='grdLeft_body_gridrow_']:not([id*='GridArea']):not([id*='Inner']):not([id*='cell_'])")
    category_rows = [r for r in rows if r.id.split('gridrow_')[-1].isdigit()]

    for cat_el in category_rows:
        txt = (cat_el.inner_text() or "").strip()
        box = cat_el.bounding_box()
        if not box:
            continue
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.0)
        mf = get_mf(page)
        items = dump_menu(mf)
        print(f"\n  [{txt}] 클릭 후 메뉴:")
        for item in items:
            print(f"    row_{item['id']}: {item['text']}")

    # 일자별매출조회로 이동 후 grdTotalDay 데이터 + 행 클릭 실험
    print("\n\n=== 일자별매출조회 진입 후 grdTotalDay 탐색 ===")
    yil_el = find_grd_row_by_text(page, ["영업일보"])
    if yil_el:
        box = yil_el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    daily_el = find_grd_row_by_text(page, ["일자별매출조회"])
    if daily_el:
        box = daily_el.bounding_box()
        cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        print("일자별매출조회 더블클릭")

    mf = get_mf(page)
    for i in range(1, 11):
        time.sleep(1)
        if mf.evaluate("() => !!document.querySelector('[id*=\"btnBeforeDay\"]')"):
            print(f"  [{i}s] 화면 로드")
            break

    # 전일 + 조회
    mf = get_mf(page)
    el = mf.query_selector("[id*='btnBeforeDay']")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1)
    el = mf.query_selector("[id*='btnCommSearch']")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print("조회 클릭")
        time.sleep(5)

    # grdTotalDay 행 내용 확인
    mf = get_mf(page)
    total_day_rows = mf.evaluate("""() => {
        var rows = Array.from(document.querySelectorAll('[id*="grdTotalDay_body_gridrow_"]'))
            .filter(el => el.id.match(/gridrow_\\d+$/));
        return rows.map(el => ({
            id: el.id.split('_body_')[1],
            text: (el.textContent||'').trim().slice(0,60),
            y: Math.round(el.getBoundingClientRect().y),
        }));
    }""")
    print(f"\n=== grdTotalDay 행 ({len(total_day_rows)}개) ===")
    for r in total_day_rows:
        print(f"  {r}")

    # NexacroN dataset 확인
    ds_total = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var grd = form.divMain.divWork.grdTotalDay;
            var ds = grd._datasets && grd._datasets[0];
            if (!ds) return {err: 'no ds'};
            var rowCount = ds.rowcount || 0;
            var colCount = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));
            var rows = [];
            for (var i = 0; i < Math.min(rowCount, 5); i++) {
                var row = {};
                for (var k = 0; k < colCount; k++) row[cols[k]] = ds.getColumn(i, k);
                rows.push(row);
            }
            return {rowCount: rowCount, cols: cols, rows: rows};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== grdTotalDay NexacroN dataset ===")
    print(json.dumps(ds_total, ensure_ascii=False, indent=2))

    # grdTotalDay 첫 번째 행 클릭 → grdSalePerDayList 나타나는지 확인
    if total_day_rows:
        first_row = total_day_rows[0]
        rid = first_row["id"]
        row_el = mf.query_selector(f"[id*='grdTotalDay_body_{rid}']")
        if row_el:
            box = row_el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                print(f"\n  grdTotalDay 첫 행 클릭 @ y={box['y']:.0f}")
                time.sleep(3)

                # grdSalePerDayList 나타났는지 확인
                mf = get_mf(page)
                sale_check = mf.evaluate("""() => {
                    var rows = document.querySelectorAll('[id*="grdSalePerDayList_body_gridrow_"]');
                    var allGrds = Array.from(document.querySelectorAll('[id*="grd"]'))
                        .filter(el => !el.id.includes('grdLeft') && !el.id.includes('grdTotalDay'))
                        .map(el => el.id.replace('mainframe_childframe_form_divMain_divWork_', ''));
                    return {saleRows: rows.length, otherGrds: allGrds.slice(0,20)};
                }""")
                print(f"  클릭 후: {sale_check}")
                page.screenshot(path=f"{OUT_DIR}/grid3_after_row_click.png", full_page=True)

    browser.close()
    print("\n=== 완료 ===")
