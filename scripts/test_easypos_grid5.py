"""
EasyPOS: grdTotalDay 행 클릭 → grdSalePerDayList 팝업 확인 + 5건 검증
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

    # 네비게이션 → 일자별매출조회
    mf = get_mf(page)
    tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    box = tab.bounding_box()
    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(1.5)
    try: mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8000)
    except: pass

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

    # grdTotalDay 행 확인
    mf = get_mf(page)
    total_rows = mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="grdTotalDay_body_gridrow_"]'))
            .filter(el => /gridrow_\\d+$/.test(el.id))
            .map(el => ({
                id: el.id.split('_body_')[1],
                text: (el.textContent||'').trim().slice(0,60),
                y: Math.round(el.getBoundingClientRect().y),
                h: Math.round(el.getBoundingClientRect().height),
                x: Math.round(el.getBoundingClientRect().x),
                w: Math.round(el.getBoundingClientRect().width),
            }));
    }""")
    print(f"\n=== grdTotalDay 행 ({len(total_rows)}개) ===")
    for r in total_rows:
        print(f"  {r}")

    page.screenshot(path=f"{OUT_DIR}/grid5_total_day.png", full_page=True)

    # NexacroN dataset 확인
    mf = get_mf(page)
    ds = mf.evaluate("""() => {
        try {
            var grd = window.application.mainframe.childframe.form.divMain.divWork.grdTotalDay;
            var ds = grd._datasets && grd._datasets[0];
            if (!ds) return {err: 'no ds'};
            var rc = ds.rowcount || 0;
            var cc = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < cc; j++) cols.push(ds.getColID(j));
            var rows = [];
            for (var i = 0; i < Math.min(rc, 10); i++) {
                var row = {};
                for (var k = 0; k < cc; k++) row[cols[k]] = ds.getColumn(i, k);
                rows.push(row);
            }
            return {rowCount: rc, cols: cols, rows: rows};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== grdTotalDay dataset ===")
    print(json.dumps(ds, ensure_ascii=False, indent=2))

    # 각 행에 더블클릭해서 뭔가 열리는지 확인
    if total_rows:
        for i, row in enumerate(total_rows[:3]):
            print(f"\n--- grdTotalDay row {i} 더블클릭 시도 ---")
            cx = row['x'] + row['w'] / 2
            cy = row['y'] + row['h'] / 2
            page.mouse.click(cx, cy); time.sleep(0.3)
            page.mouse.click(cx, cy, click_count=2)
            time.sleep(3)

            mf = get_mf(page)
            result = mf.evaluate("""() => {
                return {
                    hasSalePerDay: !!document.querySelector('[id*="grdSalePerDayList"]'),
                    hasPopupBill: !!document.querySelector('[id*="popupBill"]'),
                    hasPopupAny: !!document.querySelector('[id*="popup"]'),
                    popupIds: Array.from(document.querySelectorAll('[id*="popup"]')).map(el => el.id).filter(id => id.length < 80).slice(0,10),
                    newGrids: Array.from(document.querySelectorAll('[id*="_body_gridrow_"]'))
                        .map(el => el.id)
                        .filter(id => !id.includes('grdLeft') && !id.includes('grdTotalDay'))
                        .slice(0,5),
                };
            }""")
            print(f"  결과: {json.dumps(result, ensure_ascii=False)}")
            page.screenshot(path=f"{OUT_DIR}/grid5_dblclick_row{i}.png", full_page=True)

            # 팝업 닫기
            mf = get_mf(page)
            close_btns = mf.evaluate("""() => {
                return Array.from(document.querySelectorAll('[id*="popup"][id*="closebutton"],[id*="popup"][id*="btnClose"]'))
                    .map(el => ({id: el.id, w: el.offsetWidth}))
                    .filter(el => el.w > 0)
                    .slice(0, 5);
            }""")
            if close_btns:
                el = mf.query_selector(f"#{close_btns[0]['id']}")
                if el:
                    box = el.bounding_box()
                    if box:
                        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                        time.sleep(1)

    browser.close()
    print("\n=== 완료 ===")
