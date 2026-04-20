"""
EasyPOS 그리드 진단: 조회 후 grdSalePerDayList 실제 DOM 구조 확인
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

    # 영업속보 탭 → 영업일보 단일클릭 → 일자별매출조회 더블클릭
    mf = get_mf(page)
    tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if tab:
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

    daily_el = find_grd_row_by_text(page, ["일자별매출조회", "당일매출내역"])
    if daily_el:
        box = daily_el.bounding_box()
        cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        print("일자별매출조회 더블클릭")

    # 화면 로드 대기
    mf = get_mf(page)
    for i in range(1, 21):
        time.sleep(1)
        r = mf.evaluate("""() => ({
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
        })""")
        if r.get("hasYesterday"):
            print(f"  [{i}s] 화면 로드 성공: {r}")
            break

    # 전일 버튼 클릭
    mf = get_mf(page)
    for sel in ["[id*='btnBeforeDay']"]:
        el = mf.query_selector(sel)
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                print(f"전일 버튼 클릭: {el.get_attribute('id')}")
                time.sleep(1)
                break

    # 조회 버튼 클릭
    mf = get_mf(page)
    for sel in ["[id*='btnCommSearch']"]:
        el = mf.query_selector(sel)
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                print(f"조회 버튼 클릭: {el.get_attribute('id')}")
                time.sleep(3)
                break

    # 그리드 대기 및 DOM 구조 덤프
    print("\n=== 그리드 데이터 대기 (최대 30초) ===")
    mf = get_mf(page)
    for i in range(1, 31):
        time.sleep(1)
        result = mf.evaluate("""() => {
            var rows = document.querySelectorAll('[id*="grdSalePerDayList_body_gridrow_"]');
            return {rowCount: rows.length, firstId: rows[0] ? rows[0].id : null};
        }""")
        if result['rowCount'] > 0:
            print(f"  [{i}s] 그리드 행 발견: {result}")
            break
        if i % 5 == 0:
            print(f"  [{i}s] 대기 중... rowCount={result['rowCount']}")

    # 그리드 행의 모든 ID 덤프
    mf = get_mf(page)
    grid_dom = mf.evaluate("""() => {
        var rows = Array.from(document.querySelectorAll('[id*="grdSalePerDayList_body_gridrow_"]'));
        var result = {
            rowCount: rows.length,
            // 각 행의 ID (최대 5행)
            rowIds: rows.slice(0, 5).map(r => r.id),
            // 첫 번째 행의 모든 자식 ID
            firstRowChildren: rows[0]
                ? Array.from(rows[0].querySelectorAll('[id]')).map(el => ({
                    id: el.id.replace('mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_', ''),
                    text: (el.textContent||'').trim().slice(0,20),
                    tag: el.tagName,
                  }))
                : [],
        };
        return result;
    }""")
    print("\n=== grdSalePerDayList DOM 구조 ===")
    print(f"행 수: {grid_dom.get('rowCount')}")
    print(f"행 IDs: {grid_dom.get('rowIds')}")
    print("\n첫 번째 행의 자식 요소들:")
    for child in grid_dom.get('firstRowChildren', []):
        print(f"  {child}")

    # NexacroN dataset으로도 시도
    ds_data = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var grd = form.divMain.divWork.grdSalePerDayList;
            var ds = grd._datasets && grd._datasets[0];
            if (!ds) return {err: 'no dataset'};
            var rowCount = ds.rowcount || 0;
            var colCount = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));
            var rows = [];
            for (var i = 0; i < Math.min(rowCount, 3); i++) {
                var row = {};
                for (var k = 0; k < colCount; k++) row[cols[k]] = ds.getColumn(i, k);
                rows.push(row);
            }
            return {rowCount: rowCount, cols: cols, rows: rows};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== NexacroN dataset ===")
    print(json.dumps(ds_data, ensure_ascii=False, indent=2))

    page.screenshot(path=f"{OUT_DIR}/grid_final.png", full_page=True)
    browser.close()
    print("\n=== 완료 ===")
