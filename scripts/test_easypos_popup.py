"""
EasyPOS: popupBill grdDetailList 실제 컬럼 구조 확인
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

def find_row_by_text(page, text):
    mf = get_mf(page)
    for el in mf.query_selector_all("[id*='grdLeft_body_gridrow_']"):
        if (el.inner_text() or "").strip() == text:
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

    # 네비게이션: 영업속보 → 당일매출내역
    mf = get_mf(page)
    tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    box = tab.bounding_box()
    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(1.5)
    try: mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8000)
    except: pass

    ybs_el = find_row_by_text(page, "영업속보")
    if ybs_el:
        box = ybs_el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    daily_el = find_row_by_text(page, "당일매출내역")
    if daily_el:
        box = daily_el.bounding_box()
        cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        print("당일매출내역 더블클릭")

    mf = get_mf(page)
    for i in range(1, 11):
        time.sleep(1)
        if mf.evaluate("() => !!document.querySelector('[id*=\"grdSalePerDayList\"]')"):
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
        time.sleep(3)

    # grdSalePerDayList 헤더 컬럼 확인
    mf = get_mf(page)
    headers = mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="grdSalePerDayList_head_gridrow_"]'))
            .filter(el => /gridrow_-1$/.test(el.id) || el.id.includes('head'))
            .flatMap(el => Array.from(el.querySelectorAll('[id*="_cell_"]')))
            .filter(el => /cell_-1_\d+$/.test(el.id))
            .map(el => ({
                colIdx: parseInt(el.id.split('cell_-1_')[1]),
                text: (el.textContent||'').trim().slice(0,20),
            }));
    }""")
    print("\n=== grdSalePerDayList 헤더 컬럼 ===")
    for h in sorted(headers, key=lambda x: x['colIdx']):
        print(f"  col_{h['colIdx']}: {h['text']}")

    # 첫 번째 영수증 버튼 클릭
    mf = get_mf(page)
    btn = mf.query_selector("[id*='grdSalePerDayList_body_gridrow_0_cell_0_2_controlbutton']")
    if not btn:
        # fallback: 첫 번째 _controlbutton 찾기
        btns = mf.query_selector_all("[id*='grdSalePerDayList_body_gridrow_'][id*='_controlbutton']")
        btn = btns[0] if btns else None
        if btn:
            print(f"  fallback 버튼: {btn.get_attribute('id')}")
    if btn:
        box = btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print("첫 번째 영수증 버튼 클릭")
        time.sleep(2)

        # 팝업 구조 확인
        mf = get_mf(page)
        popup_info = mf.evaluate("""() => {
            // popupBill 안의 모든 텍스트 요소
            var popup = document.querySelector('[id*="popupBill_form"]') ||
                        document.querySelector('[id*="popupBill"]');
            if (!popup) return {err: 'no popup'};

            // 헤더 레이블
            var labels = Array.from(popup.querySelectorAll('[id*="stc"], [id*="sta"]'))
                .filter(el => el.offsetWidth > 0 && (el.textContent||'').trim())
                .map(el => ({id: el.id.replace('mainframe_childframe_popupBill_form_',''), text: (el.textContent||'').trim()}))
                .slice(0, 10);

            // grdDetailList 헤더
            var hdrs = Array.from(popup.querySelectorAll('[id*="grdDetailList_head"] [id*="_cell_"]'))
                .filter(el => /cell_-1_\d+$/.test(el.id))
                .map(el => ({
                    col: parseInt(el.id.split('cell_-1_')[1]),
                    text: (el.textContent||'').trim().slice(0,20),
                }));

            // grdDetailList 첫 번째 행 모든 셀
            var firstRowCells = Array.from(popup.querySelectorAll('[id*="grdDetailList_body_gridrow_0_cell_0_"]'))
                .filter(el => /cell_0_\d+$/.test(el.id))
                .map(el => ({
                    col: parseInt(el.id.split('cell_0_')[1]),
                    id_suffix: el.id.split('grdDetailList_body_')[1],
                    text: (el.textContent||'').trim().slice(0,30),
                }));

            return {labels: labels, headers: hdrs, firstRowCells: firstRowCells};
        }""")
        print("\n=== popupBill 구조 ===")
        print("레이블:", popup_info.get('labels'))
        print("\ngrdDetailList 헤더 컬럼:")
        for h in sorted(popup_info.get('headers', []), key=lambda x: x['col']):
            print(f"  col_{h['col']}: {h['text']}")
        print("\ngrdDetailList 첫 번째 행 셀:")
        for c in sorted(popup_info.get('firstRowCells', []), key=lambda x: x['col']):
            print(f"  col_{c['col']}: '{c['text']}' (id: {c['id_suffix']})")

        # NexacroN dataset으로도 확인
        ds = mf.evaluate("""() => {
            try {
                var popup = window.application.mainframe.childframe.popupBill;
                if (!popup) return {err: 'no popupBill'};
                var grd = popup.form.grdDetailList;
                var ds = grd._datasets && grd._datasets[0];
                if (!ds) return {err: 'no ds'};
                var rc = ds.rowcount || 0;
                var cc = ds.colcount || 0;
                var cols = [];
                for (var j = 0; j < cc; j++) cols.push(ds.getColID(j));
                var rows = [];
                for (var i = 0; i < Math.min(rc, 3); i++) {
                    var row = {};
                    for (var k = 0; k < cc; k++) row[cols[k]] = ds.getColumn(i, k);
                    rows.push(row);
                }
                return {rowCount: rc, cols: cols, rows: rows};
            } catch(e) { return {err: e.toString()}; }
        }""")
        print(f"\n=== NexacroN grdDetailList dataset ===")
        print(json.dumps(ds, ensure_ascii=False, indent=2))

        page.screenshot(path=f"{OUT_DIR}/popup_detail.png", full_page=True)

    browser.close()
    print("\n=== 완료 ===")
