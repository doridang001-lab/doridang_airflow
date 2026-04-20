"""
EasyPOS: 영업속보(row_0) 더블클릭 + 탭3 기간별/판매기록표 탐색
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
        txt = (el.inner_text() or "").strip()
        if txt == text:
            return el
    return None

def search_and_dump(page, label):
    """전일+조회 후 모든 그리드 ID 덤프"""
    mf = get_mf(page)
    el = mf.query_selector("[id*='btnBeforeDay']")
    if el:
        box = el.bounding_box()
        if box:
            page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
            time.sleep(0.8)
    mf = get_mf(page)
    el = mf.query_selector("[id*='btnCommSearch']")
    if el:
        box = el.bounding_box()
        if box:
            page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
            time.sleep(5)

    mf = get_mf(page)
    result = mf.evaluate("""() => {
        var rows = Array.from(document.querySelectorAll('[id*="_body_gridrow_"]'))
            .filter(el => /gridrow_\\d+$/.test(el.id) && !el.id.includes('grdLeft'));
        var grids = [...new Set(rows.map(el => el.id.split('_body_gridrow_')[0].split('divWork_')[1] || el.id.split('_body_gridrow_')[0]))];
        var hasSalePerDay = !!document.querySelector('[id*="grdSalePerDayList"]');
        var sampleRows = rows.slice(0, 3).map(el => (el.textContent||'').trim().slice(0,50));
        return {rowCount: rows.length, grids: grids, hasSalePerDay: hasSalePerDay, sampleRows: sampleRows};
    }""")
    print(f"  [{label}] 조회 결과: {result}")
    if result.get('hasSalePerDay'):
        print(f"  ★★★ grdSalePerDayList 발견!")
    return result

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

    # === 테스트 1: 영업속보 탭 → 영업속보(row_0) 더블클릭 ===
    print("\n=== TEST 1: 영업속보(row_0) 더블클릭 ===")
    mf = get_mf(page)
    tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    box = tab.bounding_box()
    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(2)

    mf = get_mf(page)
    el = find_row_by_text(page, "영업속보")
    if el:
        box = el.bounding_box()
        cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        print("  영업속보 더블클릭")
        time.sleep(3)

        mf = get_mf(page)
        r = mf.evaluate("""() => {
            var allIds = Array.from(document.querySelectorAll('[id*="divWork"] [id]'))
                .filter(el => !el.id.includes('grdLeft') && el.offsetWidth > 0)
                .map(el => el.id.replace('mainframe_childframe_form_divMain_divWork_', '').slice(0, 50))
                .filter((v, i, a) => a.indexOf(v) === i)
                .slice(0, 20);
            var hasSPD = !!document.querySelector('[id*="grdSalePerDayList"]');
            return {hasSPD: hasSPD, ids: allIds};
        }""")
        print(f"  결과: hasSalePerDay={r['hasSPD']}, ids={r['ids'][:10]}")
        if r['hasSPD']:
            search_and_dump(page, "영업속보")
    else:
        print("  영업속보 row 없음")

    # === 테스트 2: 탭3(분석) → 기간별매출조회 더블클릭 ===
    print("\n=== TEST 2: 탭3 → 기간별매출조회 ===")
    mf = get_mf(page)
    tab3 = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu3")
    if tab3:
        box = tab3.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(2)

        for target_text in ["기간별매출조회", "판매기록표", "상품분석", "대비조회"]:
            el = find_row_by_text(page, target_text)
            if el:
                box = el.bounding_box()
                cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
                page.mouse.click(cx, cy); time.sleep(0.3)
                page.mouse.click(cx, cy, click_count=2)
                time.sleep(3)

                mf = get_mf(page)
                has_spd = mf.evaluate("() => !!document.querySelector('[id*=\"grdSalePerDayList\"]')")
                has_search = mf.evaluate("() => !!document.querySelector('[id*=\"btnCommSearch\"]')")
                grids = mf.evaluate("""() => {
                    return [...new Set(Array.from(document.querySelectorAll('[id*="_body_gridrow_"]'))
                        .filter(el => /gridrow_\\d+$/.test(el.id) && !el.id.includes('grdLeft'))
                        .map(el => el.id.split('_body_gridrow_')[0].split('divWork_').pop()))];
                }""")
                print(f"  [{target_text}]: hasSPD={has_spd}, hasSearch={has_search}, grids={grids[:5]}")
                if has_spd:
                    search_and_dump(page, target_text)

    # === 테스트 3: 영업일보 탭 → 영업속보(row_0)에서 파생된 서브메뉴 확인 ===
    # 영업속보 클릭 시 서브메뉴 확장 내용 확인
    print("\n=== TEST 3: 영업속보 클릭 후 서브메뉴 ===")
    mf = get_mf(page)
    tab2 = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    box = tab2.bounding_box()
    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(2)

    ybs_el = find_row_by_text(page, "영업속보")
    if ybs_el:
        box = ybs_el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

        mf = get_mf(page)
        menus = mf.evaluate("""() => {
            return Array.from(document.querySelectorAll('[id*="grdLeft_body_gridrow_"]'))
                .filter(el => /gridrow_\\d+$/.test(el.id))
                .map(el => ({
                    idx: parseInt(el.id.split('gridrow_')[1]),
                    text: (el.textContent||'').trim(),
                    x: Math.round(el.getBoundingClientRect().x),
                }))
                .filter(m => m.text);
        }""")
        print(f"  영업속보 클릭 후 메뉴:")
        for m in menus:
            print(f"    row_{m['idx']} (x={m['x']}): {m['text']}")

        # 서브메뉴들 하나씩 더블클릭
        sub_menus = [m for m in menus if m['idx'] > 0]
        for sm in sub_menus[:5]:
            el = find_row_by_text(page, sm['text'])
            if el:
                box = el.bounding_box()
                if box:
                    cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
                    page.mouse.click(cx, cy); time.sleep(0.3)
                    page.mouse.click(cx, cy, click_count=2)
                    time.sleep(3)
                    mf = get_mf(page)
                    has_spd = mf.evaluate("() => !!document.querySelector('[id*=\"grdSalePerDayList\"]')")
                    grids = mf.evaluate("""() => {
                        return [...new Set(Array.from(document.querySelectorAll('[id*="_body_gridrow_"]'))
                            .filter(el => /gridrow_\\d+$/.test(el.id) && !el.id.includes('grdLeft'))
                            .map(el => el.id.split('_body_gridrow_')[0].split('divWork_').pop()))];
                    }""")
                    print(f"    [{sm['text']}]: hasSPD={has_spd}, grids={grids[:5]}")
                    if has_spd:
                        print(f"    ★★★ 발견!")
                        search_and_dump(page, sm['text'])

    browser.close()
    print("\n=== 완료 ===")
