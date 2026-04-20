"""
EasyPOS 18차: 영업속보 클릭 후 grdLeft DOM 탐색 + 당일매출내역 직접 클릭
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

def wait_sales(page, sec=20):
    for i in range(1, sec+1):
        time.sleep(1)
        mf = get_mf(page)
        r = mf.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 당일매출내역 로드 성공! {r}")
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

    # --- 1. 영업속보 탭 클릭 ---
    mf = get_mf(page)
    sales_tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if sales_tab:
        box = sales_tab.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print("영업속보 탭 클릭")
        time.sleep(2)
    else:
        print("[WARN] 영업속보 탭 없음")

    # --- 2. grdLeft 데이터셋 재확인 ---
    mf = get_mf(page)
    grd_check = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var grd = form.divLeftMenu.divLeftMainList.grdLeft;
            var ds = grd._datasets && grd._datasets[0];
            return {
                dsExists: !!ds,
                rowcount: ds && (ds.rowcount || ds._rowcount),
                dsName: ds && ds.id,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"영업속보 클릭 후 grdLeft dataset: {grd_check}")

    # --- 3. grdLeft 내 모든 텍스트 요소 탐색 ---
    left_items = mf.evaluate("""() => {
        // grdLeft 안의 모든 ID 있는 요소
        var grdEl = document.querySelector('[id*="divLeftMainList_grdLeft"]');
        if (!grdEl) return {err: 'grdLeft not found'};
        var all = Array.from(grdEl.querySelectorAll('[id]'));
        var items = all.map(el => ({
            id: el.id.slice(-50),
            text: (el.textContent||'').trim().slice(0, 30),
            tag: el.tagName,
            x: Math.round(el.getBoundingClientRect().x),
            y: Math.round(el.getBoundingClientRect().y),
            w: Math.round(el.getBoundingClientRect().width),
            h: Math.round(el.getBoundingClientRect().height),
        })).filter(el => el.text && el.text.length > 0 && el.w > 0);
        return {count: items.length, items: items.slice(0, 30)};
    }""")
    print(f"\n=== grdLeft 내 요소들 (영업속보 클릭 후) ===")
    if 'items' in left_items:
        for item in left_items['items']:
            print(f"  {item}")
    else:
        print(json.dumps(left_items, indent=2, ensure_ascii=False))

    # --- 4. 당일매출내역/일자별매출 텍스트 요소 찾아 클릭 ---
    print("\n=== 당일매출내역 텍스트 요소 클릭 시도 ===")
    target_el = mf.evaluate("""() => {
        // 텍스트 기반으로 요소 찾기
        var candidates = Array.from(document.querySelectorAll('[id*="grdLeft"] [id], [id*="divLeftMain"] [id]'))
            .filter(el => {
                var txt = (el.textContent||'').trim();
                return (txt === '당일매출내역' || txt === '일자별매출조회') && el.offsetWidth > 0;
            });
        if (candidates.length === 0) return null;
        var el = candidates[0];
        var rect = el.getBoundingClientRect();
        return {id: el.id, text: el.textContent.trim(), x: rect.x, y: rect.y, w: rect.width, h: rect.height};
    }""")
    print(f"  타겟 요소: {target_el}")

    if target_el and target_el.get('w', 0) > 0:
        cx = target_el['x'] + target_el['w']/2
        cy = target_el['y'] + target_el['h']/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)
        print(f"  더블클릭 @ ({cx:.0f}, {cy:.0f})")
        if wait_sales(page, 15):
            page.screenshot(path=f"{OUT_DIR}/dom18_success_direct.png")
    else:
        print("  [WARN] 타겟 없음 — grdLeft 내 영업일보 카테고리 확장 후 찾기")

    # --- 5. 영업일보 카테고리 클릭으로 확장 후 재탐색 ---
    print("\n=== 영업일보 카테고리 클릭 후 탐색 ===")
    mf = get_mf(page)
    yeil_el = mf.evaluate("""() => {
        var els = Array.from(document.querySelectorAll('[id*="grdLeft"] [id], [id*="divLeftMain"] [id]'))
            .filter(el => {
                var txt = (el.textContent||'').trim();
                return txt === '영업일보' && el.offsetWidth > 0;
            });
        if (!els.length) return null;
        var el = els[0];
        var rect = el.getBoundingClientRect();
        return {id: el.id, x: rect.x, y: rect.y, w: rect.width, h: rect.height};
    }""")
    print(f"  영업일보 요소: {yeil_el}")

    if yeil_el and yeil_el.get('w', 0) > 0:
        cx = yeil_el['x'] + yeil_el['w']/2
        cy = yeil_el['y'] + yeil_el['h']/2
        page.mouse.click(cx, cy); time.sleep(1.5)
        print(f"  영업일보 클릭 @ ({cx:.0f}, {cy:.0f})")

        # 확장 후 grdLeft 데이터셋 재확인
        grd_after = mf.evaluate("""() => {
            try {
                var form = window.application.mainframe.childframe.form;
                var grd = form.divLeftMenu.divLeftMainList.grdLeft;
                var ds = grd._datasets && grd._datasets[0];
                var rowCount = ds && (ds.rowcount || 0);
                var matches = [];
                var colCount = ds && (ds.colcount || 0);
                var cols = [];
                for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));
                for (var i = 0; i < Math.min(rowCount, 50); i++) {
                    var nm = ds.getColumn(i, 'MENU_NAME') || '';
                    var url = ds.getColumn(i, 'SMART_MENU_URL') || '';
                    if (nm) matches.push({i: i, nm: nm, url: url});
                }
                return {rowCount: rowCount, cols: cols, matches: matches};
            } catch(e) { return {err: e.toString()}; }
        }""")
        print(f"  영업일보 클릭 후 grdLeft dataset: {json.dumps(grd_after, ensure_ascii=False)}")

        # 당일매출내역 다시 탐색
        target_after = mf.evaluate("""() => {
            var els = Array.from(document.querySelectorAll('[id*="grdLeft"] [id], [id*="divLeftMain"] [id]'))
                .filter(el => {
                    var txt = (el.textContent||'').trim();
                    return (txt === '당일매출내역' || txt === '일자별매출조회') && el.offsetWidth > 0;
                });
            if (!els.length) {
                // 더 넓게 검색
                var all = Array.from(document.querySelectorAll('[id*="grdLeft"] [id]'))
                    .map(el => ({id: el.id.slice(-40), text: (el.textContent||'').trim().slice(0,20), w: el.offsetWidth}))
                    .filter(el => el.text && el.w > 0);
                return {notFound: true, candidates: all.slice(0, 20)};
            }
            var el = els[0];
            var rect = el.getBoundingClientRect();
            return {id: el.id, text: el.textContent.trim(), x: rect.x, y: rect.y, w: rect.width, h: rect.height};
        }""")
        print(f"  영업일보 확장 후 당일매출내역: {target_after}")

        if isinstance(target_after, dict) and 'x' in target_after and target_after.get('w', 0) > 0:
            cx = target_after['x'] + target_after['w']/2
            cy = target_after['y'] + target_after['h']/2
            all_reqs.clear()
            page.mouse.click(cx, cy); time.sleep(0.3)
            page.mouse.click(cx, cy, click_count=2)
            print(f"  당일매출내역 더블클릭 @ ({cx:.0f}, {cy:.0f})")
            if wait_sales(page, 20):
                page.screenshot(path=f"{OUT_DIR}/dom18_success.png")
            else:
                page.screenshot(path=f"{OUT_DIR}/dom18_fail.png")
                xfdl = [r for r in all_reqs if '.xfdl' in r]
                print(f"  XFDL 요청: {xfdl}")

    print(f"\n전체 콘솔: {all_console[-5:]}")
    browser.close()
print("\n=== 18차 테스트 완료 ===")
