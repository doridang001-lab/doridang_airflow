"""
EasyPOS 일자별매출조회 실제 컴포넌트 ID 덤프
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

    # 네비게이션
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

    daily_el = find_grd_row_by_text(page, ["일자별매출조회", "당일매출내역"])
    if daily_el:
        box = daily_el.bounding_box()
        cx, cy = box["x"]+box["width"]/2, box["y"]+box["height"]/2
        page.mouse.click(cx, cy); time.sleep(0.3)
        page.mouse.click(cx, cy, click_count=2)

    mf = get_mf(page)
    for i in range(1, 21):
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

    mf = get_mf(page)
    el = mf.query_selector("[id*='btnCommSearch']")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print("조회 클릭")
        time.sleep(8)  # 더 오래 대기

    page.screenshot(path=f"{OUT_DIR}/grid3_after_search.png", full_page=True)

    # divMain 하위 모든 최상위 ID 덤프
    mf = get_mf(page)
    divmain_children = mf.evaluate("""() => {
        var divMain = document.querySelector('[id*="divMain_divWork"]') ||
                      document.querySelector('[id*="_divWork"]');
        if (!divMain) return {err: 'no divWork'};
        // 직접 자식 요소
        var direct = Array.from(divMain.children).map(el => ({
            id: el.id,
            tag: el.tagName,
            w: el.offsetWidth,
            h: el.offsetHeight,
        }));
        return {divWorkId: divMain.id, children: direct.slice(0, 20)};
    }""")
    print("\n=== divWork 직접 자식 ===")
    print(json.dumps(divmain_children, ensure_ascii=False, indent=2))

    # 모든 DIV ID 중 childframe 안에서 _body_ 패턴
    all_ids = mf.evaluate("""() => {
        var all = Array.from(document.querySelectorAll('[id*="_body_"]'))
            .filter(el => el.id.includes('childframe') && !el.id.includes('grdLeft'))
            .map(el => el.id)
            .slice(0, 30);
        return all;
    }""")
    print(f"\n=== _body_ 패턴 (grdLeft 제외) ===")
    for id_ in all_ids:
        print(f"  {id_}")

    # 텍스트 있는 모든 요소 (데이터일 수 있음)
    visible_texts = mf.evaluate("""() => {
        // 메인 영역의 텍스트 있는 요소 (grdLeft 제외)
        var els = Array.from(document.querySelectorAll('[id*="childframe_form_divMain_divWork"] [id]'))
            .filter(el => {
                var txt = (el.textContent||'').trim();
                return txt.length > 0 && txt.length < 50 && el.offsetWidth > 0;
            })
            .map(el => ({id: el.id.slice(-50), text: (el.textContent||'').trim().slice(0,30)}));
        return els.slice(0, 30);
    }""")
    print(f"\n=== divMain_divWork 내 텍스트 요소 ===")
    for el in visible_texts:
        print(f"  {el}")

    # NexacroN: divWork 컴포넌트 이름 목록
    work_comps = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var divWork = form.divMain.divWork;
            var comps = divWork.components;
            if (!comps) return {err: 'no components'};
            var names = [];
            for (var i = 0; i < comps.length; i++) {
                var c = comps[i];
                names.push({id: c.id, type: c.constructor && c.constructor.name});
            }
            return {count: names.length, components: names};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== NexacroN divWork.components ===")
    print(json.dumps(work_comps, ensure_ascii=False, indent=2))

    browser.close()
    print("\n=== 완료 ===")
