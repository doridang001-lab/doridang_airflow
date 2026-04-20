"""
EasyPOS 그리드 ID 탐색: 조회 후 실제 그리드 요소 ID 덤프
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

    mf = get_mf(page)
    for i in range(1, 21):
        time.sleep(1)
        r = mf.evaluate("() => !!document.querySelector('[id*=\"btnBeforeDay\"]')")
        if r:
            print(f"  [{i}s] 화면 로드 성공")
            break

    # 전일 버튼 클릭
    mf = get_mf(page)
    el = mf.query_selector("[id*='btnBeforeDay']")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print(f"전일 버튼 클릭: {el.get_attribute('id')}")
        time.sleep(1)

    # 날짜 확인
    mf = get_mf(page)
    date_info = mf.evaluate("""() => {
        var inputs = Array.from(document.querySelectorAll('input[id*="SalesDate"], input[id*="salesDate"], [id*="divSalesDate"] input'));
        return inputs.map(i => ({id: i.id, val: i.value}));
    }""")
    print(f"날짜 입력 확인: {date_info}")

    # 조회 버튼 클릭
    mf = get_mf(page)
    el = mf.query_selector("[id*='btnCommSearch']")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        print(f"조회 버튼 클릭: {el.get_attribute('id')}")
        time.sleep(5)

    page.screenshot(path=f"{OUT_DIR}/grid_after_search.png", full_page=True)

    # 조회 후 전체 그리드 관련 요소 탐색
    mf = get_mf(page)
    all_grids = mf.evaluate("""() => {
        // grd 포함 모든 요소
        var grds = Array.from(document.querySelectorAll('[id*="grd"]'))
            .filter(el => el.offsetWidth > 0 || el.offsetHeight > 0)
            .map(el => ({
                id: el.id,
                tag: el.tagName,
                w: el.offsetWidth,
                h: el.offsetHeight,
                children: el.childElementCount,
            }));
        return grds.slice(0, 30);
    }""")
    print("\n=== grd 포함 모든 요소 ===")
    for g in all_grids:
        print(f"  {g}")

    # body/gridrow 패턴 탐색
    gridrows = mf.evaluate("""() => {
        var rows = Array.from(document.querySelectorAll('[id*="_body_gridrow_"]'));
        return rows.slice(0, 10).map(el => ({
            id: el.id,
            text: (el.textContent||'').trim().slice(0,30),
        }));
    }""")
    print(f"\n=== _body_gridrow_ 패턴 요소 ({len(gridrows)}개) ===")
    for r in gridrows:
        print(f"  {r}")

    # NexacroN form 탐색
    form_info = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var divMain = form.divMain;
            if (!divMain) return {err: 'no divMain'};
            var divWork = divMain.divWork;
            if (!divWork) return {err: 'no divWork', divMainKeys: Object.keys(divMain).slice(0,20)};
            // divWork 하위 컴포넌트
            var keys = [];
            try { keys = Object.keys(divWork).filter(k => !k.startsWith('_')).slice(0,30); } catch(e) {}
            return {ok: true, divWorkKeys: keys};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== NexacroN form 구조 ===")
    print(json.dumps(form_info, ensure_ascii=False, indent=2))

    browser.close()
    print("\n=== 완료 ===")
