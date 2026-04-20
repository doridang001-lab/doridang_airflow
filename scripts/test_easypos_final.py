"""
EasyPOS 최종 검증: 네비게이션 성공 후 어제 버튼 + 조회 + 그리드 데이터 추출
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

    # 영업속보 탭 클릭
    mf = get_mf(page)
    sales_tab = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if sales_tab:
        box = sales_tab.bounding_box()
        if box:
            page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
    time.sleep(1.5)

    # grdLeft 로드 대기
    mf = get_mf(page)
    try:
        mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8000)
    except:
        pass

    # 영업일보 단일 클릭
    yil_el = find_grd_row_by_text(page, ["영업일보"])
    if yil_el:
        box = yil_el.bounding_box()
        if box:
            cx = box["x"] + box["width"]/2
            cy = box["y"] + box["height"]/2
            page.mouse.click(cx, cy)
            print(f"영업일보 클릭 @ ({cx:.0f}, {cy:.0f})")
    time.sleep(1.5)

    # 일자별매출조회 더블클릭
    daily_el = find_grd_row_by_text(page, ["일자별매출조회", "당일매출내역"])
    if daily_el:
        box = daily_el.bounding_box()
        if box:
            cx = box["x"] + box["width"]/2
            cy = box["y"] + box["height"]/2
            page.mouse.click(cx, cy)
            time.sleep(0.3)
            page.mouse.click(cx, cy, click_count=2)
            print(f"일자별매출조회 더블클릭 @ ({cx:.0f}, {cy:.0f})")
    else:
        print("[WARN] 일자별매출조회 없음!")

    # 화면 로드 대기
    print("당일매출내역 화면 대기...")
    for i in range(1, 21):
        time.sleep(1)
        mf2 = get_mf(page)
        r = mf2.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 화면 로드 성공! {r}")
            break
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")

    page.screenshot(path=f"{OUT_DIR}/final_01_daily_page.png", full_page=True)

    # 어제 버튼 클릭
    mf = get_mf(page)
    yesterday_selectors = [
        "#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnBeforeDay",
        "#mainframe_childframe_form_divMain_divSearchTop_btnBeforeDay",
        "[id*='btnBeforeDay']",
    ]
    yesterday_clicked = False
    for sel in yesterday_selectors:
        el = mf.query_selector(sel)
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                print(f"어제 버튼 클릭: {sel}")
                yesterday_clicked = True
                time.sleep(1)
                break
    if not yesterday_clicked:
        print("[WARN] 어제 버튼 없음")

    # 조회 버튼 클릭
    mf = get_mf(page)
    search_selectors = [
        "#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch",
        "[id*='btnCommSearch']",
    ]
    search_clicked = False
    for sel in search_selectors:
        el = mf.query_selector(sel)
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                print(f"조회 버튼 클릭: {sel}")
                search_clicked = True
                time.sleep(3)
                break
    if not search_clicked:
        print("[WARN] 조회 버튼 없음")

    page.screenshot(path=f"{OUT_DIR}/final_02_after_search.png", full_page=True)

    # 그리드 데이터 확인
    mf = get_mf(page)
    grid_check = mf.evaluate("""() => {
        var grd = document.querySelector('[id*="grdSalePerDayList"]');
        if (!grd) return {err: 'grid not found'};
        var rows = Array.from(document.querySelectorAll('[id*="grdSalePerDayList_body_gridrow_"]'));
        return {
            gridFound: true,
            rowCount: rows.length,
            sampleTexts: rows.slice(0, 3).map(r => (r.textContent||'').trim().slice(0,50)),
        };
    }""")
    print(f"\n그리드 상태: {json.dumps(grid_check, ensure_ascii=False)}")

    browser.close()
    print("\n=== 최종 검증 완료 ===")
