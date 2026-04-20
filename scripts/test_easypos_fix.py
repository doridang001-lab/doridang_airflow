"""
EasyPOS 최종 검증: loadingImg 주입 후 navigation 작동 확인
"""
import time, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

def shot(page, name):
    p = f"{OUT_DIR}/fix_{name}.png"
    page.screenshot(path=p, full_page=True)
    print(f"[SHOT] {p}")

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
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
    # 핵심 픽스: loadingImg 주입
    ctx.add_init_script("""
        (function() {
            function ensureLoadingImg() {
                if (!document.getElementById('loadingImg') && document.body) {
                    var div = document.createElement('div');
                    div.id = 'loadingImg';
                    div.style.display = 'block';
                    document.body.appendChild(div);
                }
            }
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', ensureLoadingImg);
            } else {
                ensureLoadingImg();
            }
            Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
            Object.defineProperty(document, 'hidden', {get: function() { return false; }});
            Object.defineProperty(document, 'visibilityState', {get: function() { return 'visible'; }});
            document.addEventListener('visibilitychange', function(e) { e.stopImmediatePropagation(); }, true);
        })();
    """)
    page = ctx.new_page()

    console_errors = []
    page.on('console', lambda m: console_errors.append(f"[{m.type}] {m.text[:150]}") if m.type in ('error',) else None)

    print("=== loadingImg 픽스 적용 — 로그인 ===")
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)
    mf = get_mf(page)

    mf.query_selector("#mainframe_childframe_form_divMain_edtId").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtId_input").fill(EASYPOS_ID)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw_input").fill(EASYPOS_PW)
    mf.query_selector("#mainframe_childframe_form_divMain_btnLogin").click()
    time.sleep(5)

    for pid in [
        "mainframe_childframe_popupChangePasswd_form_div_popup_bottom_btnClose",
        "mainframe_childframe_placeAdPopup_titlebar_closebutton",
    ]:
        mf = get_mf(page)
        el = mf and mf.query_selector(f"#{pid}")
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                time.sleep(1)
    time.sleep(2)
    mf = get_mf(page)

    # NexacroN 앱 초기화 상태 확인
    app_state = mf.evaluate("""() => ({
        hasApp: !!(nexacro && nexacro._application_),
        getAppType: typeof nexacro?.getApplication,
        loadingImgExists: !!document.getElementById('loadingImg'),
    })""")
    print(f"\n=== NexacroN 초기화 상태: {app_state} ===")
    print(f"  콘솔 에러: {len(console_errors)}개")
    for e in console_errors[:5]: print(f"  {e}")
    console_errors.clear()

    shot(page, "01_after_login")

    # 영업속보 → 메뉴검색 → 당일매출내역
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    mf = get_mf(page)
    find_btn = mf.query_selector(
        "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]"
    )
    if find_btn:
        box = find_btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1)
        mf = get_mf(page)
        inp = mf.query_selector("#mainframe_childframe_popupFindMenu_form_edtInputText_input")
        if inp:
            inp.fill("당일매출내역"); inp.press("Enter"); time.sleep(1)
            mf = get_mf(page)
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                box = res.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(0.5)
            mf = get_mf(page)
            sel = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']")
            if sel:
                box = sel.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("\n  선택 클릭 완료 — 결과 대기...")

    success = False
    for i in range(1, 31):
        time.sleep(1)
        mf2 = get_mf(page)
        r = mf2.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 당일매출내역 화면 로드 성공! {r}")
            shot(page, "02_daily_success")
            success = True
            break
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")

    if not success:
        shot(page, "02_daily_fail")
        print(f"\n  실패 — 콘솔 에러: {console_errors[:5]}")

    browser.close()
    print("\n=== 최종 검증 완료 ===")
