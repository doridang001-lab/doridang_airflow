"""
EasyPOS 12차: window.application 직접 접근 + NexacroN API 호출 시도
- window.application, window.application.mainframe 상태 확인
- loadUrl / gotoUrl / navigate 등 직접 호출 시도
- 선택 클릭 전후 application 상태 변화 추적
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

all_console = []
post_click_reqs = []
click_done = False

def get_mf(page):
    for f in page.frames:
        if f.name == "main": return f
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

    ctx.on("request", lambda req: post_click_reqs.append(req.url[-100:]) if click_done else None)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:300]}"))

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
    all_console.clear()

    # window.application 상태 확인
    mf = get_mf(page)
    app_info = mf.evaluate("""() => {
        try {
            var app = window.application;
            if (!app) return {err: 'window.application undefined'};

            var mf = app.mainframe;
            var cf = mf && mf.childframe;

            // app 메서드 목록
            var appMethods = Object.keys(app).filter(k => typeof app[k] === 'function').slice(0, 30);
            var mfMethods = mf ? Object.keys(mf).filter(k => typeof mf[k] === 'function').slice(0, 20) : [];
            var cfMethods = cf ? Object.keys(cf).filter(k => typeof cf[k] === 'function').slice(0, 20) : [];

            // avWorkMain 확인
            var avWorkMain = app.avWorkMain;

            return {
                appType: typeof app,
                appId: app.id,
                appXadl: app.xadl,
                avWorkMain: avWorkMain ? (typeof avWorkMain + ':' + (avWorkMain.id || '?')) : null,
                mainframeType: typeof mf,
                mainframeId: mf && mf.id,
                childframeType: typeof cf,
                childframeId: cf && cf.id,
                appMethods: appMethods,
                mfMethods: mfMethods,
                cfMethods: cfMethods,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("=== window.application 상태 ===")
    print(json.dumps(app_info, indent=2, ensure_ascii=False))

    # childframe의 현재 form 확인
    form_info = mf.evaluate("""() => {
        try {
            var cf = window.application && window.application.mainframe && window.application.mainframe.childframe;
            if (!cf) return {err: 'no childframe'};
            // 현재 로드된 form 정보
            var forms = Object.keys(cf).filter(k => k.includes('form') || k.includes('Form')).slice(0, 10);
            var curForm = cf.form || cf._form || cf.currentform || null;
            return {
                cfId: cf.id,
                cfUrl: cf.url || cf._url,
                cfForms: forms,
                curFormId: curForm && curForm.id,
                // 자식 form 목록
                childKeys: Object.keys(cf).slice(0, 20),
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("\n=== childframe 상태 ===")
    print(json.dumps(form_info, indent=2, ensure_ascii=False))

    # 선택 클릭
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1.5)

    mf = get_mf(page)
    find_btn = mf.query_selector(
        "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]"
    )
    if find_btn:
        box = find_btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1)
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
                    click_done = True
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("\n선택 클릭 완료")
    time.sleep(3)

    # 선택 후 application 상태 재확인
    mf = get_mf(page)
    app_after = mf.evaluate("""() => {
        try {
            var cf = window.application && window.application.mainframe && window.application.mainframe.childframe;
            if (!cf) return {err: 'no childframe'};
            return {
                cfId: cf.id,
                cfUrl: cf.url || cf._url,
                childKeys: Object.keys(cf).slice(0, 20),
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("\n=== 선택 후 childframe 상태 ===")
    print(json.dumps(app_after, indent=2, ensure_ascii=False))

    # NexacroN 직접 navigation 시도
    print("\n=== NexacroN 직접 navigation 시도 ===")
    nav_result = mf.evaluate("""() => {
        try {
            var app = window.application;
            var cf = app && app.mainframe && app.mainframe.childframe;
            if (!cf) return {err: 'no childframe'};

            // 1. loadUrl 시도
            if (typeof cf.loadUrl === 'function') {
                return {method: 'loadUrl exists', type: typeof cf.loadUrl};
            }
            // 2. url setter 시도
            if ('url' in cf) {
                return {method: 'url prop exists', url: cf.url};
            }
            // 3. form 직접 로드 시도
            var methods = Object.keys(cf).filter(k => typeof cf[k] === 'function');
            return {cfMethods: methods.slice(0, 30)};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(json.dumps(nav_result, indent=2, ensure_ascii=False))

    print(f"\n=== 선택 후 네트워크 요청 ({len(post_click_reqs)}개) ===")
    for r in post_click_reqs: print(f"  {r}")

    print(f"\n=== 전체 콘솔 ({len(all_console)}개) ===")
    for m in all_console: print(f"  {m}")

    browser.close()
print("\n=== 12차 테스트 완료 ===")
