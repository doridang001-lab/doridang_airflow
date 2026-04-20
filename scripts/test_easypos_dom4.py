"""
EasyPOS 4차 진단: 콘솔 에러 + NexacroN 초기화 상태 파악
"""
import time, json, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

console_msgs = []
page_errors  = []

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,
        args=["--no-sandbox","--disable-dev-shm-usage","--window-size=1920,1080"],
    )
    ctx = browser.new_context(viewport={"width":1920,"height":1080})

    # 모든 프레임의 콘솔/에러 캡처
    def on_console(msg):
        if msg.type in ('error','warning') or 'nexacro' in msg.text.lower() or 'error' in msg.text.lower():
            console_msgs.append(f"[{msg.type}] {msg.text[:200]}")
    def on_pageerror(err):
        page_errors.append(str(err)[:200])

    page = ctx.new_page()
    page.on('console', on_console)
    page.on('pageerror', on_pageerror)

    # 모든 프레임 로드 시 NexacroN 상태 주입
    def on_framenavigated(frame):
        if frame.name == 'main':
            try:
                frame.evaluate("""() => {
                    window.__nexacro_checked__ = false;
                    const orig = Object.defineProperty.bind(Object);
                    // nexacro._application_ 설정 시 감지
                }""")
            except: pass

    page.on('framenavigated', on_framenavigated)

    # 로그인
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)

    def get_mf():
        for f in page.frames:
            if f.name == "main":
                return f
        return None

    mf = get_mf()
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
        mf = get_mf()
        el = mf and mf.query_selector(f"#{pid}")
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                time.sleep(1)
    time.sleep(2)

    print("=== 콘솔 에러/경고 (로그인 후) ===")
    for m in console_msgs[:30]: print(f"  {m}")
    print(f"  페이지 에러: {page_errors[:10]}")
    console_msgs.clear(); page_errors.clear()

    # NexacroN 앱 상태 상세 확인
    mf = get_mf()
    app_state = mf.evaluate("""() => {
        let result = {
            nexacroExists: typeof nexacro !== 'undefined',
            getAppType: typeof nexacro?.getApplication,
            // _application_ 직접 접근
            hasApp: !!(nexacro?._application_),
            appType: typeof nexacro?._application_,
            // 다양한 패턴 시도
            keys30: Object.keys(nexacro||{}).slice(0,30),
        };
        // nexacro prototype chain 확인
        try {
            let proto = Object.getPrototypeOf(nexacro);
            result.protoKeys = Object.getOwnPropertyNames(proto||{}).slice(0,20);
        } catch(e) { result.protoErr = e.toString(); }

        // getApplication 직접 탐색
        try {
            for(let key of Object.keys(nexacro)) {
                if(typeof nexacro[key] === 'function' && key.toLowerCase().includes('app')) {
                    result['func_'+key] = typeof nexacro[key];
                }
            }
        } catch(e) {}
        return result;
    }""")
    print("\n=== NexacroN 앱 상태 ===")
    print(json.dumps(app_state, indent=2, ensure_ascii=False))

    # 영업속보 클릭 후 상태
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    console_msgs.clear()

    # 메뉴검색 → 선택 후 콘솔 확인
    mf = get_mf()
    find_btn = mf.query_selector(
        "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]"
    )
    if find_btn:
        box = find_btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1)
        mf = get_mf()
        inp = mf.query_selector("#mainframe_childframe_popupFindMenu_form_edtInputText_input")
        if inp:
            inp.fill("당일매출내역")
            inp.press("Enter")
            time.sleep(1)
            mf = get_mf()
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                box = res.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(0.5)
            mf = get_mf()
            sel = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']")
            if sel:
                box = sel.bounding_box()
                if box and box['width'] > 0:
                    print(f"\n선택 버튼 위치: ({box['x']:.0f},{box['y']:.0f}) {box['width']:.0f}x{box['height']:.0f}")
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(0.3)
                    # mousedown/up 개별 발송도 시도
                    page.mouse.move(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    page.mouse.down()
                    time.sleep(0.1)
                    page.mouse.up()
                    print("mousedown/up 개별 발송 완료")
    time.sleep(5)

    print("\n=== 선택 클릭 후 콘솔 에러/경고 ===")
    for m in console_msgs[:20]: print(f"  {m}")
    print(f"  페이지 에러: {page_errors[:5]}")

    # 최종 상태
    mf = get_mf()
    final = mf.evaluate("""() => ({
        hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
        hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
        hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
        currentUrl: window.location.href,
        frameCount: window.frames.length,
    })""")
    print(f"\n=== 최종 상태: {final} ===")

    browser.close()
    print("\n=== 4차 테스트 완료 ===")
