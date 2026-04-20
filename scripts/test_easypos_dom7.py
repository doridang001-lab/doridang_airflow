"""
EasyPOS 7차 진단: loadingImg prototype 패치 + Grid 컴포넌트 로드 실패 추적
- 네트워크 실패 요청 캡처 (Grid.js, XFDL form 파일 등)
- 당일매출내역 form 로드 후 nexacro 객체 상태 상세 확인
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

failed_reqs = []
all_console = []

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

    # 핵심 픽스: getElementById prototype 패치 (DOM 수정 없이 null-safe)
    ctx.add_init_script("""
        (function() {
            var _origGetById = Document.prototype.getElementById;
            Document.prototype.getElementById = function(id) {
                var el = _origGetById.call(this, id);
                if (!el && id === 'loadingImg') {
                    return {
                        style: {display: 'block'},
                        id: 'loadingImg',
                        setAttribute: function() {},
                        removeAttribute: function() {}
                    };
                }
                return el;
            };
            Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
            Object.defineProperty(document, 'hidden', {get: function() { return false; }});
            Object.defineProperty(document, 'visibilityState', {get: function() { return 'visible'; }});
            document.addEventListener('visibilitychange', function(e) { e.stopImmediatePropagation(); }, true);
        })();
    """)

    # 네트워크 실패 요청 캡처
    def on_request_failed(req):
        failed_reqs.append({
            "url": req.url,
            "failure": req.failure,
            "resource_type": req.resource_type,
        })

    # 모든 응답 상태 코드 중 4xx/5xx 캡처
    def on_response(resp):
        if resp.status >= 400:
            failed_reqs.append({
                "url": resp.url,
                "status": resp.status,
                "resource_type": resp.request.resource_type,
            })

    ctx.on("requestfailed", on_request_failed)
    ctx.on("response", on_response)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:300]}"))
    page.on('pageerror', lambda e: all_console.append(f"[PAGEERR] {str(e)[:300]}"))

    # 로그인
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

    print("=== 로그인 후 네트워크 실패 ===")
    for f in failed_reqs[:20]: print(f"  {f}")
    failed_reqs.clear()

    print("\n=== 로그인 후 콘솔 (error/pageerr만) ===")
    for m in all_console:
        if '[error]' in m.lower() or '[pageerr]' in m.lower():
            print(f"  {m}")
    all_console.clear()

    # NexacroN 초기화 확인
    mf = get_mf(page)
    app_state = mf.evaluate("""() => ({
        nexacroExists: typeof nexacro !== 'undefined',
        hasApp: !!(nexacro && nexacro._application_),
        getAppType: typeof nexacro?.getApplication,
        loadingImgExists: !!document.getElementById('loadingImg'),
    })""")
    print(f"\n=== NexacroN 초기화 상태: {app_state} ===")

    # 영업속보 → 메뉴검색 → 당일매출내역 선택
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
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("\n선택 클릭 완료 — 결과 대기...")
    else:
        print("[WARN] 메뉴검색 버튼을 찾지 못했습니다")

    # 당일매출내역 화면 로드 대기 (최대 30초)
    success = False
    for i in range(1, 31):
        time.sleep(1)
        mf2 = get_mf(page)
        r = mf2.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
        })""")
        if any(r.values()):
            print(f"  ★ [{i:2d}s] 당일매출내역 화면 감지! {r}")
            success = True
            break
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")

    # 실패한 네트워크 요청 출력 (선택 클릭 이후)
    print(f"\n=== 선택 후 네트워크 실패 ({len(failed_reqs)}개) ===")
    for f in failed_reqs:
        print(f"  {f}")

    # 콘솔 에러
    print(f"\n=== 선택 후 콘솔 메시지 (전체 {len(all_console)}개) ===")
    for m in all_console:
        print(f"  {m}")

    # nexacro 앱 + 폼 상세 상태
    mf3 = get_mf(page)
    detail = mf3.evaluate("""() => {
        try {
            var app = nexacro && nexacro.getApplication && nexacro.getApplication();
            var form = app && app.getActiveFrame && app.getActiveFrame();
            var formId = form && form.id;
            var grd = document.querySelector('[id*="grdSalePerDayList"]');
            var grdNexacro = null;
            try {
                if (app && form) {
                    // 폼 내 컴포넌트 ID 목록
                    var compIds = [];
                    if (form._components) {
                        compIds = Object.keys(form._components).slice(0, 30);
                    }
                    grdNexacro = {compIds: compIds};
                }
            } catch(e) { grdNexacro = {err: e.toString()}; }
            return {
                hasApp: !!(app),
                activeFormId: formId,
                grdDomExists: !!grd,
                grdNexacro: grdNexacro,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n=== nexacro 앱/폼 상태: ===")
    print(json.dumps(detail, indent=2, ensure_ascii=False))

    if not success:
        page.screenshot(path=f"{OUT_DIR}/dom7_fail.png", full_page=True)
        print(f"\n[SHOT] {OUT_DIR}/dom7_fail.png")
    else:
        page.screenshot(path=f"{OUT_DIR}/dom7_success.png", full_page=True)
        print(f"\n[SHOT] {OUT_DIR}/dom7_success.png")

    browser.close()
    print("\n=== 7차 테스트 완료 ===")
