"""
EasyPOS 3차 진단: 네트워크 요청 캡처 + page.bring_to_front + CDP focus 시도
NexacroN이 navigation 시 어떤 XHR/fetch를 보내는지 파악
"""
import time, json, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

nav_requests = []

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
    return None

def shot(page, name):
    p = f"{OUT_DIR}/t3_{name}.png"
    page.screenshot(path=p, full_page=True)
    print(f"[SHOT] {p}")

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled"],
    )
    ctx = browser.new_context(
        viewport={"width":1920,"height":1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )
    ctx.add_init_script("""
        Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
        Object.defineProperty(document,'hidden',{get:()=>false});
        Object.defineProperty(document,'visibilityState',{get:()=>'visible'});
        document.addEventListener('visibilitychange',(e)=>e.stopImmediatePropagation(),true);
        // hasFocus 강제 override
        const _hasFocus = document.hasFocus.bind(document);
        document.hasFocus = () => true;
        window.hasFocus = () => true;
    """)
    page = ctx.new_page()

    # 네트워크 요청 캡처
    captured = []
    def on_request(req):
        if req.resource_type in ('xhr', 'fetch') and 'easypos' in req.url:
            captured.append({'url': req.url, 'method': req.method})
    page.on('request', on_request)

    # 로그인
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
        el = mf.query_selector(f"#{pid}")
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                time.sleep(1)
    time.sleep(2)
    mf = get_mf(page)

    # page.bring_to_front()로 포커스 확보
    page.bring_to_front()
    time.sleep(0.5)

    print("=== focus 상태 ===")
    focus_state = mf.evaluate("""() => ({
        hidden: document.hidden,
        visibility: document.visibilityState,
        hasFocus: document.hasFocus(),
        activeEl: document.activeElement ? document.activeElement.tagName : 'none',
    })""")
    print(json.dumps(focus_state))

    # 캡처 초기화
    captured.clear()

    # 영업속보 클릭 → 당일매출 타일 클릭 → 요청 캡처
    mf = get_mf(page)
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    shot(page, "01_dashboard")
    print(f"\n=== 영업속보 클릭 후 요청 {len(captured)}개 ===")
    for r in captured: print(f"  {r['method']} {r['url'][:100]}")
    captured.clear()

    # 당일매출 타일을 JS 이벤트로도 클릭
    mf = get_mf(page)
    tile_result = mf.evaluate("""() => {
        // 당일매출 텍스트가 있는 visible 요소 찾기
        let found = null;
        for(let el of document.querySelectorAll('*')) {
            let txt = (el.innerText||el.textContent||'').trim();
            if(txt === '당일매출' || txt === '당일매출 >' || txt === '당일매출내역') {
                let box = el.getBoundingClientRect();
                if(box.width > 5 && box.height > 5) {
                    found = {tag: el.tagName, id: el.id, txt, x: box.x, y: box.y, w: box.width, h: box.height};
                    // JS click 시도
                    el.click();
                    break;
                }
            }
        }
        return found;
    }""")
    print(f"\n=== JS click 당일매출 tile: {tile_result} ===")
    time.sleep(2)
    shot(page, "02_after_js_click")
    print(f"JS click 후 요청 {len(captured)}개:")
    for r in captured: print(f"  {r['method']} {r['url'][:100]}")
    captured.clear()

    # NexacroN 이벤트 핸들러 직접 실행 시도
    print("\n=== NexacroN handler 직접 실행 시도 ===")
    mf = get_mf(page)
    nx_result = mf.evaluate("""() => {
        try {
            // nexacro 전역에서 application 찾기
            let app = null;
            for(let key of Object.keys(nexacro)) {
                let val = nexacro[key];
                if(val && typeof val === 'object' && val._type_ === 'Application') {
                    app = val;
                    break;
                }
                if(val && typeof val === 'function' && key.includes('App')) {
                    try { app = val(); } catch(e) {}
                    if(app) break;
                }
            }

            // nexacro._object_ 패턴 시도
            let objs = [];
            for(let key of Object.keys(nexacro)) {
                let val = nexacro[key];
                if(val && typeof val === 'object' && val.constructor) {
                    objs.push({key, type: val._type_ || val.constructor.name});
                }
            }
            return {app: app ? 'found' : 'null', objCount: objs.length, objs: objs.slice(0,10)};
        } catch(e) {
            return {err: e.toString()};
        }
    }""")
    print(json.dumps(nx_result, indent=2, ensure_ascii=False))

    # 메뉴검색 → 선택 → 이후 요청 캡처
    print("\n=== 메뉴검색 → 선택 후 네트워크 요청 캡처 ===")
    captured.clear()
    mf = get_mf(page)

    # btnFind 클릭
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
            inp.fill("당일매출내역")
            inp.press("Enter")
            time.sleep(1)
            # 결과 클릭
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                box = res.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(0.5)
            # 선택 버튼
            sel_btn = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']")
            if sel_btn:
                box = sel_btn.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("  선택 버튼 클릭 완료")

    # 5초 대기 후 요청 및 DOM 변화 확인
    for i in range(1, 6):
        time.sleep(1)
        mf2 = get_mf(page)
        has_sales = mf2.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            reqs: window.__nav_reqs__ ? window.__nav_reqs__.length : 0
        })""")
        print(f"  [{i}s] {has_sales} | 요청 누적: {len(captured)}개")
        if has_sales.get('hasSalesDate') or has_sales.get('hasGrid'):
            print("  ★ 성공!")
            break

    shot(page, "03_after_findmenu")
    print(f"\n메뉴검색 후 요청 {len(captured)}개:")
    for r in captured[-20:]: print(f"  {r['method']} {r['url'][:120]}")

    browser.close()
    print("\n=== 3차 테스트 완료 ===")
