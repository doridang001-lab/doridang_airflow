"""
EasyPOS 13차: iframe 좌표 오프셋 확인 + 키보드 방식 navigation + 직접 formurl 설정 시도
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

all_console = []
all_reqs = []

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
    ctx.on("request", lambda r: all_reqs.append(r.url))

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:300]}"))

    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)

    # --- 1. iframe 오프셋 확인 ---
    # 최상위 페이지에서 'main' iframe의 위치 확인
    iframe_offset = page.evaluate("""() => {
        var frames = document.querySelectorAll('frame, iframe');
        var result = [];
        for (var f of frames) {
            var rect = f.getBoundingClientRect();
            result.push({name: f.name, src: (f.src||'').slice(-50), x: rect.x, y: rect.y, w: rect.width, h: rect.height});
        }
        return result;
    }""")
    print("=== 최상위 페이지 frame 오프셋 ===")
    for f in iframe_offset: print(f"  {f}")

    mf = get_mf(page)

    # main frame 내 element 위치와 실제 page 좌표 비교
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
    all_reqs.clear()

    # --- 2. 영업속보 → 메뉴검색 → Enter 방식 시도 ---
    print("\n=== 방법 1: 메뉴검색 + keyboard Enter ===")
    mf = get_mf(page)
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
            # Playwright 기본 click + type (좌표 없이)
            inp.click(); time.sleep(0.3)
            page.keyboard.type("당일매출내역"); time.sleep(0.5)
            page.keyboard.press("Enter"); time.sleep(1.5)

            # 방법 1a: 결과 선택 후 Enter만
            mf = get_mf(page)
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                print(f"  결과 발견, Playwright click() 사용")
                res.click(); time.sleep(0.5)  # Playwright 내장 click (좌표 자동)
                page.keyboard.press("Enter"); time.sleep(2)
                # DOM 변화 확인
                mf2 = get_mf(page)
                r = mf2.evaluate("""() => ({
                    hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
                    hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
                    popupGone: !document.querySelector('[id*="popupFindMenu"]'),
                    reqCount: window.__reqCount || 0,
                })""")
                print(f"  결과: {r}")
                if any([r['hasSalesDate'], r['hasGrid']]):
                    print("  ★ 성공!")

    # 현재 form 확인
    mf = get_mf(page)
    cf_state = mf.evaluate("""() => {
        try {
            var cf = window.application.mainframe.childframe;
            return {formurl: cf.formurl || cf._formurl, form_id: cf.form && cf.form.id};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"  childframe state: {cf_state}")
    print(f"  요청 수: {len(all_reqs)}")
    # XFDL 요청 있는지 확인
    xfdl_reqs = [r for r in all_reqs if '.xfdl' in r or 'form' in r.lower()]
    print(f"  XFDL 관련 요청: {xfdl_reqs[:5]}")

    # --- 3. 팝업을 닫고 직접 formurl 설정 시도 ---
    print("\n=== 방법 2: childframe.formurl 직접 설정 ===")
    # 먼저 모든 XFDL 경로 패턴 파악
    all_reqs_snap = list(all_reqs)
    xfdl_all = [r for r in all_reqs_snap if '.xfdl' in r or 'SC0' in r or 'BC0' in r or 'TA0' in r]
    print(f"  누적 XFDL 후보 요청:")
    for r in xfdl_all[:10]: print(f"    {r}")

    # childframe 프로토타입 메서드 확인
    cf_proto = mf.evaluate("""() => {
        try {
            var cf = window.application.mainframe.childframe;
            var proto = Object.getPrototypeOf(cf);
            var protoMethods = proto ? Object.getOwnPropertyNames(proto).filter(k => typeof proto[k] === 'function').slice(0, 30) : [];
            var proto2 = proto ? Object.getPrototypeOf(proto) : null;
            var proto2Methods = proto2 ? Object.getOwnPropertyNames(proto2).filter(k => typeof proto2[k] === 'function').slice(0, 20) : [];
            return {protoMethods: protoMethods, proto2Methods: proto2Methods};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"  childframe prototype 메서드:")
    print(json.dumps(cf_proto, indent=2, ensure_ascii=False))

    # formurl setter 시도
    nav_result = mf.evaluate("""() => {
        try {
            var cf = window.application.mainframe.childframe;
            // set_formurl 또는 formurl setter 확인
            var proto = Object.getPrototypeOf(cf);
            var hasSetFormurl = proto && (typeof proto.set_formurl === 'function' || typeof proto.loadUrl === 'function');
            var formPropDesc = Object.getOwnPropertyDescriptor(proto, 'formurl');
            return {
                hasSetFormurl: typeof proto?.set_formurl,
                hasLoadUrl: typeof proto?.loadUrl,
                formPropHasSetter: !!(formPropDesc && formPropDesc.set),
                formPropDesc: formPropDesc ? {get: !!formPropDesc.get, set: !!formPropDesc.set} : null,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"\n  formurl setter 확인: {nav_result}")

    # --- 4. 왼쪽 사이드바 메뉴 직접 접근 ---
    print("\n=== 방법 3: 왼쪽 사이드바 메뉴 트리 직접 접근 ===")
    mf = get_mf(page)
    # 영업일보 카테고리 아래에 있는 메뉴 항목들
    left_menu = mf.evaluate("""() => {
        // grdLeft 관련 모든 요소 찾기
        var items = Array.from(document.querySelectorAll('[id*="grdLeft"],[id*="divLeftMenu"]'));
        var result = items.slice(0, 10).map(el => ({
            id: el.id,
            text: (el.textContent || '').trim().slice(0, 30),
            visible: el.offsetParent !== null,
            rect: el.getBoundingClientRect ? {x: Math.round(el.getBoundingClientRect().x), y: Math.round(el.getBoundingClientRect().y)} : null,
        }));
        return result;
    }""")
    print(f"  왼쪽 메뉴 요소들:")
    for item in left_menu: print(f"    {item}")

    browser.close()
print("\n=== 13차 테스트 완료 ===")
