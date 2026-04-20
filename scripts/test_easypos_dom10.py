"""
EasyPOS 10차: 선택 클릭 후 ALL 네트워크 요청 모니터링
- nexacro.mainframe 상태 상세 확인
- 선택 클릭 후 신규 네트워크 요청이 있는지 확인 (NexacroN이 form 로드 요청 보내는지)
- gfnMsgGrdNodataSet 호출 스택 인터셉트
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

    # 선택 클릭 후 모든 요청 캡처
    def on_request(req):
        if click_done:
            post_click_reqs.append({
                "method": req.method,
                "url": req.url[-100:],
                "type": req.resource_type,
            })

    ctx.on("request", on_request)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:300]}"))
    page.on('pageerror', lambda e: all_console.append(f"[PAGEERR] {str(e)[:300]}"))

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

    # nexacro.mainframe 상세 확인
    mf = get_mf(page)
    mf_state = mf.evaluate("""() => {
        try {
            var mframe = nexacro.mainframe || nexacro['mainframe'];
            if (!mframe) return {err: 'no mainframe'};
            var child = mframe.childframe;
            var keys_mframe = Object.keys(mframe).slice(0, 20);
            var child_info = child ? {
                type: typeof child,
                keys: Object.keys(child).slice(0, 20),
                id: child.id,
            } : null;
            return {
                mframe_type: typeof mframe,
                mframe_id: mframe.id,
                mframe_keys: keys_mframe,
                child_info: child_info,
                // nexacro application 접근 방법 탐색
                has_app_prop: '_application_' in nexacro,
                app_keys_via_nexacro: Object.getOwnPropertyNames(nexacro).filter(k => k.includes('app') || k.includes('App')).slice(0, 10),
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("=== nexacro.mainframe 상세 ===")
    print(json.dumps(mf_state, indent=2, ensure_ascii=False))

    # gfnMsgGrdNodataSet 호출 인터셉트 시도
    try:
        mf.evaluate("""() => {
            // nexacro.Form의 gfnMsgGrdNodataSet을 래핑해 스택 출력
            if (nexacro && nexacro.Form && nexacro.Form.prototype && nexacro.Form.prototype.gfnMsgGrdNodataSet) {
                var orig = nexacro.Form.prototype.gfnMsgGrdNodataSet;
                nexacro.Form.prototype.gfnMsgGrdNodataSet = function() {
                    console.log('[INTERCEPT] gfnMsgGrdNodataSet called from: ' + new Error().stack.split('\\n').slice(0,5).join(' | '));
                    return orig.apply(this, arguments);
                };
                console.log('[INTERCEPT] gfnMsgGrdNodataSet 훅 설치 완료');
            } else {
                console.log('[INTERCEPT] gfnMsgGrdNodataSet 없음: Form.prototype exists=' + !!(nexacro && nexacro.Form && nexacro.Form.prototype));
            }
        }""")
    except Exception as e:
        print(f"[INTERCEPT 실패] {e}")

    all_console.clear()

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
        inp = mf.query_selector("#mainframe_childframe_form_edtInputText_input") or \
              mf.query_selector("#mainframe_childframe_popupFindMenu_form_edtInputText_input")
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
                    click_done = True  # 이제부터 네트워크 요청 캡처
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("\n선택 클릭 완료 — 네트워크 모니터링 중...")
            else:
                print("[WARN] 선택 버튼 없음")
        else:
            print("[WARN] 검색 입력란 없음")
    else:
        print("[WARN] 메뉴검색 버튼 없음")

    # 15초 대기 + 요청 모니터링
    for i in range(1, 16):
        time.sleep(1)
        if i % 5 == 0 or i == 1:
            mf2 = get_mf(page)
            r = mf2.evaluate("""() => ({
                hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
                hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
                hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
            })""")
            print(f"  [{i:2d}s] DOM: {r} | 네트워크 요청: {len(post_click_reqs)}개 누적")

    # 결과 출력
    print(f"\n=== 선택 후 ALL 네트워크 요청 ({len(post_click_reqs)}개) ===")
    for r in post_click_reqs:
        print(f"  {r['method']} [{r['type']}] ...{r['url']}")

    print(f"\n=== 콘솔 메시지 ({len(all_console)}개) ===")
    for m in all_console:
        print(f"  {m}")

    browser.close()
print("\n=== 10차 테스트 완료 ===")
