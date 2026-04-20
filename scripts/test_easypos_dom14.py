"""
EasyPOS 14차: grdLeft 데이터셋에서 당일매출내역 formurl 추출 + set_formurl 직접 호출
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

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
    all_reqs.clear()

    # --- grdLeft 데이터셋 탐색 ---
    mf = get_mf(page)
    menu_data = mf.evaluate("""() => {
        try {
            // NexacroN 방식으로 form 접근
            var cf = window.application.mainframe.childframe;
            var form = cf.form;  // 현재 form 객체
            if (!form) return {err: 'no form'};

            // divLeftMenu 내 grdLeft 찾기
            var grd = form['divLeftMenu'] && form['divLeftMenu']['divLeftMainList'] && form['divLeftMenu']['divLeftMainList']['grdLeft'];
            if (!grd) {
                // 대안: form의 모든 구성요소에서 grdLeft 찾기
                return {
                    formType: typeof form,
                    formId: form.id,
                    formKeys: Object.keys(form).filter(k => k.includes('Left') || k.includes('Menu') || k.includes('grd')).slice(0,10),
                };
            }

            // 데이터셋 접근
            var ds = grd.bind_datasets || grd._datasets || grd.datasets;
            return {grdFound: true, dsType: typeof ds};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("=== grdLeft 탐색 ===")
    print(json.dumps(menu_data, indent=2, ensure_ascii=False))

    # form 내부 구조 탐색
    form_structure = mf.evaluate("""() => {
        try {
            var cf = window.application.mainframe.childframe;
            var form = cf.form;
            if (!form) return {err: 'no form'};
            // form의 컴포넌트 접근 방법들
            var compKeys = [];
            if (form._components) compKeys = Object.keys(form._components).slice(0, 20);
            var formObjKeys = Object.keys(form).filter(k =>
                k.includes('Left') || k.includes('grd') || k.includes('Grd') ||
                k.includes('Menu') || k.includes('div')
            ).slice(0, 20);
            return {
                formId: form.id,
                hasComponents: !!form._components,
                componentKeys: compKeys,
                formRelatedKeys: formObjKeys,
            };
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("\n=== form 구조 ===")
    print(json.dumps(form_structure, indent=2, ensure_ascii=False))

    # 메뉴 데이터셋을 DOM에서 직접 추출 (NexacroN은 hidden input에 데이터 저장하기도 함)
    menu_ds = mf.evaluate("""() => {
        try {
            // grdLeft의 행 텍스트 추출
            var rows = Array.from(document.querySelectorAll('[id*="grdLeft_body_gridrow"]'));
            var rowData = rows.slice(0, 30).map(r => ({
                id: r.id,
                text: (r.textContent || '').trim().slice(0, 50),
                visible: r.offsetParent !== null || r.style.display !== 'none',
            }));

            // innerText 기반 메뉴 탐색
            var allMenuText = Array.from(document.querySelectorAll('[id*="divLeftMenu"] [id]'))
                .filter(el => el.textContent && el.textContent.trim())
                .map(el => ({id: el.id.slice(-40), text: el.textContent.trim().slice(0, 40)}))
                .filter(el => el.text && el.text.length > 1 && el.text.length < 30)
                .slice(0, 20);
            return {rowData: rowData, menuText: allMenuText};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("\n=== 왼쪽 메뉴 텍스트 목록 ===")
    print(json.dumps(menu_ds, indent=2, ensure_ascii=False))

    # --- 직접 set_formurl 호출 시도 ---
    print("\n=== set_formurl 직접 호출 시도 ===")
    # EasyPOS NexacroN 일반적인 formurl 패턴
    # 당일매출내역은 영업일보 카테고리 TA 계열일 가능성
    candidates = [
        "easyposnx/ta/TA01001.xfdl",
        "easyposnx/ta/TA_01001.xfdl",
        "easyposnx/sc/SC01001.xfdl",
        "ta/TA01001.xfdl",
        "frm::TA01001.xfdl",
    ]

    all_reqs.clear()
    nav_result = mf.evaluate("""(candidates) => {
        try {
            var cf = window.application.mainframe.childframe;
            // set_formurl로 첫 번째 후보 시도
            if (typeof cf.set_formurl === 'function') {
                cf.set_formurl(candidates[0]);
                return {called: true, url: candidates[0]};
            }
            return {err: 'set_formurl not function: ' + typeof cf.set_formurl};
        } catch(e) { return {err: e.toString()}; }
    }""", candidates)
    print(f"  set_formurl 결과: {nav_result}")
    time.sleep(3)
    reqs_after = list(all_reqs)
    print(f"  이후 요청들: {reqs_after[:10]}")

    # 화면 확인
    page.screenshot(path=f"{OUT_DIR}/dom14_after_setformurl.png", full_page=True)
    print(f"  스크린샷: {OUT_DIR}/dom14_after_setformurl.png")

    browser.close()
print("\n=== 14차 테스트 완료 ===")
