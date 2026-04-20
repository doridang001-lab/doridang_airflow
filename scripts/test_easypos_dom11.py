"""
EasyPOS 11차: 전체 초기화 로그 + 전역 변수 스캔
- 로그인 전부터 모든 콘솔 메시지 캡처
- 페이지 로드 후 window 전역 변수에서 NexacroN 앱 관련 항목 탐색
- application_onload 발화 여부 확인 (prototype 패치 없이 순수 상태)
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

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

    # 패치 없이: loadingImg null 에러가 console에 찍히는지 확인
    ctx.add_init_script("""
        (function() {
            Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
            // application_onload 발화 추적: getElementById 호출 로깅
            var _origGetById = Document.prototype.getElementById;
            Document.prototype.getElementById = function(id) {
                if (id === 'loadingImg') {
                    var el = _origGetById.call(this, id);
                    console.log('[PATCH] getElementById(loadingImg) → ' + (el ? 'FOUND' : 'NULL'));
                    if (!el) {
                        console.log('[PATCH] null 반환됨 → application_onload 크래시 예정');
                        // 이번엔 null 그대로 반환 (패치 안 함)
                    }
                    return el;
                }
                return _origGetById.call(this, id);
            };
        })();
    """)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:400]}"))
    page.on('pageerror', lambda e: all_console.append(f"[PAGEERR] {str(e)[:400]}"))

    print("=== 페이지 로드 시작 ===")
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(3)

    print(f"\n=== 로드 직후 콘솔 메시지 ({len(all_console)}개) ===")
    for m in all_console: print(f"  {m}")
    all_console.clear()

    # main frame 상태 (로그인 전)
    mf = get_mf(page)
    if mf:
        pre_state = mf.evaluate("""() => {
            // window 전역 변수 중 nexacro 관련
            var appVars = Object.keys(window).filter(k =>
                k.includes('nexacro') || k.includes('Nexacro') ||
                k.includes('application') || k.includes('Application') ||
                k.includes('mainframe') || k.includes('xadl') || k.includes('gfn')
            ).slice(0, 30);
            return {
                nexacroType: typeof nexacro,
                nexacroKeys_app: Object.keys(nexacro).filter(k =>
                    k.toLowerCase().includes('app') || k.includes('main') || k.includes('_application')
                ).slice(0, 20),
                windowAppVars: appVars,
                loadingImgInHTML: !!document.getElementById('loadingImg'),
            };
        }""")
        print("\n=== 로그인 전 main frame 상태 ===")
        print(json.dumps(pre_state, indent=2, ensure_ascii=False))

    # 로그인
    mf.query_selector("#mainframe_childframe_form_divMain_edtId").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtId_input").fill(EASYPOS_ID)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw_input").fill(EASYPOS_PW)
    mf.query_selector("#mainframe_childframe_form_divMain_btnLogin").click()
    time.sleep(7)

    print(f"\n=== 로그인 후 콘솔 메시지 ({len(all_console)}개) ===")
    for m in all_console: print(f"  {m}")
    all_console.clear()

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

    # 로그인 후 상태
    mf = get_mf(page)
    post_state = mf.evaluate("""() => {
        var appVars = Object.keys(window).filter(k =>
            k.includes('nexacro') || k.includes('Nexacro') ||
            k.includes('application') || k.includes('Application') ||
            k.includes('mainframe') || k.includes('xadl') || k.includes('gfn')
        ).slice(0, 30);
        // nexacro 키 중 app/main 관련
        var nexAppKeys = Object.keys(nexacro || {}).filter(k =>
            k.toLowerCase().includes('app') || k.toLowerCase().includes('main') ||
            k.includes('_application') || k.includes('frame')
        ).slice(0, 30);
        // nexacro 키 중 대소문자 무관 application
        var nexAllKeys = Object.keys(nexacro || {});
        var hasGetApp = nexAllKeys.includes('getApplication');
        var hasSetApp = nexAllKeys.includes('setApplication');
        var hasAppObj = nexAllKeys.includes('_application_') || nexAllKeys.includes('application_');
        return {
            nexacroType: typeof nexacro,
            hasGetApplication: hasGetApp,
            hasSetApplication: hasSetApp,
            has_application_: hasAppObj,
            nexAppKeys: nexAppKeys,
            windowAppVars: appVars,
            loadingImgInHTML: !!document.getElementById('loadingImg'),
        };
    }""")
    print("\n=== 로그인 후 main frame 상태 ===")
    print(json.dumps(post_state, indent=2, ensure_ascii=False))

    print(f"\n=== 팝업 닫기 후 콘솔 ({len(all_console)}개) ===")
    for m in all_console: print(f"  {m}")

    browser.close()
print("\n=== 11차 테스트 완료 ===")
