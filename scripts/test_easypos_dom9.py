"""
EasyPOS 9차: 전체 프레임 nexacro 상태 + xadl.js application_onload 전체 내용 확인
- 모든 frame에서 nexacro 상태 점검
- 캡처된 xadl.js에서 application_onload 전체 코드 출력
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

XADL_PATH = f"{OUT_DIR}/easypos_xadl.js"

# 캡처된 xadl.js에서 application_onload 찾기
if os.path.exists(XADL_PATH):
    with open(XADL_PATH, encoding='utf-8') as f:
        lines = f.readlines()
    print(f"=== xadl.js ({len(lines)}줄) — application_onload 검색 ===")
    in_block = False
    for i, line in enumerate(lines, 1):
        if 'application_onload' in line:
            in_block = True
        if in_block:
            print(f"  {i:4d}: {line.rstrip()[:120]}")
            # 블록 종료 추정 (빈 줄 3개 연속 또는 다음 함수 시작)
            if i > 10 and line.strip() == '' and in_block:
                pass  # 빈 줄 계속 출력
            if in_block and i > 1 and 'application_onload' not in line and line.strip().startswith('"') and i > 5:
                if in_block and line.strip().endswith('",') or line.strip().endswith('"'):
                    # 새 함수 정의 시작으로 보임
                    if not line.strip().startswith('"application_'):
                        pass
        if in_block and i > 350:  # xadl.js 전체가 짧으니 충분
            break
    print()

    # _application_ 관련 라인 찾기
    print("=== xadl.js _application_ / getApplication 관련 라인 ===")
    for i, line in enumerate(lines, 1):
        if '_application_' in line or 'getApplication' in line or 'setApplication' in line:
            print(f"  {i:4d}: {line.rstrip()[:120]}")
else:
    print(f"[WARN] {XADL_PATH} 없음 — 다시 캡처 필요")

all_console = []
xadl_lines = []

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

    # xadl.js 재캡처
    def on_resp(resp):
        if 'easypos.xadl.js' in resp.url:
            try: xadl_lines.extend(resp.text().split('\n'))
            except: pass

    ctx.on('response', on_resp)

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

    # 모든 프레임에서 nexacro 상태 확인
    print(f"\n=== 전체 프레임 목록 ({len(page.frames)}개) ===")
    for fi, frame in enumerate(page.frames):
        try:
            state = frame.evaluate("""() => {
                try {
                    return {
                        nexacroType: typeof nexacro,
                        hasApp: typeof nexacro !== 'undefined' ? !!(nexacro._application_) : null,
                        getAppFn: typeof nexacro !== 'undefined' ? typeof nexacro.getApplication : null,
                        keys: typeof nexacro !== 'undefined' ? Object.keys(nexacro).slice(0,10) : [],
                    };
                } catch(e) { return {err: e.toString()}; }
            }""")
        except Exception as e:
            state = {"evalErr": str(e)[:80]}
        print(f"  Frame[{fi}] name={frame.name!r} url={frame.url[:60]!r}")
        print(f"    {state}")

    # xadl.js 재캡처한 경우 application_onload 분석
    if xadl_lines:
        print(f"\n=== xadl.js 재캡처 ({len(xadl_lines)}줄) — application_onload 구간 ===")
        in_fn = False
        fn_start = 0
        for i, line in enumerate(xadl_lines, 1):
            if '"application_onload"' in line or 'application_onload' in line and 'function' in line:
                in_fn = True
                fn_start = i
            if in_fn:
                print(f"  {i:4d}: {line[:120]}")
                if i > fn_start + 3 and (line.strip() == '' or (i > fn_start + 5 and line.strip().startswith('"') and '"application_' not in line)):
                    break

    # nexacro 키 상세
    print("\n=== main frame nexacro 키 전체 (50개) ===")
    mf = get_mf(page)
    if mf:
        keys = mf.evaluate("() => { try { return Object.keys(nexacro); } catch(e) { return []; } }")
        print(f"  총 {len(keys)}개: {keys[:50]}")

    # app_onload 수동 호출 시도
    print("\n=== application_onload 수동 호출 시도 ===")
    result = mf.evaluate("""() => {
        try {
            // nexacro._application_ 강제 확인
            var app = nexacro.getApplication ? nexacro.getApplication() : null;
            if (!app) {
                // mainframe으로 직접 접근 시도
                var mf2 = nexacro['mainframe'] || nexacro['_mainframe_'] || null;
                return {app: null, mf2type: typeof mf2, nexacroKeys: Object.keys(nexacro).slice(0,20)};
            }
            return {app: !!app, appType: typeof app};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(f"  {json.dumps(result, ensure_ascii=False)}")

    browser.close()
print("\n=== 9차 테스트 완료 ===")
