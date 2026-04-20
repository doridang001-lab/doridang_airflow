"""
EasyPOS 8차: Xvfb 가상 디스플레이 + prototype 패치 + 상세 콘솔 추적
실제 Airflow worker와 동일한 환경으로 테스트
"""
import time, os, json, subprocess
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

# Xvfb 시작 (non-headless 모드를 위한 가상 디스플레이)
if not os.environ.get("DISPLAY"):
    xvfb = subprocess.Popen(
        ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)
    os.environ["DISPLAY"] = ":99"
    print("Xvfb :99 시작")
else:
    xvfb = None
    print(f"기존 DISPLAY={os.environ['DISPLAY']} 사용")

all_console = []
failed_reqs = []

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
    return None

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,  # Xvfb 위에서 실제 GUI 모드
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

    ctx.on("requestfailed", lambda req: failed_reqs.append({"url": req.url[-80:], "fail": req.failure}))
    ctx.on("response", lambda resp: failed_reqs.append({"url": resp.url[-80:], "status": resp.status}) if resp.status >= 400 else None)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:250]}"))
    page.on('pageerror', lambda e: all_console.append(f"[PAGEERR] {str(e)[:250]}"))

    print("=== 로그인 시작 ===")
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

    mf = get_mf(page)
    app_state = mf.evaluate("""() => ({
        nexacroExists: typeof nexacro !== 'undefined',
        hasApp: !!(nexacro && nexacro._application_),
        getAppType: typeof nexacro?.getApplication,
        loadingImgExists: !!document.getElementById('loadingImg'),
    })""")
    print(f"\n=== NexacroN 초기화 상태 (Xvfb non-headless): {app_state} ===")

    # 콘솔 에러만 출력
    errs = [m for m in all_console if '[error]' in m.lower() or '[pageerr]' in m.lower()]
    print(f"  로그인 후 콘솔 에러: {len(errs)}개")
    for e in errs[:5]: print(f"    {e}")
    all_console.clear()

    page.screenshot(path=f"{OUT_DIR}/dom8_01_login.png", full_page=True)

    # 영업속보 → 메뉴검색 → 당일매출내역
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1.5)
    else:
        print("[WARN] 영업속보 버튼 없음")

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
                print("[WARN] 선택 버튼 없음")
        else:
            print("[WARN] 검색 입력란 없음")
    else:
        print("[WARN] 메뉴검색 버튼 없음")

    # 당일매출내역 화면 로드 대기
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
            print(f"  ★★★ [{i:2d}s] 당일매출내역 화면 로드 성공! {r}")
            page.screenshot(path=f"{OUT_DIR}/dom8_02_success.png", full_page=True)
            success = True
            break
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")

    # 결과 콘솔
    print(f"\n=== 선택 후 전체 콘솔 ({len(all_console)}개) ===")
    for m in all_console: print(f"  {m}")

    print(f"\n=== 네트워크 실패 ({len(failed_reqs)}개) ===")
    for f in failed_reqs: print(f"  {f}")

    if not success:
        page.screenshot(path=f"{OUT_DIR}/dom8_02_fail.png", full_page=True)
        print(f"\n★ 실패. 스크린샷: {OUT_DIR}/dom8_02_fail.png")

    browser.close()

if xvfb:
    xvfb.terminate()

print("\n=== 8차 테스트 완료 ===")
