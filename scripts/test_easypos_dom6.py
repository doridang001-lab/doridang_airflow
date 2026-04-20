"""
easypos.xadl.js 전체 저장 + 모든 콘솔 캡처
"""
import time, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

xadl_lines = []
all_console = []

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
    # loadingImg getElementById 패치 (DOM 직접 추가 없이 null-safe)
    ctx.add_init_script("""
        (function() {
            var _orig = Document.prototype.getElementById;
            Document.prototype.getElementById = function(id) {
                var el = _orig.call(this, id);
                if (!el && id === 'loadingImg') {
                    // null-safe 가짜 객체 반환 (DOM 수정 없이)
                    return {style: {display:'block'}, id:'loadingImg', setAttribute:function(){}, removeAttribute:function(){}};
                }
                return el;
            };
            Object.defineProperty(navigator,'webdriver',{get:function(){return undefined;}});
        })();
    """)

    def on_resp(resp):
        if 'easypos.xadl.js' in resp.url:
            try:
                xadl_lines.extend(resp.text().split('\n'))
                print(f"[XADL 캡처] {len(xadl_lines)}줄")
            except: pass
    ctx.on('response', on_resp)

    page = ctx.new_page()
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:200]}"))
    page.on('pageerror', lambda e: all_console.append(f"[PAGEERR] {str(e)[:200]}"))

    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)

    def get_mf():
        for f in page.frames:
            if f.name == "main": return f
        return None

    mf = get_mf()
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
        mf = get_mf()
        el = mf and mf.query_selector(f"#{pid}")
        if el:
            box = el.bounding_box()
            if box:
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1)
    time.sleep(2)

    print("\n=== 로그인 후 콘솔 메시지 (전체) ===")
    for m in all_console: print(f"  {m}")
    all_console.clear()

    # XADL 파일 저장
    if xadl_lines:
        xadl_path = f"{OUT_DIR}/easypos_xadl.js"
        with open(xadl_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(xadl_lines))
        print(f"\n[XADL] {xadl_path} 저장 ({len(xadl_lines)}줄)")
        # 300~400줄 출력 (초기화 관련)
        print("\n=== xadl.js 1~50줄 (초기화 부분) ===")
        for i, line in enumerate(xadl_lines[:50], start=1):
            print(f"  {i:4d}: {line[:120]}")
    else:
        print("[WARN] XADL 캡처 실패")

    # 영업속보 → 메뉴검색 → 선택
    mf = get_mf()
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1.5)

    mf = get_mf()
    find_btn = mf.query_selector(
        "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]"
    )
    if find_btn:
        box = find_btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(1)
        mf = get_mf()
        inp = mf.query_selector("#mainframe_childframe_popupFindMenu_form_edtInputText_input")
        if inp:
            inp.fill("당일매출내역"); inp.press("Enter"); time.sleep(1)
            mf = get_mf()
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                box = res.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2); time.sleep(0.5)
            mf = get_mf()
            sel = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']")
            if sel:
                box = sel.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("선택 클릭 완료")
    time.sleep(5)

    print("\n=== 선택 후 콘솔 메시지 (전체) ===")
    for m in all_console: print(f"  {m}")

    browser.close()
    print("\n=== 6차 테스트 완료 ===")
