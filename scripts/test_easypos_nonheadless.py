"""
EasyPOS non-headless 테스트 (Xvfb DISPLAY=:99)
navigation이 동작하는지 확인
"""
import time, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

def shot(page, name):
    p = f"{OUT_DIR}/nh_{name}.png"
    page.screenshot(path=p, full_page=True)
    print(f"[SHOT] {p}")

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
    return None

def check_daily(mf):
    return mf.evaluate("""() => ({
        hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
        hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
        hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
        hasSearch: document.querySelectorAll('[id*="btnCommSearch"]').length,
    })""")

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,   # ← non-headless (Xvfb 사용)
        args=["--no-sandbox","--disable-dev-shm-usage","--window-size=1920,1080"],
    )
    ctx = browser.new_context(viewport={"width":1920,"height":1080})
    page = ctx.new_page()

    print("=== non-headless 모드 로그인 ===")
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
    print("  로그인 완료")
    shot(page, "01_login")

    # 영업속보 클릭
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)
    mf = get_mf(page)
    shot(page, "02_sales_menu")

    # 메뉴검색 → 당일매출내역
    print("\n=== 메뉴검색 시도 ===")
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
            res = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'당일매출내역')]")
            if res:
                box = res.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    time.sleep(0.5)
            sel_btn = mf.query_selector("xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']")
            if sel_btn:
                box = sel_btn.bounding_box()
                if box and box['width'] > 0:
                    page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                    print("  선택 클릭 완료")

    # 결과 대기
    for i in range(1, 16):
        time.sleep(1)
        mf2 = get_mf(page)
        r = check_daily(mf2)
        print(f"  [{i:2d}s] {r}")
        if r.get('hasSalesDate') or r.get('hasGrid') or r.get('hasYesterday'):
            print("  ★★★ 당일매출내역 화면 로드 성공!")
            shot(page, "03_daily_success")
            break
    else:
        shot(page, "03_daily_fail")
        print("  실패 — 프레임 dump:")
        for f in page.frames:
            print(f"    ({f.name!r}, {f.url})")

    browser.close()
    print("\n=== non-headless 테스트 완료 ===")
