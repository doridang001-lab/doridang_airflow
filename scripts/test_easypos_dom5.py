"""
easypos.xadl.js 319번 줄 확인 + 선택 클릭 후 DOM 변화 상세 파악
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

xadl_content = []

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,
        args=["--no-sandbox","--disable-dev-shm-usage","--window-size=1920,1080"],
    )
    ctx = browser.new_context(viewport={"width":1920,"height":1080})

    # easypos.xadl.js 캡처
    def on_response(resp):
        if 'easypos.xadl.js' in resp.url:
            try:
                body = resp.text()
                lines = body.split('\n')
                xadl_content.extend(lines)
                print(f"[XADL] {resp.url} — {len(lines)}줄 캡처")
            except: pass
    ctx.on('response', on_response)

    page = ctx.new_page()

    # 로그인
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)

    def get_mf():
        for f in page.frames:
            if f.name == "main":
                return f
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
                page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
                time.sleep(1)
    time.sleep(2)

    # easypos.xadl.js 319번 줄 출력
    if xadl_content:
        print("\n=== easypos.xadl.js 주변 줄 (310~330) ===")
        for i, line in enumerate(xadl_content[308:332], start=309):
            marker = " <<<" if i == 319 else ""
            print(f"  {i:4d}: {line[:120]}{marker}")
    else:
        print("\n[WARN] easypos.xadl.js 캡처 실패 — 별도 download 시도")
        try:
            # 별도 세션으로 다운로드
            import urllib.request
            # 이미 로그인된 쿠키가 없으면 실패할 수 있음
            with urllib.request.urlopen("https://smart.easypos.net/easyposnx/easypos.xadl.js") as r:
                body = r.read().decode('utf-8', errors='replace')
                lines = body.split('\n')
                for i, line in enumerate(lines[308:332], start=309):
                    marker = " <<<" if i == 319 else ""
                    print(f"  {i:4d}: {line[:120]}{marker}")
        except Exception as e:
            print(f"  다운로드 실패: {e}")

    # 선택 클릭 전 DOM id 스냅샷
    mf = get_mf()
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        box = el.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1.5)

    before_ids = set(get_mf().evaluate("""
        () => Array.from(document.querySelectorAll('[id]')).map(e=>e.id).filter(x=>x)
    """))
    print(f"\n선택 전 DOM id 수: {len(before_ids)}")

    # 메뉴검색 → 선택
    mf = get_mf()
    find_btn = mf.query_selector(
        "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]"
    )
    if find_btn:
        box = find_btn.bounding_box()
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(1)
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

    # 클릭 후 DOM 변화 추적
    for sec in [1, 2, 5, 10, 20]:
        time.sleep(1 if sec == 1 else (sec - (1 if sec==2 else (sec-2 if sec==5 else (5 if sec==10 else 10)))))
        mf2 = get_mf()
        after_ids = set(mf2.evaluate("""
            () => Array.from(document.querySelectorAll('[id]')).map(e=>e.id).filter(x=>x)
        """))
        new_ids = after_ids - before_ids
        removed_ids = before_ids - after_ids
        print(f"\n[{sec:2d}s 후] DOM 변화: 추가={len(new_ids)} 제거={len(removed_ids)}")
        if new_ids:
            for nid in sorted(new_ids)[:20]:
                print(f"  + {nid[:80]}")
        if removed_ids:
            for rid in sorted(removed_ids)[:5]:
                print(f"  - {rid[:80]}")

    browser.close()
    print("\n=== 5차 테스트 완료 ===")
