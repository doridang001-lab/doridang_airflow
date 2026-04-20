"""
EasyPOS DOM 2차 진단 - divLeftMenu 구조 파악
"""
import time, json, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

def shot(page, name):
    p = f"{OUT_DIR}/t2_{name}.png"
    page.screenshot(path=p, full_page=True)
    print(f"[SHOT] {p}")

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
    return None

def scan_daily(mf, label):
    rows = mf.evaluate("""() => {
        let out = [];
        for(let el of document.querySelectorAll('*')) {
            let txt = (el.innerText||el.textContent||'').trim();
            if(!txt.includes('당일매출')) continue;
            if(txt.length > 60) continue;
            let box = el.getBoundingClientRect();
            out.push({
                tag: el.tagName, id: el.id.slice(0,70),
                txt: txt.slice(0,30),
                x: Math.round(box.x), y: Math.round(box.y),
                w: Math.round(box.width), h: Math.round(box.height)
            });
        }
        return out;
    }""")
    print(f"\n=== {label} — '당일매출' 요소 {len(rows)}개 ===")
    for r in rows:
        vis = r['w'] > 0 and r['h'] > 0
        print(f"  {'[VIS]' if vis else '[HID]'} {r['tag']:<6} ({r['x']:>4},{r['y']:>4}) {r['w']:>4}x{r['h']:>4}  id={r['id'][:50]}")

def scan_grdleft(mf, label, limit=40):
    rows = mf.evaluate(f"""() => {{
        let out = [];
        for(let el of document.querySelectorAll('[id*="grdLeft_body_gridrow_"]')) {{
            let txt = (el.innerText||el.textContent||'').trim().slice(0,20);
            let box = el.getBoundingClientRect();
            out.push({{id:el.id.slice(0,70), txt, x:Math.round(box.x), y:Math.round(box.y), w:Math.round(box.width), h:Math.round(box.height)}});
        }}
        return out.slice(0,{limit});
    }}""")
    print(f"\n=== {label} — grdLeft 행 {len(rows)}개 ===")
    for r in rows:
        vis = r['w'] > 0 and r['h'] > 0
        if vis: print(f"  [VIS] ({r['x']:>4},{r['y']:>4}) {r['w']:>4}x{r['h']:>4}  txt={r['txt']:<20}  id={r['id'][:50]}")

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
    """)
    page = ctx.new_page()

    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)
    mf = get_mf(page)

    # 로그인
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

    # ── Step1: 영업속보 클릭 ──────────────────────────────
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el: el.click(); time.sleep(1.5)
    mf = get_mf(page)
    shot(page, "01_after_top_menu")
    scan_grdleft(mf, "영업속보 클릭 후", limit=40)
    scan_daily(mf, "영업속보 클릭 후")

    # ── Step2: 영업일보 single click ─────────────────────
    print("\n=== 영업일보 single click ===")
    mf = get_mf(page)
    yb_row = mf.query_selector(
        "xpath=(//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'영업일보')])[1]"
    )
    if yb_row:
        box = yb_row.bounding_box()
        cx = box["x"]+box["width"]/2; cy = box["y"]+box["height"]/2
        print(f"  영업일보 행 위치: ({box['x']:.0f},{box['y']:.0f}) {box['width']:.0f}x{box['height']:.0f} → 클릭({cx:.0f},{cy:.0f})")
        page.mouse.click(cx, cy)
        time.sleep(2.0)
    else:
        print("  영업일보 행 없음")

    mf = get_mf(page)
    shot(page, "02_after_yb_click")
    scan_grdleft(mf, "영업일보 single click 후", limit=60)
    scan_daily(mf, "영업일보 single click 후")

    # ── Step3: 당일매출내역 클릭 시도 ─────────────────────
    print("\n=== 당일매출내역 클릭 시도 ===")
    mf = get_mf(page)

    # grdLeft에서 먼저 시도
    dm_selectors = [
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일매출내역')]",
        "xpath=//*[contains(@id,'divLeftMenu')][contains(normalize-space(.),'당일매출내역')][not(contains(normalize-space(.),'전체'))]",
        "xpath=//*[contains(normalize-space(.),'당일매출내역')][not(contains(normalize-space(.),'전체'))]",
    ]
    clicked = False
    for sel in dm_selectors:
        els = mf.query_selector_all(sel)
        for el in els:
            box = el.bounding_box()
            if box and box['width'] > 0 and box['height'] > 0:
                cx = box["x"]+box["width"]/2; cy = box["y"]+box["height"]/2
                print(f"  발견: sel={sel[:50]} xy=({cx:.0f},{cy:.0f})")
                page.mouse.click(cx, cy)
                time.sleep(3)
                mf2 = get_mf(page)
                shot(page, "03_after_dm_click")
                found = mf2.evaluate("""() => ({
                    hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
                    hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
                    hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
                    hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
                })""")
                print(f"  결과: {found}")
                if found.get('hasSalesDate') or found.get('hasGrid') or found.get('hasYesterday'):
                    print("  ★★★ 당일매출내역 화면 로드 성공!")
                    clicked = True
                    break
        if clicked: break

    if not clicked:
        print("  모든 셀렉터 실패 — 전체 visible 요소 재스캔")
        mf = get_mf(page)
        all_vis = mf.evaluate("""() => {
            let out = [];
            for(let el of document.querySelectorAll('[id*="divLeft"], [id*="grdLeft"]')) {
                let box = el.getBoundingClientRect();
                if(box.width < 1 || box.height < 1) continue;
                let txt = (el.innerText||el.textContent||'').trim().slice(0,25);
                if(!txt) continue;
                out.push({id: el.id.slice(0,60), txt, x:Math.round(box.x), y:Math.round(box.y), w:Math.round(box.width), h:Math.round(box.height)});
            }
            return out.slice(0,30);
        }""")
        print(f"  divLeft/grdLeft visible 요소 {len(all_vis)}개:")
        for r in all_vis:
            print(f"    ({r['x']:>4},{r['y']:>4}) {r['w']:>4}x{r['h']:>4}  txt={r['txt']:<25}  id={r['id']}")

    browser.close()
    print("\n=== 2차 테스트 완료 ===")
