"""
EasyPOS DOM 진단 스크립트
- 로그인 후 대시보드 DOM 구조 파악
- 당일매출 타일/메뉴 클릭 후 실제로 어떤 JavaScript 이벤트가 발생하는지 추적
- NexacroN 내부 API 탐색
"""
import time, json, os
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"

os.makedirs(OUT_DIR, exist_ok=True)

def shot(page, name):
    p = f"{OUT_DIR}/test_{name}.png"
    page.screenshot(path=p, full_page=True)
    print(f"[SHOT] {p}")

def get_mf(page):
    for f in page.frames:
        if f.name == "main":
            return f
    return None

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

    # ── 로그인 ──────────────────────────────────────────────
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
    time.sleep(5)
    mf = get_mf(page)

    mf.query_selector("#mainframe_childframe_form_divMain_edtId").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtId_input").fill(EASYPOS_ID)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw").click(); time.sleep(0.2)
    mf.query_selector("#mainframe_childframe_form_divMain_edtPw_input").fill(EASYPOS_PW)
    mf.query_selector("#mainframe_childframe_form_divMain_btnLogin").click()
    time.sleep(5)

    # 팝업 닫기
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
    shot(page, "01_after_login")

    # ── NexacroN 전역 객체 탐색 ─────────────────────────────
    print("\n=== NexacroN global keys ===")
    nexacro_info = mf.evaluate("""() => {
        let keys = [];
        if(typeof nexacro !== 'undefined') {
            keys = Object.keys(nexacro).slice(0,30);
        }
        return {
            hasNexacro: typeof nexacro !== 'undefined',
            keys: keys,
            appType: typeof nexacro !== 'undefined' ? typeof nexacro.getApplication : 'N/A'
        };
    }""")
    print(json.dumps(nexacro_info, indent=2, ensure_ascii=False))

    # ── 당일매출 DOM 탐색 ─────────────────────────────────
    print("\n=== '당일매출' 텍스트 포함 요소 탐색 ===")
    tile_info = mf.evaluate("""() => {
        let results = [];
        let all = document.querySelectorAll('*');
        for(let el of all) {
            let txt = (el.innerText || el.textContent || '').trim();
            if(txt.includes('당일매출') && txt.length < 50) {
                let box = el.getBoundingClientRect();
                results.push({
                    tag: el.tagName,
                    id: el.id,
                    class: el.className,
                    text: txt.slice(0,30),
                    x: Math.round(box.x),
                    y: Math.round(box.y),
                    w: Math.round(box.width),
                    h: Math.round(box.height),
                    visible: box.width > 0 && box.height > 0
                });
            }
        }
        return results.slice(0, 20);
    }""")
    for r in tile_info:
        print(f"  {r['tag']:<8} id={r['id'][:40]:<40} txt={r['text']:<20} xy=({r['x']},{r['y']}) visible={r['visible']}")

    # ── 영업속보 클릭 후 grdLeft 탐색 ─────────────────────
    print("\n=== 영업속보 클릭 → grdLeft 행 탐색 ===")
    mf = get_mf(page)
    el = mf.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
    if el:
        el.click()
        time.sleep(1.5)
    mf = get_mf(page)
    shot(page, "02_after_sales_menu")

    grid_rows = mf.evaluate("""() => {
        let results = [];
        let rows = document.querySelectorAll('[id*="grdLeft_body_gridrow_"]');
        for(let r of rows) {
            let box = r.getBoundingClientRect();
            let txt = (r.innerText || r.textContent || '').trim().slice(0,30);
            results.push({id: r.id.slice(0,60), txt, x:Math.round(box.x), y:Math.round(box.y),
                          w:Math.round(box.width), h:Math.round(box.height)});
        }
        return results.slice(0,20);
    }""")
    print(f"  grdLeft 행 {len(grid_rows)}개")
    for r in grid_rows:
        print(f"  id={r['id']:<60} txt={r['txt']:<20} xy=({r['x']},{r['y']}) wh=({r['w']},{r['h']})")

    # ── 당일매출 타일 실제 클릭 시도 ─────────────────────
    print("\n=== 당일매출 타일 마우스 클릭 시도 ===")
    # 스크린샷에서 보이는 좌표(약 176, 207)로 직접 클릭
    for tx, ty in [(176, 207), (178, 208)]:
        page.mouse.click(tx, ty)
        time.sleep(2)
        mf2 = get_mf(page)
        shot(page, f"03_tile_click_{tx}_{ty}")
        # 당일매출내역 로드 여부 확인
        found = mf2.evaluate("""() => {
            return {
                hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
                hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
                hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
                hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
                bodyIds: Array.from(document.querySelectorAll('[id]')).map(e=>e.id).filter(id=>id.includes('divMain')).slice(0,10)
            };
        }""")
        print(f"  클릭({tx},{ty}) → {found}")
        if found.get('hasSalesDate') or found.get('hasGrid'):
            print("  ★ 당일매출내역 화면 로드 성공!")
            break

    # ── JavaScript 직접 NexacroN 함수 호출 시도 ─────────
    print("\n=== NexacroN 내부 함수 직접 호출 시도 ===")
    mf = get_mf(page)
    js_result = mf.evaluate("""() => {
        try {
            // NexacroN 앱 객체 접근
            let app = nexacro.getApplication ? nexacro.getApplication() : null;
            if(!app) return {err: 'no app'};

            // childframe form 접근
            let cf = app.getComponent ? app.getComponent('mainframe.childframe') : null;
            let form = cf ? (cf.form || cf.getComponent('form')) : null;

            return {
                appType: typeof app,
                appKeys: Object.keys(app||{}).slice(0,10),
                cfType: typeof cf,
                formType: typeof form,
            };
        } catch(e) {
            return {err: e.toString()};
        }
    }""")
    print(json.dumps(js_result, indent=2, ensure_ascii=False))

    # ── grdLeft 더블클릭 → 당일매출내역 직접 시도 ────────
    print("\n=== grdLeft 당일매출내역 행 더블클릭 ===")
    mf = get_mf(page)
    # 영업일보 펼치기
    day_menu = mf.query_selector(
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'영업일보')]"
    )
    if day_menu:
        box = day_menu.bounding_box()
        if box:
            cx = box["x"]+box["width"]/2; cy = box["y"]+box["height"]/2
            page.mouse.click(cx, cy)
            time.sleep(0.8)
            page.mouse.dblclick(cx, cy)
            time.sleep(1.5)
            print(f"  영업일보 더블클릭 ({cx:.0f},{cy:.0f})")

    mf = get_mf(page)
    target = mf.query_selector(
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일매출내역')]"
    )
    if target:
        box = target.bounding_box()
        if box:
            cx = box["x"]+box["width"]/2; cy = box["y"]+box["height"]/2
            page.mouse.click(cx, cy); time.sleep(0.5)
            page.mouse.dblclick(cx, cy); time.sleep(3)
            shot(page, "04_daily_dblclick")
            mf2 = get_mf(page)
            found = mf2.evaluate("""() => ({
                hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
                hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
                hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            })""")
            print(f"  당일매출내역 dblclick({cx:.0f},{cy:.0f}) → {found}")
    else:
        print("  당일매출내역 행 없음")

    browser.close()
    print("\n=== 테스트 완료 ===")
