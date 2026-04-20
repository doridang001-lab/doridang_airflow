"""
EasyPOS: 전체 상단 탭 탐색 + grdSalePerDayList 위치 찾기
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

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

    # 상단 탭 목록 확인
    mf = get_mf(page)
    tabs = mf.evaluate("""() => {
        return Array.from(document.querySelectorAll('[id*="TA_top_menu"]'))
            .filter(el => /top_menu\\d+$/.test(el.id) && el.offsetWidth > 0)
            .map(el => ({
                id: el.id,
                text: (el.textContent||'').trim().slice(0,20),
                x: Math.round(el.getBoundingClientRect().x),
                y: Math.round(el.getBoundingClientRect().y),
                w: Math.round(el.getBoundingClientRect().width),
                h: Math.round(el.getBoundingClientRect().height),
            }));
    }""")
    print(f"=== 상단 탭 목록 ===")
    for t in tabs:
        print(f"  {t['id']}: '{t['text']}'")

    # 각 탭 클릭 후 grdLeft 메뉴 + grdSalePerDayList 탐색
    for tab in tabs:
        tab_id = tab['id']
        tab_text = tab['text']
        print(f"\n=== 탭 [{tab_text}] 클릭 ===")

        mf = get_mf(page)
        el = mf.query_selector(f"#{tab_id}")
        if not el:
            print("  요소 없음")
            continue
        box = el.bounding_box()
        if not box:
            print("  bounding box 없음")
            continue
        page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
        time.sleep(2)

        # 메뉴 목록
        mf = get_mf(page)
        menus = mf.evaluate("""() => {
            return Array.from(document.querySelectorAll('[id*="grdLeft_body_gridrow_"]'))
                .filter(el => /gridrow_\\d+$/.test(el.id))
                .map(el => (el.textContent||'').trim())
                .filter(t => t);
        }""")
        print(f"  메뉴: {menus[:15]}")

        # 각 서브메뉴 더블클릭
        menu_els = mf.query_selector_all('[id*="grdLeft_body_gridrow_"]')
        menu_els_data = []
        for el in menu_els:
            try:
                eid = el.get_attribute('id')
                if not eid or not eid.split('gridrow_')[-1].isdigit():
                    continue
                txt = (el.inner_text() or "").strip()
                box = el.bounding_box()
                if txt and box and box['w'] > 0:
                    menu_els_data.append({'el': el, 'txt': txt, 'box': box, 'idx': int(eid.split('gridrow_')[-1])})
            except:
                pass

        # 카테고리 항목 (indent=0)들 먼저 클릭해서 서브메뉴 확장
        for m in sorted(menu_els_data, key=lambda x: x['idx']):
            if m['box']['x'] > 30:  # 들여쓰기 있는 서브메뉴 스킵
                continue
            box = m['box']
            page.mouse.click(box["x"]+box["width"]/2, box["y"]+box["height"]/2)
            time.sleep(0.8)

            # 서브메뉴 더블클릭
            mf = get_mf(page)
            sub_els = mf.query_selector_all('[id*="grdLeft_body_gridrow_"]')
            for sub_el in sub_els:
                try:
                    sub_id = sub_el.get_attribute('id')
                    if not sub_id or not sub_id.split('gridrow_')[-1].isdigit():
                        continue
                    sub_txt = (sub_el.inner_text() or "").strip()
                    sub_box = sub_el.bounding_box()
                    if not sub_txt or not sub_box or sub_box['w'] <= 0:
                        continue
                    if sub_box['x'] <= 30:  # 카테고리는 스킵
                        continue
                    cx = sub_box["x"] + sub_box["width"] / 2
                    cy = sub_box["y"] + sub_box["height"] / 2
                    page.mouse.click(cx, cy); time.sleep(0.2)
                    page.mouse.click(cx, cy, click_count=2)
                    time.sleep(3)

                    mf2 = get_mf(page)
                    has_sale_per_day = mf2.evaluate("() => !!document.querySelector('[id*=\"grdSalePerDayList\"]')")
                    if has_sale_per_day:
                        print(f"  ★★★ [{sub_txt}] 에서 grdSalePerDayList 발견!")
                        ds = mf2.evaluate("""() => {
                            var rows = Array.from(document.querySelectorAll('[id*="grdSalePerDayList_body_gridrow_"]'))
                                .filter(el => /gridrow_\\d+$/.test(el.id));
                            return {rowCount: rows.length, firstText: rows[0] ? (rows[0].textContent||'').trim().slice(0,50) : null};
                        }""")
                        print(f"    rows: {ds}")
                except:
                    pass

    browser.close()
    print("\n=== 완료 ===")
