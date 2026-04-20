"""
EasyPOS 16차: dsLeftMenuParnas 전체 덤프 + 올바른 formurl 확인 + set_formurl 호출
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

all_reqs = []
all_console = []

def get_mf(page):
    for f in page.frames:
        if f.name == "main": return f
    return None

def wait_for_sales_page(page, seconds=20):
    for i in range(1, seconds+1):
        time.sleep(1)
        mf = get_mf(page)
        r = mf.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
            hasSearch: !!document.querySelector('[id*="btnCommSearch"]'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 당일매출내역 화면 로드 성공! {r}")
            return True
        if i % 5 == 0:
            print(f"  [{i:2d}s] 대기 중... {r}")
    return False

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
    page.on('console', lambda m: all_console.append(f"[{m.type}] {m.text[:200]}"))

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
    all_console.clear()

    mf = get_mf(page)

    # --- 전체 메뉴 목록 덤프 ---
    all_menus = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var results = {};

            // dsLeftMenuParnas 전체
            ['dsLeftMenuParnas', 'dsLeftMenuParnas00', 'dsLeftMenuSample', 'dsTopMenu'].forEach(function(dsName) {
                var ds = form[dsName];
                if (!ds) { results[dsName] = 'not found'; return; }
                var rowCount = ds.rowcount || 0;
                var colCount = ds.colcount || 0;
                var rows = [];
                for (var i = 0; i < rowCount; i++) {
                    var row = {};
                    for (var j = 0; j < colCount; j++) {
                        var col = ds.getColID(j);
                        row[col] = ds.getColumn(i, col);
                    }
                    rows.push(row);
                }
                results[dsName] = rows;
            });
            return results;
        } catch(e) { return {err: e.toString()}; }
    }""")

    # 저장
    with open(f"{OUT_DIR}/menu_datasets.json", 'w', encoding='utf-8') as f:
        json.dump(all_menus, f, ensure_ascii=False, indent=2)
    print(f"메뉴 데이터셋 저장: {OUT_DIR}/menu_datasets.json")

    # 당일매출내역 검색
    print("\n=== 당일매출내역 관련 메뉴 항목 ===")
    target_url = None
    for ds_name, rows in all_menus.items():
        if not isinstance(rows, list): continue
        for row in rows:
            menu_name = str(row.get('MENU_NAME', '') or row.get('MENU_NM', ''))
            if '당일' in menu_name or '일자별' in menu_name or '매출내역' in menu_name:
                print(f"  [{ds_name}] {row}")
                url = row.get('SMART_MENU_URL') or row.get('PROGRAM_PATH')
                if url:
                    target_url = url
                    print(f"    → TARGET URL: {url}")

    # 모든 데이터셋의 첫 5행 출력 (패턴 확인)
    print("\n=== 각 데이터셋 샘플 (첫 3행) ===")
    for ds_name, rows in all_menus.items():
        if not isinstance(rows, list): continue
        print(f"  {ds_name} ({len(rows)}행):")
        for row in rows[:3]:
            print(f"    {row}")

    # --- set_formurl 직접 호출 ---
    if target_url:
        print(f"\n=== set_formurl('{target_url}') 호출 ===")
        result = mf.evaluate("""(url) => {
            try {
                var cf = window.application.mainframe.childframe;
                cf.set_formurl(url);
                return {called: true, url: url};
            } catch(e) { return {err: e.toString()}; }
        }""", target_url)
        print(f"  결과: {result}")
        if wait_for_sales_page(page, 20):
            page.screenshot(path=f"{OUT_DIR}/dom16_success.png", full_page=True)
        else:
            page.screenshot(path=f"{OUT_DIR}/dom16_fail.png", full_page=True)
            xfdl_reqs = [r for r in all_reqs if '.xfdl' in r]
            print(f"  XFDL 요청: {xfdl_reqs}")
    else:
        print("\n[WARN] target_url을 찾지 못함 — 전체 데이터 확인 필요")
        # PROGRAM_PATH 패턴 확인
        print("\n=== 모든 PROGRAM_PATH 값 ===")
        for ds_name, rows in all_menus.items():
            if not isinstance(rows, list): continue
            for row in rows:
                path = row.get('PROGRAM_PATH') or row.get('SMART_MENU_URL')
                name = row.get('MENU_NAME', '')
                if path:
                    print(f"  [{ds_name}] {name!r}: {path}")

    browser.close()
print("\n=== 16차 테스트 완료 ===")
