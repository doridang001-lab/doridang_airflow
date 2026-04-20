"""
EasyPOS 17차: grdLeft 데이터셋 + gfnGetMenuUrl 호출 + set_formurl 네비게이션
"""
import time, os, json
from playwright.sync_api import sync_playwright

LOGIN_URL  = "https://smart.easypos.net/index.jsp"
EASYPOS_ID = "3322002029"
EASYPOS_PW = "1"
OUT_DIR    = "/opt/airflow/Local_DB/temp/easypos_debug"
os.makedirs(OUT_DIR, exist_ok=True)

all_reqs = []

def get_mf(page):
    for f in page.frames:
        if f.name == "main": return f
    return None

def wait_sales(page, sec=25):
    for i in range(1, sec+1):
        time.sleep(1)
        mf = get_mf(page)
        r = mf.evaluate("""() => ({
            hasSalesDate: !!document.querySelector('[id*="divSalesDate"]'),
            hasGrid: !!document.querySelector('[id*="grdSalePerDayList"]'),
            hasYesterday: !!document.querySelector('[id*="btnBeforeDay"]'),
        })""")
        if any(r.values()):
            print(f"  ★★★ [{i:2d}s] 당일매출내역 로드 성공! {r}")
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

    mf = get_mf(page)

    # --- 1. grdLeft 데이터셋 탐색 ---
    grd_ds = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            var grd = form.divLeftMenu && form.divLeftMenu.divLeftMainList && form.divLeftMenu.divLeftMainList.grdLeft;
            if (!grd) return {err: 'grdLeft not found'};

            // dataset 접근 시도
            var ds = null;
            if (grd._datasets && grd._datasets.length > 0) ds = grd._datasets[0];
            else if (grd.datasets && grd.datasets.length > 0) ds = grd.datasets[0];
            else if (typeof grd.getBindDataset === 'function') ds = grd.getBindDataset();

            if (!ds) {
                // form에서 데이터셋 찾기 - grdLeft가 bound된 데이터셋
                var allDsKeys = Object.keys(form).filter(k => {
                    var v = form[k];
                    return v && typeof v === 'object' && v.rowcount !== undefined;
                });
                return {grdFound: true, dsNull: true, formDsKeys: allDsKeys};
            }

            var colCount = ds.colcount || 0;
            var cols = [];
            for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));

            var rowCount = ds.rowcount || 0;
            var matches = [];
            for (var i = 0; i < rowCount; i++) {
                var nm = ds.getColumn(i, 'MENU_NAME') || ds.getColumn(i, 'MENU_NM') || '';
                var url = ds.getColumn(i, 'SMART_MENU_URL') || ds.getColumn(i, 'PROGRAM_PATH') || '';
                matches.push({i: i, nm: nm, url: url});
            }
            return {cols: cols, rowCount: rowCount, rows: matches};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print("=== grdLeft 데이터셋 ===")

    # form의 모든 dataset 목록 및 당일매출내역 검색
    if 'formDsKeys' in grd_ds:
        print(f"  form의 모든 dataset: {grd_ds['formDsKeys']}")

        # 모든 dataset에서 당일매출내역 검색
        all_ds_search = mf.evaluate("""(dsKeys) => {
            try {
                var form = window.application.mainframe.childframe.form;
                var results = {};
                for (var k of dsKeys) {
                    var ds = form[k];
                    if (!ds || !ds.rowcount) continue;
                    var rowCount = ds.rowcount || 0;
                    var colCount = ds.colcount || 0;
                    var cols = [];
                    for (var j = 0; j < colCount; j++) cols.push(ds.getColID(j));

                    var found = [];
                    for (var i = 0; i < rowCount; i++) {
                        var row = {};
                        for (var c of cols) row[c] = ds.getColumn(i, c);
                        var vals = JSON.stringify(row);
                        if (vals.includes('당일') || vals.includes('일자별') || vals.includes('DAY_SALE')) {
                            found.push(row);
                        }
                    }
                    if (found.length > 0) results[k] = found;
                    else results[k] = {rows: rowCount, cols: cols};
                }
                return results;
            } catch(e) { return {err: e.toString()}; }
        }""", grd_ds['formDsKeys'])
        print("\n=== 모든 Dataset 당일매출내역 검색 결과 ===")
        print(json.dumps(all_ds_search, indent=2, ensure_ascii=False))

    elif 'rows' in grd_ds:
        print(f"  열: {grd_ds['cols']}")
        print(f"  행 수: {grd_ds['rowCount']}")
        target_url = None
        for row in grd_ds['rows']:
            nm = str(row.get('nm', ''))
            url = str(row.get('url', ''))
            if '당일' in nm or '일자별' in nm:
                print(f"  ★ {row}")
                target_url = url
        if not target_url:
            print("  당일매출내역 없음 — 첫 10행:")
            for row in grd_ds['rows'][:10]: print(f"  {row}")
    else:
        print(json.dumps(grd_ds, indent=2, ensure_ascii=False))

    # --- 2. gfnGetMenuUrl 호출로 모든 메뉴 URL 추출 ---
    print("\n=== gfnGetMenuUrl 호출 ===")
    menu_urls = mf.evaluate("""() => {
        try {
            var form = window.application.mainframe.childframe.form;
            if (typeof form.gfnGetMenuUrl !== 'function') return {err: 'gfnGetMenuUrl not function'};

            var results = [];
            // grdLeft 데이터 행 수 확인
            var grd = form.divLeftMenu && form.divLeftMenu.divLeftMainList && form.divLeftMenu.divLeftMainList.grdLeft;
            var rowCount = grd && grd._datasets && grd._datasets[0] && (grd._datasets[0].rowcount || 0);

            // 0~100 범위 시도
            for (var i = 0; i < (rowCount || 100); i++) {
                try {
                    var nm = form.gfnGetMenuName(i);
                    var url = form.gfnGetMenuUrl(i);
                    var id = form.gfnGetMenuId(i);
                    if (nm || url) results.push({i: i, nm: nm, url: url, id: id});
                    if (nm && (nm.includes('당일') || nm.includes('일자별'))) {
                        results[results.length-1]._TARGET = true;
                    }
                } catch(e) { break; }
            }
            return {count: results.length, rows: results};
        } catch(e) { return {err: e.toString()}; }
    }""")
    print(json.dumps(menu_urls, indent=2, ensure_ascii=False))

    # --- 3. 찾은 URL로 set_formurl 호출 ---
    target_url = None
    if isinstance(menu_urls, dict) and 'rows' in menu_urls:
        for row in menu_urls.get('rows', []):
            if row.get('_TARGET') or '당일' in str(row.get('nm','')):
                target_url = row.get('url')
                print(f"\n★ 타겟 URL 발견: {target_url} (메뉴: {row.get('nm')})")
                break

    if target_url:
        print(f"\n=== set_formurl('{target_url}') 직접 호출 ===")
        all_reqs.clear()
        result = mf.evaluate("""(url) => {
            try {
                window.application.mainframe.childframe.set_formurl(url);
                return {ok: true};
            } catch(e) { return {err: e.toString()}; }
        }""", target_url)
        print(f"  결과: {result}")
        if wait_sales(page, 25):
            page.screenshot(path=f"{OUT_DIR}/dom17_success.png")
        else:
            xfdl = [r for r in all_reqs if '.xfdl' in r]
            print(f"  XFDL 요청: {xfdl}")
            page.screenshot(path=f"{OUT_DIR}/dom17_fail.png")
    else:
        print("\n[WARN] target_url 미발견")

    browser.close()
print("\n=== 17차 테스트 완료 ===")
