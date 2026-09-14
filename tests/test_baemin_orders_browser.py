"""외부 접속 없이 Chromium DOM으로 확장 수집의 실패 경계를 검증한다."""
import os
from pathlib import Path
import pytest
from playwright.sync_api import sync_playwright


@pytest.fixture
def page():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chromium")
        page = browser.new_page()
        page.route("**/*", lambda route: route.abort())
        page.set_content('<meta charset="utf-8"><body></body>')
        page.evaluate("window.Sites={}; window.Utils={};")
        root = Path(__file__).resolve().parents[1]
        source = Path(os.environ.get("BAEMIN_TEST_JS", root / 'coupang_extension_build/content/02_baemin.js'))
        page.add_script_tag(content=source.read_text(encoding="utf-8"))
        page.evaluate("""() => {
          window.site=Sites.baemin;
          Utils.showProgressModal=Utils.updateProgressModal=Utils.showSuccessModal=()=>{};
          Utils.cleanPrice=s=>String(s).replace(/[^0-9]/g,'');
          Utils.getTodayStr=()=> '20260908';
          Utils.toCSV=(_headers,rows)=>JSON.stringify(rows);
          Utils.downloadCSV=async()=> 'pipeline_captured';
          site._ordersLog=()=>{};
          site._ordersDateRangeText=()=>'';
          site._randomDelay=async()=>{};
          site._getCurrentPage=()=>1;
          site._expandAllOrders=async()=>{};
          site._clickNextPage=()=>false;
          site._getFirstOrderId=()=>'';
        }""")
        yield page
        browser.close()


def test_first_page_error_is_not_no_data(page):
    result = page.evaluate("""async () => {
      site._collectCurrentPageData=async()=>{throw new Error('행 상세 미완료')};
      return site._collectOrdersAllPages({store_name:'테스트'});
    }""")
    assert result["success"] is False and result["partial"] is True
    assert result["error"] == "행 상세 미완료"


def test_page_failure_preserves_prior_pages_as_partial(page):
    result = page.evaluate("""async () => {
      let calls=0;
      site._collectCurrentPageData=async()=>{
        if(calls++) throw new Error('다음 페이지 실패');
        return {rows:[{'주문번호':'TEST','주문시각':'2026. 09. 08.'}],orderCount:1};
      };
      site._clickNextPage=()=>calls===1;
      site._waitForPageLoad=async()=>true;
      return site._collectOrdersAllPages({store_name:'테스트'});
    }""")
    assert result["rows"] == 1 and result["partial"] is True
    assert result["error"] == "다음 페이지 실패"


def test_zero_and_success_remain_distinct(page):
    result = page.evaluate("""async () => {
      site._collectCurrentPageData=async()=>({rows:[],orderCount:0});
      const empty=await site._collectOrdersAllPages({store_name:'테스트'});
      site._collectCurrentPageData=async()=>({rows:[{'주문번호':'TEST','주문시각':'2026. 09. 08.'}],orderCount:1});
      return {empty,full:await site._collectOrdersAllPages({store_name:'테스트'})};
    }""")
    assert result["empty"]["error"] == "수집된 주문 없음"
    assert result["full"]["success"] is True


def test_discount_re_resolves_detached_amount_after_scroll(page):
    page.set_content('''<table><tbody><tr class="Table_b_r4ax_1dwbr4on" data-index="0">
      <td data-td-index="1">TEST</td></tr><tr><td colspan="9">
      <div class="DetailInfo-module__pZYe"><div class="InstantDiscountDetailPageSheet-module__u8LB">
      <span class="InstantDiscountDetailPageSheet-module__siYJ">100원</span>
      </div></div></td></tr></tbody></table>''')
    result = page.evaluate("""async () => {
      site._resetDiscountStats();
      const original=document.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
      original.scrollIntoView=()=>original.replaceWith(original.cloneNode(true));
      site._findAllDiscountSheets=site._findAllDiscountSheetShells=()=>[];
      site._sheetSignature=()=>'';
      let clicked=false;
      site._activateInstantDiscountTarget=(el)=>{if(!el.isConnected) throw new Error('분리된 행'); clicked=true;};
      site._waitDiscountSheetForOrder=async()=>({sheet:document.body});
      site._parseDiscountSheet=()=>({partner:'60',support:'40'});
      site._closeDiscountSheet=async()=>{};
      const split=await site._readInstantDiscount(document.querySelector('.DetailInfo-module__pZYe'),'TEST');
      return {split,clicked,oldConnected:original.isConnected};
    }""")
    assert result["oldConnected"] is False and result["clicked"] is True
    assert result["split"]["파트너부담"] == "60"


def test_discount_wait_budget_does_not_multiply_by_targets(page):
    page.set_content('<div id="detail"><span id="amount">100원</span></div>')
    result = page.evaluate("""async () => {
      site._resetDiscountStats();
      site._findInstantDiscountAmountEl=()=>document.getElementById('amount');
      site._orderRows=()=>[];
      site._findAllDiscountSheets=site._findAllDiscountSheetShells=()=>[];
      site._sheetSignature=()=>'';
      site._findInstantDiscountClickTargets=()=>Array(30).fill(document.getElementById('amount'));
      site._activateInstantDiscountTarget=site._dispatchHoverExit=()=>{};
      site._sweepDiscountSheets=async()=>0;
      site._dumpDiscountSheet=()=>{};
      site._dumpDiscountRow=async()=>{};
      let elapsed=0;
      const oldNow=Date.now, oldTimer=window.setTimeout;
      Date.now=()=>elapsed;
      window.setTimeout=(cb,ms)=>{elapsed+=ms;cb();return 1;};
      site._waitDiscountSheetForOrder=async(_b,_p,ms)=>{elapsed+=ms;return {sheet:null,reason:'시트없음'};};
      try {
        await site._readInstantDiscount(document.getElementById('detail'),'TEST');
        return {elapsed,blanks:site._discountBlankCount};
      } finally { Date.now=oldNow;window.setTimeout=oldTimer; }
    }""")
    assert result["elapsed"] < 8500
    assert result["blanks"] == 1


@pytest.mark.parametrize("count,total", [(9, 238700), (8, 121800)])
def test_observed_order_table_shape_keeps_order_count_and_total(page, count, total):
    amounts = [10000] * (count - 1) + [total - 10000 * (count - 1)]
    page.set_content('<table><tbody>' + ''.join(
        f'<tr class="Table_b_r4ax_1dwbr4on" data-index="{i}">'
        f'<td data-td-index="1">TEST{i}</td><td data-td-index="2">2026. 09. 08.</td>'
        f'<td data-td-index="8">{amount}원</td></tr>' for i, amount in enumerate(amounts)
    ) + '</tbody></table>')
    result = page.evaluate("""async()=>{
      site._currentShopInfo={store_name:'테스트'};
      site._sweepDiscountSheets=async()=>0;
      return site._collectCurrentPageData('2026-09-09');
    }""")
    assert result["orderCount"] == count
    assert len({row["주문번호"] for row in result["rows"]}) == count
    assert sum(int(row["결제금액"]) for row in result["rows"]) == total


def test_async_collector_timeout_stops_and_blocks_overlapping_calls(page):
    import ast
    source = (Path(__file__).resolve().parents[1] / 'modules/transform/pipelines/db/DB_Beamin_04_orders.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    func = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_collect_all_pages_with_extension')
    call = next(node for node in ast.walk(func) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'execute_async_script')
    bridge = ast.literal_eval(call.args[0])
    result = page.evaluate("""async bridge => {
      window.Sites=Sites;
      window.Utils=Utils;
      let release;
      site._getShopInfo=()=>({store_name:'테스트'});
      site._collectOrdersAllPages=()=>new Promise(resolve=>release=resolve);
      const run=new Function(bridge);
      const invoke=()=>new Promise(resolve=>run('테스트','1',5,resolve));
      const timedOut=await invoke();
      const stopped=site._stopFlag;
      const overlapping=await invoke();
      release({success:false,error:'중단'});
      await new Promise(resolve=>setTimeout(resolve,1));
      return {timedOut,stopped,overlapping,active:site._airflowCollectorActive};
    }""", bridge)
    assert result["timedOut"]["timeout"] is True
    assert result["stopped"] is True
    assert "종료 대기" in result["overlapping"]["error"]
    assert result["active"] is False
