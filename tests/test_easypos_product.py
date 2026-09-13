"""EasyPOS 상품 파일 검증과 수집 실패 시 운영 파일 보호 회귀 검증."""
import ast
import logging
import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import urljoin, urlsplit

import pandas as pd
from playwright.sync_api import Frame, Page, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError


SOURCE = Path(__file__).resolve().parents[1] / "modules/transform/pipelines/db/DB_EasyPOS_Product.py"


def helpers():
    # Airflow/OneDrive 설정 로딩 없이 실제 운영 함수들을 독립 실행한다.
    ns = dict(Path=Path, pd=pd, os=os, shutil=shutil, time=time, datetime=datetime,
              logger=logging.getLogger(__name__), Frame=Frame, Page=Page,
              PlaywrightTimeoutError=PlaywrightTimeoutError, PlaywrightError=PlaywrightError,
              TargetClosedError=TargetClosedError, urljoin=urljoin, urlsplit=urlsplit)
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    sales = ast.parse(SOURCE.with_name('DB_EasyPOS_Sales.py').read_text(encoding='utf-8'))
    ns['_NEXACRO_INIT_SCRIPT'] = ast.literal_eval(next(
        n.value for n in sales.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == '_NEXACRO_INIT_SCRIPT' for t in n.targets)))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    ns[target.id] = value
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), ns)
    return ns


class ProductFileTests(unittest.TestCase):
    def setUp(self):
        self.ns = helpers()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.src = self.root / "download.xlsx"
        self.dest = self.root / "product.xlsx"
        self.ti = Mock()
        self.ti.xcom_pull.return_value = str(self.src)

    def write_product(self):
        pd.DataFrame({"상품코드": ["0012", "0013"], "상품명": ["상품 가", "상품 나"]}).to_excel(self.src, index=False)

    def test_valid_product(self):
        self.write_product()
        self.assertEqual(self.ns["_validate_product_xlsx"](self.src), 2)

    def test_corrupt_or_error_response_is_rejected(self):
        for content in [b"", b"<html>export failed</html>", b"PKbroken"]:
            with self.subTest(content=content):
                self.src.write_bytes(content)
                with self.assertRaises(RuntimeError):
                    self.ns["_validate_product_xlsx"](self.src)

    def test_wrong_screen_or_empty_products_are_rejected(self):
        for data in [{"매출일자": ["2026-09-08"], "총매출": [100]},
                     {"상품코드": [], "상품명": []},
                     {"상품코드": [None, " "], "상품명": ["가", "나"]}]:
            with self.subTest(data=data):
                pd.DataFrame(data).to_excel(self.src, index=False)
                with self.assertRaises(RuntimeError):
                    self.ns["_validate_product_xlsx"](self.src)

    def test_invalid_download_preserves_destination(self):
        self.dest.write_bytes(b"existing product")
        self.src.write_bytes(b"export error")
        with patch.dict(os.environ, EASYPOS_PRODUCT_XLSX_PATH=str(self.dest)):
            with self.assertRaises(RuntimeError):
                self.ns["save_easypos_product"](ti=self.ti, ds="2026-09-08")
        self.assertEqual(self.dest.read_bytes(), b"existing product")
        self.ti.xcom_push.assert_not_called()

    def test_save_replaces_local_file_after_validation(self):
        self.write_product()
        self.dest.write_bytes(b"old")
        with patch.dict(os.environ, EASYPOS_PRODUCT_XLSX_PATH=str(self.dest)):
            self.ns["save_easypos_product"](ti=self.ti, ds="2026-09-08")
        self.assertEqual(self.dest.read_bytes(), self.src.read_bytes())
        self.ti.xcom_push.assert_called_once_with(key="dest_xlsx", value=str(self.dest))


def export_body(url="https://smart.easypos.net/export/test/products.xlsx", eof="1", code="0"):
    return (f"SSV:UTF-8\x1eErrorCode:int={code}\x1eErrorMsg:string=SUCCESS\x1e"
            "Dataset:RESPONSE\x1e_RowType_\x1fcommand:string(32)\x1feof:int(2)\x1furl:string(1024)\x1e"
            f"N\x1fexport\x1f{eof}\x1f{url}\x1e")


class ProductDownloadTests(unittest.TestCase):
    def setUp(self):
        self.ns = helpers()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dest = Path(self.temp.name) / "product.xlsx"
        self.page = Mock()
        self.events = {}
        self.page.on.side_effect = self.events.__setitem__
        self.page.remove_listener.side_effect = lambda event, callback: self.events.pop(event)
        self.frame = Mock()
        self.ns['_wait_product_screen'] = Mock(return_value=self.frame)
        self.ns['time'] = Mock()
        self.ns['time'].monotonic.return_value = 0
        self.response = Mock(url="https://smart.easypos.net/XExportImport", ok=True, status=200)
        self.response.text.return_value = export_body()

    def emit_export(self, _):
        self.events['response'](self.response)

    def test_completed_export_url_and_partial_chunk(self):
        parse = self.ns['_export_url_from_response']
        self.assertEqual(parse(export_body(), self.response.url),
                         "https://smart.easypos.net/export/test/products.xlsx")
        self.assertIsNone(parse(export_body(eof="0"), self.response.url))

    def test_error_or_unexpected_export_location_rejected(self):
        for body in ['<html>login</html>', export_body(code="-1"),
                     export_body(url="https://other.example/export/file.xlsx"),
                     export_body(url="https://smart.easypos.net/login")]:
            with self.subTest(body=body):
                with self.assertRaises(RuntimeError):
                    self.ns['_export_url_from_response'](body, self.response.url)

    def test_native_download_is_saved_without_extra_request(self):
        download = Mock()
        self.frame.locator.return_value.click.side_effect = lambda **kw: self.events['download'](download)
        self.ns['_download_product_xlsx'](self.page, self.dest)
        download.save_as.assert_called_once_with(str(self.dest))
        self.page.context.request.get.assert_not_called()
        self.assertEqual(self.events, {})

    def test_server_export_recovers_missing_browser_event(self):
        self.page.wait_for_timeout.side_effect = self.emit_export
        response = self.page.context.request.get.return_value
        response.ok = True
        response.body.return_value = b"server workbook"
        self.ns['_download_product_xlsx'](self.page, self.dest)
        self.assertEqual(self.dest.read_bytes(), b"server workbook")
        self.page.context.request.get.assert_called_once_with(
            "https://smart.easypos.net/export/test/products.xlsx", timeout=60_000)
        response.dispose.assert_called_once()
        self.assertEqual(self.events, {})

    def test_export_http_error_and_timeout_remove_listeners(self):
        self.response.ok = False
        self.response.status = 503
        self.page.wait_for_timeout.side_effect = self.emit_export
        with self.assertRaisesRegex(RuntimeError, "503"):
            self.ns['_download_product_xlsx'](self.page, self.dest)
        self.assertEqual(self.events, {})
        self.assertFalse(self.dest.exists())
        self.ns['time'].monotonic.side_effect = [0, 121]
        with self.assertRaises(PlaywrightTimeoutError):
            self.ns['_download_product_xlsx'](self.page, self.dest)
        self.assertEqual(self.events, {})

    def test_download_http_error_does_not_create_file(self):
        self.page.wait_for_timeout.side_effect = self.emit_export
        response = self.page.context.request.get.return_value
        response.ok = False
        response.status = 404
        with self.assertRaisesRegex(RuntimeError, "404"):
            self.ns['_download_product_xlsx'](self.page, self.dest)
        self.assertFalse(self.dest.exists())
        response.dispose.assert_called_once()


class ProductRetryTests(unittest.TestCase):
    def test_browser_closed_restarts_and_only_valid_file_is_published(self):
        ns = helpers()
        with tempfile.TemporaryDirectory() as tmp:
            ns['TEMP_DIR'] = Path(tmp)
            ns['HEADLESS_MODE'] = True
            ns['time'] = Mock()
            ns['sync_playwright'] = MagicMock()
            first, second = Mock(), Mock()
            ns['launch_chromium'] = Mock(side_effect=[first, second])
            first.new_context.return_value.new_page.return_value.is_closed.return_value = True
            ns['_login'] = Mock(side_effect=[TargetClosedError('closed'), Mock()])
            ns['_navigate_to_product_search'] = Mock()
            ns['_search_product'] = Mock(return_value=2)
            ns['_download_product_xlsx'] = Mock()
            ns['_validate_product_xlsx'] = Mock(return_value=2)
            ti = Mock()
            ns['download_easypos_product'](ti=ti, ds='2026-09-08')
            self.assertEqual(ns['launch_chromium'].call_count, 2)
            first.close.assert_called_once()
            second.close.assert_called_once()
            ns['_validate_product_xlsx'].assert_called_once()
            ti.xcom_push.assert_called_once()

    def test_repeated_invalid_workbook_never_publishes_xcom(self):
        ns = helpers()
        with tempfile.TemporaryDirectory() as tmp:
            ns.update(TEMP_DIR=Path(tmp), HEADLESS_MODE=True, time=Mock(), sync_playwright=MagicMock())
            browser = Mock()
            browser.new_context.return_value.new_page.return_value.is_closed.return_value = True
            ns['launch_chromium'] = Mock(return_value=browser)
            for name in ['_login', '_navigate_to_product_search', '_search_product', '_download_product_xlsx']:
                ns[name] = Mock()
            ns['_validate_product_xlsx'] = Mock(side_effect=RuntimeError('invalid workbook'))
            ti = Mock()
            with self.assertRaisesRegex(RuntimeError, 'invalid workbook'):
                ns['download_easypos_product'](ti=ti, ds='2026-09-08')
            self.assertEqual(browser.close.call_count, 3)
            ti.xcom_push.assert_not_called()


@unittest.skipUnless(os.environ.get('EASYPOS_BROWSER_TESTS') == '1', '로컬 Chrome DOM 검증은 명시적으로 실행')
class ProductBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.chromium.launch(channel='chrome', headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.ns = helpers()
        self.page = self.browser.new_page()
        self.addCleanup(self.page.close)
        self.page.set_content('''
            <div id="mainframe_childframe_form_divMain_divWork_grdItem">상품 그리드</div>
            <button id="mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch">조회</button>
            <button id="mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommExcel">엑셀</button>
        ''')
        self.page.evaluate('''() => {
            const w = {url:'mst::MST004.xfdl', _is_loaded:true, _is_created:true, dsItem:{rowcount:55}, grdItem:{}, grdItemExcel:{},
                fnCallBack:function(){}, fnCommSearch_onclick:function(){}, fnCommExcel_onclick:function(){}};
            window.originalCallback = w.fnCallBack;
            window.application = {mainframe:{childframe:{form:{pvMenuNm:'상품조회',pvUrl:w.url,divMain:{_is_loaded:true,divWork:w}}}}};
        }''')
        frame = Mock()
        frame.wait_for_function.side_effect = lambda expression, **kw: self.page.wait_for_function(expression, timeout=1500)
        frame.locator.side_effect = self.page.locator
        frame.evaluate.side_effect = self.page.evaluate
        self.ns['_get_main_frame'] = lambda _: frame

    def test_common_buttons_do_not_make_wrong_screen_ready(self):
        self.page.evaluate("window.application.mainframe.childframe.form.pvMenuNm='상품등록'")
        with self.assertRaises(PlaywrightTimeoutError):
            self.ns['_wait_product_screen'](self.page)

    def test_shared_init_prevents_visibility_suspension(self):
        context = self.browser.new_context()
        self.addCleanup(context.close)
        self.ns['_add_nexacro_init_script'](context)
        page = context.new_page()
        page.goto('data:text/html,<html></html>')
        state = page.evaluate('''() => {
            let suspended=false;
            document.addEventListener('visibilitychange',()=>{suspended=true;});
            document.dispatchEvent(new Event('visibilitychange'));
            return {hidden:document.hidden,visibility:document.visibilityState,suspended};
        }''')
        self.assertEqual(state, {'hidden':False,'visibility':'visible','suspended':False})

    def test_empty_work_frame_is_not_ready(self):
        self.page.evaluate("delete window.application.mainframe.childframe.form.divMain.divWork.dsItem")
        with self.assertRaises(PlaywrightTimeoutError):
            self.ns['_wait_product_screen'](self.page)

    def test_created_components_do_not_skip_form_loading(self):
        self.page.evaluate("window.application.mainframe.childframe.form.divMain.divWork._is_loaded=false")
        with self.assertRaises(PlaywrightTimeoutError):
            self.ns['_wait_product_screen'](self.page)

    def test_query_waits_for_new_callback_instead_of_stale_rows(self):
        self.page.evaluate('''() => {
            const w=window.application.mainframe.childframe.form.divMain.divWork;
            document.querySelector('button').onclick=() => {
                setTimeout(() => {w.dsItem.rowcount=2;w.fnCallBack('svcItemSearch',0,'');},500);
            };
        }''')
        self.assertEqual(self.ns['_search_product'](self.page), 2)
        self.assertTrue(self.page.evaluate('window.application.mainframe.childframe.form.divMain.divWork.fnCallBack === window.originalCallback'))

    def test_empty_result_stops_before_export(self):
        self.page.evaluate('''() => {
            const w=window.application.mainframe.childframe.form.divMain.divWork;
            document.querySelector('button').onclick=() => {w.dsItem.rowcount=0;w.fnCallBack('svcItemSearch',0,'');};
        }''')
        with self.assertRaises(RuntimeError):
            self.ns['_search_product'](self.page)

    def test_navigation_waits_for_dashboard_preloads_before_closing_popups(self):
        self.page.evaluate('''() => {
            const a=window.application,m=a.mainframe.childframe.form.divMain;
            m._is_created=false;m._load_manager={preloadCnt:4};
            a.gdsLeftMenu={rowcount:1,getColumn:()=> '상품조회'};
            const b=document.createElement('button');
            b.id='mainframe_childframe_form_divTop_img_TA_top_menu1';document.body.appendChild(b);
            setTimeout(()=>{m._is_created=true;m._load_manager.preloadCnt=0;},500);
        }''')
        def close_popups(*args):
            self.assertEqual(self.page.evaluate('window.application.mainframe.childframe.form.divMain._load_manager.preloadCnt'), 0)
        self.ns['_close_blocking_popups'] = Mock(side_effect=close_popups)
        self.ns['_navigate_product_by_menu_url'] = Mock(return_value=True)
        self.ns['_navigate_to_product_search'](self.page, self.ns['_get_main_frame'](self.page))
        self.ns['_close_blocking_popups'].assert_called_once()


if __name__ == "__main__":
    unittest.main()
