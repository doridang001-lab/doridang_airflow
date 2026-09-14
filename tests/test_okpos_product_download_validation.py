from pathlib import Path
from zipfile import ZipFile

from openpyxl import Workbook
import pandas as pd
import pytest
from selenium.common.exceptions import WebDriverException

from modules.transform.pipelines.db import DB_FinProduct, DB_OKPOS_Card_Test, DB_OKPOS_Product, DB_OKPOS_Sales


class _FakeTI:
    def __init__(self, pulls=None):
        self._pulls = pulls or {}
        self.pushed = {}

    def xcom_pull(self, task_ids=None, key=None):
        return self._pulls.get((task_ids, key))

    def xcom_push(self, key=None, value=None):
        self.pushed[key] = value


def _make_minimal_xlsx(path: Path) -> None:
    with ZipFile(path, "w") as zf:
        zf.writestr("[Content_Types].xml", "<Types></Types>")


def _make_product_xlsx(path: Path, rows=None) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append([
        "대메뉴",
        "중메뉴",
        "No.",
        "상품코드",
        "상품명",
        "바코드",
        "상품군",
        "원가단가",
        "공급단가",
        "판매단가",
        "원산지",
        "주문단위",
    ])
    for row in rows or []:
        worksheet.append(row)
    workbook.save(path)


def test_wait_for_download_ignores_html(monkeypatch, tmp_path):
    monkeypatch.setenv("OKPOS_DOWNLOAD_STABLE_WINDOW_SEC", "0.1")
    monkeypatch.setenv("OKPOS_DOWNLOAD_NO_ACTIVITY_SEC", "0.1")

    html = tmp_path / "downloads.html"
    html.write_text("<html></html>", encoding="utf-8")

    result = DB_OKPOS_Sales._wait_for_download(
        tmp_path,
        existing_files=set(),
        timeout=0.3,
        expected_suffixes={".xlsx"},
        filename_predicate=lambda path: "product" in path.stem,
    )
    assert result is None


def test_wait_for_download_accepts_matching_xlsx(monkeypatch, tmp_path):
    monkeypatch.setenv("OKPOS_DOWNLOAD_STABLE_WINDOW_SEC", "0.1")
    monkeypatch.setenv("OKPOS_DOWNLOAD_NO_ACTIVITY_SEC", "0.1")

    xlsx = tmp_path / "product.xlsx"
    _make_minimal_xlsx(xlsx)

    result = DB_OKPOS_Sales._wait_for_download(
        tmp_path,
        existing_files=set(),
        timeout=1,
        expected_suffixes={".xlsx"},
        filename_predicate=lambda path: "product" in path.stem,
    )
    assert result == xlsx


def test_save_okpos_product_rejects_invalid_workbook(monkeypatch, tmp_path):
    src = tmp_path / "product.xlsx"
    src.write_text("<html>not xlsx</html>", encoding="utf-8")

    monkeypatch.setattr(DB_OKPOS_Product, "ANALYTICS_DB", tmp_path / "analytics")
    monkeypatch.setattr(DB_OKPOS_Product, "OKPOS_PRODUCT_FILE_NAME", "product.xlsx")
    ti = _FakeTI({("download_okpos_product", "downloaded_path"): str(src)})

    with pytest.raises(ValueError, match="not a real Excel workbook|corrupted"):
        DB_OKPOS_Product.save_okpos_product(ti=ti)

    assert not (tmp_path / "analytics" / "okpos_product" / "product.xlsx").exists()


def test_save_okpos_product_keeps_existing_file_on_validation_failure(monkeypatch, tmp_path):
    analytics_root = tmp_path / "analytics"
    dest_dir = analytics_root / "okpos_product"
    dest_dir.mkdir(parents=True)
    existing_dest = dest_dir / "product.xlsx"
    _make_minimal_xlsx(existing_dest)
    original_bytes = existing_dest.read_bytes()

    src = tmp_path / "product.xlsx"
    src.write_text("bad workbook", encoding="utf-8")

    monkeypatch.setattr(DB_OKPOS_Product, "ANALYTICS_DB", analytics_root)
    monkeypatch.setattr(DB_OKPOS_Product, "OKPOS_PRODUCT_FILE_NAME", "product.xlsx")
    ti = _FakeTI({("download_okpos_product", "downloaded_path"): str(src)})

    with pytest.raises(ValueError):
        DB_OKPOS_Product.save_okpos_product(ti=ti)

    assert existing_dest.read_bytes() == original_bytes


def test_save_okpos_product_rejects_header_only_workbook(monkeypatch, tmp_path):
    analytics_root = tmp_path / "analytics"
    dest_dir = analytics_root / "okpos_product"
    dest_dir.mkdir(parents=True)
    existing_dest = dest_dir / "product.xlsx"
    _make_product_xlsx(existing_dest, rows=[["도리당", "치킨", "1", "P001", "기존상품"]])
    original_bytes = existing_dest.read_bytes()

    src = tmp_path / "product.xlsx"
    _make_product_xlsx(src)

    monkeypatch.setattr(DB_OKPOS_Product, "ANALYTICS_DB", analytics_root)
    monkeypatch.setattr(DB_OKPOS_Product, "OKPOS_PRODUCT_FILE_NAME", "product.xlsx")
    ti = _FakeTI({("download_okpos_product", "downloaded_path"): str(src)})

    with pytest.raises(ValueError, match="no product rows"):
        DB_OKPOS_Product.save_okpos_product(ti=ti)

    assert existing_dest.read_bytes() == original_bytes


def test_save_okpos_product_accepts_valid_workbook(monkeypatch, tmp_path):
    src = tmp_path / "product.xlsx"
    _make_product_xlsx(src, rows=[["도리당", "치킨", "1", "P001", "테스트상품"]])

    monkeypatch.setattr(DB_OKPOS_Product, "ANALYTICS_DB", tmp_path / "analytics")
    monkeypatch.setattr(DB_OKPOS_Product, "OKPOS_PRODUCT_FILE_NAME", "product.xlsx")
    ti = _FakeTI({("download_okpos_product", "downloaded_path"): str(src)})

    result = DB_OKPOS_Product.save_okpos_product(ti=ti)

    saved = tmp_path / "analytics" / "okpos_product" / "product.xlsx"
    assert saved.exists()
    assert ti.pushed["saved_path"] == str(saved)
    assert "saved:" in result


def test_okpos_auth_failure_detects_lockout_message():
    text = "로그인 시도를 초과했어요\n로그인 시도를 초과하여 10분간 계정이 비활성화됩니다."

    assert DB_OKPOS_Sales._is_okpos_auth_failure_text(text)


def test_okpos_auth_failure_ignores_plain_login_form():
    text = "로그인\n시작하려면 계정 정보를 입력해주세요.\n아이디\n비밀번호\n로그인"

    assert not DB_OKPOS_Sales._is_okpos_auth_failure_text(text)


def test_okpos_logged_in_page_accepts_main_dashboard_without_login_inputs():
    class FakeDriver:
        current_url = "https://my.okpos.co.kr/asp/main"

        def execute_script(self, script):
            if "document.body" in script:
                return "기초관리 매출관리 회원관리 오케이포스 가이드 매출 현황"
            return {
                "title": "OKPOS 영업관리 시스템",
                "inputs": [{"id": "supersetUrl", "name": "supersetUrl", "type": "hidden"}],
            }

    assert DB_OKPOS_Sales._is_okpos_logged_in_page(FakeDriver())


def test_okpos_card_download_retries_dead_browser_session(monkeypatch, tmp_path):
    class FakeDriver:
        def __init__(self):
            self.quit_called = False

        def quit(self):
            self.quit_called = True

    class FakeTI:
        def __init__(self):
            self.pushed = {}

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    drivers = []
    login_calls = {"count": 0}

    def fake_resolve(**context):
        context["ti"].xcom_push(key="sale_date", value="2026-08-02")
        return "2026-08-02"

    def fake_launch(download_dir):
        driver = FakeDriver()
        drivers.append(driver)
        return driver

    def fake_login(_driver, _wait):
        login_calls["count"] += 1
        if login_calls["count"] == 1:
            raise WebDriverException("invalid session id")

    def fake_download(_driver, _wait, download_dir, sale_date):
        path = download_dir / f"매장현황_{sale_date}.xlsx"
        path.write_bytes(b"xlsx")
        return path

    monkeypatch.setenv("OKPOS_CARD_TASK_RETRY_MAX", "2")
    monkeypatch.setenv("OKPOS_CARD_TASK_RETRY_WAIT", "0")
    monkeypatch.setattr(DB_OKPOS_Card_Test, "OKPOS_CARD_DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_resolve_sale_date", fake_resolve)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_launch_browser", fake_launch)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_setup_download_dir", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_login", fake_login)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_download_one_date", fake_download)
    monkeypatch.setattr(DB_OKPOS_Card_Test, "_missing_lookback_dates", lambda _sale_date: [])

    ti = FakeTI()
    result = DB_OKPOS_Card_Test.download_okpos_card_test(ti=ti)

    assert login_calls["count"] == 2
    assert len(drivers) == 2
    assert all(driver.quit_called for driver in drivers)
    assert "downloaded primary=2026-08-02" in result
    assert ti.pushed["downloaded_path"].endswith("primary_2026-08-02_매장현황_2026-08-02.xlsx")


def test_find_zero_daily_sales_detail_suspects_with_receipt(monkeypatch, tmp_path):
    store = {"name": "도리당 테스트점", "shopCd": "TEST"}
    monkeypatch.setattr(DB_OKPOS_Sales, "RAW_OKPOS_SALES", tmp_path / "okpos_sales_raw")
    monkeypatch.setattr(DB_OKPOS_Sales, "STORES", [store])
    monkeypatch.setattr(DB_OKPOS_Sales, "_should_collect", lambda store_name, sale_date: True)

    base = tmp_path / "okpos_sales_raw" / "brand=도리당" / "store=테스트점" / "ym=2026-06"
    base.mkdir(parents=True)
    pd.DataFrame(
        [{"sale_date": "2026-06-29", "실매출액": "0", "매장명": "도리당 테스트점"}]
    ).to_csv(base / "okpos_daily.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [{"sale_date": "2026-06-29", "실매출액": "12000", "영수증번호": "1"}]
    ).to_csv(base / "okpos_order_item.csv", index=False, encoding="utf-8-sig")

    suspects, details = DB_OKPOS_Sales._find_zero_daily_sales_detail_suspects(["2026-06-29"])

    assert suspects == [("2026-06-29", store)]
    assert "receipt=12000/1(ok)" in details[0]


def test_find_zero_daily_sales_detail_suspects_ignores_no_detail(monkeypatch, tmp_path):
    store = {"name": "도리당 테스트점", "shopCd": "TEST"}
    monkeypatch.setattr(DB_OKPOS_Sales, "RAW_OKPOS_SALES", tmp_path / "okpos_sales_raw")
    monkeypatch.setattr(DB_OKPOS_Sales, "STORES", [store])
    monkeypatch.setattr(DB_OKPOS_Sales, "_should_collect", lambda store_name, sale_date: True)

    base = tmp_path / "okpos_sales_raw" / "brand=도리당" / "store=테스트점" / "ym=2026-06"
    base.mkdir(parents=True)
    pd.DataFrame(
        [{"sale_date": "2026-06-29", "실매출액": "0", "매장명": "도리당 테스트점"}]
    ).to_csv(base / "okpos_daily.csv", index=False, encoding="utf-8-sig")

    suspects, details = DB_OKPOS_Sales._find_zero_daily_sales_detail_suspects(["2026-06-29"])

    assert suspects == []
    assert details == []


def test_check_and_fill_missing_today_skips_both_no_data(monkeypatch, tmp_path):
    store = {"name": "도리당 테스트점", "shopCd": "TEST"}
    monkeypatch.setattr(DB_OKPOS_Sales, "RAW_OKPOS_SALES", tmp_path / "okpos_sales_raw")
    monkeypatch.setattr(DB_OKPOS_Sales, "STORES", [store])
    monkeypatch.setattr(DB_OKPOS_Sales, "_should_collect", lambda store_name, sale_date: True)

    base = tmp_path / "okpos_sales_raw" / "brand=도리당" / "store=테스트점" / "ym=2026-03"
    base.mkdir(parents=True)
    (base / ".no_data__okpos_order.txt").write_text(
        "2026-03-03\ttoday:empty_after_transform\t2026-06-18 16:31:08\n",
        encoding="utf-8",
    )
    (base / ".no_data__okpos_order_item.txt").write_text(
        "2026-03-03\treceipt:empty_after_transform\t2026-06-18 16:31:09\n",
        encoding="utf-8",
    )

    def _unexpected_download(*args, **kwargs):
        raise AssertionError("NO_DATA 양쪽 마커는 재다운로드하면 안 됩니다")

    monkeypatch.setattr(DB_OKPOS_Sales, "_download_with_retry", _unexpected_download)

    result = DB_OKPOS_Sales.check_and_fill_missing_today(
        ti=_FakeTI({("resolve_dates", "sale_dates"): ["2026-03-03"]})
    )

    assert result == "누락 항목 없음"


def test_check_and_fill_missing_today_redownloads_one_side_no_data(monkeypatch, tmp_path):
    store = {"name": "도리당 테스트점", "shopCd": "TEST"}
    monkeypatch.setattr(DB_OKPOS_Sales, "RAW_OKPOS_SALES", tmp_path / "okpos_sales_raw")
    monkeypatch.setattr(DB_OKPOS_Sales, "STORES", [store])
    monkeypatch.setattr(DB_OKPOS_Sales, "_should_collect", lambda store_name, sale_date: True)

    base = tmp_path / "okpos_sales_raw" / "brand=도리당" / "store=테스트점" / "ym=2026-03"
    base.mkdir(parents=True)
    pd.DataFrame(
        [{"sale_date": "2026-03-03", "실매출액": "12000", "영수번호": "1"}]
    ).to_csv(base / "okpos_order.csv", index=False, encoding="utf-8-sig")
    (base / ".no_data__okpos_order_item.txt").write_text(
        "2026-03-03\treceipt:empty_after_transform\t2026-06-18 16:31:09\n",
        encoding="utf-8",
    )

    calls = []

    def _capture_download(page_type, pending_keys, download_dir, session_fn, current_key=None):
        calls.append((page_type, pending_keys))
        return {}

    monkeypatch.setattr(DB_OKPOS_Sales, "_download_with_retry", _capture_download)

    result = DB_OKPOS_Sales.check_and_fill_missing_today(
        ti=_FakeTI({("resolve_dates", "sale_dates"): ["2026-03-03"]})
    )

    assert "receipt 성공=0" in result
    assert calls == [("receipt", [("2026-03-03", store)])]


def test_check_and_fill_missing_today_skips_old_both_missing(monkeypatch, tmp_path):
    store = {"name": "도리당 테스트점", "shopCd": "TEST"}
    monkeypatch.setattr(DB_OKPOS_Sales, "RAW_OKPOS_SALES", tmp_path / "okpos_sales_raw")
    monkeypatch.setattr(DB_OKPOS_Sales, "STORES", [store])
    monkeypatch.setattr(DB_OKPOS_Sales, "_should_collect", lambda store_name, sale_date: True)

    class _FixedNow:
        @staticmethod
        def date():
            from datetime import date

            return date(2026, 6, 30)

    monkeypatch.setattr(DB_OKPOS_Sales, "_kst_now", lambda: _FixedNow())
    monkeypatch.setenv("OKPOS_FILL_BOTH_MISSING_DAYS", "7")

    def _unexpected_download(*args, **kwargs):
        raise AssertionError("오래된 양쪽 MISSING은 재다운로드하면 안 됩니다")

    monkeypatch.setattr(DB_OKPOS_Sales, "_download_with_retry", _unexpected_download)

    result = DB_OKPOS_Sales.check_and_fill_missing_today(
        ti=_FakeTI({("resolve_dates", "sale_dates"): ["2026-03-03"]})
    )

    assert result == "누락 항목 없음"


def test_finproduct_read_xlsx_wraps_excel_load_error(monkeypatch):
    def _raise(*args, **kwargs):
        raise ValueError("bad zip")

    monkeypatch.setattr(DB_FinProduct.pd, "read_excel", _raise)

    with pytest.raises(ValueError, match="actual Excel file is missing, not a real workbook, or corrupted"):
        DB_FinProduct._read_xlsx()


def test_finproduct_split_brand_store_daemenu_extracts_store():
    assert DB_FinProduct._split_brand_store_daemenu("도리당 테스트점") == (
        "도리당",
        "도리당",
        "테스트점",
    )


def test_finproduct_split_brand_store_daemenu_keeps_non_store_daemenu():
    assert DB_FinProduct._split_brand_store_daemenu("도리당") == ("도리당", "", "")
