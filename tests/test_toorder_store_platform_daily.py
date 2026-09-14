from pathlib import Path

import openpyxl
import pandas as pd

from modules.transform.pipelines.sales import DB_Toorder_store_platform_daily as pipeline


DATEDETAIL_PREFIX = "종합보고서_일별상세_매출보고서"


def _make_datedetail_xlsx(
    path: Path,
    *,
    sheet_date: str = "2026-05-11",
    store: str = "Store A",
    platform: str = "배민",
    price: int = 100,
    receipts: int = 1,
) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_date
    ws.cell(3, 6).value = store
    ws.cell(7, 1).value = platform
    ws.cell(7, 6).value = price
    ws.cell(7, 9).value = receipts
    ws.cell(8, 1).value = "합계"
    wb.save(path)


def test_find_pending_datedetail_file_matches_month_and_ignores_archived(tmp_path):
    keep = tmp_path / "ignore.xlsx"
    raw = tmp_path / f"{DATEDETAIL_PREFIX}_2026-05_raw.xlsx"
    bak = tmp_path / f"{DATEDETAIL_PREFIX}_2026-05_bak_010101.xlsx"
    temp = tmp_path / f"~${DATEDETAIL_PREFIX}_2026-05.xlsx"
    target = tmp_path / f"{DATEDETAIL_PREFIX}_2026-05.xlsx"

    keep.write_text("x", encoding="utf-8")
    _make_datedetail_xlsx(raw)
    _make_datedetail_xlsx(bak)
    _make_datedetail_xlsx(temp)
    _make_datedetail_xlsx(target)

    assert pipeline._find_pending_datedetail_file(tmp_path, "2026-05") == target
    assert pipeline._find_pending_datedetail_file(tmp_path, "2026-06") is None


def test_upsert_parquet_keeps_last_duplicate_key(tmp_path):
    parquet_path = tmp_path / "toorder_store_platform_daily.parquet"
    old_df = pd.DataFrame(
        [
            {"date": "2026-05-11", "store": "Store A", "platform": "배민", "price": 100, "receipts_num": 1},
            {"date": "2026-05-11", "store": "Store B", "platform": "배민", "price": 50, "receipts_num": 1},
        ]
    )
    pipeline._upsert_parquet(old_df, parquet_path)

    new_df = pd.DataFrame(
        [
            {"date": "2026-05-11", "store": "Store A", "platform": "배민", "price": 200, "receipts_num": 2},
            {"date": "2026-05-12", "store": "Store A", "platform": "쿠팡이츠", "price": 300, "receipts_num": 3},
        ]
    )
    pipeline._upsert_parquet(new_df, parquet_path)

    result = pd.read_parquet(parquet_path).sort_values(["date", "store", "platform"]).reset_index(drop=True)

    assert len(result) == 3
    replaced = result[(result["date"] == "2026-05-11") & (result["store"] == "Store A") & (result["platform"] == "배민")]
    assert int(replaced.iloc[0]["price"]) == 200
    assert int(replaced.iloc[0]["receipts_num"]) == 2


def test_new_rows_quality_reason_flags_partial_day(tmp_path):
    parquet_path = tmp_path / "toorder_store_platform_daily.parquet"
    rows = []
    for day in range(17, 24):
        for store_idx in range(50):
            rows.append(
                {
                    "date": f"2026-08-{day:02d}",
                    "store": f"Store {store_idx:02d}",
                    "platform": "배민",
                    "price": 100_000,
                    "receipts_num": 1,
                }
            )
    pd.DataFrame(rows).to_parquet(parquet_path, index=False)

    partial = pd.DataFrame(
        [
            {
                "date": "2026-08-24",
                "store": f"Store {store_idx:02d}",
                "platform": "배민",
                "price": 10_000,
                "receipts_num": 1,
            }
            for store_idx in range(10)
        ]
    )

    reason = pipeline._new_rows_quality_reason(partial, parquet_path)

    assert "2026-08-24 stores 10/50 (20%)" in reason
    assert "rows 10/50 (20%)" in reason
    assert "total 100000/5000000 (2%)" in reason


def test_toorder_partial_dates_reports_bad_middle_date(tmp_path):
    parquet_path = tmp_path / "toorder_store_platform_daily.parquet"
    rows = []
    for day in range(17, 24):
        for store_idx in range(50):
            rows.append(
                {
                    "date": f"2026-08-{day:02d}",
                    "store": f"Store {store_idx:02d}",
                    "platform": "배민",
                    "price": 100_000,
                    "receipts_num": 1,
                }
            )
    for store_idx in range(10):
        rows.append(
            {
                "date": "2026-08-24",
                "store": f"Store {store_idx:02d}",
                "platform": "배민",
                "price": 10_000,
                "receipts_num": 1,
            }
        )
    for store_idx in range(50):
        rows.append(
            {
                "date": "2026-08-25",
                "store": f"Store {store_idx:02d}",
                "platform": "배민",
                "price": 100_000,
                "receipts_num": 1,
            }
        )
    pd.DataFrame(rows).to_parquet(parquet_path, index=False)

    result = pipeline.toorder_partial_dates(
        parquet_path,
        date_from="2026-08-23",
        date_to="2026-08-25",
    )

    assert sorted(result) == ["2026-08-24"]
    assert "stores" in result["2026-08-24"]


def test_run_toorder_store_platform_daily_uses_pending_datedetail_and_writes_parquet(tmp_path, monkeypatch):
    manual_dir = tmp_path / "manual"
    dest_dir = tmp_path / "dest"
    manual_dir.mkdir()

    pending_path = manual_dir / f"{DATEDETAIL_PREFIX}_2026-05.xlsx"
    _make_datedetail_xlsx(pending_path, store="Manual Store", price=111, receipts=11)

    def _unexpected_download(**_kwargs):
        raise AssertionError("pending datedetail workbook should avoid download")

    monkeypatch.setattr(pipeline, "run_crawling_datedetail_months", _unexpected_download)

    parquet_path = Path(
        pipeline.run_toorder_store_platform_daily(
            date_from="2026-05-11",
            date_to="2026-05-11",
            dest_dir=dest_dir,
            manual_dir=manual_dir,
        )
    )

    result = pd.read_parquet(parquet_path).sort_values(["store", "platform"]).reset_index(drop=True)

    assert parquet_path.name == "toorder_store_platform_daily.parquet"
    assert len(result) == 1
    assert result.iloc[0]["store"] == "Manual Store"
    assert int(result.iloc[0]["price"]) == 111
    assert not pending_path.exists()
    assert (manual_dir / f"{DATEDETAIL_PREFIX}_2026-05_raw.xlsx").exists()


def test_run_toorder_store_platform_daily_force_download_ignores_pending_workbook(tmp_path, monkeypatch):
    manual_dir = tmp_path / "manual"
    dest_dir = tmp_path / "dest"
    manual_dir.mkdir()

    pending_path = manual_dir / f"{DATEDETAIL_PREFIX}_2026-05.xlsx"
    downloaded_path = manual_dir / f"{DATEDETAIL_PREFIX}_2026-05_downloaded.xlsx"
    _make_datedetail_xlsx(pending_path, store="Stale Store", price=111, receipts=11)
    _make_datedetail_xlsx(downloaded_path, store="Fresh Store", price=222, receipts=22)

    def _download(**_kwargs):
        return [{"success": True, "file": str(downloaded_path), "month": "2026-05", "error": None}]

    monkeypatch.setattr(pipeline, "run_crawling_datedetail_months", _download)

    parquet_path = Path(
        pipeline.run_toorder_store_platform_daily(
            date_from="2026-05-11",
            date_to="2026-05-11",
            dest_dir=dest_dir,
            manual_dir=manual_dir,
            force_download=True,
        )
    )

    result = pd.read_parquet(parquet_path).sort_values(["store", "platform"]).reset_index(drop=True)

    assert len(result) == 1
    assert result.iloc[0]["store"] == "Fresh Store"
    assert int(result.iloc[0]["price"]) == 222
    assert pending_path.exists()
    assert not downloaded_path.exists()
    assert (manual_dir / f"{DATEDETAIL_PREFIX}_2026-05_downloaded_raw.xlsx").exists()


def test_downloaded_file_not_found_is_retriable():
    from modules.extract import crawling_toorder_sales_report as crawler

    assert crawler._is_retriable_driver_error("downloaded file not found")


def test_datedetail_month_retry_keeps_previous_success(tmp_path, monkeypatch):
    from modules.extract import crawling_toorder_sales_report as crawler

    class Driver:
        current_url = "https://ceo.toorder.co.kr/dashboard/sales-report/datedetail"

        def get(self, _url):
            self.current_url = "https://ceo.toorder.co.kr/dashboard/sales-report/datedetail"

        def quit(self):
            pass

    calls = []

    monkeypatch.setattr(crawler, "PIPELINE_RETRIES", 2)
    monkeypatch.setattr(crawler, "PIPELINE_RETRY_BASE_SEC", 0)
    monkeypatch.setattr(crawler, "_cleanup_expected_report_downloads", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(crawler, "_launch_report_browser", lambda *_args, **_kwargs: Driver())
    monkeypatch.setattr(crawler, "_do_login", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(crawler, "_open_datedetail_report_tab", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(crawler.time, "sleep", lambda *_args, **_kwargs: None)

    def _fake_download(_driver, _account, month_start, _month_end, _download_dir):
        month = month_start[:7]
        calls.append(month)
        if month == "2026-07":
            return {"success": True, "file": str(tmp_path / "july.xlsx"), "month": month, "error": None}
        if calls.count("2026-08") == 1:
            return {"success": False, "file": None, "month": month, "error": "downloaded file not found"}
        return {"success": True, "file": str(tmp_path / "august.xlsx"), "month": month, "error": None}

    monkeypatch.setattr(crawler, "_download_datedetail_month", _fake_download)

    result = crawler.run_crawling_datedetail_months(
        toorder_id="account",
        toorder_pw="pw",
        month_spans=[("2026-07-01", "2026-07-31"), ("2026-08-01", "2026-08-09")],
        download_dir=tmp_path,
    )

    assert [item["success"] for item in result] == [True, True]
    assert calls == ["2026-07", "2026-08", "2026-08"]


def test_cleanup_expected_report_downloads_preserves_success_file(tmp_path):
    from modules.extract import crawling_toorder_sales_report as crawler

    expected_stem = Path(crawler.ORIG_DATEDETAIL_FILENAME).stem
    preserved = tmp_path / f"{expected_stem}_2026-07.xlsx"
    stale = tmp_path / f"{expected_stem}.xlsx"
    crdownload = tmp_path / f"{expected_stem}.xlsx.crdownload"
    other = tmp_path / "other.xlsx"
    for path in (preserved, stale, crdownload, other):
        path.write_text("x", encoding="utf-8")

    crawler._cleanup_expected_report_downloads(
        tmp_path,
        expected_stem,
        preserve_paths=[preserved],
    )

    assert preserved.exists()
    assert not stale.exists()
    assert not crdownload.exists()
    assert other.exists()


def test_cleanup_datedetail_xlsx_deletes_only_datedetail_files(tmp_path):
    datedetail = tmp_path / f"{DATEDETAIL_PREFIX}_2026-05_raw.xlsx"
    datedetail_bak = tmp_path / f"{DATEDETAIL_PREFIX}_2026-05_raw_bak_010101.xlsx"
    other_toorder = tmp_path / "종합보고서_일별매출보고서_2026-05_raw.xlsx"
    keep = tmp_path / "ignore.xlsx"

    for path in (datedetail, datedetail_bak, other_toorder, keep):
        path.write_text("x", encoding="utf-8")

    result = pipeline.cleanup_datedetail_xlsx(tmp_path)

    assert result == "deleted=2"
    assert not datedetail.exists()
    assert not datedetail_bak.exists()
    assert other_toorder.exists()
    assert keep.exists()
