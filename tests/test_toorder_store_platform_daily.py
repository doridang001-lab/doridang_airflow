from pathlib import Path

import openpyxl
import pandas as pd

from modules.transform.pipelines.sales import DB_Toorder_store_platform_daily as pipeline


MANUAL_PREFIX = "종합보고서_일별매출보고서"


def _make_manual_xlsx(path: Path, *, store: str, price: int, receipts: int) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.cell(2, 1).value = "2026-05-11"
    ws.cell(4, 1).value = "row"
    ws.cell(4, 2).value = store
    ws.cell(4, 6).value = price
    ws.cell(4, 42).value = receipts
    ws.cell(4, 48).value = 0
    wb.save(path)


def test_ingest_manual_files_reads_all_matching_reports_and_renames(tmp_path):
    keep = tmp_path / "ignore.xlsx"
    keep.write_text("x", encoding="utf-8")

    first = tmp_path / f"{MANUAL_PREFIX}.xlsx"
    second = tmp_path / f"{MANUAL_PREFIX} (1).xlsx"
    raw = tmp_path / f"{MANUAL_PREFIX}_raw.xlsx"
    bak = tmp_path / f"{MANUAL_PREFIX}_bak_010101.xlsx"
    temp = tmp_path / f"~${MANUAL_PREFIX}.xlsx"

    _make_manual_xlsx(first, store="Store A", price=100, receipts=1)
    _make_manual_xlsx(second, store="Store A", price=200, receipts=2)
    _make_manual_xlsx(raw, store="Store A", price=300, receipts=3)
    _make_manual_xlsx(bak, store="Store A", price=400, receipts=4)
    _make_manual_xlsx(temp, store="Store A", price=500, receipts=5)

    dfs = pipeline._ingest_manual_files(tmp_path, fallback_store_name="Fallback")
    merged = pd.concat(dfs, ignore_index=True)

    assert len(dfs) == 2
    assert sorted(merged["price"].tolist()) == [100, 200]
    assert not first.exists()
    assert not second.exists()
    assert (tmp_path / f"{MANUAL_PREFIX}_raw.xlsx").exists()
    assert (tmp_path / f"{MANUAL_PREFIX} (1)_raw.xlsx").exists()
    assert bak.exists()
    assert temp.exists()
    assert keep.exists()


def test_upsert_csv_keeps_last_duplicate_key(tmp_path):
    csv_path = tmp_path / "toorder_store_platform_daily.csv"
    old_df = pd.DataFrame(
        [
            {"date": "2026-05-11", "store": "Store A", "platform": "배민", "price": 100, "receipts_num": 1},
            {"date": "2026-05-11", "store": "Store B", "platform": "배민", "price": 50, "receipts_num": 1},
        ]
    )
    pipeline._upsert_csv(old_df, csv_path)

    new_df = pd.DataFrame(
        [
            {"date": "2026-05-11", "store": "Store A", "platform": "배민", "price": 200, "receipts_num": 2},
            {"date": "2026-05-12", "store": "Store A", "platform": "쿠팡이츠", "price": 300, "receipts_num": 3},
        ]
    )
    pipeline._upsert_csv(new_df, csv_path)

    result = pd.read_csv(csv_path, dtype=str).sort_values(["date", "store", "platform"]).reset_index(drop=True)

    assert len(result) == 3
    replaced = result[(result["date"] == "2026-05-11") & (result["store"] == "Store A") & (result["platform"] == "배민")]
    assert replaced.iloc[0]["price"] == "200"
    assert replaced.iloc[0]["receipts_num"] == "2"


def test_run_toorder_store_platform_daily_merges_manual_and_downloaded_rows(tmp_path, monkeypatch):
    manual_dir = tmp_path / "manual"
    dest_dir = tmp_path / "dest"
    manual_dir.mkdir()

    manual_path = manual_dir / f"{MANUAL_PREFIX}.xlsx"
    _make_manual_xlsx(manual_path, store="Manual Store", price=111, receipts=11)

    def _fake_run_crawling_single_date(**kwargs):
        downloaded_path = manual_dir / "종합보고서_일별매출보고서_260511.xlsx"
        _make_manual_xlsx(downloaded_path, store="API Store", price=222, receipts=22)
        return {
            "success": True,
            "file": str(downloaded_path),
            "date": kwargs["target_date"],
            "error": None,
        }

    monkeypatch.setattr(pipeline, "run_crawling_single_date", _fake_run_crawling_single_date)

    csv_path = pipeline.run_toorder_store_platform_daily(
        sale_dates=["2026-05-11"],
        dest_dir=dest_dir,
        manual_dir=manual_dir,
        store_name="API Store",
    )

    result = pd.read_csv(csv_path, dtype=str).sort_values(["store", "platform"]).reset_index(drop=True)

    assert len(result) == 2
    assert set(result["store"]) == {"API Store", "Manual Store"}
    assert (manual_dir / f"{MANUAL_PREFIX}_raw.xlsx").exists()
    assert (manual_dir / "종합보고서_일별매출보고서_260511_raw.xlsx").exists()
