from pathlib import Path
from zipfile import ZipFile

import pytest

from modules.transform.pipelines.db import DB_FinProduct, DB_OKPOS_Product, DB_OKPOS_Sales


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


def test_save_okpos_product_accepts_valid_workbook(monkeypatch, tmp_path):
    src = tmp_path / "product.xlsx"
    _make_minimal_xlsx(src)

    monkeypatch.setattr(DB_OKPOS_Product, "ANALYTICS_DB", tmp_path / "analytics")
    monkeypatch.setattr(DB_OKPOS_Product, "OKPOS_PRODUCT_FILE_NAME", "product.xlsx")
    ti = _FakeTI({("download_okpos_product", "downloaded_path"): str(src)})

    result = DB_OKPOS_Product.save_okpos_product(ti=ti)

    saved = tmp_path / "analytics" / "okpos_product" / "product.xlsx"
    assert saved.exists()
    assert ti.pushed["saved_path"] == str(saved)
    assert "saved:" in result


def test_finproduct_read_xlsx_wraps_excel_load_error(monkeypatch):
    def _raise(*args, **kwargs):
        raise ValueError("bad zip")

    monkeypatch.setattr(DB_FinProduct.pd, "read_excel", _raise)

    with pytest.raises(ValueError, match="actual Excel file is missing, not a real workbook, or corrupted"):
        DB_FinProduct._read_xlsx()
