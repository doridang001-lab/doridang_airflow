import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_Beamin_pc2_distribute as dist
from modules.transform.pipelines.db import beamin_staging as staging


def _failed(account_id: str) -> dict[str, list]:
    return {
        "accounts": [{"account_id": account_id}],
        "stores": [],
        "orders": [],
        "ads": [],
    }


def _meta(target_date: str, account_id: str) -> dict:
    return {
        "target_date": target_date,
        "account_list": [{"account_id": account_id}],
        "validation": [{"account_id": account_id, "ok": True}],
        "ad_stores": [{"account_id": account_id, "store_id": f"store-{account_id}"}],
        "store_info_per_account": [{"account_id": account_id, "stores": [f"store-{account_id}"]}],
        "original_failed": _failed(account_id),
        "failed": _failed(account_id),
    }


def _write_upload_folder(inbox: Path, folder_name: str, target_date: str, account_id: str) -> Path:
    folder = inbox / folder_name
    data_dir = folder / "baemin_macro" / "orders" / "brand=도리당" / "store=우가클송파삼전점" / "ym=2026-07"
    data_dir.mkdir(parents=True)
    (folder / "_meta.json").write_text(
        json.dumps(_meta(target_date, account_id), ensure_ascii=False),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "주문번호": f"order-{account_id}",
                "주문시각": f"{target_date.replace('-', '. ')}. 12:00",
                "결제금액": "10000",
            }
        ]
    ).to_csv(data_dir / "orders_2026-07.csv", index=False, encoding="utf-8-sig")
    return folder


def _write_metric_file(folder: Path) -> Path:
    data_dir = (
        folder
        / "baemin_macro"
        / "metrics_our_store_clicks"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "woori_shop_click.csv"
    pd.DataFrame(
        [
            {
                "날짜": "2026-07-16",
                "광고지출": "1000",
                "노출수": "10",
                "클릭수": "1",
            }
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _patch_table_io(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(dist, "ANALYTICS_DB", tmp_path / "analytics")

    def fake_read_table(stem_path: Path, columns: list[str] | None = None):
        parquet_path = stem_path.with_suffix(".parquet")
        if parquet_path.exists():
            return pd.read_parquet(parquet_path, columns=columns).fillna("").astype(str)
        csv_path = stem_path.with_suffix(".csv")
        if not csv_path.exists():
            return None
        return pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", usecols=columns)

    def fake_write_table(df: pd.DataFrame, stem_path: Path) -> Path:
        parquet_path = stem_path.with_suffix(".parquet")
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.fillna("").astype(str).to_parquet(parquet_path, index=False)
        csv_path = stem_path.with_suffix(".csv")
        if csv_path.exists():
            csv_path.unlink()
        return parquet_path

    monkeypatch.setattr(dist, "read_table", fake_read_table)
    monkeypatch.setattr(dist, "write_table", fake_write_table)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("상위", {"range": "상위", "slug": "top"}),
        ("TOP", {"range": "상위", "slug": "top"}),
        ("upper", {"range": "상위", "slug": "top"}),
        ("1", {"range": "상위", "slug": "top"}),
        ("하위", {"range": "하위", "slug": "bottom"}),
        ("BOTTOM", {"range": "하위", "slug": "bottom"}),
        ("lower", {"range": "하위", "slug": "bottom"}),
        ("2", {"range": "하위", "slug": "bottom"}),
        (None, {"range": None, "slug": ""}),
        ("unsupported", {"range": None, "slug": ""}),
    ],
)
def test_resolve_macro_role(raw, expected):
    assert staging.resolve_macro_role(raw) == expected


def test_role_prefixed_exports_are_ingested_together(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    run_id = "scheduled__2026-07-21T18:15:00+00:00"

    exported = []
    for role, account_id in (("top", "acct-top"), ("bottom", "acct-bottom")):
        local_baemin = tmp_path / f"stage-{role}" / "baemin_macro"
        source = (
            local_baemin
            / "orders"
            / "brand=도리당"
            / "store=우가클송파삼전점"
            / "ym=2026-07"
            / "orders_2026-07.csv"
        )
        source.parent.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "주문번호": f"order-{account_id}",
                    "주문시각": "2026. 07. 21. 12:00",
                    "결제금액": "10000",
                }
            ]
        ).to_csv(source, index=False, encoding="utf-8-sig")
        exported.append(
            staging.export_staging_to_inbox(
                local_baemin,
                run_id,
                inbox,
                meta=_meta("2026-07-21", account_id),
                folder_prefix=role,
            )
        )

    assert {path.name for path in exported} == {
        "manual__top__scheduled__2026-07-21T18_15_00_00_00",
        "manual__bottom__scheduled__2026-07-21T18_15_00_00_00",
    }

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert result["stats"]["cleaned"] == 2
    assert {item["account_id"] for item in result["meta"]["account_list"]} == {
        "acct-top",
        "acct-bottom",
    }
    orders = pd.read_parquet(
        tmp_path
        / "analytics"
        / "baemin_macro"
        / "orders"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
        / "orders_2026-07.parquet"
    )
    assert set(orders["주문번호"]) == {"order-acct-top", "order-acct-bottom"}


def test_export_without_role_keeps_legacy_folder_name(tmp_path):
    local_baemin = tmp_path / "stage" / "baemin_macro"
    source = local_baemin / "orders" / "sample.csv"
    source.parent.mkdir(parents=True)
    source.write_text("value\n1\n", encoding="utf-8")

    exported = staging.export_staging_to_inbox(
        local_baemin,
        "manual__legacy",
        tmp_path / "inbox",
    )

    assert exported.name == "manual__manual__legacy"


def test_upload_inbox_cleans_all_folders_but_merges_latest_meta(monkeypatch, tmp_path, caplog):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    older = _write_upload_folder(inbox, "manual__old", "2026-07-15", "acct-old")
    newer = _write_upload_folder(inbox, "manual__new", "2026-07-16", "acct-new")

    caplog.set_level(logging.WARNING)
    result = dist.ingest_inbox(inbox, read_meta=True)

    assert not older.exists()
    assert not newer.exists()
    assert "cleaned=2" in result["summary"]
    assert result["meta"]["target_date"] == "2026-07-16"
    assert result["meta"]["account_list"] == [{"account_id": "acct-new"}]
    assert result["meta"]["failed"]["accounts"] == [{"account_id": "acct-new"}]
    assert result["stats"]["folders"] == 2
    assert result["stats"]["cleaned"] == 2
    assert result["stats"]["failed"] == 0
    assert "지난 날짜 meta는 downstream 검증에서 제외" in caplog.text


def test_upload_inbox_single_date_meta_is_preserved(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = _write_upload_folder(inbox, "manual__single", "2026-07-16", "acct-one")

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert not folder.exists()
    assert result["meta"]["target_date"] == "2026-07-16"
    assert result["meta"]["account_list"] == [{"account_id": "acct-one"}]
    assert result["meta"]["validation"] == [{"account_id": "acct-one", "ok": True}]


def test_merge_meta_still_rejects_mixed_target_dates():
    with pytest.raises(ValueError, match="target_date 혼재"):
        dist._merge_meta(
            [
                {"target_date": "2026-07-15"},
                {"target_date": "2026-07-16"},
            ]
        )


def test_upload_inbox_writes_orders_as_parquet_and_non_orders_as_csv(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = _write_upload_folder(inbox, "manual__formats", "2026-07-16", "acct-format")
    _write_metric_file(folder)

    dist.ingest_inbox(inbox, read_meta=True)

    analytics = tmp_path / "analytics" / "baemin_macro"
    orders_stem = (
        analytics / "orders" / "brand=도리당" / "store=우가클송파삼전점" / "ym=2026-07" / "orders_2026-07"
    )
    metric_stem = (
        analytics
        / "metrics_our_store_clicks"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
        / "woori_shop_click"
    )

    assert orders_stem.with_suffix(".parquet").exists()
    assert not orders_stem.with_suffix(".csv").exists()
    assert metric_stem.with_suffix(".csv").exists()
    assert not metric_stem.with_suffix(".parquet").exists()
