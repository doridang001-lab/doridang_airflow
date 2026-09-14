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


def _write_ad_funnel_file(folder: Path, target_date: str, value: str) -> Path:
    data_dir = (
        folder
        / "baemin_macro"
        / "ad_funnel"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "baemin_ad_funnel.csv"
    pd.DataFrame([{"target_date": target_date, "value": value}]).to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )
    return path


def _write_now_file(folder: Path, date: str, rating: str, status: str | None = None) -> Path:
    data_dir = (
        folder
        / "baemin_macro"
        / "metrics_now"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "collected_at": f"{date}T08:22:34.752Z",
        "store_id": "14356015",
        "store_name": "[음식배달] 닭도리탕 전문 도리당 송파삼전점",
        "date": date,
        "최근별점": rating,
    }
    if status is not None:
        row["준비시간정확도_상태"] = status
    path = data_dir / "baemin_now.csv"
    pd.DataFrame([row]).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _write_shop_change_file(folder: Path) -> Path:
    data_dir = (
        folder
        / "baemin_macro"
        / "shop_change"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "shop_change.parquet"
    pd.DataFrame([{"수집일시": "2026-07-23 15:02:34", "영업상태": "영업"}]).to_parquet(
        path,
        index=False,
    )
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


def test_orders_upsert_preserves_existing_settlement_when_new_is_blank(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    stem = (
        tmp_path
        / "analytics"
        / "baemin_macro"
        / "orders"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
        / "orders_2026-07"
    )
    existing = pd.DataFrame(
        [
            {
                "주문번호": "order-1",
                "결제금액": "10000",
                "입금예정금액": "8000",
                "주문중개": "1000",
            }
        ]
    )
    dist.write_table(existing, stem)
    new_df = pd.DataFrame([{"주문번호": "order-1", "결제금액": "10000"}])

    out = dist._upsert_by_key(stem, new_df, dist.ORDER_KEY)

    row = out[out["주문번호"].eq("order-1")].iloc[0]
    assert row["입금예정금액"] == "8000"
    assert row["주문중개"] == "1000"


def test_orders_upsert_preserves_existing_instant_discount_when_new_is_blank(
    monkeypatch, tmp_path
):
    """즉시할인 분해는 상세시트를 열어야 읽히므로 재수집에서 통째로 비는 일이 잦다.

    공란 업로드가 기존 정상값을 덮어쓰면 DB_DeliveryCommission 이 전액 파트너부담으로
    폴백해 마트가 틀어진다. 0 은 '배민 전액지원'이라는 값이므로 보존 대상이다.
    """
    _patch_table_io(monkeypatch, tmp_path)
    stem = (
        tmp_path
        / "analytics"
        / "baemin_macro"
        / "orders"
        / "brand=도리당"
        / "store=우가클송파삼전점"
        / "ym=2026-07"
        / "orders_2026-07"
    )
    existing = pd.DataFrame(
        [
            {
                "주문번호": "order-1",
                "결제금액": "10000",
                "즉시할인": "2000",
                "즉시할인_파트너부담": "1500",
                "즉시할인_배민지원": "500",
                "배민부담_쿠폰할인": "300",
            },
            {
                "주문번호": "order-2",
                "결제금액": "5000",
                "즉시할인": "1000",
                "즉시할인_파트너부담": "0",
                "즉시할인_배민지원": "1000",
                "배민부담_쿠폰할인": "",
            },
        ]
    )
    dist.write_table(existing, stem)
    new_df = pd.DataFrame(
        [
            {
                "주문번호": "order-1",
                "결제금액": "10000",
                "즉시할인": "2000",
                "즉시할인_파트너부담": "",
                "즉시할인_배민지원": "",
                "배민부담_쿠폰할인": "",
            },
            {
                "주문번호": "order-2",
                "결제금액": "5000",
                "즉시할인": "1000",
                "즉시할인_파트너부담": "",
                "즉시할인_배민지원": "",
                "배민부담_쿠폰할인": "",
            },
        ]
    )

    out = dist._upsert_by_key(stem, new_df, dist.ORDER_KEY)

    row1 = out[out["주문번호"].eq("order-1")].iloc[0]
    assert row1["즉시할인_파트너부담"] == "1500"
    assert row1["즉시할인_배민지원"] == "500"
    assert row1["배민부담_쿠폰할인"] == "300"

    # '0'은 수집 실패가 아니라 배민 전액지원이므로 그대로 살아남아야 한다
    row2 = out[out["주문번호"].eq("order-2")].iloc[0]
    assert row2["즉시할인_파트너부담"] == "0"
    assert row2["즉시할인_배민지원"] == "1000"


def test_export_meta_recursively_removes_credentials(tmp_path):
    local_baemin = tmp_path / "stage" / "baemin_macro"
    source = local_baemin / "orders" / "sample.csv"
    source.parent.mkdir(parents=True)
    source.write_text("value\n1\n", encoding="utf-8")
    meta = {
        "account_list": [{"account_id": "acct", "password": "secret"}],
        "ad_stores": [{"account_id": "acct", "pw": "secret"}],
        "failed": {
            "ads": [{"account": {"account_id": "acct", "계정PW": "secret"}}],
        },
    }

    exported = staging.export_staging_to_inbox(local_baemin, "run", tmp_path / "inbox", meta=meta)
    exported_meta = json.loads((exported / "_meta.json").read_text(encoding="utf-8"))
    serialized = json.dumps(exported_meta, ensure_ascii=False).lower()

    assert "secret" not in serialized
    assert "password" not in serialized
    assert "계정pw" not in serialized
    assert exported_meta["account_list"] == [{"account_id": "acct"}]


def test_ingest_inbox_filters_by_folder_pattern(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    top = _write_upload_folder(inbox, "manual__top__run1", "2026-07-22", "acct-top")
    bottom = _write_upload_folder(inbox, "manual__bottom__run1", "2026-07-22", "acct-bottom")

    result = dist.ingest_inbox(
        inbox,
        read_meta=True,
        folder_pattern=dist.BOTTOM_FOLDER_PATTERN,
    )

    assert result["stats"]["cleaned"] == 1
    assert not bottom.exists()
    assert top.exists()
    assert {item["account_id"] for item in result["meta"]["account_list"]} == {"acct-bottom"}


def test_ingest_inbox_pattern_defaults_to_all(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    _write_upload_folder(inbox, "manual__top__run1", "2026-07-22", "acct-top")
    _write_upload_folder(inbox, "manual__bottom__run1", "2026-07-22", "acct-bottom")

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert result["stats"]["cleaned"] == 2


def test_ingest_inbox_no_matching_pattern_returns_zero(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    top = _write_upload_folder(inbox, "manual__top__run1", "2026-07-22", "acct-top")

    result = dist.ingest_inbox(
        inbox,
        read_meta=True,
        folder_pattern=dist.BOTTOM_FOLDER_PATTERN,
    )

    assert result["stats"]["cleaned"] == 0
    assert result["stats"]["folders"] == 0
    assert top.exists()


def test_upload_wrapper_rejects_unapproved_folder_pattern():
    with pytest.raises(ValueError, match="허용되지 않은"):
        dist.ingest_baemin_upload_inbox(folder_pattern="*")


def test_exact_folder_name_pattern_allowed():
    dist._validate_upload_folder_pattern("manual__top__run1")
    dist._validate_upload_folder_pattern(dist.BOTTOM_FOLDER_PATTERN)

    for pattern in ("*", "../x", "manual__a/b", r"manual__a\b", "manual__top__?"):
        with pytest.raises(ValueError, match="허용되지 않은"):
            dist._validate_upload_folder_pattern(pattern)


def test_bottom_folder_count_ignores_incomplete_and_top_exports(monkeypatch, tmp_path):
    inbox = tmp_path / "_baemin_upload_inbox"
    (inbox / "_tmp__manual__bottom__run1").mkdir(parents=True)
    (inbox / "manual__top__run1").mkdir()
    monkeypatch.setattr(dist, "UPLOAD_INBOX_DIR", inbox)

    assert dist.count_baemin_upload_inbox_folders(dist.BOTTOM_FOLDER_PATTERN) == 0

    (inbox / "manual__bottom__run1").mkdir()

    assert dist.count_baemin_upload_inbox_folders(dist.BOTTOM_FOLDER_PATTERN) == 1


def test_meta_uses_latest_successful_folder_when_newest_fails(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    older = _write_upload_folder(inbox, "manual__old", "2026-07-21", "acct-old")
    newer = _write_upload_folder(inbox, "manual__new", "2026-07-22", "acct-new")
    original_distribute = dist._distribute_one_file

    def fail_newest(folder, src_file):
        if folder == newer:
            raise RuntimeError("newest failed")
        return original_distribute(folder, src_file)

    monkeypatch.setattr(dist, "_distribute_one_file", fail_newest)

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert not older.exists()
    assert newer.exists()
    assert result["stats"]["cleaned"] == 1
    assert result["stats"]["failed"] == 1
    assert result["meta"]["target_date"] == "2026-07-21"
    assert result["meta"]["account_list"] == [{"account_id": "acct-old"}]


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


def test_ad_funnel_keeps_existing_dates(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    folder = tmp_path / "inbox" / "manual__bottom__run1"
    src_file = _write_ad_funnel_file(folder, "2026-07-22", "new")
    rel = src_file.relative_to(folder)
    dst_stem = (tmp_path / "analytics" / rel).with_suffix("")
    dst_stem.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"target_date": "2026-07-01", "value": "old-1"},
            {"target_date": "2026-07-02", "value": "old-2"},
            {"target_date": "2026-07-22", "value": "old-22"},
        ]
    ).to_csv(dst_stem.with_suffix(".csv"), index=False, encoding="utf-8-sig")

    dist._distribute_one_file(folder, src_file)

    result = pd.read_csv(dst_stem.with_suffix(".csv"), dtype=str, encoding="utf-8-sig")
    assert set(result["target_date"]) == {"2026-07-01", "2026-07-02", "2026-07-22"}
    assert result.loc[result["target_date"].eq("2026-07-22"), "value"].tolist() == ["new"]


def test_metrics_now_upsert_keeps_existing_dates_and_normalizes_schema(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    folder = tmp_path / "inbox" / "manual__bottom__run1"
    src_file = _write_now_file(folder, "2026-07-22", "4.9", "좋아요")
    rel = src_file.relative_to(folder)
    dst_stem = (tmp_path / "analytics" / rel).with_suffix("")
    dst_stem.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": "2026-07-01",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 송파삼전점",
                "최근별점": "4.1",
            },
            {
                "date": "2026-07-22",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 송파삼전점",
                "최근별점": "3.8",
            },
        ]
    ).to_csv(dst_stem.with_suffix(".csv"), index=False, encoding="utf-8-sig")

    dist._distribute_one_file(folder, src_file)

    result = pd.read_csv(
        dst_stem.with_suffix(".csv"),
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )
    assert result["date"].tolist() == ["2026-07-01", "2026-07-22"]
    assert result.loc[result["date"].eq("2026-07-22"), "최근별점"].tolist() == ["4.9"]
    assert result.loc[result["date"].eq("2026-07-22"), "준비시간정확도_상태"].tolist() == ["좋아요"]
    assert result.loc[result["date"].eq("2026-07-01"), "영업시간운영률_상태"].tolist() == [""]
    assert result.loc[result["date"].eq("2026-07-01"), "주문취소율_상세"].tolist() == [""]
    assert result.loc[result["date"].eq("2026-07-22"), "brand_store"].tolist() == ["도리당|송파삼전점"]
    assert list(result.columns[-3:]) == ["brand", "store", "brand_store"]


def test_top_pattern_does_not_match_bottom_folder(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    top = _write_upload_folder(inbox, "manual__top__run1", "2026-07-22", "acct-top")
    bottom = _write_upload_folder(inbox, "manual__bottom__run1", "2026-07-22", "acct-bottom")

    result = dist.ingest_inbox(
        inbox,
        read_meta=True,
        folder_pattern=dist.TOP_FOLDER_PATTERN,
    )

    assert result["stats"]["cleaned"] == 1
    assert not top.exists()
    assert bottom.exists()
    assert {item["account_id"] for item in result["meta"]["account_list"]} == {"acct-top"}


def test_parquet_source_stays_parquet(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    folder = tmp_path / "inbox" / "manual__bottom__run1"
    src_file = _write_shop_change_file(folder)
    rel = src_file.relative_to(folder)
    dst_stem = (tmp_path / "analytics" / rel).with_suffix("")

    dist._distribute_one_file(folder, src_file)

    assert dst_stem.with_suffix(".parquet").exists()
    assert not dst_stem.with_suffix(".csv").exists()


def test_folder_without_target_files_is_quarantined(tmp_path):
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = inbox / "manual__bottom__empty"
    folder.mkdir(parents=True)
    (folder / "_meta.json").write_text("{}", encoding="utf-8")

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert result["stats"]["folders"] == 0
    assert result["stats"]["skipped"] == 1
    assert result["stats"]["quarantined"] == 1
    assert not folder.exists()
    assert (inbox / dist.QUARANTINE_DIR_NAME / folder.name).is_dir()
    assert "quarantined=1" in result["summary"]


def test_source_file_read_error_is_quarantined(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = _write_upload_folder(inbox, "manual__bottom__io_error", "2026-07-22", "acct-one")

    def fail_read(path):
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(dist, "read_file", fail_read)

    result = dist.ingest_inbox(inbox, read_meta=True)

    assert result["stats"]["folders"] == 1
    assert result["stats"]["cleaned"] == 0
    assert result["stats"]["failed"] == 0
    assert result["stats"]["quarantined"] == 1
    assert not folder.exists()
    assert (inbox / dist.QUARANTINE_DIR_NAME / folder.name).is_dir()


def test_newer_overlapping_folder_wins_for_snapshot(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    old_folder = inbox / "manual__bottom__no_inference__20260723_144446"
    new_folder = inbox / "manual__bottom__first_popup__20260723_150234"
    old_file = _write_metric_file(old_folder)
    new_file = _write_metric_file(new_folder)
    old_df = pd.read_csv(old_file, dtype=str, encoding="utf-8-sig")
    new_df = pd.read_csv(new_file, dtype=str, encoding="utf-8-sig")
    old_df["광고지출"] = "1000"
    new_df["광고지출"] = "2000"
    old_df.to_csv(old_file, index=False, encoding="utf-8-sig")
    new_df.to_csv(new_file, index=False, encoding="utf-8-sig")

    result = dist.ingest_inbox(inbox, folder_pattern=dist.BOTTOM_FOLDER_PATTERN)

    rel = new_file.relative_to(new_folder)
    output = pd.read_csv(
        (tmp_path / "analytics" / rel).with_suffix(".csv"),
        dtype=str,
        encoding="utf-8-sig",
    )
    assert result["stats"]["cleaned"] == 2
    assert output["광고지출"].tolist() == ["2000"]


def test_cleanup_retries_temporary_onedrive_lock(monkeypatch, tmp_path):
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = inbox / "manual__bottom__run1"
    folder.mkdir(parents=True)
    real_rmtree = dist.shutil.rmtree
    attempts = []
    sleeps = []

    def flaky_rmtree(path):
        attempts.append(path)
        if len(attempts) < 3:
            raise OSError("locked")
        real_rmtree(path)

    monkeypatch.setattr(dist.shutil, "rmtree", flaky_rmtree)
    monkeypatch.setattr(dist.time, "sleep", sleeps.append)

    dist._cleanup_processed_folder(folder, inbox)

    assert len(attempts) == 3
    assert sleeps == [2, 4]
    assert not folder.exists()


def test_cleanup_raises_after_three_failures(monkeypatch, tmp_path):
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = inbox / "manual__bottom__run1"
    folder.mkdir(parents=True)
    attempts = []
    sleeps = []

    def always_locked(path):
        attempts.append(path)
        raise OSError("locked")

    monkeypatch.setattr(dist.shutil, "rmtree", always_locked)
    monkeypatch.setattr(dist.time, "sleep", sleeps.append)

    with pytest.raises(OSError, match="locked"):
        dist._cleanup_processed_folder(folder, inbox)

    assert len(attempts) == 3
    assert sleeps == [2, 4]
    assert folder.exists()


def test_acl_gate_verifies_outputs_before_cleanup(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = _write_upload_folder(inbox, "manual__bottom__run1", "2026-07-22", "acct-one")
    observed = {}

    def verify_acl(target_folder, output_paths):
        observed["folder_exists"] = target_folder.exists()
        observed["outputs"] = list(output_paths)
        assert all(path.exists() for path in output_paths)
        return len(output_paths)

    monkeypatch.setattr(dist, "_request_and_wait_acl_repair", verify_acl)

    result = dist.ingest_inbox(
        inbox,
        read_meta=True,
        require_acl_ack=True,
    )

    assert observed["folder_exists"] is True
    assert len(observed["outputs"]) == 1
    assert result["stats"]["acl_verified_files"] == 1
    assert result["stats"]["cleaned"] == 1
    assert not folder.exists()


def test_acl_gate_timeout_keeps_inbox_folder(monkeypatch, tmp_path):
    _patch_table_io(monkeypatch, tmp_path)
    inbox = tmp_path / "_baemin_upload_inbox"
    folder = _write_upload_folder(inbox, "manual__bottom__run1", "2026-07-22", "acct-one")

    def fail_acl(*args, **kwargs):
        raise TimeoutError("acl timeout")

    monkeypatch.setattr(dist, "_request_and_wait_acl_repair", fail_acl)

    result = dist.ingest_inbox(
        inbox,
        read_meta=True,
        require_acl_ack=True,
    )

    assert result["stats"]["folders"] == 1
    assert result["stats"]["cleaned"] == 0
    assert result["stats"]["failed"] == 1
    assert result["stats"]["acl_verified_files"] == 0
    assert folder.exists()


def test_acl_request_accepts_matching_ack_and_removes_queue_files(monkeypatch, tmp_path):
    analytics = tmp_path / "analytics"
    output = (
        analytics
        / "baemin_macro"
        / "orders"
        / "brand=test"
        / "store=test"
        / "ym=2026-07"
        / "orders_2026-07.parquet"
    )
    output.parent.mkdir(parents=True)
    output.write_bytes(b"test")
    folder = tmp_path / "manual__bottom__run1"
    folder.mkdir()
    queue = tmp_path / "queue"
    queue.mkdir()
    request_id = f"{folder.name}__fixed"
    relative_files = ["baemin_macro/orders/brand=test/store=test/ym=2026-07/orders_2026-07.parquet"]
    (queue / f"{request_id}.done.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "request_id": request_id,
                "ok": True,
                "files": relative_files,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dist, "ANALYTICS_DB", analytics)
    monkeypatch.setattr(dist.uuid, "uuid4", lambda: type("Fixed", (), {"hex": "fixed"})())

    count = dist._request_and_wait_acl_repair(
        folder,
        [output],
        queue_dir=queue,
        timeout_seconds=0,
        poll_seconds=0,
    )

    assert count == 1
    assert not list(queue.iterdir())


def test_acl_request_timeout_preserves_request(monkeypatch, tmp_path):
    analytics = tmp_path / "analytics"
    output = analytics / "baemin_macro" / "ad_funnel" / "sample.csv"
    output.parent.mkdir(parents=True)
    output.write_text("value\n1\n", encoding="utf-8")
    folder = tmp_path / "manual__bottom__run1"
    folder.mkdir()
    queue = tmp_path / "queue"
    monkeypatch.setattr(dist, "ANALYTICS_DB", analytics)
    monkeypatch.setattr(dist.uuid, "uuid4", lambda: type("Fixed", (), {"hex": "fixed"})())

    with pytest.raises(TimeoutError, match="ACL 복구 응답 timeout"):
        dist._request_and_wait_acl_repair(
            folder,
            [output],
            queue_dir=queue,
            timeout_seconds=0,
            poll_seconds=0,
        )

    requests = list(queue.glob("*.request.json"))
    assert len(requests) == 1
    payload = json.loads(requests[0].read_text(encoding="utf-8"))
    assert payload["files"] == ["baemin_macro/ad_funnel/sample.csv"]
