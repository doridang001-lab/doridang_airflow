from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_FinProduct as fin_product
from modules.transform.pipelines.db import DB_FinProduct_Map as product_map
from modules.transform.utility import schedule


def _target_item(**overrides) -> dict[str, str]:
    row = {
        "item_id": "200004114",
        "item_key": "메뉴미상(배민)",
        "store_seq": "411",
        "item_seq": "4",
        "store": "경북상주점",
        "source": "배민수동",
        "brand": "도리당",
        "item_name": "메뉴미상(배민)",
        "unitprice": "0",
        "대표메뉴": "",
    }
    row.update(overrides)
    return row


def test_scan_includes_manual_unknown_from_all_stores_only(monkeypatch):
    raw = pd.DataFrame([
        {
            "item_id": "200004114",
            "store": "경북상주점",
            "source": "배민수동",
            "brand": "도리당",
            "item_name": "메뉴미상(배민)",
            "unit_price": "0",
            "menu_name": "",
        },
        {
            "item_id": "200004115",
            "store": "경북상주점",
            "source": "배민수동",
            "brand": "도리당",
            "item_name": "일반메뉴",
            "unit_price": "12000",
            "menu_name": "일반메뉴",
        },
        {
            "item_id": "200000001",
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_name": "대상매장메뉴",
            "unit_price": "13000",
            "menu_name": "대상매장메뉴",
        },
    ])
    monkeypatch.setattr(product_map, "iter_unified_sales_files", lambda: [Path("sample.parquet")])
    monkeypatch.setattr(product_map.pd, "read_parquet", lambda *args, **kwargs: raw.copy())

    result = product_map.scan_target_items(persist_identity=False)

    assert set(result["item_name"]) == {"메뉴미상(배민)", "대상매장메뉴"}
    unknown = result[result["item_name"] == "메뉴미상(배민)"].iloc[0]
    assert unknown["item_id"] == "200004114"
    assert unknown["store"] == "경북상주점"


def test_manual_unknown_is_rule_pending_and_skips_llm(monkeypatch):
    item = _target_item()
    all_items = pd.DataFrame([item])
    monkeypatch.setattr(
        product_map,
        "scan_target_items",
        lambda persist_identity=True: all_items.copy(),
    )

    mapped = product_map.build_initial_map(persist_identity=False)
    row = mapped.iloc[0]

    assert row["item_id"] == "200004114"
    assert row["표준_메뉴명_edit"] == "메뉴미상(배민)"
    assert row["수동분류_edit"] == "기타"
    assert row[product_map.REVIEW_STATUS_COLUMN] == product_map.REVIEW_PENDING
    assert row["classified_by"] == "rule"
    assert product_map.find_llm_targets(all_items, mapped).empty


def test_llm_pipeline_reapplies_unknown_rule_after_pending_review(monkeypatch):
    all_items = pd.DataFrame([_target_item()])
    pending = all_items.copy()
    pending["표준_메뉴명_edit"] = "메뉴미상(배민)"
    pending["수동분류_edit"] = "기타"
    pending[product_map.REVIEW_STATUS_COLUMN] = product_map.REVIEW_PENDING
    pending["classified_by"] = ""
    pending["updated_at"] = product_map.TODAY
    pending = pending.reindex(columns=product_map.MAP_COLUMNS, fill_value="")
    monkeypatch.setattr(product_map, "scan_target_items", lambda persist_identity=True: all_items.copy())
    monkeypatch.setattr(product_map, "load_map", lambda: pending.copy())
    monkeypatch.setattr(product_map, "load_review_map", product_map._empty_review_map)
    monkeypatch.setattr(product_map, "load_recently_map", lambda: pd.DataFrame(columns=product_map.RECENTLY_COLUMNS))

    seen = {}

    def fake_classify(unmapped, map_df, limit, dry_run):
        seen["llm_targets"] = len(unmapped)
        return []

    monkeypatch.setattr(product_map, "classify_unmapped", fake_classify)

    summary = product_map.llm_product_map(dry_run=True)

    assert seen["llm_targets"] == 0
    assert summary["llm_targets"] == 0


def test_manual_unknown_is_excluded_from_join_until_review(monkeypatch):
    item = _target_item()
    monkeypatch.setattr(
        product_map,
        "scan_target_items",
        lambda persist_identity=True: pd.DataFrame([item]),
    )
    mapped = product_map.build_initial_map(persist_identity=False)

    join_rows, conflicts = product_map.build_join_map(mapped)

    assert conflicts.empty
    assert join_rows.empty

    mapped.loc[:, product_map.REVIEW_STATUS_COLUMN] = product_map.REVIEW_APPROVED
    approved_rows, approved_conflicts = product_map.build_join_map(mapped)

    assert approved_conflicts.empty
    assert approved_rows.to_dict("records") == [{
        "item_id": "200004114",
        "store": "경북상주점",
        "source": "배민수동",
        "brand": "도리당",
        "standard_menu_name": "메뉴미상(배민)",
        "category": "기타",
    }]


def test_fin_product_mart_enriches_approved_manual_unknown(monkeypatch, tmp_path):
    review_path = tmp_path / "fin_product_map_review_input.csv"
    mart_path = tmp_path / "fin_product_mart.csv"
    pd.DataFrame([{
        "source": "배민수동",
        "item_id": "200004114",
        "표준_메뉴명_edit": "메뉴미상(배민)",
        "수동분류_edit": "기타",
        "중복_수동분류": "N",
        "검수유무": "1",
    }]).to_csv(review_path, index=False, encoding="utf-8-sig")
    master = pd.DataFrame([{
        "source": "배민수동",
        "상품코드": "200004114",
        "상품명": "메뉴미상(배민)",
        "exclude_check": "N",
        "is_latest": "Y",
    }])
    monkeypatch.setattr(fin_product, "FIN_PRODUCT_MAP_REVIEW_CSV_PATH", review_path)
    monkeypatch.setattr(fin_product, "FIN_PRODUCT_MART_CSV_PATH", mart_path)
    monkeypatch.setattr(fin_product, "_read_master", lambda: master.copy())
    monkeypatch.setattr(fin_product, "_load_representative_menu_lookup", lambda: {})

    fin_product.build_fin_product_mart()

    result = pd.read_csv(mart_path, dtype=str, encoding="utf-8-sig").fillna("")
    row = result.iloc[0]
    assert row["상품코드"] == "200004114"
    assert row["표준상품명"] == "메뉴미상(배민)"
    assert row["수동분류_edit"] == "기타"
    assert row["검수유무"] == "1"
    assert row["exclude_check"] == "N"


def test_fin_product_runs_after_unified_sales_guard():
    assert schedule.DB_FIN_PRODUCT_MAP_TIME == "30 9 * * *"
    assert schedule.DB_FIN_PRODUCT_TIME == "0 10 * * *"
