"""분류된 품목 연결표와 원본으로 독립적인 pandas 조인/집계 대조를 수행한다."""
from __future__ import annotations

import json
import pandas as pd

from modules.transform.pipelines.db.DB_OrderCrossAnalysis_core import DIM, IDENTITY, UNIQUE_KEY, compact, is_fee, source_name, special_category, review_name


def check_reference(raw, bundle):
    df = raw.copy()
    for col in ["sale_date", "source", "brand", "store", "order_type", "order_id", "item_id", "item_name", "menu_name", "sale_type"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["source"] = df.source.map(source_name)
    if "_pk" in df:
        populated = df._pk.notna() & df._pk.astype(str).ne("")
        df = df[~(populated & df.duplicated("_pk"))]
    df["qty"] = pd.to_numeric(df.qty)
    df["total_price"] = pd.to_numeric(df.total_price)
    df = df[df.sale_type.ne("취소") & df.qty.gt(0) & df.total_price.ge(0) & ~df.item_name.map(is_fee)]
    for col in ["source", "brand", "store", "order_id", "item_id"]:
        df = df[df[col].ne("")]
    mapping = bundle["item_mapping"]
    order = ["sale_date", "source", "brand", "store", "order_type", "order_id"]
    fields = IDENTITY + ["category", "disp_name", "canonical_key", "is_main"]
    facts = df.merge(mapping[fields], on=IDENTITY, validate="many_to_one")
    facts = facts.loc[~facts.disp_name.map(is_fee).astype(bool)]
    items = facts.groupby(order + ["item_id", "category", "disp_name", "canonical_key", "is_main"], as_index=False, dropna=False)[["qty", "total_price"]].sum()
    anchors = items.loc[items.is_main.astype(bool), order + ["item_id", "disp_name", "canonical_key"]].copy()
    anchors["main_source"] = "item_id"
    headers = facts[order + ["menu_name"]].drop_duplicates()
    headers = headers[headers.menu_name.ne("") & ~headers.menu_name.map(
        lambda name: bool(special_category([name]) or review_name(name) or is_fee(name)))]
    headers["normalized_menu"] = headers.menu_name.map(compact)
    physical = facts[order + ["item_id", "item_name", "category", "disp_name", "canonical_key"]].drop_duplicates()
    physical = physical[~physical.category.isin(["리뷰", "리뷰참여", "미선택"])]
    physical["normalized_menu"] = physical.item_name.map(compact)
    matched = headers.merge(physical, on=order + ["normalized_menu"])
    matched["main_source"] = "item_id"
    anchors = pd.concat([anchors, matched[anchors.columns]], ignore_index=True)
    synthetic = mapping[mapping.basis.eq("order_menu_name")]
    if not synthetic.empty:
        unmatched = headers.merge(matched[order + ["menu_name"]].drop_duplicates().assign(has_match=True),
                                 on=order + ["menu_name"], how="left")
        candidates = unmatched[unmatched.has_match.isna()][order + ["menu_name"]].merge(
            synthetic[["source", "brand", "store", "item_id", "item_name", "disp_name", "canonical_key"]],
            left_on=["source", "brand", "store", "menu_name"], right_on=["source", "brand", "store", "item_name"])
        candidates["main_source"] = "menu_name"
        anchors = pd.concat([anchors, candidates[order + ["item_id", "disp_name", "canonical_key", "main_source"]]], ignore_index=True)
    anchors = anchors.drop_duplicates(order + ["item_id"])
    anchors["order_key"] = [json.dumps(list(r), ensure_ascii=False) for r in anchors[order].itertuples(index=False, name=None)]
    main_ref = anchors.groupby(DIM + ["item_id"], as_index=False).order_key.nunique().rename(columns={"item_id": "main_key", "order_key": "main_order_cnt"})
    main_actual = bundle["main_orders"].query("level == 'item'")[DIM + ["main_key", "main_order_cnt"]]
    _equal(main_ref, main_actual, DIM + ["main_key"])
    merged = anchors.rename(columns={"item_id": "main_item_id", "disp_name": "main_name", "canonical_key": "main_key"}).merge(
        items.rename(columns={"item_id": "pair_item_id", "disp_name": "pair_name", "category": "pair_type", "canonical_key": "pair_key"}), on=order)
    merged = merged[merged.main_item_id.ne(merged.pair_item_id)]
    same_name = merged.main_name.map(compact).eq(merged.pair_name.map(compact))
    merged = merged[~(merged.main_source.eq("menu_name") & same_name)]
    expected = merged.groupby(UNIQUE_KEY, as_index=False).agg(co_order_cnt=("order_key", "nunique"), co_qty=("qty", "sum"), co_amount=("total_price", "sum"))
    _equal(expected, bundle["cross"][UNIQUE_KEY + ["co_order_cnt", "co_qty", "co_amount"]], UNIQUE_KEY)
    # 표준 메뉴도 원본 주문에서 독립적으로 합산한다. 상품번호별 결과를 더하지 않는다.
    main_std = anchors.drop_duplicates(order + ["canonical_key"])
    item_std = items.groupby(order + ["canonical_key", "category", "disp_name"], as_index=False)[["qty", "total_price"]].sum()
    std = main_std.rename(columns={"canonical_key": "main_key", "disp_name": "main_name"}).merge(
        item_std.rename(columns={"canonical_key": "pair_key", "disp_name": "pair_name", "category": "pair_type"}), on=order)
    std = std[std.main_key.ne(std.pair_key)]
    std = std[~(std.main_source.eq("menu_name") & std.main_name.map(compact).eq(std.pair_name.map(compact)))]
    skeys = DIM + ["main_key", "pair_type", "pair_key"]
    expected_std = std.groupby(skeys, as_index=False).agg(co_order_cnt=("order_key", "nunique"), co_qty=("qty", "sum"), co_amount=("total_price", "sum"))
    _equal(expected_std, bundle["standard_pairs"][skeys + ["co_order_cnt", "co_qty", "co_amount"]], skeys)
    return {"raw_pairs": len(expected), "standard_pairs": len(expected_std), "main_denominators": len(main_ref)}


def _equal(expected, actual, keys):
    expected = expected.sort_values(keys).reset_index(drop=True)
    actual = actual[expected.columns].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False, check_index_type=False)
