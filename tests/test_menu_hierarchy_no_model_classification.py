import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_MenuHierarchy_Test as menu_hierarchy
from modules.transform.utility import paths as path_utils


@pytest.fixture(autouse=True)
def _isolate_menu_hierarchy_operating_files(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_guard_manager_input_loss", lambda df: df)
    monkeypatch.setattr(menu_hierarchy, "_guard_chicken_usage_total", lambda df: None)
    monkeypatch.setattr(menu_hierarchy, "_backup_manual_workbook_before_write", lambda: None)
    monkeypatch.setattr(menu_hierarchy, "_archive_legacy_root_outputs", lambda: None)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", lambda sheet: pd.DataFrame())
    menu_hierarchy._std_menu_override_manual_name_lookup.cache_clear()


def test_menu_hierarchy_test_pipeline_allows_only_llm_review_candidates():
    menu_hierarchy.assert_no_model_classification_dependencies()


def test_classification_rule_omits_placeholder_unresolved_when_confirmed():
    row = pd.Series(
        {
            menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN: "확정",
            "닭유형_판정": "메뉴명",
            "사이즈_판정": "미해결",
            "미해결사유": "",
        }
    )

    assert menu_hierarchy._classification_rule_from_row(row) == "확정:메뉴명"


def test_chicken_confidence_prefers_visible_menu_signal_over_ratio_value():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "line_role": "main",
        "닭유형": "순살",
        "사이즈": "1인",
        "닭유형_판정": "메뉴명",
        "사이즈_판정": "메뉴명",
        menu_hierarchy.CHICKEN_SIGNAL_COLUMN: menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
        menu_hierarchy.CHICKEN_RATIO_APPLIED_COLUMN: "0.061",
    })

    out = menu_hierarchy._attach_chicken_confidence_columns(pd.DataFrame([row]))
    out[menu_hierarchy.CLASSIFICATION_RULE_COLUMN] = out.apply(
        menu_hierarchy._classification_rule_from_row,
        axis=1,
    )

    assert out.iloc[0][menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN] == "확정"
    assert out.iloc[0][menu_hierarchy.CLASSIFICATION_RULE_COLUMN] == "확정:메뉴명"


def test_std_name_uses_existing_reviewed_tables_before_rule_fallback():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.std_by_item = {
        ("배민수동", "도리당", "송파삼전점", "ITEM1"): "검수표준명",
    }
    lookup.category_by_item = {}
    lookup.representative_by_item = {}
    lookup.alias_by_name = {
        menu_hierarchy._normalize_item_key("원본별칭"): "사람별칭",
    }

    assert lookup.std_name("배민수동", "도리당", "송파삼전점", "ITEM1", "원본") == "검수표준명"
    assert lookup.std_name("배민수동", "도리당", "송파삼전점", "MISS", "원본별칭") == "사람별칭"
    assert lookup.std_name("배민수동", "도리당", "송파삼전점", "MISS", "[재주문 1위] 도리당 닭도리탕") == "도리당 닭도리탕"
    assert lookup.std_name("쿠팡수동", "도리당", "송파삼전점", "MISS", "[단짠단짠] 순살 갈비찜닭") == "순살 갈비찜닭"
    assert lookup.std_name("쿠팡수동", "도리당", "송파삼전점", "MISS", "[한그릇] 누룽지 1인 순살 나만의 백도리당") == "누룽지 1인 순살 백도리당"
    assert lookup.std_name("배민수동", "도리당", "송파삼전점", "MISS", "[3~6인] 시그니처 반반") == ""


def test_coupang_std_name_prefers_visible_alias_over_reused_item_id():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.std_by_item = {
        ("쿠팡수동", "도리당", "송파삼전점", "300004062"): "도리당 닭도리탕",
    }
    lookup.category_by_item = {}
    lookup.representative_by_item = {}
    lookup.alias_by_name = {}

    assert (
        lookup.std_name(
            "쿠팡수동",
            "도리당",
            "송파삼전점",
            "300004062",
            "[1인] 순살 닭도리탕 (밥포함) 1인분",
        )
        == "1인 순살 닭도리탕(밥포함)"
    )
    assert (
        lookup.std_name(
            "쿠팡수동",
            "도리당",
            "송파삼전점",
            "300004062",
            "[복날한정] 1인 미나리 수삼 백숙",
        )
        == "1인 미나리 수삼 백숙"
    )


def test_item_id_lookup_uses_reviewed_manual_rows_without_tmp_ids():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.map_target = pd.DataFrame()
    lookup.review_target = pd.DataFrame(
        [
            {
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "MANUAL_SIZE1",
                "item_name": "[대] 한마리반+우거지 200g",
                "unitprice": "17000",
                "review_status_edit": "1",
            },
            {
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "TMP_should_not_map",
                "item_name": "미확정 옵션",
                "unitprice": "1000",
                "review_status_edit": "1",
            },
            {
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "UNREVIEWED1",
                "item_name": "미검수 옵션",
                "unitprice": "1000",
                "review_status_edit": "0",
            },
        ]
    )
    lookup.item_id_by_row = lookup._build_item_id_lookup()
    lookup.canonical_profile = {}

    assert lookup.item_id("배민수동", "도리당", "송파삼전점", "[대] 한마리반+우거지 200g", "17000") == "MANUAL_SIZE1"
    assert lookup.item_id("배민수동", "도리당", "송파삼전점", "[대] 한마리반+우거지 200g", "") == "MANUAL_SIZE1"
    assert lookup.item_id("배민수동", "도리당", "송파삼전점", "미검수 옵션", "1000").startswith("TMP_")
    assert lookup.item_id("배민수동", "도리당", "송파삼전점", "미확정 옵션", "1000").startswith("TMP_")


def test_coupang_multi_main_total_is_redistributed_by_main_unit_price():
    rows = [
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-15",
            "order_id": "0XLXUC",
            "item_seq": "1",
            "line_role": "main",
            "item_name": "[복날한정] 1인 미나리 수삼 백숙",
            "qty": "1",
            "unit_price": "16900",
            "total_price": "52300",
            "discount_amount": "0",
            "sale_type": "정상",
        },
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-15",
            "order_id": "0XLXUC",
            "item_seq": "2",
            "line_role": "main",
            "item_name": "[우삼겹살] 우도리탕",
            "qty": "1",
            "unit_price": "35400",
            "total_price": "0",
            "discount_amount": "0",
            "sale_type": "정상",
        },
    ]

    out = menu_hierarchy._redistribute_coupang_multi_main_totals(pd.DataFrame(rows))
    by_item = out.set_index("item_seq")

    assert by_item.at["1", "total_price"] == 16900
    assert by_item.at["2", "total_price"] == 35400
    assert pd.to_numeric(out["total_price"], errors="coerce").sum() == 52300


def test_coupang_multi_main_total_is_not_redistributed_when_unit_sum_differs():
    rows = [
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-15",
            "order_id": "O_MISMATCH",
            "item_seq": "1",
            "line_role": "main",
            "qty": "1",
            "unit_price": "16900",
            "total_price": "50000",
            "discount_amount": "0",
            "sale_type": "정상",
        },
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-15",
            "order_id": "O_MISMATCH",
            "item_seq": "2",
            "line_role": "main",
            "qty": "1",
            "unit_price": "35400",
            "total_price": "0",
            "discount_amount": "0",
            "sale_type": "정상",
        },
    ]

    out = menu_hierarchy._redistribute_coupang_multi_main_totals(pd.DataFrame(rows))
    by_item = out.set_index("item_seq")

    assert by_item.at["1", "total_price"] == "50000"
    assert by_item.at["2", "total_price"] == "0"


def test_order_sequence_deciding_option_indexes_marks_multi_main_options():
    rows = [
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "1", "line_role": "main", "option_kind": "메인", "item_name": "도리당 닭도리탕", "std_menu_name": "도리당 닭도리탕", "menu_name": "도리당 닭도리탕", "닭유형": "뼈닭", "사이즈": "중"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "2", "line_role": "main", "option_kind": "메인", "item_name": "누룽지 백도리탕", "std_menu_name": "누룽지 백도리탕", "menu_name": "누룽지 백도리탕", "닭유형": "순살", "사이즈": "중"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "3", "line_role": "option", "option_kind": "사이즈", "item_name": "[중] 2인"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "4", "line_role": "option", "option_kind": "사이즈", "item_name": "[중] 2인"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "5", "line_role": "option", "option_kind": "닭유형", "item_name": "뼈"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "6", "line_role": "option", "option_kind": "닭유형", "item_name": "순살"},
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "sale_date": "2026-04-19", "order_id": "O1", "item_seq": "7", "line_role": "option", "option_kind": "맛선택", "item_name": "기본맛"},
    ]
    frame = pd.DataFrame(rows)

    assert menu_hierarchy._order_sequence_deciding_option_indexes(frame, ["source", "brand", "store", "sale_date", "order_id"]) == {2, 3, 4, 5}


def test_std_name_uses_current_parent_before_representative_menu_for_non_main_items():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.std_by_item = {
        ("쿠팡수동", "도리당", "송파삼전점", "REV1"): "리뷰 분모자",
    }
    lookup.category_by_item = {
        ("쿠팡수동", "도리당", "송파삼전점", "REV1"): "리뷰",
    }
    lookup.representative_by_item = {
        ("쿠팡수동", "도리당", "송파삼전점", "REV1"): "[재주문 1위] 도리당 닭도리탕",
    }
    lookup.alias_by_name = {}

    assert lookup.std_name("쿠팡수동", "도리당", "송파삼전점", "REV1", "[한우 대창] 순살 곱도리탕") == "[한우 대창] 순살 곱도리탕"
    assert lookup.std_name("쿠팡수동", "도리당", "송파삼전점", "REV1", "") == "[재주문 1위] 도리당 닭도리탕"


def test_is_main_uses_product_category_before_price_heuristic():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.main_codes = {("배민수동", "도리당", "송파삼전점", "MAIN1")}
    lookup.category_by_item = {
        ("배민수동", "도리당", "송파삼전점", "OPT1"): "옵션",
        ("배민수동", "도리당", "송파삼전점", "REV1"): "리뷰",
        ("배민수동", "도리당", "송파삼전점", "SIDE1"): "사이드",
    }

    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "OPT1", "[중] 한마리", 9000)
    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "REV1", "[후.참] 계란", 100)
    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "SIDE1", "흑미 공기밥", 1000)
    assert lookup.is_main("배민수동", "도리당", "송파삼전점", "MAIN1", "도리당 닭도리탕", 20800)
    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "KNOWN_NO_CATEGORY", "새 옵션", 3000)
    assert lookup.is_main("배민수동", "도리당", "송파삼전점", "TMP_abc", "새 메인 메뉴", 12000)
    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "TMP_abc", "기본", 12000)
    assert not lookup.is_main("배민수동", "도리당", "송파삼전점", "TMP_abc", "기본맛", 12000)


def test_is_main_blocks_taste_option_even_when_product_tables_say_main():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.main_codes = {("okpos", "도리당", "송파삼전점", "BASIC")}
    lookup.category_by_item = {
        ("okpos", "도리당", "송파삼전점", "BASIC"): "메인",
    }
    lookup.canonical_profile = {
        menu_hierarchy._canonical_menu_key("기본맛"): {
            "category": "메인",
            "std_menu_name": "기본맛",
            "representative": "기본맛",
            "item_id": "BASIC",
        },
    }

    assert not lookup.is_main("okpos", "도리당", "송파삼전점", "BASIC", "기본맛", 0)
    assert not lookup.is_main("okpos", "도리당", "송파삼전점", "BASIC", "기본", 0)
    assert not lookup.is_main("okpos", "도리당", "송파삼전점", "BASIC", "중간맛(신라면보다매운)", 0)


def test_delivery_one_person_boneless_chicken_menu_can_force_main_when_product_category_is_side():
    row = pd.Series(
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_name": "[한그릇] 누룽지 1인 순살 나만의 백도리당",
            "menu_name": "[재주문 1위] 도리당 닭도리탕",
            "std_menu_name": "[한그릇] 누룽지 1인 순살 나만의 백도리당",
            "unit_price": "19800",
            "_role_category": "사이드",
        }
    )

    assert menu_hierarchy._can_force_order_main(row)


def test_main_line_option_kind_ignores_stale_non_main_confirmation(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update(
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "300004066",
            "item_name": "[한그릇] 누룽지 1인 순살 나만의 백도리당",
            "line_role": "main",
            "std_menu_name": "[한그릇] 누룽지 1인 순살 나만의 백도리당",
        }
    )
    stale_master = pd.DataFrame(
        [
            {
                "source": "쿠팡수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "300004066",
                "item_name": "[한그릇] 누룽지 1인 순살 나만의 백도리당",
                "option_kind_확정": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
                "재료명_확정": "",
            }
        ]
    )
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)

    out = menu_hierarchy._attach_option_kind(pd.DataFrame([row]))

    assert out.iloc[0]["option_kind"] == menu_hierarchy.OPTION_KIND_MAIN


def test_chicken_addon_candidate_overrides_stale_material_confirmation(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "10030872",
            "item_name": "1인 추가",
            "line_role": "option",
            "std_menu_name": "도리당 닭도리탕",
        }
    )
    stale_master = pd.DataFrame(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "10030872",
                "item_name": "1인 추가",
                "option_kind_확정": menu_hierarchy.OPTION_KIND_MATERIAL,
                "재료명_확정": "1인",
            }
        ]
    )
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)

    out = menu_hierarchy._attach_option_kind(pd.DataFrame([row]))

    assert out.iloc[0]["option_kind"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert out.iloc[0]["재료명"] == ""


def test_one_serving_option_is_chicken_addon_but_main_stays_main(monkeypatch):
    rows = [
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "200008000",
            "item_name": "1인분",
            "line_role": "main",
            "std_menu_name": "1인 순살 닭도리탕(밥포함)",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "200008000",
            "item_name": "1인분",
            "line_role": "option",
            "std_menu_name": "누룽지 닭한마리",
        },
    ]
    stale_master = pd.DataFrame(
        [
            {
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "200008000",
                "item_name": "1인분",
                "line_role": "option",
                "std_menu_name": "누룽지 닭한마리",
                "option_kind_확정": menu_hierarchy.OPTION_KIND_SIZE,
            }
        ]
    ).reindex(columns=[*menu_hierarchy.OPTION_KIND_MASTER_KEY_COLUMNS, *menu_hierarchy.OPTION_KIND_MASTER_EDIT_COLUMNS], fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)

    out = menu_hierarchy._attach_option_kind(pd.DataFrame(rows))

    by_role = out.set_index("line_role")
    assert by_role.at["main", "option_kind"] == menu_hierarchy.OPTION_KIND_MAIN
    assert by_role.at["option", "option_kind"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert by_role.at["option", "재료명"] == ""


def test_one_person_chicken_addon_defaults_to_half_bird(monkeypatch):
    rows = []
    main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    main.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "ORDER1",
            "menu_seq": "1",
            "item_seq": "1",
            "parent_item_seq": "1",
            "item_name": "도리당 닭도리탕",
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
            "qty": "1",
            "닭유형": "뼈닭",
            menu_hierarchy.CHICKEN_RATIO_APPLIED_COLUMN: "1",
            menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "1",
            menu_hierarchy.BONE_USAGE_TOTAL_COLUMN: "1",
            menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN: "0",
        }
    )
    addon = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    addon.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "ORDER1",
            "menu_seq": "1",
            "item_seq": "2",
            "parent_item_seq": "1",
            "item_id": "10030872",
            "item_name": "1인 추가",
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
            "qty": "2",
        }
    )
    rows.extend([main, addon])
    monkeypatch.setattr(
        menu_hierarchy,
        "_option_kind_master_attrs",
        lambda: pd.DataFrame(
            [
                {
                    "source": "okpos",
                    "brand": "도리당",
                    "store": "송파삼전점",
                    "item_id": "10030872",
                    "item_name": "1인 추가",
                    "닭가산_manual": "",
                    "닭가산유형_manual": "",
                }
            ]
        ),
    )

    out = menu_hierarchy._attach_chicken_addon_columns(pd.DataFrame(rows))
    by_item = out.set_index("item_seq")

    assert by_item.at["2", menu_hierarchy.CHICKEN_ADDON_REASON_COLUMN] == ""
    assert by_item.at["2", menu_hierarchy.CHICKEN_ADDON_USAGE_COLUMN] == "1"
    assert by_item.at["2", menu_hierarchy.CHICKEN_ADDON_BONE_COLUMN] == "1"
    assert by_item.at["1", menu_hierarchy.CHICKEN_ADDON_REASON_COLUMN] == ""
    assert by_item.at["1", menu_hierarchy.CHICKEN_ADDON_USAGE_COLUMN] == "1"
    assert by_item.at["1", menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "2"


def test_one_person_addon_under_non_chicken_parent_does_not_add_chicken(monkeypatch):
    main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    main.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "ORDER1",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "item_name": "대새S(대창150g+새우8마리)",
        "std_menu_name": "대새 추가 (대창 150g + 새우 8마리)",
        "line_role": "main",
        "qty": "1",
        "닭유형": menu_hierarchy.CHICKEN_TYPE_NONE,
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "0",
        menu_hierarchy.BONE_USAGE_TOTAL_COLUMN: "0",
        menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN: "0",
    })
    addon = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    addon.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "ORDER1",
        "menu_seq": "1",
        "item_seq": "2",
        "parent_item_seq": "1",
        "item_id": "10030872",
        "item_name": "1인 추가",
        "std_menu_name": "대새 추가 (대창 150g + 새우 8마리)",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
        "qty": "1",
    })
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_chicken_addon_columns(pd.DataFrame([main, addon]))
    by_item = out.set_index("item_seq")

    assert by_item.at["2", menu_hierarchy.CHICKEN_ADDON_REASON_COLUMN] == ""
    assert by_item.at["2", menu_hierarchy.CHICKEN_ADDON_USAGE_COLUMN] == ""
    assert by_item.at["1", menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "0"


def test_option_parent_repair_syncs_to_main_parent():
    rows = []
    main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    main.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "ORDER1",
            "menu_seq": "1",
            "item_seq": "1",
            "parent_item_seq": "1",
            "item_name": "도리당 닭도리탕",
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
        }
    )
    side = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    side.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "ORDER1",
            "menu_seq": "2",
            "item_seq": "2",
            "parent_item_seq": "2",
            "item_name": "라면사리 1개",
            "std_menu_name": "라면사리 1개",
            "line_role": "side",
        }
    )
    option = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    option.update(
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "ORDER1",
            "menu_seq": "2",
            "item_seq": "3",
            "parent_item_seq": "2",
            "item_name": "흑미 공기밥",
            "std_menu_name": "라면사리 1개",
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_RICE,
        }
    )
    rows.extend([main, side, option])

    out = menu_hierarchy._repair_option_parent_to_main(pd.DataFrame(rows)).set_index("item_seq")

    assert out.at["3", "parent_item_seq"] == "1"
    assert out.at["3", "menu_seq"] == "1"
    assert out.at["3", "std_menu_name"] == "도리당 닭도리탕"


def test_manual_profit_profile_uses_parent_axis_only_for_menu_dependent_options():
    material = pd.Series(
        {
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_MATERIAL,
            "수익채널": "홀",
            "item_name": "라면사리 1개",
            "재료명": "라면사리",
            "std_menu_name": "도리당 닭도리탕",
            "사이즈": "중",
            "닭유형": "뼈닭",
        }
    )
    drink = pd.Series(
        {
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
            "수익채널": "홀",
            "item_name": "펩시콜라 355ml (캔)",
            "std_menu_name": "도리당 닭도리탕",
        }
    )

    assert menu_hierarchy._manual_profit_profile(material)[0] == "품목|홀|도리당 닭도리탕|중|라면사리"
    assert menu_hierarchy._manual_profit_profile(drink)[0] == "품목|홀|펩시콜라 355ml (캔)"


def test_category_lookup_uses_existing_review_input_fallback():
    lookup = menu_hierarchy._ProductLookup.__new__(menu_hierarchy._ProductLookup)
    lookup.master_target = pd.DataFrame([
        {"source": "okpos", "brand": "도리당", "store": "송파삼전점", "상품코드": "MASTER_OPT", "수동분류": "옵션", "is_latest": "Y"},
    ])
    lookup.join_target = pd.DataFrame([
        {"source": "배민수동", "brand": "도리당", "store": "송파삼전점", "item_id": "JOIN1", "category": "메인"},
    ])
    lookup.review_target = pd.DataFrame([
        {"source": "배민수동", "brand": "도리당", "store": "송파삼전점", "item_id": "REV1", "수동분류_edit": "리뷰"},
        {"source": "배민수동", "brand": "도리당", "store": "송파삼전점", "item_id": "JOIN1", "수동분류_edit": "옵션"},
    ])
    lookup.map_target = pd.DataFrame([
        {"source": "배민수동", "brand": "도리당", "store": "송파삼전점", "item_id": "MAP1", "수동분류_edit": "사이드"},
    ])

    categories = lookup._build_category_lookup()

    assert categories[("okpos", "도리당", "송파삼전점", "MASTER_OPT")] == "옵션"
    assert categories[("배민수동", "도리당", "송파삼전점", "JOIN1")] == "옵션"
    assert categories[("배민수동", "도리당", "송파삼전점", "REV1")] == "리뷰"
    assert categories[("배민수동", "도리당", "송파삼전점", "MAP1")] == "사이드"


def test_new_review_path_helper_falls_back_to_existing_review(tmp_path, monkeypatch):
    legacy_path = tmp_path / "fin_product_map_review_input.csv"
    new_path = tmp_path / "new_fin_product_map_review_input.csv"
    legacy_path.write_text("item_id\nOLD\n", encoding="utf-8")

    monkeypatch.setattr(path_utils, "FIN_PRODUCT_MAP_REVIEW_CSV_PATH", legacy_path)
    monkeypatch.setattr(path_utils, "FIN_PRODUCT_MAP_REVIEW_LEGACY_CSV_PATH", tmp_path / "legacy.csv")
    monkeypatch.setattr(path_utils, "NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH", new_path)

    assert path_utils.existing_new_fin_product_map_review_csv_path() == legacy_path
    new_path.write_text("item_id\nNEW\n", encoding="utf-8")
    assert path_utils.existing_new_fin_product_map_review_csv_path() == new_path


def test_menu_hierarchy_review_input_combines_existing_and_new_priority(tmp_path, monkeypatch):
    base_path = tmp_path / "fin_product_map_review_input.csv"
    new_path = tmp_path / "new_fin_product_map_review_input.csv"
    pd.DataFrame([
        {
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_id": "ITEM1",
            "item_name": "기존명",
            "수동분류_edit": "메인",
            "표준_메뉴명_edit": "기존표준",
            "검수유무": "1",
        },
        {
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_id": "ITEM2",
            "item_name": "기존만",
            "수동분류_edit": "사이드",
            "표준_메뉴명_edit": "기존만표준",
            "검수유무": "1",
        },
    ]).to_csv(base_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([
        {
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_id": "ITEM1",
            "item_name": "새명",
            "수동분류_edit": "옵션",
            "표준_메뉴명_edit": "새표준",
            "검수유무": "1",
        },
        {
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_id": "ITEM2",
            "item_name": "기존만",
            "수동분류_edit": "옵션",
            "표준_메뉴명_edit": "새기존표준",
            "검수유무": "1",
        },
    ]).to_csv(new_path, index=False, encoding="utf-8-sig")

    monkeypatch.setattr(menu_hierarchy, "existing_fin_product_map_review_csv_path", lambda: base_path)
    monkeypatch.setattr(menu_hierarchy, "NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH", new_path)

    result = menu_hierarchy._load_menu_hierarchy_review_input()
    by_key = result.set_index(["item_id", "item_name"])

    assert by_key.at[("ITEM1", "기존명"), "수동분류_edit"] == "메인"
    assert by_key.at[("ITEM1", "새명"), "수동분류_edit"] == "옵션"
    assert by_key.at[("ITEM2", "기존만"), "수동분류_edit"] == "옵션"
    assert by_key.at[("ITEM2", "기존만"), "표준_메뉴명_edit"] == "새기존표준"


def test_build_new_fin_product_map_review_input_seeds_missing_order_items(tmp_path, monkeypatch):
    base_path = tmp_path / "fin_product_map_review_input.csv"
    new_path = tmp_path / "new_fin_product_map_review_input.csv"
    pd.DataFrame([
        {
            "item_id": "OLD1",
            "item_key": "기존",
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_name": "기존",
            "unitprice": "1000",
            "표준_메뉴명_edit": "기존표준",
            "수동분류_edit": "메인",
            "중복_수동분류": "",
            "검수유무": "1",
            "검수사유": "검수완료",
            "닭유형_manual": "순살",
            "사이즈_manual": "중",
            "닭사용량_manual": "0.8",
            "수익률_manual": "25%",
        },
    ]).to_csv(base_path, index=False, encoding="utf-8-sig")
    source_orders = pd.DataFrame([
        {
            "store": "송파삼전점",
            "source": "배민수동",
            "brand": "도리당",
            "item_id": "OLD1",
            "item_name": "기존",
            "unit_price": "1000",
            "std_menu_name": "기존표준",
            "line_role": "main",
            "_canonical": True,
        },
        {
            "store": "송파삼전점",
            "source": "쿠팡수동",
            "brand": "도리당",
            "item_id": "NEW1",
            "item_name": "신규 메뉴",
            "unit_price": "15000",
            "std_menu_name": "신규 표준",
            "line_role": "main",
            "_canonical": True,
        },
    ])

    monkeypatch.setattr(menu_hierarchy, "existing_fin_product_map_review_csv_path", lambda: base_path)
    monkeypatch.setattr(menu_hierarchy, "NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH", new_path)
    monkeypatch.setattr(menu_hierarchy, "resolve_yms", lambda ym=None: ["2026-04", "2026-08"])
    monkeypatch.setattr(menu_hierarchy, "_all_source_orders", lambda ym: source_orders)

    summary = menu_hierarchy.build_new_fin_product_map_review_input(dry_run=False)

    result = pd.read_csv(new_path, dtype=str, encoding="utf-8-sig").fillna("")
    by_item = result.set_index("item_id")
    assert summary["seed_rows"] == 1
    assert by_item.at["OLD1", "닭사용량_manual"] == "0.8"
    assert by_item.at["OLD1", "수익률_manual"] == "25%"
    assert by_item.at["NEW1", "검수유무"] == "0"
    assert by_item.at["NEW1", "검수사유"] == "메뉴계층 전용 신규 적재"
    assert by_item.at["NEW1", "수동분류_edit"] == "메인"


def test_new_review_seed_prefills_side_and_profit_review_reason(monkeypatch):
    source_orders = pd.DataFrame([{
        "store": "송파삼전점",
        "source": "쿠팡수동",
        "brand": "도리당",
        "item_id": "TMP_SIDE",
        "item_name": "새로 360ml",
        "unit_price": "5000",
        "total_price": "5000",
        "std_menu_name": "새로",
        "line_role": "side",
        "_canonical": True,
    }])

    monkeypatch.setattr(menu_hierarchy, "_all_source_orders", lambda ym: source_orders)

    seed = menu_hierarchy._build_new_review_seed_rows(["2026-08"])
    row = seed.iloc[0]

    assert row["수동분류_edit"] == "사이드"
    assert row["검수사유"] == "메뉴계층 전용 신규 적재; 수익률 입력 필요"


def test_line_role_prefers_product_category():
    assert menu_hierarchy._line_role("기본맛", True, "옵션") == "option"
    assert menu_hierarchy._line_role("[후.참] 계란", True, "리뷰") == "side"
    assert menu_hierarchy._line_role("흑미 공기밥", False, "사이드") == "option"
    assert menu_hierarchy._line_role("도리당 닭도리탕", False, "메인") == "main"
    assert menu_hierarchy._line_role("[한우 대창] 순살 곱도리탕", True, "기타", forced=True) == "main"


def test_canonical_menu_key_preserves_product_identity_tags():
    assert menu_hierarchy._canonical_menu_key("[한우 대창] 순살 곱도리탕") == menu_hierarchy._canonical_menu_key("한우순살곱도리탕")
    assert menu_hierarchy._canonical_menu_key("[재주문 1위] 도리당 닭도리탕") == menu_hierarchy._canonical_menu_key("도리당 닭도리탕")
    assert menu_hierarchy._canonical_menu_key("[보양식] 1인 미나리 수삼 백숙") == menu_hierarchy._canonical_menu_key("1인 미나리 수삼 백숙")


def test_canonical_menu_key_keeps_set_identity_for_cost_split():
    assert menu_hierarchy._canonical_menu_key("메밀 물 막국수") != menu_hierarchy._canonical_menu_key("메밀 물 막국수 세트")
    assert menu_hierarchy._canonical_menu_key("2인 순살 반반") != menu_hierarchy._canonical_menu_key("2인 순살 반반세트")


def test_resolve_yms_defaults_to_active_july_august(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "available_yms", lambda: ["2026-04", "2026-07", "2026-08"])

    assert menu_hierarchy.resolve_yms(None) == ["2026-07", "2026-08"]
    assert menu_hierarchy.resolve_yms("all") == ["2026-04", "2026-07", "2026-08"]


def test_boneless_lunch_chicken_profile_suggestion_precedes_bone_noodle_rule():
    suggested = menu_hierarchy._suggest_menu_chicken_profile("[점심] 순살 닭한마리 칼국수 정식 (2인이상)")

    assert suggested["허용닭유형_제안"] == "순살"
    assert suggested["기본닭유형_제안"] == "순살"
    assert suggested["허용사이즈_제안"] == "중"
    assert suggested["기본사이즈_제안"] == "중"
    assert suggested["옵션닭유형적용_제안"] == "N"


def test_std_menu_override_flows_to_profit_item_name_and_key(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-07-15",
        "ym": "2026-07",
        "platform": "홀",
        "order_type": "홀_테이블",
        "order_id": "O_STD_OVERRIDE_PROFIT",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "10030905",
        "menu_name": "순살 닭한마리 칼국수 정식 (2인이상)",
        "item_name": "닭한마리 칼국수 정식 (2인이상)",
        "std_menu_name": "[점심] 닭한마리 칼국수 정식 (2인이상)",
        "qty": "1",
        "total_price": "21000",
        "닭유형": "순살",
        "사이즈": "중",
        "닭유형_판정": "메뉴프로필",
        "사이즈_판정": "메뉴프로필",
        menu_hierarchy.CHICKEN_SIGNAL_COLUMN: menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
    })
    override = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "main",
        "item_id": "10030905",
        "item_name": "닭한마리 칼국수 정식 (2인이상)",
        "현재_std_menu_name": "[점심] 닭한마리 칼국수 정식 (2인이상)",
        "std_menu_name_manual": "[점심] 순살 닭한마리 칼국수 정식 (2인이상)",
    }]).reindex(columns=menu_hierarchy.STD_MENU_OVERRIDE_COLUMNS, fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_std_menu_override_attrs", lambda: override)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    corrected = menu_hierarchy._apply_std_menu_name_overrides(pd.DataFrame([row]))
    profit = menu_hierarchy._build_manual_profit_rate_master(corrected)
    result = profit.iloc[0]

    assert corrected.iloc[0]["std_menu_name"] == "[점심] 순살 닭한마리 칼국수 정식 (2인이상)"
    assert result["대표품목명"] == "[점심] 순살 닭한마리 칼국수 정식 (2인이상)"
    assert result["수익키"] == "메뉴|홀|[점심] 순살 닭한마리 칼국수 정식 (2인이상)|중|순살"
    assert result["대표품목원문"] == "닭한마리 칼국수 정식 (2인이상)"


def test_std_menu_override_splits_drink_profit_item_name(monkeypatch):
    rows = []
    for item_id, item_name, manual_name in [
        ("10030803", "펩시콜라 355ml (캔)", "펩시 콜라"),
        ("10030805", "펩시 제로 355ml (캔)", "펩시 제로"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-15",
            "ym": "2026-07",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": f"O_DRINK_{item_id}",
            "menu_seq": "1",
            "item_seq": item_id,
            "parent_item_seq": "1",
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
            "item_id": item_id,
            "menu_name": "도리당 닭도리탕",
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "qty": "1",
            "total_price": "2000",
        })
        rows.append(row)
    override = pd.DataFrame([
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "line_role": "option",
            "item_id": item_id,
            "item_name": item_name,
            "현재_std_menu_name": "도리당 닭도리탕",
            "std_menu_name_manual": manual_name,
        }
        for item_id, item_name, manual_name in [
            ("10030803", "펩시콜라 355ml (캔)", "펩시 콜라"),
            ("10030805", "펩시 제로 355ml (캔)", "펩시 제로"),
        ]
    ]).reindex(columns=menu_hierarchy.STD_MENU_OVERRIDE_COLUMNS, fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_std_menu_override_attrs", lambda: override)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    corrected = menu_hierarchy._apply_std_menu_name_overrides(pd.DataFrame(rows))
    profit = menu_hierarchy._build_manual_profit_rate_master(corrected)

    assert set(profit["수익키"]) == {"품목|홀|펩시 콜라", "품목|홀|펩시 제로"}
    assert set(profit["대표품목명"]) == {"펩시 콜라", "펩시 제로"}


def test_drink_profit_splits_pepsi_and_saero_variants_with_legacy_group_cost(monkeypatch):
    rows = []
    for seq, item_name, total_price in [
        ("2", "펩시콜라 355ml (캔)", "2000"),
        ("3", "펩시 제로 355ml (캔)", "2000"),
        ("4", "새로 360ml", "5000"),
        ("5", "새로 다래", "5500"),
        ("6", "새로 오미자", "5500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": f"O_DRINK_SPLIT_{seq}",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "item_id": seq,
            "item_name": item_name,
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
            "qty": "1",
            "unit_price": total_price,
            "total_price": total_price,
        })
        rows.append(row)
    previous = pd.DataFrame([
        {
            "수익키": "품목|홀|콜라",
            "판매가_manual": "2000",
            "메뉴원가_manual": "699",
            "상차림비_manual": "0",
            "메모": "콜라 기존 입력",
        },
        {
            "수익키": "품목|홀|새로",
            "판매가_manual": "5250",
            "메뉴원가_manual": "1722",
            "상차림비_manual": "0",
            "메모": "새로 기존 입력",
        },
    ]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    profit = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame(rows)).set_index("수익키")

    assert "품목|홀|콜라" not in profit.index
    assert "품목|홀|새로" not in profit.index
    expected = {
        "품목|홀|펩시콜라 355ml (캔)": ("펩시콜라 355ml (캔)", "699", "콜라 기존 입력 | 기존 묶음값 이관: 종류별 재검토 필요"),
        "품목|홀|펩시 제로 355ml (캔)": ("펩시 제로 355ml (캔)", "699", "콜라 기존 입력 | 기존 묶음값 이관: 종류별 재검토 필요"),
        "품목|홀|새로 360ml": ("새로 360ml", "1722", "새로 기존 입력 | 기존 묶음값 이관: 종류별 재검토 필요"),
        "품목|홀|새로 다래": ("새로 다래", "1722", "새로 기존 입력 | 기존 묶음값 이관: 종류별 재검토 필요"),
        "품목|홀|새로 오미자": ("새로 오미자", "1722", "새로 기존 입력 | 기존 묶음값 이관: 종류별 재검토 필요"),
    }
    assert set(expected).issubset(set(profit.index))
    for key, (name, cost, memo) in expected.items():
        assert profit.at[key, "대표품목명"] == name
        assert profit.at[key, "option_kind"] == menu_hierarchy.OPTION_KIND_DRINK
        assert profit.at[key, "메뉴원가_manual"] == cost
        assert profit.at[key, "메모"] == memo


def test_hall_paid_and_review_cheese_keep_separate_profit_kinds(monkeypatch):
    rows = []
    for order_id, item_id, item_name, kind, total_price in [
        ("O_PAID", "10030908", "모짜렐라 체다 치즈 추가", menu_hierarchy.OPTION_KIND_MATERIAL, "6000"),
        ("O_REVIEW", "10030930", "리뷰)모짜렐라 체다 치즈", menu_hierarchy.OPTION_KIND_REVIEW, "0"),
    ]:
        main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        main.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": order_id,
            "menu_seq": "1",
            "item_seq": "1",
            "parent_item_seq": "1",
            "item_id": "MAIN",
            "item_name": "도리당 닭도리탕",
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
            "qty": "1",
            "total_price": "25000",
        })
        option = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        option.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": order_id,
            "menu_seq": "1",
            "item_seq": "2",
            "parent_item_seq": "1",
            "item_id": item_id,
            "item_name": item_name,
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "option",
            "option_kind": kind,
            "재료명": "모짜렐라체다치즈" if kind == menu_hierarchy.OPTION_KIND_MATERIAL else "",
            "qty": "1",
            "unit_price": total_price,
            "total_price": total_price,
        })
        rows.extend([main, option])
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    profit = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame(rows))

    paid = profit[profit["대표품목명"].eq("모짜렐라체다치즈")].iloc[0]
    review = profit[profit["대표품목명"].eq("리뷰)모짜렐라체다치즈")].iloc[0]
    assert paid["option_kind"] == menu_hierarchy.OPTION_KIND_MATERIAL
    assert paid["판매수량"] == "1"
    assert paid["총매출합계"] == "6000"
    assert review["option_kind"] == menu_hierarchy.OPTION_KIND_REVIEW
    assert review["판매수량"] == "1"
    assert review["총매출합계"] == "0"


def test_infer_chicken_attrs_prefers_option_selection_over_menu_name_and_ignores_addons():
    rows = [
        {
            "line_role": "main",
            "menu_name": "1인 순살 닭도리탕 (밥포함)",
            "std_menu_name": "1인 순살 닭도리탕 (밥포함)",
            "item_name": "1인 순살 닭도리탕 (밥포함)",
        },
        {"line_role": "option", "menu_name": "", "std_menu_name": "", "item_name": "뼈"},
        {"line_role": "option", "menu_name": "", "std_menu_name": "", "item_name": "순살 (100%닭다리살)300g 추가"},
        {"line_role": "option", "menu_name": "", "std_menu_name": "", "item_name": "[소] 반마리"},
    ]

    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame(rows))

    assert attrs["닭유형_auto"] == "뼈닭"
    assert attrs["닭유형_판정_auto"] == "선택"
    assert attrs["사이즈_auto"] == "소"
    assert attrs["사이즈_판정_auto"] == "선택"


def test_infer_chicken_attrs_ignores_chicken_addon_kind_without_addon_word():
    rows = [
        {
            "line_role": "main",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
            "menu_name": "묵은지 닭도리탕",
            "std_menu_name": "묵은지 닭도리탕",
            "item_name": "묵은지 닭도리탕",
        },
        {
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
            "menu_name": "",
            "std_menu_name": "",
            "item_name": "[대] 3인",
        },
        {
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
            "menu_name": "",
            "std_menu_name": "",
            "item_name": "뼈",
        },
        {
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
            "menu_name": "",
            "std_menu_name": "",
            "item_name": "순살(닭다리살 100%) 300g",
        },
    ]

    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame(rows))

    assert attrs["닭유형_auto"] == "뼈닭"
    assert attrs["닭유형_판정_auto"] == "선택"
    assert attrs["사이즈_auto"] == "대"
    assert attrs["사이즈_판정_auto"] == "선택"


def test_infer_chicken_attrs_uses_menu_name_context_and_new_size_tokens():
    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame([
        {
            "line_role": "main",
            "menu_name": "순살 닭도리 정식(2인이상)",
            "std_menu_name": "도리당 닭도리탕",
            "item_name": "닭도리 정식(2인이상)",
        },
    ]))
    assert attrs["닭유형_auto"] == "순살"
    assert attrs["닭유형_판정_auto"] == "메뉴명"

    assert menu_hierarchy._infer_chicken_sizes("갈비찜닭 반반 [3~6인]") == ["대"]
    assert menu_hierarchy._infer_chicken_sizes("[보양식] 1인 미나리 수삼 백숙") == ["1인"]
    assert menu_hierarchy._infer_chicken_sizes("1인 추가") == []


def test_infer_chicken_attrs_prefers_explicit_size_option_over_menu_default():
    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame([
        {
            "line_role": "main",
            "menu_name": "[재주문 1위] 도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "item_name": "[재주문 1위] 도리당 닭도리탕",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        },
        {
            "line_role": "option",
            "menu_name": "[재주문 1위] 도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "item_name": "[대] 한마리반",
            "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
        },
        {
            "line_role": "option",
            "menu_name": "[재주문 1위] 도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "item_name": "뼈",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        },
    ]))

    assert attrs["닭유형_auto"] == "뼈닭"
    assert attrs["사이즈_auto"] == "대"
    assert attrs["사이즈_판정_auto"] == "선택"
    assert attrs["닭사용량_auto"] == "1.5"


def test_repair_deciding_option_parent_moves_non_chicken_child_to_previous_chicken_main():
    rows = []
    for item_seq, parent_seq, item_name, line_role, std_name, option_kind in [
        ("1", "1", "도리당 닭도리탕", "main", "도리당 닭도리탕", menu_hierarchy.OPTION_KIND_MAIN),
        ("2", "2", "미나리 비빔칼국수", "main", "미나리 비빔칼국수", menu_hierarchy.OPTION_KIND_MAIN),
        ("3", "2", "[중] 2인", "option", "미나리 비빔칼국수", menu_hierarchy.OPTION_KIND_SIZE),
        ("4", "2", "뼈", "option", "미나리 비빔칼국수", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE),
    ]:
        rows.append({
            "source": "okpos",
            "sale_date": "2026-06-01",
            "order_id": "O1",
            "menu_seq": parent_seq,
            "item_seq": item_seq,
            "parent_item_seq": parent_seq,
            "item_name": item_name,
            "line_role": line_role,
            "std_menu_name": std_name,
            "option_kind": option_kind,
        })

    out = menu_hierarchy._repair_deciding_option_parent_to_chicken_main(pd.DataFrame(rows))
    repaired = out[out["line_role"].eq("option")]

    assert set(repaired["parent_item_seq"]) == {"1"}
    assert set(repaired["std_menu_name"]) == {"도리당 닭도리탕"}


def test_judgement_option_input_refreshes_auto_generated_rows(monkeypatch):
    existing = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "도리당 닭도리탕",
        "조건": "사이즈옵션없음",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[소] 반마리 + 기본제공",
        "닭유형": "뼈닭",
        "사이즈": "중",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
        menu_hierarchy.HALF_COMBO_COLUMN: "",
        menu_hierarchy.HALF_BONE_RATIO_COLUMN: "",
        menu_hierarchy.HALF_SLOT1_COLUMN: "",
        menu_hierarchy.HALF_SLOT2_COLUMN: "",
        "메모": "자동생성 16행 / 매출 381200",
    }])
    monkeypatch.setattr(menu_hierarchy, "_judgement_option_attrs", lambda: existing)

    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "item_name": "[재주문 1위] 도리당 닭도리탕",
        "qty": "1",
        "total_price": "22400",
        "닭유형": "뼈닭",
        "사이즈": "소",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "0.5",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[소] 반마리 + 기본제공",
        "미해결사유": "사이즈옵션없음",
        menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN: "입력필요",
    })

    out = menu_hierarchy._build_judgement_option_input(pd.DataFrame([row]))
    refreshed = out[out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN].eq("[소] 반마리 + 기본제공")].iloc[0]

    assert refreshed["사이즈"] == "소"
    assert refreshed[menu_hierarchy.CHICKEN_USAGE_COLUMN] == "0.5"
    assert refreshed["메모"] == "자동생성 1행 / 매출 22400"


def test_fixed_lunch_chicken_menus_ignore_unsupported_boneless_option():
    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame([
        {
            "line_role": "main",
            "menu_name": "닭칼국수",
            "std_menu_name": "[점심] 닭칼국수",
            "item_name": "닭칼국수",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        },
        {
            "line_role": "option",
            "menu_name": "닭칼국수",
            "std_menu_name": "[점심] 닭칼국수",
            "item_name": "순살",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        },
        {
            "line_role": "option",
            "menu_name": "닭칼국수",
            "std_menu_name": "[점심] 닭칼국수",
            "item_name": "[대] 3인",
            "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
        },
    ]))

    assert attrs["닭유형_auto"] == "뼈닭"
    assert attrs["사이즈_auto"] == "중"
    assert attrs["닭유형_판정_auto"] == "메뉴명"
    assert attrs["닭사용량_auto"] == "1"


def test_fixed_non_chicken_menu_ignores_cross_menu_boneless_option():
    attrs = menu_hierarchy._infer_chicken_attrs_from_group(pd.DataFrame([
        {
            "line_role": "main",
            "menu_name": "메밀 물 막국수 세트",
            "std_menu_name": "[점심] 메밀 물 막국수 세트",
            "item_name": "메밀 물 막국수 세트",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        },
        {
            "line_role": "option",
            "menu_name": "메밀 물 막국수 세트",
            "std_menu_name": "[점심] 메밀 물 막국수 세트",
            "item_name": "닭도리탕 [순살]",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        },
    ]))

    assert attrs["닭유형_auto"] == menu_hierarchy.CHICKEN_TYPE_NONE
    assert attrs["사이즈_auto"] == menu_hierarchy.CHICKEN_SIZE_NONE
    assert attrs["닭사용량_auto"] == "0"
    assert attrs["닭유형_판정_auto"] == menu_hierarchy.CHICKEN_METHOD_NONE


def test_fixed_lunch_menu_profit_key_does_not_split_to_boneless(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, option_kind, total_price in [
        ("1", "닭칼국수", "main", menu_hierarchy.OPTION_KIND_MAIN, "9000"),
        ("2", "순살", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "parent_item_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "닭칼국수",
            "std_menu_name": "[점심] 닭칼국수",
            "line_role": line_role,
            "option_kind": option_kind,
            "qty": "1",
            "unit_price": total_price,
            "total_price": total_price,
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    with_chicken = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))
    with_profit = menu_hierarchy._attach_manual_profit_columns(with_chicken, rate_master=pd.DataFrame())
    main = with_profit[with_profit["line_role"].eq("main")].iloc[0]

    assert main["닭유형"] == "뼈닭"
    assert main["사이즈"] == "중"
    assert main["수익키"] == "메뉴|홀|[점심] 닭칼국수|중"
    assert main[menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_ABSENT


def test_menu_chicken_profile_forces_boneless_only_lunch_tteokbokki(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, option_kind, total_price in [
        ("1", "철판 즉석 닭떡볶이 (2인이상)", "main", menu_hierarchy.OPTION_KIND_MAIN, "24000"),
        ("2", "기본맛", "option", menu_hierarchy.OPTION_KIND_SPICE, "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "parent_item_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "철판 즉석 닭떡볶이 (2인이상)",
            "std_menu_name": "[점심] 철판 즉석 닭떡볶이 (2인이상)",
            "line_role": line_role,
            "option_kind": option_kind,
            "qty": "1",
            "unit_price": total_price,
            "total_price": total_price,
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())

    base = pd.DataFrame(rows)
    profile = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile)
    out = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    main = out[out["line_role"].eq("main")].iloc[0]

    assert profile.iloc[0]["허용닭유형_제안"] == "순살"
    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "중"
    assert main["닭유형_판정"] == "메뉴프로필"


def test_judgement_option_respects_boneless_only_menu_profile(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())
    menu_hierarchy._cached_menu_chicken_profile_lookup.cache_clear()
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "platform": "홀",
        "order_type": "홀_테이블",
        "order_id": "O1",
        "menu_seq": "1",
        "line_role": "main",
        "std_menu_name": "한우 순살 곱도리탕",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[중] 2인",
        "닭유형": "혼합",
        "사이즈": "중",
        "qty": "1",
        "total_price": "28900",
        menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN: "입력필요",
        "미해결사유": "사이즈옵션없음",
    })
    judgement = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "한우 순살 곱도리탕",
        "조건": "사이즈옵션없음",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[중] 2인",
        "닭유형": "혼합",
        "사이즈": "중",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
        menu_hierarchy.HALF_BONE_RATIO_COLUMN: "1.000",
    }])

    out, result = menu_hierarchy._apply_judgement_options(pd.DataFrame([row]), judgement)
    main = out.iloc[0]

    assert main["닭유형"] == "순살"
    assert main[menu_hierarchy.CHICKEN_USAGE_COLUMN] == menu_hierarchy._usage_for("순살", "중")
    assert main[menu_hierarchy.CHICKEN_RATIO_APPLIED_COLUMN] == ""
    assert result.iloc[0]["닭유형"] == "순살"


def test_menu_chicken_profile_hides_ignored_boneless_option_from_profit_source_list(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, option_kind, total_price in [
        ("1", "닭칼국수", "main", menu_hierarchy.OPTION_KIND_MAIN, "9000"),
        ("2", "순살", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "2000"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "parent_item_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "닭칼국수",
            "std_menu_name": "[점심] 닭칼국수",
            "line_role": line_role,
            "option_kind": option_kind,
            "qty": "1",
            "unit_price": total_price,
            "total_price": total_price,
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame())

    base = pd.DataFrame(rows)
    profile = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile)
    with_chicken = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    master = menu_hierarchy._build_manual_profit_rate_master(with_chicken)
    target = master[master["수익키"].eq("메뉴|홀|[점심] 닭칼국수|중")].iloc[0]

    assert "순살" not in target["원본품목명목록"]
    assert target["원본품목명목록"] == "닭칼국수"
    assert target["계산닭유형"] == "뼈닭"
    assert target[menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_ABSENT


def test_manual_profit_rate_master_keeps_multi_person_lunch_set_on_manual_size_axis(monkeypatch):
    previous = pd.DataFrame([{
        "수익키": "메뉴|홀|[점심] 닭한마리 칼국수 정식 (2인이상)|중",
        "판매가": "24000",
        "메뉴원가_manual": "8000",
        "상차림비_manual": "250",
        "메모": "2/3인 임시 수기 입력",
    }])
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "item_id": "MAIN1",
        "item_name": "닭한마리 칼국수 정식 (2인이상)",
        "menu_name": "순살 닭한마리 칼국수 정식 (2인이상)",
        "std_menu_name": "[점심] 닭한마리 칼국수 정식 (2인이상)",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "3인",
        "qty": "1",
        "total_price": "24000",
        menu_hierarchy.PROFIT_SALES_COLUMN: "24000",
        menu_hierarchy.OPTION_COMBO_COLUMN: "[대] 3인",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row]))
    target = master.iloc[0]

    assert target["수익키"] == "메뉴|홀|[점심] 닭한마리 칼국수 정식 (2인이상)|중"
    assert target["사이즈"] == "중"
    assert target["계산사이즈"] == "3인"
    assert target["닭유형"] == ""
    assert target["계산닭유형"] == "뼈닭"
    assert target["상차림비_manual"] == "250"
    assert target["상차림포함원가"] == "8250"
    assert target["메모"] == "2/3인 임시 수기 입력"


def test_half_slot_single_signal_fills_from_single_type_menu_name():
    assert menu_hierarchy._infer_half_slots_detail("2인 순살 반반", None, None, ["순살"]) == ("순살", "순살", "")


def test_chicken_ratio_master_uses_half_rule_and_unanimous_low_sample(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_chicken_ratio_master_attrs", lambda: {})
    rows = []
    for name, signal, chicken_type, qty in [
        ("베스트 반반", menu_hierarchy.CHICKEN_SIGNAL_ABSENT, "뼈닭", "10"),
        ("저표본 순살 메뉴", menu_hierarchy.CHICKEN_SIGNAL_PRESENT, "순살", "2"),
        ("저표본 순살 메뉴", menu_hierarchy.CHICKEN_SIGNAL_ABSENT, "순살", "5"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "std_menu_name": name,
            "line_role": "main",
            "닭유형": chicken_type,
            "사이즈": "중",
            "qty": qty,
            "total_price": "10000",
            menu_hierarchy.CHICKEN_SIGNAL_COLUMN: signal,
        })
        rows.append(row)

    master = menu_hierarchy._build_chicken_ratio_master(pd.DataFrame(rows))
    by_name = master.set_index("std_menu_name")

    assert by_name.at["베스트 반반", "적용비율"] == "0.500"
    assert by_name.at["베스트 반반", "비율출처"] == menu_hierarchy.CHICKEN_RATIO_SOURCE_HALF_RULE
    assert by_name.at["저표본 순살 메뉴", "적용비율"] == "0.000"
    assert by_name.at["저표본 순살 메뉴", "비율출처"] == menu_hierarchy.CHICKEN_RATIO_SOURCE_UNANIMOUS


def test_menu_weight_master_seeds_chicken_usage_from_conversion(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "닭유형": "순살",
        "사이즈": "중",
        "qty": "1",
        "total_price": "20000",
    })
    monkeypatch.setattr(menu_hierarchy, "_menu_weight_master_attrs", lambda: pd.DataFrame())

    master = menu_hierarchy._build_menu_weight_master(pd.DataFrame([row]))

    assert master.iloc[0]["닭사용량_manual"] == "0.8"
    assert menu_hierarchy._usage_for("뼈닭", "2인") == "1"


def test_default_manager_input_profit_applies_to_side_lines(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_seq": "1",
        "item_id": "SIDE1",
        "item_name": "미니 계란찜",
        "menu_name": "미니 계란찜",
        "std_menu_name": "미니 계란찜",
        "line_role": "side",
        "qty": "1",
        "total_price": "2000",
    })
    manager = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "미니 계란찜",
        "옵션조합": menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO,
        "수익률_manual": "30%",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["수익률"] == "30%"
    assert out.iloc[0]["추정수익"] == "600"
    assert out.iloc[0]["닭유형"] == menu_hierarchy.CHICKEN_TYPE_NONE
    assert out.iloc[0]["미해결사유"] == ""


def test_manual_profit_rate_is_derived_from_manual_cost_fields():
    row = pd.Series({
        "판매가": "9,000",
        "메뉴원가_manual": "3116",
        "상차림비_manual": "400",
    })

    rate, reason = menu_hierarchy._manual_profit_rate_from_cost(row)

    assert rate == pytest.approx(0.6093333333)
    assert reason == ""


def test_manual_profit_rate_prefers_manual_price_over_auto_price():
    row = pd.Series({
        "판매가": "9,000",
        "판매가_manual": "10,000",
        "메뉴원가_manual": "3000",
        "상차림비_manual": "500",
    })

    rate, reason = menu_hierarchy._manual_profit_rate_from_cost(row)

    assert rate == pytest.approx(0.65)
    assert reason == ""


def test_manual_profit_rate_falls_back_to_menu_cost_plus_setting_cost():
    row = pd.Series({
        "판매가": "9000",
        "메뉴원가_manual": "3116",
        "상차림비_manual": "400",
    })

    rate, reason = menu_hierarchy._manual_profit_rate_from_cost(row)

    assert rate == pytest.approx(0.6093333333)
    assert reason == ""


def test_takeout_manual_profit_rate_ignores_setting_cost_and_inclusive_cost():
    row = pd.Series({
        menu_hierarchy.PROFIT_CHANNEL_COLUMN: "홀_포장",
        "판매가": "10000",
        "메뉴원가_manual": "3000",
        "상차림비_manual": "400",
        "상차림포함원가": "3400",
    })

    rate, reason = menu_hierarchy._manual_profit_rate_from_cost(row)

    assert rate == pytest.approx(0.7)
    assert reason == ""


def test_takeout_manual_profit_rate_master_keeps_setting_cost_but_ignores_it(monkeypatch):
    previous = pd.DataFrame([{
        "수익키": "메뉴|홀_포장|도리당 닭도리탕|중",
        "판매가": "10000",
        "판매가_manual": "12000",
        "메뉴원가_manual": "3000",
        "상차림비_manual": "400",
    }])
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "PACK1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "std_menu_name": "도리당 닭도리탕",
        "사이즈": "중",
        "닭유형": "뼈닭",
        "platform": "홀",
        "order_type": "홀_포장",
        "qty": "1",
        "total_price": "10000",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_preserved_attrs", lambda: pd.DataFrame())

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row]))
    target = master.iloc[0]

    assert target["수익키"] == "메뉴|홀_포장|도리당 닭도리탕|중"
    assert target["판매가"] == "10000"
    assert target["판매가_manual"] == "12000"
    assert target["판매가기준"] == "수기"
    # 포장 상차림비는 담당자가 적은 값을 지우지 않는다. 계산에서만 0으로 본다.
    assert target["상차림비_manual"] == "400"
    assert target["상차림포함원가"] == "3000"


def test_relaxed_profit_key_drops_size_and_chicken_axes():
    assert (
        menu_hierarchy._relaxed_manual_profit_key("품목|홀|도리당 닭도리탕|중|뼈닭|계란찜")
        == "품목|홀|도리당 닭도리탕|계란찜"
    )
    # 닭유형 세그먼트가 아직 없던 옛 키도 같은 완화키로 모인다.
    assert (
        menu_hierarchy._relaxed_manual_profit_key("품목|홀|도리당 닭도리탕|2인|계란찜")
        == "품목|홀|도리당 닭도리탕|계란찜"
    )
    # 메뉴 행은 닭유형만 뺀다. 사이즈와 조합은 원가가 달라지므로 남긴다.
    assert (
        menu_hierarchy._relaxed_manual_profit_key("메뉴|홀|2인 순살 반반|2인|순살|닭도리탕+닭한마리")
        == "메뉴|홀|2인 순살 반반|2인|닭도리탕+닭한마리"
    )
    # 축이 없는 짧은 키는 흔들릴 일이 없으니 완화하지 않는다.
    assert menu_hierarchy._relaxed_manual_profit_key("품목|홀|공기밥") == ""


def test_manual_profit_rate_master_recovers_preserved_row_after_axis_change(monkeypatch):
    """분류가 흔들려 수익키 축이 바뀌어도 보존시트에 있던 수기값을 되붙인다."""
    # 지금 회차의 수익키는 사이즈.닭유형이 '중|뼈닭'인데, 담당자는 '2인|순살'일 때 값을 넣어뒀다.
    preserved = pd.DataFrame([{
        "수익키": "품목|홀|도리당 닭도리탕|2인|순살|계란찜",
        "메뉴원가_manual": "766",
    }]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_preserved_attrs", lambda: preserved)

    base = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    base.update({
        "source": "okpos", "brand": "도리당", "store": "송파삼전점",
        "order_id": "AXIS1", "line_role": "main", "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕", "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "사이즈": "중", "닭유형": "뼈닭",
        "platform": "홀", "order_type": "홀_테이블", "qty": "1", "total_price": "29800",
    })
    option = dict(base)
    option.update({
        "item_id": "OPT1", "item_seq": "2", "item_name": "계란찜",
        "std_menu_name": "계란찜", "line_role": "option",
        "option_kind": "재료추가", "qty": "1", "total_price": "3500",
    })

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([base, option]))
    egg = master[master["수익키"].str.endswith("계란찜")]

    assert not egg.empty, "옵션 행이 수익률 시트에 없다"
    assert egg.iloc[0]["메뉴원가_manual"] == "766"
    assert "축변경 이관" in egg.iloc[0]["메모"]


def test_manual_profit_uses_manual_price_basis_when_sales_is_zero():
    """쿠팡이츠처럼 옵션 매출이 0인 채널은 수기 판매가 x 수량을 매출 기준으로 쓴다."""
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "coupangeats", "brand": "도리당", "store": "송파삼전점",
        "order_id": "CPE1", "item_id": "OPT1", "item_name": "밀떡",
        "line_role": "option", "std_menu_name": "밀떡",
        "option_kind": menu_hierarchy.OPTION_KIND_MATERIAL,
        "platform": "쿠팡이츠", "order_type": "배달",
        "qty": "2", "total_price": "0",
    })
    rate_master = pd.DataFrame([{
        "수익키": "품목|쿠팡이츠|밀떡||밀떡",
        "판매가": "0",
        "판매가_manual": "1500",
        "메뉴원가_manual": "500",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    got = out.iloc[0]

    # 수익률은 수기 판매가 기준: 1 - 500/1500
    assert got["수익률"] == "0.666667"
    assert got[menu_hierarchy.MANUAL_PROFIT_SALES_BASIS_COLUMN] == menu_hierarchy.MANUAL_PROFIT_SALES_BASIS_MANUAL
    # 매출 기준 1500 x 2 = 3000, 원가 500 x 2 = 1000
    assert got["수기수익"] == "2000"


def test_cost_base_columns_use_manual_unit_price():
    """원가_기준매출/원가_기준수익은 수기 단가 x 수량 기준이다. 할인이 섞인 실매출을 쓰지 않는다."""
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos", "brand": "도리당", "store": "송파삼전점",
        "order_id": "BASE1", "item_id": "MAIN1", "item_name": "도리당 닭도리탕",
        "line_role": "main", "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "사이즈": "중", "닭유형": "뼈닭",
        "platform": "홀", "order_type": "홀_테이블",
        "qty": "2", "total_price": "50000",   # 정가 29,800 x 2 보다 싸게 나간 할인 건
    })
    rate_master = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중",
        "판매가": "25000",
        "판매가_manual": "29800",
        "메뉴원가_manual": "9822.6",
        "상차림비_manual": "710",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    got = out.iloc[0]

    # 29,800 x 2 = 59,600. 실매출 50,000이 아니다.
    assert got[menu_hierarchy.COST_BASE_SALES_COLUMN] == "59600"
    # 상차림포함원가 10,532.6 x 2 = 21,065.2
    assert got[menu_hierarchy.MANUAL_PROFIT_COST_COLUMN] == "21065.2"
    assert got[menu_hierarchy.COST_BASE_PROFIT_COLUMN] == "38534.8"
    # 수기수익은 종전대로 실매출 기준이라 값이 다르다
    assert got["수기수익"] == "28934.8"


def test_cost_base_columns_fall_back_to_auto_unit_price():
    """수기 판매가가 없으면 자동 판매가로 기준을 잡는다."""
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos", "brand": "도리당", "store": "송파삼전점",
        "order_id": "BASE2", "item_id": "MAIN2", "item_name": "도리당 닭도리탕",
        "line_role": "main", "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "사이즈": "중", "닭유형": "뼈닭",
        "platform": "홀", "order_type": "홀_테이블",
        "qty": "1", "total_price": "20000",
    })
    rate_master = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중",
        "판매가": "25000",
        "판매가_manual": "",
        "메뉴원가_manual": "10000",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    got = out.iloc[0]

    assert got[menu_hierarchy.COST_BASE_SALES_COLUMN] == "25000"
    assert got[menu_hierarchy.COST_BASE_PROFIT_COLUMN] == "15000"


def test_cost_base_columns_blank_without_cost():
    """원가가 없으면 원가율을 못 만드니 두 열도 비운다."""
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos", "brand": "도리당", "store": "송파삼전점",
        "order_id": "BASE3", "item_id": "MAIN3", "item_name": "도리당 닭도리탕",
        "line_role": "main", "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "사이즈": "중", "닭유형": "뼈닭",
        "platform": "홀", "order_type": "홀_테이블",
        "qty": "1", "total_price": "29800",
    })
    rate_master = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중",
        "판매가": "29800",
        "판매가_manual": "29800",
        "메뉴원가_manual": "",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    got = out.iloc[0]

    assert got[menu_hierarchy.COST_BASE_SALES_COLUMN] == ""
    assert got[menu_hierarchy.COST_BASE_PROFIT_COLUMN] == ""


def test_manual_profit_keeps_actual_sales_basis_when_no_manual_price():
    """수기 판매가가 없으면 종전대로 실매출을 쓴다. 임의로 추정하지 않는다."""
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "coupangeats", "brand": "도리당", "store": "송파삼전점",
        "order_id": "CPE2", "item_id": "OPT2", "item_name": "대파",
        "line_role": "option", "std_menu_name": "대파",
        "option_kind": menu_hierarchy.OPTION_KIND_MATERIAL,
        "platform": "쿠팡이츠", "order_type": "배달",
        "qty": "3", "total_price": "0",
    })
    rate_master = pd.DataFrame([{
        "수익키": "품목|쿠팡이츠|대파||대파",
        "판매가": "0",
        "판매가_manual": "",
        "메뉴원가_manual": "100",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    got = out.iloc[0]

    assert got[menu_hierarchy.MANUAL_PROFIT_SALES_BASIS_COLUMN] == menu_hierarchy.MANUAL_PROFIT_SALES_BASIS_ACTUAL
    assert got["수기수익"] == "-300"


def test_manual_profit_uses_auto_rate_from_cost_for_hall_single_menu():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "HALL_SINGLE_1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "사이즈": "중",
        "닭유형": "뼈닭",
        "platform": "홀",
        "order_type": "홀_테이블",
        "qty": "1",
        "total_price": "10000",
    })
    rate_master = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중",
        "판매가": "10000",
        "메뉴원가_manual": "3510",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)

    assert "수익률_manual" not in out.columns
    assert out.iloc[0]["수익률"] == "0.649"
    assert out.iloc[0]["수기수익"] == "6490"


def test_manual_profit_splits_okpos_hall_takeout_channel():
    rows = []
    for order_id, order_type in [("TABLE1", "홀_테이블"), ("PACK1", "홀_포장")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": order_id,
            "item_id": "MAIN1",
            "item_name": "도리당 닭도리탕",
            "line_role": "main",
            "std_menu_name": "도리당 닭도리탕",
            "사이즈": "중",
            "닭유형": "뼈닭",
            "platform": "홀",
            "order_type": order_type,
            "qty": "1",
            "total_price": "10000",
        })
        rows.append(row)
    rate_master = pd.DataFrame([
        {"수익키": "메뉴|홀|도리당 닭도리탕|중", "판매가": "10000", "메뉴원가_manual": "3500", "상차림비_manual": ""},
        {"수익키": "메뉴|홀_포장|도리당 닭도리탕|중", "판매가": "10000", "메뉴원가_manual": "3000", "상차림비_manual": ""},
    ])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame(rows), rate_master=rate_master)

    by_channel = out.set_index(menu_hierarchy.PROFIT_CHANNEL_COLUMN)
    assert set(by_channel.index) == {"홀", "홀_포장"}
    assert by_channel.at["홀", "수익키"] == "메뉴|홀|도리당 닭도리탕|중"
    assert by_channel.at["홀_포장", "수익키"] == "메뉴|홀_포장|도리당 닭도리탕|중"
    assert by_channel.at["홀", "수기수익"] == "6500"
    assert by_channel.at["홀_포장", "수기수익"] == "7000"


def test_manual_profit_rate_master_groups_cola_variants_and_preserves_rate(monkeypatch):
    rows = []
    for item_id, item_name, source, total_price in [
        ("COKE1", "코카콜라 355ml", "okpos", "2000"),
        ("PEPSI1", "펩시 제로 355ml (캔)", "배민수동", "1500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": source,
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": f"O_{item_id}",
            "item_id": item_id,
            "item_name": item_name,
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
            "platform": "홀" if source == "okpos" else "배달의민족",
            "order_type": "홀_테이블" if source == "okpos" else "배달",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    previous = pd.DataFrame([
        {
            "수익키": "품목|홀|콜라",
            "판매가": "2000",
            "메뉴원가_manual": "700",
            "상차림비_manual": "",
            "메모": "콜라 마진",
        },
        {
            "수익키": "품목|배달의민족|콜라",
            "판매가": "2000",
            "메뉴원가_manual": "699",
            "상차림비_manual": "",
            "메모": "배달 콜라 마진",
        },
    ])

    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame(rows))

    assert "기존수익키" not in master.columns
    assert set(master["수익키"]) == {"품목|홀|콜라", "품목|배달의민족|펩시 제로 355ml (캔)"}
    assert set(master["대표품목명"]) == {"콜라", "펩시 제로 355ml (캔)"}
    assert set(master["사이즈"]) == {""}
    assert set(master["판매가"]) == {"1500", "2000"}
    by_key = master.set_index("수익키")
    assert by_key.at["품목|홀|콜라", "메뉴원가_manual"] == "700"
    assert by_key.at["품목|배달의민족|펩시 제로 355ml (캔)", "메뉴원가_manual"] == "699"
    assert set(master["상차림비_manual"]) == {""}
    assert by_key.at["품목|홀|콜라", "상차림포함원가"] == "700"
    assert by_key.at["품목|배달의민족|펩시 제로 355ml (캔)", "상차림포함원가"] == "699"
    assert by_key.at["품목|홀|콜라", "메모"] == "콜라 마진"
    assert by_key.at["품목|배달의민족|펩시 제로 355ml (캔)", "메모"] == "배달 콜라 마진 | 기존 묶음값 이관: 종류별 재검토 필요"


def test_manual_profit_rate_master_splits_menu_dependent_option_by_size(monkeypatch):
    rows = []
    for size, total_price in [("중", "3000"), ("대", "4000")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": f"O_{size}",
            "item_id": f"ADD_{size}",
            "item_name": "1인 추가",
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
            "std_menu_name": "도리당 닭도리탕",
            "사이즈": size,
            "닭유형": "뼈닭",
            "platform": "홀",
            "order_type": "홀_테이블",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    previous = pd.DataFrame([{
        "수익키": "품목|홀|도리당 닭도리탕|중|1인분 추가",
        "판매가": "3000",
        "메뉴원가_manual": "1053",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame(rows))

    assert set(master["수익키"]) == {
        "품목|홀|도리당 닭도리탕|중|1인분 추가",
        "품목|홀|도리당 닭도리탕|대|1인분 추가",
    }
    by_size = master.set_index("사이즈")
    assert by_size.at["중", "닭유형"] == ""
    assert by_size.at["중", "계산닭유형"] == "뼈닭"
    assert by_size.at["중", "판매가"] == "3000"
    assert by_size.at["중", "메뉴원가_manual"] == "1053"
    assert by_size.at["중", "상차림포함원가"] == "1053"
    assert by_size.at["대", "판매가"] == "4000"


def test_manual_profit_columns_and_summary_use_manual_cost_fields():
    rows = []
    for item_id, item_name, total_price in [
        ("COKE1", "코카콜라 355ml", "2000"),
        ("PEPSI1", "펩시 제로 355ml (캔)", "1500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": f"O_{item_id}",
            "item_id": item_id,
            "item_name": item_name,
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
            "platform": "홀",
            "order_type": "홀_테이블",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    rate_master = pd.DataFrame([{
        "수익키": "품목|홀|콜라",
        "판매가": "2000",
        "메뉴원가_manual": "1280",
        "상차림비_manual": "",
    }])

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame(rows), rate_master=rate_master)
    summary = menu_hierarchy._build_manual_profit_summary(out)

    assert set(out["수익키"]) == {"품목|홀|콜라", "품목|홀|펩시 제로 355ml (캔)"}
    assert out["수익계상수량"].tolist() == ["1", "1"]
    assert out["수익계상원가"].tolist() == ["1280", "1280"]
    assert out["수기수익"].tolist() == ["720", "220"]
    by_name = summary.set_index("대표품목명")
    assert by_name.at["콜라", "매출합계"] == "2000"
    assert by_name.at["콜라", "수익합계"] == "720"
    assert by_name.at["펩시 제로 355ml (캔)", "매출합계"] == "1500"
    assert by_name.at["펩시 제로 355ml (캔)", "수익합계"] == "220"


def test_contextual_std_menu_name_splits_makguksu_and_two_person_set():
    rows = []
    for order_id, item_name, menu_name, std_name in [
        ("O_MAK_SET", "메밀 물 막국수", "메밀 물 막국수 세트", "메밀 물 막국수"),
        ("O_MAK_SINGLE", "메밀 물 막국수", "도리당 닭도리탕", "[점심] 메밀 물 막국수 세트"),
        ("O_HALF_SET", "2인 순살 반반", "2인 순살 반반세트", "2인 순살 반반"),
        ("O_HALF_SINGLE", "2인 순살 반반", "2인 순살 반반", "2인 순살 반반"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-19",
            "order_id": order_id,
            "menu_seq": "1",
            "item_seq": "1",
            "parent_item_seq": "1",
            "line_role": "main",
            "item_id": order_id,
            "item_name": item_name,
            "menu_name": menu_name,
            "std_menu_name": std_name,
        })
        rows.append(row)

    out = menu_hierarchy._apply_contextual_std_menu_name_corrections(pd.DataFrame(rows)).set_index("order_id")

    assert out.at["O_MAK_SET", "std_menu_name"] == "[점심] 메밀 물 막국수 세트"
    assert out.at["O_MAK_SINGLE", "std_menu_name"] == "메밀 물 막국수"
    assert out.at["O_HALF_SET", "std_menu_name"] == "2인 순살 반반세트"
    assert out.at["O_HALF_SINGLE", "std_menu_name"] == "2인 순살 반반"


def test_contextual_std_menu_name_keeps_manual_override_priority(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-19",
        "order_id": "O_MANUAL_OVERRIDE",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "item_id": "10030976",
        "item_name": "2인 순살 반반",
        "menu_name": "2인 순살 반반세트",
        "std_menu_name": "2인 순살 반반",
    })
    overrides = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "main",
        "item_id": "10030976",
        "item_name": "2인 순살 반반",
        "현재_std_menu_name": "2인 순살 반반세트",
        "std_menu_name_manual": "수기 반반 세트명",
    }])
    monkeypatch.setattr(menu_hierarchy, "_std_menu_override_attrs", lambda: overrides)

    corrected = menu_hierarchy._apply_contextual_std_menu_name_corrections(pd.DataFrame([row]))
    out = menu_hierarchy._apply_std_menu_name_overrides(corrected)

    assert out.iloc[0]["std_menu_name"] == "수기 반반 세트명"


def test_manual_profit_preserves_contextual_set_costs_without_crossing_single_cost(monkeypatch):
    previous = pd.DataFrame([
        {
            "수익키": "메뉴|홀|[점심] 메밀 물 막국수 세트|-",
            "메뉴원가_manual": "4206",
            "상차림비_manual": "500",
        },
        {
            "수익키": "메뉴|홀|2인 순살 반반|2인|순살",
            "메뉴원가_manual": "10367",
            "상차림비_manual": "710",
        },
    ])
    rows = []
    for order_id, item_id, item_name, menu_name, std_name, total_price, size, chicken_type in [
        ("O_MAK_SET", "MAK_SET", "메밀 물 막국수", "메밀 물 막국수 세트", "메밀 물 막국수", "11000", menu_hierarchy.CHICKEN_SIZE_NONE, menu_hierarchy.CHICKEN_TYPE_NONE),
        ("O_MAK_SINGLE", "MAK_SINGLE", "메밀 물 막국수", "도리당 닭도리탕", "[점심] 메밀 물 막국수 세트", "6500", menu_hierarchy.CHICKEN_SIZE_NONE, menu_hierarchy.CHICKEN_TYPE_NONE),
        ("O_HALF_SET", "HALF_SET", "2인 순살 반반", "2인 순살 반반세트", "2인 순살 반반", "39000", "2인", "순살"),
        ("O_HALF_SINGLE", "HALF_SINGLE", "2인 순살 반반", "2인 순살 반반", "2인 순살 반반", "33000", "2인", "순살"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-19",
            "order_id": order_id,
            "menu_seq": "1",
            "item_seq": "1",
            "parent_item_seq": "1",
            "line_role": "main",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
            "item_id": item_id,
            "item_name": item_name,
            "menu_name": menu_name,
            "std_menu_name": std_name,
            "platform": "홀",
            "order_type": "홀_테이블",
            "qty": "1",
            "total_price": total_price,
            "사이즈": size,
            "닭유형": chicken_type,
        })
        rows.append(row)
    corrected = menu_hierarchy._apply_contextual_std_menu_name_corrections(pd.DataFrame(rows))
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(corrected).set_index("수익키")

    assert master.at["메뉴|홀|[점심] 메밀 물 막국수 세트|-", "메뉴원가_manual"] == "4206"
    assert master.at["메뉴|홀|메밀 물 막국수|-", "메뉴원가_manual"] == ""
    assert master.at["메뉴|홀|2인 순살 반반|2인|순살", "메뉴원가_manual"] == "10367"
    assert master.at["메뉴|홀|2인 순살 반반세트|2인|순살", "메뉴원가_manual"] == ""


def test_half_set_profit_key_splits_by_cost_combo_and_seeds_blank_cost(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_name, total_price in [
        ("1", "main", menu_hierarchy.OPTION_KIND_MAIN, "2인 순살 반반", "33000"),
        ("2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "2인 순살 닭도리탕", "0"),
        ("3", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "2인 순살 곱도리탕", "0"),
        ("4", "option", menu_hierarchy.OPTION_KIND_MATERIAL, "미나리 새우전", "12900"),
        ("5", "option", menu_hierarchy.OPTION_KIND_MATERIAL, "가브리 수육", "14900"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-19",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O_TWO_HALF_SET",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "option_kind": option_kind,
            "item_id": f"ITEM_{seq}",
            "menu_name": "2인 순살 반반세트",
            "item_name": item_name,
            "std_menu_name": "2인 순살 반반세트",
            menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "2인 순살 닭도리탕 | 2인 순살 곱도리탕",
            menu_hierarchy.OPTION_COMBO_COLUMN: "2인 순살 닭도리탕 | 2인 순살 곱도리탕 | 미나리 새우전 | 가브리 수육",
            "사이즈": "2인",
            "닭유형": "순살",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame(rows)).set_index("수익키")

    set_key = "메뉴|홀|2인 순살 반반세트|2인|순살|닭도리탕+곱도리탕+미나리 새우전"
    extra_key = "품목|홀|2인 순살 반반세트|2인|순살|가브리수육"
    assert set_key in master.index
    assert extra_key in master.index
    assert master.at[set_key, menu_hierarchy.COST_COMBO_COLUMN] == "닭도리탕+곱도리탕+미나리 새우전"
    assert master.at[set_key, "매출합계"] == "45900"
    assert master.at[set_key, "판매가_manual"] == "51900"
    assert master.at[set_key, "메뉴원가_manual"] == "20081"
    assert master.at[extra_key, "매출합계"] == "14900"


def test_half_set_cost_seed_does_not_overwrite_existing_blank_manual_price(monkeypatch):
    key = "메뉴|홀|2인 순살 반반세트|2인|순살|닭도리탕+곱도리탕+가브리살"
    previous = pd.DataFrame([{
        "수익키": key,
        "판매가_manual": "",
        "메뉴원가_manual": "19007.369210526318",
        "상차림비_manual": "390",
        "메모": "사용자 입력 유지",
    }]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-19",
        "platform": "홀",
        "order_type": "홀_테이블",
        "order_id": "O_HALF_SET_EXISTING",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "HALF_MAIN",
        "menu_name": "2인 순살 반반세트",
        "item_name": "2인 순살 반반",
        "std_menu_name": "2인 순살 반반세트",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "2인 순살 닭도리탕 | 2인 순살 곱도리탕",
        menu_hierarchy.OPTION_COMBO_COLUMN: "가브리 수육 | 2인 순살 닭도리탕 | 2인 순살 곱도리탕",
        "사이즈": "2인",
        "닭유형": "순살",
        "qty": "1",
        "total_price": "49900",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row])).set_index("수익키")

    assert master.at[key, "판매가_manual"] == ""
    assert master.at[key, "메뉴원가_manual"] == "19007.369210526318"
    assert master.at[key, "상차림비_manual"] == "390"
    assert master.at[key, "메모"] == "사용자 입력 유지"


def test_signature_half_cost_combo_seed_uses_size_type_and_side(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-19",
        "platform": "홀",
        "order_type": "홀_테이블",
        "order_id": "O_SIGNATURE_SET",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "SIG_MAIN",
        "menu_name": "시그니처 반반세트 [3~6인]",
        "item_name": "시그니처 반반세트 [3~6인]",
        "std_menu_name": "시그니처 반반세트",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "3~4인 | 닭도리탕 [뼈] | 닭한마리 [순살]",
        menu_hierarchy.OPTION_COMBO_COLUMN: "미나리 새우전 | 3~4인 | 닭도리탕 [뼈] | 닭한마리 [순살]",
        "사이즈": "중",
        "닭유형": menu_hierarchy.CHICKEN_TYPE_MIXED,
        "닭유형_판정": menu_hierarchy.CHICKEN_METHOD_HALF_SLOT,
        menu_hierarchy.CHICKEN_SIGNAL_COLUMN: menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
        "qty": "1",
        "total_price": "52900",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row])).set_index("수익키")

    key = "메뉴|홀|시그니처 반반세트|중|혼합|닭도리탕(뼈)+닭한마리(순)+미나리 새우전"
    assert key in master.index
    assert master.at[key, "판매가_manual"] == "52900"
    assert master.at[key, "메뉴원가_manual"] == "17514"


def test_manual_profit_rate_master_splits_net_and_gross_sales_totals(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "B_DISCOUNT",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "platform": "배달의민족",
        "order_type": "배달",
        "qty": "1",
        "total_price": "30000",
        "discount_amount": "3000",
        menu_hierarchy.NORMAL_PRICE_COLUMN: "30000",
        "사이즈": "중",
        "닭유형": "뼈닭",
    })

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row]))

    result = master.iloc[0]
    assert result["매출합계"] == "27000"
    assert result[menu_hierarchy.GROSS_SALES_TOTAL_COLUMN] == "30000"
    assert result["판매가"] == "30000"


def test_normalize_manual_profit_amounts_uses_gross_sales_total_for_price():
    frame = pd.DataFrame(
        [
            {
                "수익채널": "배달의민족",
                "수익키": "메뉴|배달의민족|도리당 닭도리탕|중|뼈닭",
                "판매수량": "2",
                "매출합계": "54000",
                menu_hierarchy.GROSS_SALES_TOTAL_COLUMN: "60000",
                "메뉴원가_manual": "",
                "상차림비_manual": "",
            }
        ]
    ).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")

    out = menu_hierarchy._normalize_manual_profit_amounts(frame)

    assert out.iloc[0]["판매가"] == "30000"


def test_normalize_manual_profit_amounts_marks_manual_price_basis():
    frame = pd.DataFrame(
        [
            {
                "수익채널": "배달의민족",
                "수익키": "메뉴|배달의민족|도리당 닭도리탕|중|뼈닭",
                "판매수량": "2",
                "매출합계": "54000",
                menu_hierarchy.GROSS_SALES_TOTAL_COLUMN: "60000",
                "판매가_manual": "32000",
                "메뉴원가_manual": "12000",
                "상차림비_manual": "500",
            }
        ]
    ).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")

    out = menu_hierarchy._normalize_manual_profit_amounts(frame)

    assert out.iloc[0]["판매가"] == "30000"
    assert out.iloc[0]["판매가_manual"] == "32000"
    assert out.iloc[0]["판매가기준"] == "수기"


def test_manual_profit_uses_zero_sales_for_zero_price_discount_lines():
    rows = []
    for item_id, total_price, discount in [
        ("MAIN1", "0", "30000"),
        ("OPT1", "0", "2000"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "item_id": item_id,
            "item_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main" if item_id == "MAIN1" else "option",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN if item_id == "MAIN1" else menu_hierarchy.OPTION_KIND_DRINK,
            "platform": "홀",
            "order_type": "홀_테이블",
            "qty": "1",
            "total_price": total_price,
            "discount_amount": discount,
            "사이즈": "중",
            "닭유형": "뼈닭" if item_id == "MAIN1" else "",
        })
        rows.append(row)
    rate_master = pd.DataFrame([
        {"수익키": "메뉴|홀|도리당 닭도리탕|중", "판매가": "10000", "메뉴원가_manual": "8000", "상차림비_manual": ""},
        {"수익키": "품목|홀|도리당닭도리탕", "판매가": "10000", "메뉴원가_manual": "8000", "상차림비_manual": ""},
    ])

    out = menu_hierarchy._attach_profit_sales_base(pd.DataFrame(rows))
    out = menu_hierarchy._attach_manual_profit_columns(out, rate_master=rate_master)
    summary = menu_hierarchy._build_manual_profit_summary(out)

    assert out[menu_hierarchy.PROFIT_SALES_COLUMN].tolist() == ["0", "0"]
    assert out["수익계상원가"].tolist() == ["8000", "8000"]
    assert out["수기수익"].tolist() == ["-8000", "-8000"]
    assert set(summary["매출합계"]) == {"0"}


def test_zero_price_cost_bearing_rice_counts_cost_and_stays_in_profit_master(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame())
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O_ZERO_RICE",
        "item_id": "RICE1",
        "item_name": "공기밥 추가",
        "std_menu_name": "[점심] 1인 순살 닭도리 정식",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_RICE,
        "platform": "홀",
        "order_type": "홀_테이블",
        "qty": "5",
        "total_price": "0",
    })

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row]))
    rate_master = pd.DataFrame([{
        "수익키": "품목|홀|공기밥",
        "판매가": "0",
        "메뉴원가_manual": "300",
        "상차림비_manual": "",
    }])
    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame([row]), rate_master=rate_master)
    summary = menu_hierarchy._build_manual_profit_summary(out)

    assert master.iloc[0]["수익키"] == "품목|홀|공기밥"
    assert master.iloc[0]["판매수량"] == "5"
    assert master.iloc[0]["매출합계"] == "0"
    assert out.iloc[0][menu_hierarchy.PROFIT_SALES_COLUMN] == "0"
    assert out.iloc[0][menu_hierarchy.MANUAL_PROFIT_QTY_COLUMN] == "5"
    assert out.iloc[0][menu_hierarchy.MANUAL_PROFIT_COST_COLUMN] == "1500"
    assert out.iloc[0]["수기수익"] == "-1500"
    assert summary.iloc[0]["매출합계"] == "0"
    assert summary.iloc[0]["수익합계"] == "-1500"
    assert summary.iloc[0]["수익률"] == ""


def test_zero_price_non_cost_option_kinds_are_not_manual_profit_targets():
    rows = []
    for item_id, item_name, kind in [
        ("SPICE1", "기본맛", menu_hierarchy.OPTION_KIND_SPICE),
        ("SIZE1", "[중] 2인", menu_hierarchy.OPTION_KIND_SIZE),
        ("TYPE1", "순살", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE),
        ("REQ1", "치킨무 빼주세요", menu_hierarchy.OPTION_KIND_REQUEST),
        ("FEE1", "배달비", menu_hierarchy.OPTION_KIND_FEE),
        ("EX1", "직원 확인", menu_hierarchy.OPTION_KIND_SETTLEMENT_EXCLUDE),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": f"O_{item_id}",
            "item_id": item_id,
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "option",
            "option_kind": kind,
            "platform": "홀",
            "order_type": "홀_테이블",
            "qty": "1",
            "total_price": "0",
        })
        rows.append(row)

    out = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame(rows), rate_master=pd.DataFrame())

    assert out["수익키"].tolist() == [""] * len(rows)
    assert out[menu_hierarchy.MANUAL_PROFIT_QTY_COLUMN].tolist() == [""] * len(rows)
    assert out["수기수익"].tolist() == [""] * len(rows)


def test_negative_event_order_counts_cost_with_positive_reference_price():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-01",
        "order_id": "O_NEG_EVENT",
        "line_role": "main",
        "sale_type": "취소",
        "qty": "0",
        "total_price": "-29800",
        "menu_name": "도리당 닭도리탕",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "platform": "홀",
        "order_type": "홀_테이블",
        "사이즈": "중",
        "닭유형": "뼈닭",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "0",
    })
    manual = pd.DataFrame([{
        "source": "okpos",
        "order_id": "O_NEG_EVENT",
        "sale_date": "2026-08-01",
        "자동판정": "취소",
        "구분_manual": "이벤트",
        "닭계상_manual": "Y",
    }]).reindex(columns=menu_hierarchy.ORDER_EXCEPTION_COLUMNS, fill_value="")
    rate_master = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중",
        "판매가": "29800",
        "메뉴원가_manual": "8000",
        "상차림비_manual": "",
    }])

    adjusted = menu_hierarchy._apply_order_exception_policy(pd.DataFrame([row]), order_exception_input=manual)
    out = menu_hierarchy._attach_manual_profit_columns(adjusted, rate_master=rate_master)
    master = menu_hierarchy._build_manual_profit_rate_master(out)

    result = out.iloc[0]
    assert result[menu_hierarchy.ORDER_EXCEPTION_TYPE_COLUMN] == "이벤트"
    assert result[menu_hierarchy.PRE_DISCOUNT_PRICE_COLUMN] == "29800"
    assert result[menu_hierarchy.PROFIT_SALES_COLUMN] == "0"
    assert result["수익계상수량"] == "1"
    assert result["수익계상원가"] == "8000"
    assert result["수기수익"] == "-8000"
    assert master.iloc[0]["판매가"] == "29800"


def test_order_exception_adjustment_uses_line_discount(monkeypatch):
    rows = []
    for item_seq, discount in [("1", "1000"), ("2", "2000")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-01",
            "order_id": "O1",
            "item_seq": item_seq,
            "line_role": "main" if item_seq == "1" else "option",
            "total_price": "0",
            "discount_amount": discount,
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_order_exception_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._apply_order_exception_policy(pd.DataFrame(rows))

    assert out[menu_hierarchy.ADJUSTED_SALES_COLUMN].tolist() == ["0", "0"]
    assert out[menu_hierarchy.PRE_DISCOUNT_PRICE_COLUMN].tolist() == ["1000", "2000"]


def test_normal_price_column_uses_gross_total_and_discount_fallback():
    rows = []
    for item_seq, total_price, discount in [("1", "30000", "3000"), ("2", "0", "2000")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-01",
            "order_id": "B1",
            "item_seq": item_seq,
            "line_role": "main",
            "total_price": total_price,
            "discount_amount": discount,
        })
        rows.append(row)

    out = menu_hierarchy._attach_normal_price_column(pd.DataFrame(rows))

    assert out[menu_hierarchy.NORMAL_PRICE_COLUMN].tolist() == ["30000", "2000"]


def test_menu_chicken_profile_preserves_manual_rows_missing_from_current_orders(monkeypatch):
    previous = pd.DataFrame(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "std_menu_name": "[점심] 순살 닭한마리 칼국수 정식 (2인이상)",
                "허용닭유형": "순살",
                "기본닭유형": "순살",
                "허용사이즈": "중",
                "기본사이즈": "중",
                "옵션닭유형적용": "N",
                "메모": "Codex: 순살 전용 점심 정식",
            }
        ]
    ).reindex(columns=menu_hierarchy.MENU_CHICKEN_PROFILE_COLUMNS, fill_value="")
    current = pd.DataFrame(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "std_menu_name": "도리당 닭도리탕",
                "item_name": "도리당 닭도리탕",
                "line_role": "main",
                "order_id": "O1",
                "qty": "1",
                "total_price": "30000",
            }
        ]
    )
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: previous)

    out = menu_hierarchy._build_menu_chicken_profile_master(current)

    preserved = out[out["std_menu_name"].eq("[점심] 순살 닭한마리 칼국수 정식 (2인이상)")].iloc[0]
    assert preserved["허용닭유형"] == "순살"
    assert preserved["기본닭유형"] == "순살"
    assert preserved["허용사이즈"] == "중"
    assert preserved["기본사이즈"] == "중"
    assert preserved["옵션닭유형적용"] == "N"


def test_cancel_order_manual_chicken_count_restores_usage_for_zero_qty():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-01",
        "order_id": "O_CANCEL_EVENT",
        "line_role": "main",
        "sale_type": "취소",
        "qty": "0",
        "total_price": "-29800",
        "menu_name": "도리당 닭도리탕",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "닭유형": "뼈닭",
        "사이즈": "중",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "0",
        menu_hierarchy.BONE_USAGE_TOTAL_COLUMN: "0",
        menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN: "0",
    })
    manual = pd.DataFrame([{
        "source": "okpos",
        "order_id": "O_CANCEL_EVENT",
        "sale_date": "2026-08-01",
        "자동판정": "취소",
        "닭계상_manual": "Y",
    }]).reindex(columns=menu_hierarchy.ORDER_EXCEPTION_COLUMNS, fill_value="")

    out = menu_hierarchy._apply_order_exception_policy(pd.DataFrame([row]), order_exception_input=manual)
    result = out.iloc[0]

    assert result[menu_hierarchy.ORDER_EXCEPTION_TYPE_COLUMN] == "취소"
    assert result[menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "1"
    assert result[menu_hierarchy.BONE_USAGE_TOTAL_COLUMN] == "1"
    assert result[menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN] == "0"


def _cancel_offset_test_row(
    order_id: str,
    total_price: str,
    *,
    order_time: str,
    sale_type: str = "정상",
    platform: str = "홀",
    order_type: str = "홀_테이블",
    chicken_type: str = "뼈닭",
) -> dict[str, str]:
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-01",
        "order_id": order_id,
        "order_time": order_time,
        "item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "platform": platform,
        "order_type": order_type,
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "qty": "1" if sale_type != "취소" else "0",
        "total_price": total_price,
        "sale_type": sale_type,
        "사이즈": "중",
        "닭유형": chicken_type,
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "1",
    })
    return row


def test_cancel_offset_excludes_matched_normal_and_cancel_from_manual_profit(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))
    rows = [
        _cancel_offset_test_row("NORMAL1", "29800", order_time="12:00:00"),
        _cancel_offset_test_row("CANCEL1", "-29800", order_time="12:10:00", sale_type="취소"),
        _cancel_offset_test_row("NORMAL2", "31000", order_time="12:20:00"),
    ]

    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame(rows))
    profit = menu_hierarchy._build_manual_profit_rate_master(offset)
    weight = menu_hierarchy._build_menu_weight_master(offset)

    status_by_order = offset.drop_duplicates("order_id").set_index("order_id")[menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN].to_dict()
    assert status_by_order["NORMAL1"] == menu_hierarchy.CANCEL_OFFSET_NORMAL_STATUS
    assert status_by_order["CANCEL1"] == menu_hierarchy.CANCEL_OFFSET_CANCEL_STATUS
    assert status_by_order["NORMAL2"] == menu_hierarchy.CANCEL_OFFSET_NONE_STATUS
    assert set(profit["매출합계"]) == {"31000"}
    assert set(weight["매출합계"]) == {"31000"}


def test_cancel_offset_does_not_match_different_profit_channel():
    rows = [
        _cancel_offset_test_row("NORMAL_PACK", "29800", order_time="12:00:00", platform="PACK"),
        _cancel_offset_test_row("CANCEL_TABLE", "-29800", order_time="12:10:00", sale_type="취소", platform="TABLE"),
    ]

    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame(rows))

    status_by_order = offset.drop_duplicates("order_id").set_index("order_id")[menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN].to_dict()
    assert status_by_order["NORMAL_PACK"] == menu_hierarchy.CANCEL_OFFSET_NONE_STATUS
    assert status_by_order["CANCEL_TABLE"] == menu_hierarchy.CANCEL_OFFSET_UNMATCHED_STATUS


def test_cancel_offset_matches_duplicate_amounts_by_nearest_preceding_time():
    """같은 금액 후보가 여럿이면 취소 직전 주문을 고른다. 두 취소가 같은 주문을 가져가지 않는다."""
    rows = [
        _cancel_offset_test_row("NORMAL1", "10000", order_time="12:00:00"),
        _cancel_offset_test_row("NORMAL2", "10000", order_time="12:01:00"),
        _cancel_offset_test_row("NORMAL3", "10000", order_time="12:02:00"),
        _cancel_offset_test_row("CANCEL1", "-10000", order_time="12:10:00", sale_type="취소"),
        _cancel_offset_test_row("CANCEL2", "-10000", order_time="12:11:00", sale_type="취소"),
    ]

    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame(rows))
    by_order = offset.drop_duplicates("order_id").set_index("order_id")

    # 취소 2건은 시각이 가까운 NORMAL3, NORMAL2를 가져가고 가장 이른 NORMAL1이 남는다.
    assert by_order.at["NORMAL3", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NORMAL_STATUS
    assert by_order.at["NORMAL2", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NORMAL_STATUS
    assert by_order.at["NORMAL1", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NONE_STATUS
    assert by_order.at["CANCEL1", menu_hierarchy.CANCEL_OFFSET_CANDIDATES_COLUMN] == "3"
    assert by_order.at["CANCEL2", menu_hierarchy.CANCEL_OFFSET_CANDIDATES_COLUMN] == "3"
    # 서로 다른 묶음에 들어갔는지
    groups = {by_order.at[o, menu_hierarchy.CANCEL_OFFSET_GROUP_COLUMN] for o in ("CANCEL1", "CANCEL2")}
    assert len(groups) == 2


def test_cancel_offset_picks_order_just_before_cancel_not_earliest():
    """실제 사례: 같은 날 40,500원 정상 3건 중 취소 직전(17:27) 주문이 묶여야 한다.

    예전에는 그날 가장 이른 11:51 주문이 묶여서, 점심 메뉴가 저녁 취소와 상계됐다.
    """
    rows = [
        _cancel_offset_test_row("LUNCH", "40500", order_time="11:51:27"),
        _cancel_offset_test_row("DINNER1", "40500", order_time="17:27:06"),
        _cancel_offset_test_row("CANCEL", "-40500", order_time="18:25:10", sale_type="취소"),
        _cancel_offset_test_row("DINNER2", "40500", order_time="18:26:35"),
    ]

    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame(rows))
    by_order = offset.drop_duplicates("order_id").set_index("order_id")

    assert by_order.at["DINNER1", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NORMAL_STATUS
    assert by_order.at["LUNCH", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NONE_STATUS
    assert by_order.at["DINNER2", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NONE_STATUS
    assert by_order.at["CANCEL", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_CANCEL_STATUS


def test_cancel_offset_falls_back_to_later_normal_when_none_precedes():
    """취소가 정상보다 먼저 찍힌 경우(2026-07-14 79,900원)도 붙는다."""
    rows = [
        _cancel_offset_test_row("CANCEL", "-79900", order_time="12:12:05", sale_type="취소"),
        _cancel_offset_test_row("NORMAL", "79900", order_time="12:14:22"),
    ]

    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame(rows))
    by_order = offset.drop_duplicates("order_id").set_index("order_id")

    assert by_order.at["NORMAL", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_NORMAL_STATUS
    assert by_order.at["CANCEL", menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_CANCEL_STATUS


def test_unmatched_cancel_is_not_manual_profit_target(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))
    offset = menu_hierarchy._attach_cancel_offset_columns(pd.DataFrame([
        _cancel_offset_test_row("CANCEL_ONLY", "-29800", order_time="12:10:00", sale_type="취소"),
    ]))
    with_profit = menu_hierarchy._attach_profit_sales_base(offset)
    with_profit = menu_hierarchy._attach_manual_profit_columns(with_profit, rate_master=pd.DataFrame())
    profit = menu_hierarchy._build_manual_profit_rate_master(with_profit)
    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), with_profit, pd.DataFrame())

    assert with_profit.iloc[0][menu_hierarchy.CANCEL_OFFSET_STATUS_COLUMN] == menu_hierarchy.CANCEL_OFFSET_UNMATCHED_STATUS
    assert with_profit.iloc[0]["수익키"] == ""
    assert profit.empty
    assert "cancel_offset_unmatched" in set(issues["issue_type"])


def _price_match_test_row(
    order_id: str,
    item_name: str,
    total_price: str,
    chicken_option_key: str,
    chicken_type: str,
    chicken_size: str,
    *,
    signal: str,
    method: str,
) -> dict[str, str]:
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-01",
        "order_id": order_id,
        "order_time": "12:00:00",
        "item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "platform": "홀",
        "order_type": "홀_테이블",
        "item_name": item_name,
        "menu_name": item_name,
        "std_menu_name": "도리당 닭도리탕",
        "qty": "1",
        "unit_price": total_price,
        "total_price": total_price,
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: chicken_option_key,
        menu_hierarchy.OPTION_COMBO_COLUMN: chicken_option_key,
        "닭유형": chicken_type,
        "사이즈": chicken_size,
        menu_hierarchy.CHICKEN_USAGE_COLUMN: menu_hierarchy._usage_for(chicken_type, chicken_size),
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: menu_hierarchy._usage_for(chicken_type, chicken_size),
        "닭유형_판정": method,
        "사이즈_판정": method,
        menu_hierarchy.CHICKEN_SIGNAL_COLUMN: signal,
    })
    return row


def test_option_none_uses_price_matched_classified_order_without_defaulting():
    rows = [
        _price_match_test_row(
            "EVIDENCE",
            "도리당 닭도리탕",
            "31800",
            "[중] 2인 | 순살",
            "순살",
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
            method="선택",
        ),
        _price_match_test_row(
            "OPTION_NONE",
            "도리당 닭도리탕",
            "31800",
            menu_hierarchy.OPTION_COMBO_NONE,
            menu_hierarchy.CHICKEN_TYPE_MIXED,
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
            method="판정옵션",
        ),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == "순살"
    assert target["사이즈"] == "중"
    assert target["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_PRICE_MATCH
    assert target[menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_ABSENT


def test_option_none_price_match_keeps_ambiguous_price_unclassified():
    rows = [
        _price_match_test_row("BONE", "도리당 닭도리탕", "29800", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("BONELESS", "도리당 닭도리탕", "29800", "[중] 2인 | 순살", "순살", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("OPTION_NONE", "도리당 닭도리탕", "29800", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="판정옵션"),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert target["닭유형_판정"] == "판정옵션"


def test_option_none_price_match_uses_dominant_classified_price_pattern():
    rows = [
        _price_match_test_row(f"BONE{i}", "도리당 닭도리탕", "29800", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택")
        for i in range(4)
    ]
    rows.append(
        _price_match_test_row("NOISE", "도리당 닭도리탕", "29800", "[대] 3인 | 순살", "순살", "대", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택")
    )
    rows.append(
        _price_match_test_row("OPTION_NONE", "도리당 닭도리탕", "29800", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="판정옵션")
    )

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == "뼈닭"
    assert target["사이즈"] == "중"
    assert target["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_PRICE_MATCH


def test_option_none_price_match_does_not_override_visible_half_menu():
    rows = [
        _price_match_test_row("EVIDENCE", "도리당 닭도리탕", "47500", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("HALF", "베스트 반반 [3~6인]", "47500", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="판정옵션"),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("HALF")].iloc[0]

    assert target["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert target["닭유형_판정"] == "판정옵션"


def test_option_none_price_match_does_not_classify_cancel_or_negative_rows():
    rows = [
        _price_match_test_row("EVIDENCE", "도리당 닭도리탕", "31800", "[중] 2인 | 순살", "순살", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("CANCEL", "도리당 닭도리탕", "-31800", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="판정옵션"),
    ]
    rows[1]["sale_type"] = "취소"

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("CANCEL")].iloc[0]

    assert target["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert target["닭유형_판정"] == "판정옵션"


def test_option_none_visible_text_signal_overrides_judgement_option_mixed():
    rows = [
        _price_match_test_row(
            "VISIBLE",
            "[1인] 순살 닭도리탕 (밥포함) 1인분",
            "16900",
            menu_hierarchy.OPTION_COMBO_NONE,
            menu_hierarchy.CHICKEN_TYPE_MIXED,
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
            method="판정옵션",
        )
    ]

    out = menu_hierarchy._repair_option_none_attrs_from_visible_text(pd.DataFrame(rows))
    target = out[out["order_id"].eq("VISIBLE")].iloc[0]

    assert target["닭유형"] == "순살"
    assert target["사이즈"] == "1인"
    assert target["닭유형_판정"] == "메뉴명"
    assert target[menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_PRESENT


def test_option_none_near_price_match_uses_nearest_dominant_pattern():
    rows = [
        _price_match_test_row(f"BONE{i}", "도리당 닭도리탕", "29800", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택")
        for i in range(3)
    ]
    rows.append(
        _price_match_test_row("OPTION_NONE", "도리당 닭도리탕", "29900", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT, method="판정옵션")
    )

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == "뼈닭"
    assert target["사이즈"] == "중"
    assert target["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_NEAR_PRICE_MATCH


def test_option_none_near_price_match_keeps_tied_nearest_price_unclassified():
    rows = [
        _price_match_test_row("BONE1", "도리당 닭도리탕", "29800", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("BONE2", "도리당 닭도리탕", "29800", "[중] 2인 | 뼈", "뼈닭", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("BONELESS1", "도리당 닭도리탕", "29800", "[중] 2인 | 순살", "순살", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("BONELESS2", "도리당 닭도리탕", "29800", "[중] 2인 | 순살", "순살", "중", signal=menu_hierarchy.CHICKEN_SIGNAL_PRESENT, method="선택"),
        _price_match_test_row("OPTION_NONE", "도리당 닭도리탕", "29900", menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.CHICKEN_TYPE_MIXED, "중", signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT, method="판정옵션"),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert target["닭유형_판정"] == "판정옵션"


def test_option_none_price_match_learns_from_manual_judgement_without_order_signal():
    rows = [
        _price_match_test_row(
            "MANUAL_EVIDENCE",
            "도리당 닭도리탕",
            "31800",
            "[중] 2인 | 선택없음",
            "순살",
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
            method="수기",
        ),
        _price_match_test_row(
            "OPTION_NONE",
            "도리당 닭도리탕",
            "31800",
            menu_hierarchy.OPTION_COMBO_NONE,
            menu_hierarchy.CHICKEN_TYPE_MIXED,
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
            method="판정옵션",
        ),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == "순살"
    assert target["사이즈"] == "중"
    assert target["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_PRICE_MATCH


def test_option_none_price_match_does_not_learn_from_near_price_match_itself():
    rows = [
        _price_match_test_row(
            "AUTO_NEAR",
            "도리당 닭도리탕",
            "31800",
            "[중] 2인 | 선택없음",
            "순살",
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
            method=menu_hierarchy.CHICKEN_METHOD_NEAR_PRICE_MATCH,
        ),
        _price_match_test_row(
            "OPTION_NONE",
            "도리당 닭도리탕",
            "31800",
            menu_hierarchy.OPTION_COMBO_NONE,
            menu_hierarchy.CHICKEN_TYPE_MIXED,
            "중",
            signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
            method="판정옵션",
        ),
    ]

    out = menu_hierarchy._repair_option_none_attrs_by_price(pd.DataFrame(rows))
    target = out[out["order_id"].eq("OPTION_NONE")].iloc[0]

    assert target["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert target["닭유형_판정"] == "판정옵션"


def test_chicken_decision_option_review_uses_chicken_key_not_representative_combo():
    row = _price_match_test_row(
        "OPTION_NONE_WITH_VISIBLE_OPTIONS",
        "도리당 닭도리탕",
        "31800",
        menu_hierarchy.OPTION_COMBO_NONE,
        "순살",
        "중",
        signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
        method=menu_hierarchy.CHICKEN_METHOD_PRICE_MATCH,
    )
    row[menu_hierarchy.OPTION_COMBO_COLUMN] = "라면사리 | 계란찜"
    row[menu_hierarchy.CHICKEN_USAGE_COLUMN] = "0.8"
    row[menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN] = "확정"
    row["수익키"] = "메뉴|홀|도리당 닭도리탕|중"
    row["수기수익"] = "1000"
    row["수익미산출사유"] = ""

    review = menu_hierarchy._build_chicken_decision_option_review(pd.DataFrame([row]))

    assert len(review) == 1
    assert review.iloc[0]["검토상태"] == "판정완료"
    assert review.iloc[0][menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == menu_hierarchy.OPTION_COMBO_NONE
    assert review.iloc[0][menu_hierarchy.OPTION_COMBO_COLUMN] == "라면사리 | 계란찜"


def test_chicken_decision_option_review_marks_near_price_match_for_review():
    row = _price_match_test_row(
        "NEAR_PRICE",
        "도리당 닭도리탕",
        "29900",
        menu_hierarchy.OPTION_COMBO_NONE,
        "뼈닭",
        "중",
        signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
        method=menu_hierarchy.CHICKEN_METHOD_NEAR_PRICE_MATCH,
    )
    row[menu_hierarchy.CHICKEN_USAGE_COLUMN] = "1"
    row[menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN] = "확정"
    row["수익키"] = "메뉴|홀|도리당 닭도리탕|중"
    row["수기수익"] = "1000"

    review = menu_hierarchy._build_chicken_decision_option_review(pd.DataFrame([row]))

    assert review.iloc[0]["검토상태"] == "검토권장"
    assert review.iloc[0]["입력위치"] == "판정옵션"


def test_chicken_decision_option_review_marks_missing_attrs_before_manual_profit():
    row = _price_match_test_row(
        "MISSING_ATTRS",
        "도리당 닭도리탕",
        "31800",
        menu_hierarchy.OPTION_COMBO_NONE,
        "",
        "",
        signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
        method="미해결",
    )
    row[menu_hierarchy.CHICKEN_USAGE_COLUMN] = ""
    row[menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN] = "입력필요"
    row["수익키"] = "메뉴|홀|도리당 닭도리탕|중"
    row["수기수익"] = ""
    row["수익미산출사유"] = "원가_manual미입력"

    review = menu_hierarchy._build_chicken_decision_option_review(pd.DataFrame([row]))

    assert review.iloc[0]["검토상태"] == "입력필요"
    assert review.iloc[0]["입력위치"] == "판정옵션"


def test_chicken_decision_option_review_marks_manual_profit_missing_when_classified():
    row = _price_match_test_row(
        "COST_MISSING",
        "도리당 닭도리탕",
        "31800",
        menu_hierarchy.OPTION_COMBO_NONE,
        "뼈닭",
        "중",
        signal=menu_hierarchy.CHICKEN_SIGNAL_ABSENT,
        method=menu_hierarchy.CHICKEN_METHOD_PRICE_MATCH,
    )
    row[menu_hierarchy.CHICKEN_USAGE_COLUMN] = "1"
    row[menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN] = "확정"
    row["수익키"] = "메뉴|홀|도리당 닭도리탕|중"
    row["수기수익"] = ""
    row["수익미산출사유"] = "원가_manual미입력"

    review = menu_hierarchy._build_chicken_decision_option_review(pd.DataFrame([row]))

    assert review.iloc[0]["검토상태"] == "원가입력필요"
    assert review.iloc[0]["입력위치"] == "수익률"


def test_manual_profit_profile_menu_key_omits_inferred_chicken_type_without_order_signal():
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "수익채널": "홀",
        "std_menu_name": "도리당 닭도리탕",
        "사이즈": "중",
        "닭유형": "뼈닭",
    }))[0]

    assert key == "메뉴|홀|도리당 닭도리탕|중"


def test_manual_profit_profile_menu_key_includes_visible_chicken_type():
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "수익채널": "홀",
        "std_menu_name": "순살 닭도리탕",
        "사이즈": "중",
        "닭유형": "순살",
        "닭유형_판정": "메뉴명",
        menu_hierarchy.CHICKEN_SIGNAL_COLUMN: menu_hierarchy.CHICKEN_SIGNAL_PRESENT,
    }))[0]

    assert key == "메뉴|홀|순살 닭도리탕|중|순살"


def test_manual_profit_profile_chicken_type_option_uses_option_signal():
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "수익채널": "홀",
        "std_menu_name": "도리당 닭도리탕",
        "item_name": "순살(닭다리살 100%) 300g",
        "사이즈": "중",
        "닭유형": "뼈닭",
    }))[0]

    assert key == "메뉴|홀|도리당 닭도리탕|중|순살"


def test_manual_profit_profile_size_option_uses_chicken_option_key_signal(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())
    menu_hierarchy._cached_menu_chicken_profile_lookup.cache_clear()
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
        "수익채널": "홀",
        "menu_name": "미나리 수삼 백숙",
        "std_menu_name": "미나리 수삼 백숙",
        "item_name": "[대] 3인",
        "사이즈": "중",
        "닭유형": "순살",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인 | 순살",
        menu_hierarchy.OPTION_COMBO_COLUMN: "[대] 3인 | 순살 | 육수 더 주세요 | 닭한마리]누룽지 추가",
    }))[0]

    assert key == "메뉴|홀|미나리 수삼 백숙|중|순살"


def test_manual_profit_profile_locked_menu_ignores_unsupported_boneless_option_signal():
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "수익채널": "홀",
        "menu_name": "닭칼국수",
        "std_menu_name": "[점심] 닭칼국수",
        "item_name": "순살",
        "사이즈": "중",
        "닭유형": "뼈닭",
    }))[0]

    assert key == "메뉴|홀|[점심] 닭칼국수|중"


def test_manual_profit_profile_non_chicken_locked_menu_keeps_none_type_for_cross_menu_option():
    key = menu_hierarchy._manual_profit_profile(pd.Series({
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "수익채널": "홀",
        "menu_name": "메밀 물 막국수 세트",
        "std_menu_name": "[점심] 메밀 물 막국수 세트",
        "item_name": "닭도리탕 [순살]",
        "사이즈": menu_hierarchy.CHICKEN_SIZE_NONE,
        "닭유형": menu_hierarchy.CHICKEN_TYPE_NONE,
    }))[0]

    assert key == "메뉴|홀|[점심] 메밀 물 막국수 세트|-"


def test_manual_profit_rate_master_inherits_from_legacy_five_axis_key(monkeypatch):
    previous = pd.DataFrame([{
        "수익키": "메뉴|홀|도리당 닭도리탕|중|뼈닭",
        "판매가": "20000",
        "메뉴원가_manual": "14000",
        "상차림비_manual": "",
        "메모": "기존 입력",
    }])
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "중",
        "qty": "1",
        "total_price": "20000",
        menu_hierarchy.PROFIT_SALES_COLUMN: "20000",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: previous)

    master = menu_hierarchy._build_manual_profit_rate_master(pd.DataFrame([row]))

    assert master.iloc[0]["수익키"] == "메뉴|홀|도리당 닭도리탕|중"
    assert master.iloc[0]["판매가"] == "20000"
    assert master.iloc[0]["상차림포함원가"] == "14000"
    assert master.iloc[0]["메모"] == "기존 입력"
    assert master.iloc[0]["계산닭유형"] == "뼈닭"


def test_validation_issues_warn_missing_manual_profit_rate_by_source():
    rows = []
    for source, platform, order_type, item_id in [
        ("okpos", "홀", "홀_테이블", "HALL1"),
        ("쿠팡수동", "쿠팡이츠", "배달", "COUPANG1"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "_pk": item_id,
            "source": source,
            "platform": platform,
            "order_type": order_type,
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-06-01",
            "ym": "2026-06",
            "order_id": f"O_{item_id}",
            "item_seq": "1",
            "parent_item_seq": "1",
            "menu_seq": "1",
            "item_id": item_id,
            "item_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
            "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
            "닭유형": "뼈닭",
            "사이즈": "중",
            "사용용량": "1",
            "total_price": "20000",
        })
        rows.append(row)

    with_profit_keys = menu_hierarchy._attach_manual_profit_columns(pd.DataFrame(rows), rate_master=pd.DataFrame())
    issues = menu_hierarchy._build_validation_issues(
        with_profit_keys,
        with_profit_keys,
        pd.DataFrame(),
    )
    manual_issues = issues[issues["issue_type"].eq("manual_profit_missing")]

    assert set(manual_issues["source"]) == {"okpos", "쿠팡수동"}
    assert set(manual_issues["severity"]) == {"WARN"}
    assert set(with_profit_keys["수익키"]) == {
        "메뉴|홀|도리당 닭도리탕|중",
        "메뉴|쿠팡이츠|도리당 닭도리탕|중",
    }


def test_new_classification_pattern_alert_sends_once(monkeypatch, tmp_path):
    sent = []
    state_path = tmp_path / "classification_pattern_alerts.json"
    issues = pd.DataFrame([{
        "issue_type": "profit_key_drift",
        "severity": "ERROR",
        "source": "okpos",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "detail": "수익키 축 불일치",
    }])
    completeness = pd.DataFrame(columns=menu_hierarchy.COMPLETENESS_COLUMNS)

    monkeypatch.setattr(menu_hierarchy, "CLASSIFICATION_PATTERN_ALERT_STATE_PATH", state_path)
    monkeypatch.setattr(menu_hierarchy, "send_telegram_chunks", lambda text: sent.append(text) or True)

    menu_hierarchy._notify_new_classification_patterns(issues, completeness)
    menu_hierarchy._notify_new_classification_patterns(issues, completeness)

    assert len(sent) == 1
    assert "profit_key_drift" in sent[0]


def test_classification_pattern_alert_ignores_input_wait_warn(monkeypatch, tmp_path):
    sent = []
    issues = pd.DataFrame([{
        "issue_type": "manual_profit_missing",
        "severity": "WARN",
        "source": "okpos",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "detail": "수기원가 미입력",
    }])
    completeness = pd.DataFrame([{
        "차원": "manual_profit",
        "분모": "1",
        "분자": "0",
        "완결률": "0",
        "기준선": "100",
        "상태": "후퇴",
        "미완요약": "수익률 입력 대기",
    }])

    monkeypatch.setattr(menu_hierarchy, "CLASSIFICATION_PATTERN_ALERT_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(menu_hierarchy, "send_telegram_chunks", lambda text: sent.append(text) or True)

    menu_hierarchy._notify_new_classification_patterns(issues, completeness)

    assert sent == []


def test_validation_issues_block_missing_delivery_commission_rate():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "_pk": "PK1",
        "source": "posfeed",
        "platform": "땡겨요",
        "order_type": "배달",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-07-01",
        "ym": "2026-07",
        "order_id": "ORDER1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "ITEM1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "중",
        "사용용량": "1",
        "total_price": "20000",
        "수익채널": "땡겨요",
        "수익키": "메뉴|땡겨요|도리당 닭도리탕|중",
        "수수료율": "",
        "수수료율_출처": "없음",
    })

    issues = menu_hierarchy._build_validation_issues(
        pd.DataFrame([row]),
        pd.DataFrame([row]),
        pd.DataFrame(),
    )
    commission_issues = issues[issues["issue_type"].eq("commission_missing")]

    assert len(commission_issues) == 1
    assert commission_issues.iloc[0]["severity"] == "ERROR"


def test_validation_does_not_block_unsupported_commission_platform_when_profit_key_tracks_channel():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "_pk": "PK1",
        "source": "posfeed",
        "platform": "땡겨요",
        "order_type": "배달",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-07-01",
        "ym": "2026-07",
        "order_id": "ORDER1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "ITEM1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "중",
        "사용용량": "1",
        "total_price": "20000",
        "수익채널": "땡겨요",
        "수익키": "메뉴|땡겨요|도리당 닭도리탕|중",
        "수수료율": "",
        "수수료율_출처": "마트미지원",
    })

    issues = menu_hierarchy._build_validation_issues(
        pd.DataFrame([row]),
        pd.DataFrame([row]),
        pd.DataFrame(),
    )

    assert "commission_missing" not in set(issues["issue_type"])


def test_repair_missing_half_menu_attrs_sets_default_slots_and_clears_reason():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "_pk": "PK1",
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-20",
        "ym": "2026-06",
        "order_id": "ORDER1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "HALF1",
        "item_name": "베스트 반반세트 [3~6인]",
        "menu_name": "베스트 반반세트 [3~6인]",
        "std_menu_name": "베스트 반반세트",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "",
        "사이즈": "대",
        "qty": "1",
        "미해결사유": "닭유형없음 | 반반슬롯미입력",
    })

    out = menu_hierarchy._repair_missing_half_menu_attrs(pd.DataFrame([row]))
    fixed = out.iloc[0]

    assert fixed["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert fixed["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_HALF_SLOT
    assert fixed[menu_hierarchy.HALF_COMBO_COLUMN] == "뼈+순살"
    assert fixed[menu_hierarchy.HALF_SLOT1_COLUMN] == "뼈닭"
    assert fixed[menu_hierarchy.HALF_SLOT2_COLUMN] == "순살"
    assert fixed["미해결사유"] == ""


def test_chicken_addon_defaults_300g_to_one_bird_when_conversion_sheet_blank(monkeypatch):
    main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    main.update({
        "_pk": "PK1",
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-11",
        "ym": "2026-08",
        "order_id": "ORDER1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "순살",
        "사이즈": "중",
        "qty": "1",
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "0.8",
        menu_hierarchy.BONE_USAGE_TOTAL_COLUMN: "0",
        menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN: "0.8",
    })
    addon = main.copy()
    addon.update({
        "_pk": "PK2",
        "item_seq": "2",
        "parent_item_seq": "1",
        "item_name": "순살 (100%닭다리살)300g 추가",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
        "qty": "1",
    })
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", lambda sheet: pd.DataFrame())

    out = menu_hierarchy._attach_chicken_addon_columns(pd.DataFrame([main, addon]))
    main_out = out[out["line_role"].eq("main")].iloc[0]
    addon_out = out[out["line_role"].eq("option")].iloc[0]

    assert addon_out[menu_hierarchy.CHICKEN_ADDON_USAGE_COLUMN] == "1"
    assert addon_out[menu_hierarchy.CHICKEN_ADDON_BONELESS_COLUMN] == "1"
    assert addon_out[menu_hierarchy.CHICKEN_ADDON_REASON_COLUMN] == ""
    assert main_out[menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "1.8"
    assert main_out[menu_hierarchy.CHICKEN_ADDON_REASON_COLUMN] == ""


def test_validation_does_not_flag_item_level_discount_when_order_total_positive():
    main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    main.update({
        "_pk": "PK1",
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-04-17",
        "ym": "2026-04",
        "order_id": "ORDER1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "total_price": "20000",
        "discount_amount": "0",
        "수익률": "0",
        "추정수익": "0",
    })
    discounted_option = main.copy()
    discounted_option.update({
        "_pk": "PK2",
        "item_seq": "2",
        "parent_item_seq": "1",
        "item_id": "OPT1",
        "item_name": "펩시콜라 355ml (캔)",
        "line_role": "option",
        "option_kind": "음료",
        "total_price": "0",
        "discount_amount": "2000",
    })

    issues = menu_hierarchy._build_validation_issues(
        pd.DataFrame([main, discounted_option]),
        pd.DataFrame([main, discounted_option]),
        pd.DataFrame(),
    )

    assert "order_exception_unresolved" not in set(issues["issue_type"])
    assert "full_discount_without_exception" not in set(issues["issue_type"])


def test_order_exception_policy_groups_reused_order_id_by_sale_date():
    paid = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    paid.update({
        "_pk": "PK1",
        "source": "okpos",
        "sale_date": "2026-05-15",
        "order_id": "송파삼전점_1-25_19:30:12",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "total_price": "29800",
        "discount_amount": "0",
    })
    discounted = paid.copy()
    discounted.update({
        "_pk": "PK2",
        "sale_date": "2026-06-07",
        "total_price": "0",
        "discount_amount": "29800",
    })

    out = menu_hierarchy._apply_order_exception_policy(pd.DataFrame([paid, discounted]), order_exception_input=pd.DataFrame())
    old_order = out[out["sale_date"].eq("2026-05-15")].iloc[0]
    full_discount = out[out["sale_date"].eq("2026-06-07")].iloc[0]

    assert old_order[menu_hierarchy.ORDER_EXCEPTION_TYPE_COLUMN] == ""
    assert full_discount[menu_hierarchy.ORDER_EXCEPTION_TYPE_COLUMN] == "전액할인"
    assert full_discount[menu_hierarchy.ADJUSTED_SALES_COLUMN] == "0"


def test_coupang_priced_row_uses_parent_menu_as_item_name(monkeypatch):
    raw = pd.DataFrame([
        {
            "order_date": "2026.07.01 20:44",
            "order_id": "ORDER1",
            "delivery_type": "배달",
            "order_summary": "[한우 대창] 순살 곱도리탕 외 1건",
            "total_price": "34500",
            "is_cancelled": "N",
            "menu_name": "[한우 대창] 순살 곱도리탕",
            "menu_qty": "1",
            "menu_price": "29500",
            "menu_options": "[후.참] 분모자",
            "매출액": "34500",
        },
        {
            "order_date": "2026.07.01 20:44",
            "order_id": "ORDER1",
            "delivery_type": "배달",
            "order_summary": "[한우 대창] 순살 곱도리탕 외 1건",
            "total_price": "34500",
            "is_cancelled": "N",
            "menu_name": "[한우 대창] 순살 곱도리탕",
            "menu_qty": "1",
            "menu_price": "",
            "menu_options": "기본맛",
            "매출액": "",
        },
    ])

    class Lookup:
        def item_id(self, source, brand, store, item_name, price):
            return item_name

    monkeypatch.setattr(menu_hierarchy, "_find_partition_files", lambda root, ym, filename: ["dummy.parquet"])
    monkeypatch.setattr(menu_hierarchy.pd, "read_parquet", lambda path: raw)
    monkeypatch.setattr(menu_hierarchy, "_path_part", lambda path, prefix: "도리당")
    monkeypatch.setattr(menu_hierarchy, "_attach_common_fields", lambda out: out.assign(store="송파삼전점", region="", 담당자="", 실오픈일="", collected_at=""))
    monkeypatch.setattr(
        menu_hierarchy,
        "_finalize_hierarchy",
        lambda out, boundary, attr_method, lookup, name_col="item_name": out.assign(_boundary=boundary, attr_method=attr_method),
    )

    out = menu_hierarchy._build_coupang("2026-07", Lookup())

    assert out.loc[0, "item_name"] == "[한우 대창] 순살 곱도리탕"
    assert out.loc[0, "item_id"] == "[후.참] 분모자"
    assert out.loc[0, "_raw_item_name"] == "[후.참] 분모자"
    assert out.loc[0, "_hier_name"] == "[한우 대창] 순살 곱도리탕"
    assert bool(out.loc[0, "_boundary"])
    assert out.loc[1, "item_name"] == "기본맛"
    assert out.loc[1, "item_id"] == "기본맛"
    assert not bool(out.loc[1, "_boundary"])


def test_baemin_build_uses_source_gross_amount_for_menu_hierarchy(monkeypatch):
    raw = pd.DataFrame([
        {
            "주문시각": "2026. 07. 01. (수) 오후 8:44:00",
            "주문번호": "BORDER1",
            "주문상태": "배달완료",
            "수령방법": "배달",
            "주문내역": "테스트 메뉴 외 1건",
            "주문옵션상세": "테스트 메뉴",
            "주문수량": "1",
            "주문옵션금액": "25000",
            "상품금액": "30000",
            "결제금액": "27000",
        },
        {
            "주문시각": "2026. 07. 01. (수) 오후 8:44:00",
            "주문번호": "BORDER1",
            "주문상태": "배달완료",
            "수령방법": "배달",
            "주문내역": "테스트 메뉴 외 1건",
            "주문옵션상세": "추가 옵션",
            "주문수량": "1",
            "주문옵션금액": "5000",
            "상품금액": "",
            "결제금액": "",
        },
    ])

    class Lookup:
        def item_id(self, source, brand, store, item_name, price):
            return item_name

        def is_main(self, source, brand, store, item_id, item_name, price):
            return item_name == "테스트 메뉴"

        def is_standalone_candidate(self, source, brand, store, item_id, item_name, price, menu_vocab):
            return False

    monkeypatch.setattr(menu_hierarchy, "_find_partition_files", lambda root, ym, filename: ["dummy.parquet"])
    monkeypatch.setattr(menu_hierarchy.pd, "read_parquet", lambda path: raw)
    monkeypatch.setattr(menu_hierarchy, "_path_part", lambda path, prefix: "도리당")
    monkeypatch.setattr(menu_hierarchy, "_attach_common_fields", lambda out: out.assign(store="송파삼전점", region="", 담당자="", 실오픈일="", collected_at=""))
    monkeypatch.setattr(
        menu_hierarchy,
        "_finalize_hierarchy",
        lambda out, boundary, attr_method, lookup, name_col="item_name", **kwargs: out.assign(_boundary=boundary, attr_method=attr_method),
    )

    out = menu_hierarchy._build_baemin("2026-07", Lookup())

    assert out["total_price"].astype(int).sum() == 30000
    assert out["discount_amount"].astype(int).sum() == 3000
    assert out.loc[out["item_name"].eq("테스트 메뉴"), "total_price"].astype(int).iloc[0] == 25000
    assert out.loc[out["item_name"].eq("추가 옵션"), "total_price"].astype(int).iloc[0] == 5000


def test_coupang_item_id_name_uses_parent_for_generic_priced_option():
    assert menu_hierarchy._coupang_item_id_name("미니 계란찜", "기본", True) == "미니 계란찜"
    assert menu_hierarchy._coupang_item_id_name("[재주문 1위] 도리당 닭도리탕", "[후.참] 분모자", True) == "[후.참] 분모자"
    assert menu_hierarchy._coupang_item_id_name("[재주문 1위] 도리당 닭도리탕", "기본맛", False) == "기본맛"


def test_assign_menu_hierarchy_sorts_item_seq_numerically():
    df = pd.DataFrame([
        {"order_id": "O1", "item_seq": "1", "item_name": "메인1"},
        {"order_id": "O1", "item_seq": "2", "item_name": "옵션1"},
        {"order_id": "O1", "item_seq": "10", "item_name": "옵션2"},
        {"order_id": "O1", "item_seq": "11", "item_name": "메인2"},
    ])
    boundary = pd.Series([True, False, False, True], index=df.index)

    out = menu_hierarchy.assign_menu_hierarchy(df, boundary, sort_cols=["item_seq"])

    assert out.loc[1, "parent_item_seq"] == "1"
    assert out.loc[2, "parent_item_seq"] == "1"
    assert out.loc[3, "parent_item_seq"] == "11"


def test_build_orders_writes_unified_and_left_joined_order_files(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "platform": "배달의민족",
        "order_id": "ORDER1",
        "item_seq": "1",
        "item_id": "ITEM1",
        "item_name": "옵션",
        "_pk": "PK1",
        "menu_seq": "1",
        "line_role": "main",
        "parent_item_seq": "1",
        "std_menu_name": "표준",
        "attr_method": "raw_parent",
        "_canonical": True,
    })
    written = {}
    workbooks = {}
    texts = {}

    monkeypatch.setattr(menu_hierarchy, "resolve_yms", lambda ym=None: ["2026-06"])
    monkeypatch.setattr(menu_hierarchy, "_all_source_orders", lambda ym: pd.DataFrame([row]))
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_option_material_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_menu_weight_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_build_gap", lambda canonical: pd.DataFrame(columns=menu_hierarchy.GAP_COLUMNS))
    monkeypatch.setattr(menu_hierarchy, "_write_jsonl", lambda rows, path: written.setdefault(path.name, list(rows)))
    monkeypatch.setattr(
        menu_hierarchy,
        "_write_csv",
        lambda df, path: written.setdefault(path.name, list(df.columns)),
    )
    monkeypatch.setattr(
        menu_hierarchy,
        "_write_excel_workbook",
        lambda sheets, path: workbooks.setdefault(path.name, {name: list(df.columns) for name, df in sheets.items()}),
    )
    monkeypatch.setattr(menu_hierarchy, "_write_text", lambda path, text: texts.setdefault(path.name, text))

    menu_hierarchy.build_orders(None, debug_outputs=True, archive_legacy=False)

    assert written["02_최종주문.csv"] == menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS
    assert set(workbooks["01_수기입력.xlsx"]) == {
            "수익률",
            "수익률_미매칭보존",
            "메뉴명보정",
            "메뉴중량",
            "옵션분류",
        "재료단가",
        "뼈순살비율",
        "메뉴닭프로필",
        "판정옵션",
        "닭환산",
        "옵션재료",
        "예외주문",
        "예외보정",
    }
    assert {"메뉴별_닭사용량", "메뉴별_수익", "source별_분류검증", "미분류_TOP", "판정옵션_적용결과", "완결률", "검증이슈"}.issubset(
        set(workbooks["03_요약.xlsx"])
    )
    assert "04_사용법.md" in texts
    assert written["10_orders.csv"] == menu_hierarchy.ORDER_OUTPUT_COLUMNS
    assert "line_role" not in written["10_orders.csv"]
    assert "std_menu_name" not in written["10_orders.csv"]
    assert written["11_hierarchy.csv"] == menu_hierarchy.HIERARCHY_OUTPUT_COLUMNS
    assert written["12_orders_left.csv"] == menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS
    assert written["13_manager_input.csv"] == menu_hierarchy.MANAGER_INPUT_COLUMNS
    assert written["14_manager_input_llm_payload.jsonl"] == []
    assert written["15_manager_input_llm_result.jsonl"] == []
    assert written["16_option_material_input.csv"] == menu_hierarchy.OPTION_MATERIAL_INPUT_COLUMNS
    assert written["17_menu_weight_input.csv"] == menu_hierarchy.MENU_WEIGHT_INPUT_COLUMNS
    assert written["18_material_usage_summary.csv"] == menu_hierarchy.MATERIAL_USAGE_SUMMARY_COLUMNS
    assert written["04_product_gap.csv"] == menu_hierarchy.GAP_COLUMNS
    assert written["19_validation_issues.csv"] == menu_hierarchy.VALIDATION_ISSUE_COLUMNS
    assert written["20_classification_audit.csv"] == menu_hierarchy.CLASSIFICATION_AUDIT_COLUMNS
    assert "line_role" in written["12_orders_left.csv"]
    assert "std_menu_name" in written["12_orders_left.csv"]
    assert menu_hierarchy.OPTION_COMBO_COLUMN in written["12_orders_left.csv"]
    assert menu_hierarchy.MATERIAL_USAGE_COLUMN in written["12_orders_left.csv"]
    assert menu_hierarchy.MENU_WEIGHT_USAGE_COLUMN in written["12_orders_left.csv"]
    assert menu_hierarchy.OPTION_MATERIAL_USAGE_COLUMN in written["12_orders_left.csv"]
    for column in [
        menu_hierarchy.OPTION_COMBO_COLUMN,
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN,
        *menu_hierarchy.CHICKEN_FINAL_COLUMNS,
        menu_hierarchy.MATERIAL_USAGE_COLUMN,
        menu_hierarchy.MENU_WEIGHT_USAGE_COLUMN,
        menu_hierarchy.OPTION_MATERIAL_USAGE_COLUMN,
        *menu_hierarchy.MANUAL_PROFIT_COLUMNS,
        *menu_hierarchy.CHICKEN_TRACE_COLUMNS,
    ]:
        assert column in written["02_최종주문.csv"]


def test_validation_issues_detects_blocking_cases():
    good_main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    good_main.update({
        "_pk": "PK1",
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "total_price": "20000",
        "수익률": "30%",
        "추정수익": "6000",
    })
    bad_option = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    bad_option.update({
        "_pk": "PK2",
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O2",
        "item_seq": "2",
        "parent_item_seq": "9",
        "menu_seq": "1",
        "item_id": "TMP_abc",
        "item_name": "도리당 닭도리탕",
        "line_role": "option",
        "std_menu_name": "도리당 닭도리탕",
        "total_price": "21000",
        "사용용량": "1",
        "미해결사유": "메인없음",
    })
    orders = pd.DataFrame([
        {"_pk": "PK1"},
        {"_pk": "PK2"},
    ])
    gap = pd.DataFrame([{
        "item_id": "TMP_abc",
        "source": "쿠팡수동",
        "item_name": "도리당 닭도리탕",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "main",
    }])

    issues = menu_hierarchy._build_validation_issues(orders, pd.DataFrame([good_main, bad_option]), gap)

    assert {
        "product_gap",
        "tmp_item",
        "option_orphan",
        "zero_main_candidate",
        "profit_missing",
        "chicken_attr_unresolved",
        "non_main_chicken_usage",
    }.issubset(set(issues["issue_type"]))
    severity_by_type = issues.drop_duplicates("issue_type").set_index("issue_type")["severity"].to_dict()
    assert severity_by_type["profit_missing"] == "WARN"
    assert severity_by_type["chicken_attr_unresolved"] == "ERROR"


def test_validation_issues_ignores_tmp_discount_rows():
    discount = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    discount.update({
        "_pk": "PK_DISCOUNT",
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-12",
        "ym": "2026-08",
        "order_id": "O_DISCOUNT",
        "item_seq": "8",
        "parent_item_seq": "8",
        "menu_seq": "3",
        "item_id": "TMP_discount",
        "item_name": "배달앱 금액 표기 오류 (-25000원/할인 금액 참고)",
        "line_role": "discount",
        "option_kind": "배달비수수료",
        "std_menu_name": "",
        "total_price": "0",
        "수익매출": "0",
    })
    gap = pd.DataFrame([{
        "item_id": "TMP_discount",
        "source": "posfeed",
        "item_name": "배달앱 금액 표기 오류 (-25000원/할인 금액 참고)",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "discount",
    }])

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([discount]), gap)

    assert "tmp_item" not in set(issues["issue_type"])
    assert "product_gap" not in set(issues["issue_type"])


def test_validation_issues_detects_semantic_menu_hierarchy_conflicts():
    taste_main = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    taste_main.update({
        "_pk": "PK1",
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "BASIC",
        "item_name": "기본맛",
        "line_role": "main",
        "std_menu_name": "기본맛",
        "total_price": "0",
    })
    bad_chicken = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    bad_chicken.update({
        "_pk": "PK2",
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O2",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
        "line_role": "main",
        "std_menu_name": "[재주문 1위] 도리당 닭도리탕",
        "닭유형": "뼈닭",
        "사이즈": "중",
        "사용용량": "1",
        "수익률": "0",
        "추정수익": "0",
        "total_price": "16900",
    })

    issues = menu_hierarchy._build_validation_issues(
        pd.DataFrame([{"_pk": "PK1"}, {"_pk": "PK2"}]),
        pd.DataFrame([taste_main, bad_chicken]),
        pd.DataFrame(),
    )

    assert {
        "option_like_main",
        "chicken_attr_conflict",
        "chicken_size_conflict",
        "fixed_main_chicken_mismatch",
    }.issubset(set(issues["issue_type"]))


def test_validation_issues_warns_parent_child_deciding_option_conflicts(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())
    menu_hierarchy._cached_menu_chicken_profile_lookup.cache_clear()
    parent = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    parent.update({
        "_pk": "PK1",
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "line_role": "main",
        "std_menu_name": "도리당 닭도리탕",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "중",
        "total_price": "29800",
        "수익매출": "29800",
        "수익키": "메뉴|홀|도리당 닭도리탕|중|뼈닭",
    })
    child = parent.copy()
    child.update({
        "_pk": "PK2",
        "item_seq": "2",
        "parent_item_seq": "1",
        "item_id": "OPT1",
        "item_name": "순살",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "total_price": "2000",
        "수익매출": "2000",
    })

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([parent, child]), pd.DataFrame())

    conflict = issues[issues["issue_type"].eq("parent_child_chicken_type_conflict")]
    assert len(conflict) == 1
    assert conflict.iloc[0]["severity"] == "WARN"


def test_validation_ignores_fixed_lunch_parent_child_option_conflict(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_menu_chicken_profile_attrs", lambda: pd.DataFrame())
    menu_hierarchy._cached_menu_chicken_profile_lookup.cache_clear()
    parent = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    parent.update({
        "_pk": "PK1",
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "닭칼국수",
        "line_role": "main",
        "std_menu_name": "[점심] 닭칼국수",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": "뼈닭",
        "사이즈": "중",
        "total_price": "9000",
        "수익매출": "9000",
        "수익키": "메뉴|홀|[점심] 닭칼국수|중",
    })
    child = parent.copy()
    child.update({
        "_pk": "PK2",
        "item_seq": "2",
        "parent_item_seq": "1",
        "item_id": "OPT1",
        "item_name": "순살",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "total_price": "0",
        "수익매출": "0",
    })
    size_child = parent.copy()
    size_child.update({
        "_pk": "PK3",
        "item_seq": "3",
        "parent_item_seq": "1",
        "item_id": "OPT2",
        "item_name": "[대] 3인",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
        "total_price": "8000",
        "수익매출": "8000",
    })

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([parent, child, size_child]), pd.DataFrame())

    assert "parent_child_chicken_type_conflict" not in set(issues["issue_type"])
    assert "parent_child_size_conflict" not in set(issues["issue_type"])


def test_validation_issues_warns_set_and_single_under_same_profit_key():
    single = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    single.update({
        "_pk": "PK1",
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "order_id": "O1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "메밀 물 막국수",
        "line_role": "main",
        "std_menu_name": "[점심] 메밀 물 막국수 세트",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "닭유형": menu_hierarchy.CHICKEN_TYPE_NONE,
        "사이즈": menu_hierarchy.CHICKEN_SIZE_NONE,
        "total_price": "8500",
        "수익매출": "8500",
        "수익키": "메뉴|홀|[점심] 메밀 물 막국수 세트|-",
    })
    set_row = single.copy()
    set_row.update({
        "_pk": "PK2",
        "order_id": "O2",
        "item_name": "메밀 물 막국수 세트",
        "total_price": "11000",
        "수익매출": "11000",
    })

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([single, set_row]), pd.DataFrame())

    mixed = issues[issues["issue_type"].eq("profit_key_set_single_mixed")]
    assert len(mixed) == 1
    assert mixed.iloc[0]["severity"] == "WARN"


def test_manual_profit_profile_uses_std_name_when_std_has_set_identity():
    row = pd.Series({
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_name": "메밀 물 막국수",
        "std_menu_name": "[점심] 메밀 물 막국수 세트",
        "사이즈": menu_hierarchy.CHICKEN_SIZE_NONE,
        "닭유형": menu_hierarchy.CHICKEN_TYPE_NONE,
        "닭유형_판정": "메뉴프로필",
    })

    key, name, kind = menu_hierarchy._manual_profit_profile(row)

    assert key == "메뉴|홀|[점심] 메밀 물 막국수 세트|-"
    assert name == "[점심] 메밀 물 막국수 세트"
    assert kind == menu_hierarchy.OPTION_KIND_MAIN


def test_manual_profit_profile_non_chicken_row_omits_visible_chicken_signal():
    row = pd.Series({
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
        "item_name": "순살",
        "std_menu_name": "미나리 비빔칼국수",
        "사이즈": menu_hierarchy.CHICKEN_SIZE_NONE,
        "닭유형": menu_hierarchy.CHICKEN_TYPE_NONE,
        "닭유형_판정": "메뉴프로필",
    })

    key, name, kind = menu_hierarchy._manual_profit_profile(row)

    assert key == "메뉴|홀|미나리 비빔칼국수|-"
    assert name == "미나리 비빔칼국수"
    assert kind == menu_hierarchy.OPTION_KIND_MAIN


def test_manual_profit_profile_keeps_set_item_when_std_name_is_single():
    row = pd.Series({
        "source": "okpos",
        "platform": "홀",
        "order_type": "홀_테이블",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_name": "시그니처 반반세트 [3~6인]",
        "std_menu_name": "시그니처 반반",
        "사이즈": "중",
        "닭유형": "혼합",
        "닭유형_판정": menu_hierarchy.CHICKEN_METHOD_HALF,
    })

    key, name, kind = menu_hierarchy._manual_profit_profile(row)

    assert key == "메뉴|홀|시그니처 반반세트|중|혼합"
    assert name == "시그니처 반반세트"
    assert kind == menu_hierarchy.OPTION_KIND_MAIN


def test_build_orders_fails_when_validation_issues_remain(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "_pk": "PK1",
        "sale_date": "2026-06-01",
        "ym": "2026-06",
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "platform": "쿠팡이츠",
        "order_id": "ORDER1",
        "item_seq": "1",
        "item_id": "TMP_abc",
        "item_name": "도리당 닭도리탕",
        "menu_seq": "1",
        "line_role": "main",
        "parent_item_seq": "1",
        "std_menu_name": "도리당 닭도리탕",
        "total_price": "20000",
        "_canonical": True,
    })
    written = {}

    monkeypatch.setattr(menu_hierarchy, "resolve_yms", lambda ym=None: ["2026-06"])
    monkeypatch.setattr(menu_hierarchy, "_all_source_orders", lambda ym: pd.DataFrame([row]))
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_option_material_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_menu_weight_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_build_gap", lambda canonical: pd.DataFrame([{
        "item_id": "TMP_abc",
        "source": "쿠팡수동",
        "item_name": "도리당 닭도리탕",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "main",
    }]))
    monkeypatch.setattr(menu_hierarchy, "_write_jsonl", lambda rows, path: None)

    def fake_write_csv(df, path):
        written[path.name] = df.copy()

    monkeypatch.setattr(menu_hierarchy, "_write_csv", fake_write_csv)
    monkeypatch.setattr(menu_hierarchy, "_write_excel_workbook", lambda sheets, path: None)
    monkeypatch.setattr(menu_hierarchy, "_write_text", lambda path, text: None)

    with pytest.raises(RuntimeError, match="메뉴계층 검증 실패"):
        menu_hierarchy.build_orders(None, debug_outputs=True, archive_legacy=False)

    assert "19_validation_issues.csv" in written
    assert {"product_gap", "tmp_item", "profit_missing"}.issubset(
        set(written["19_validation_issues.csv"]["issue_type"])
    )


def test_left_joined_orders_use_manual_chicken_columns_without_auto_fill(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "ITEM1",
        "item_name": "도리당 닭도리탕",
    })
    manual = pd.DataFrame([
            {
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "item_id": "ITEM1",
                "item_name": "도리당 닭도리탕",
                "닭유형_manual": "순살",
                "사이즈_manual": "2인",
                "닭사용량_manual": "0.6",
        }
    ])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: manual)
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == "2인"
    assert out.iloc[0]["사용용량"] == "0.6"
    assert out.iloc[0]["수익률"] == ""


def test_left_joined_orders_keep_chicken_columns_blank_without_manual_values(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "ITEM1",
        "item_name": "도리당 닭도리탕",
    })

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == ""
    assert out.iloc[0]["사이즈"] == ""
    assert out.iloc[0]["사용용량"] == ""
    assert out.iloc[0]["수익률"] == ""


def test_left_joined_orders_use_manager_input_profit_over_default(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "ITEM1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
    })
    manager = pd.DataFrame([
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "std_menu_name": "도리당 닭도리탕",
            "옵션조합": "",
            "닭유형_manual": "뼈닭",
            "사이즈_manual": "중",
            "닭사용량_manual": "1.0",
            "수익률_manual": "24%",
            "메모": "담당자 입력",
        }
    ])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "뼈닭"
    assert out.iloc[0]["사이즈"] == "중"
    assert out.iloc[0]["사용용량"] == "1.0"
    assert out.iloc[0]["수익률"] == "24%"


def test_left_joined_orders_do_not_use_llm_candidate_when_rule_and_manual_are_blank(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "[한우 대창] 순살 곱도리탕",
        "std_menu_name": "[한우 대창] 순살 곱도리탕",
        "line_role": "main",
        "qty": "1",
        "total_price": "30000",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == ""
    assert out.iloc[0]["사용용량"] == ""
    assert out.iloc[0]["수익률"] == ""


def test_left_joined_orders_keep_rule_candidate_without_llm(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "도리당 닭도리탕", "main"),
        ("2", "[중] 2인", "option"),
        ("3", "뼈", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "line_role": line_role,
            "qty": "1",
            "total_price": "0",
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out["닭유형"]) == {"뼈닭"}
    assert set(out["사이즈"]) == {"중"}
    assert out[out["line_role"].eq("main")].iloc[0]["사용용량"] == "1"
    assert set(out[out["line_role"].eq("option")]["사용용량"]) == {""}
    assert set(out["수익률"]) == {""}


def test_left_joined_orders_infer_chicken_type_from_order_group_options(monkeypatch):
    rows = []
    for item_seq, item_id, item_name, line_role, total_price in [
        ("1", "MAIN1", "도리당 닭도리탕", "main", "20800"),
        ("2", "SIZE1", "[중] 한마리 + 기본제공", "option", "9000"),
        ("3", "TYPE1", "뼈", "option", "0"),
        ("4", "FEE1", "배달비", "fee", "3000"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_id,
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "line_role": line_role,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    non_fee = out[out["line_role"].ne("fee")]
    assert set(non_fee["닭유형"]) == {"뼈닭"}
    assert set(non_fee["사이즈"]) == {"중"}
    assert non_fee[non_fee["line_role"].eq("main")].iloc[0]["사용용량"] == "1"
    assert set(non_fee[non_fee["line_role"].eq("option")]["사용용량"]) == {""}
    assert set(non_fee["수익률"]) == {""}
    assert out[out["line_role"].eq("fee")].iloc[0]["닭유형"] == ""


def test_order_group_auto_chicken_attrs_override_manager_default(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "item_id": "MAIN1",
        "menu_name": "[재주문 1위] 도리당 닭도리탕",
        "item_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
        "std_menu_name": "[재주문 1위] 도리당 닭도리탕",
        "line_role": "main",
        "qty": "1",
        "total_price": "16900",
    })
    manager = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "[재주문 1위] 도리당 닭도리탕",
        "옵션조합": menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO,
        "닭유형_manual": "뼈닭",
        "사이즈_manual": "중",
        "닭사용량_manual": "1",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == "1인"
    assert out.iloc[0]["사용용량"] == "0.3"
    assert out.iloc[0]["닭유형_판정"] == "메뉴명"
    assert out.iloc[0]["사이즈_판정"] == "메뉴명"


def test_left_joined_orders_leave_type_blank_when_order_group_has_no_bone_or_boneless(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "도리당 닭도리탕",
        "std_menu_name": "도리당 닭도리탕",
        "line_role": "main",
        "qty": "1",
        "total_price": "20800",
    })

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "뼈닭"
    assert out.iloc[0]["사용용량"] == ""
    assert out.iloc[0]["수익률"] == ""


def test_okpos_bracket_size_with_people_count_prefers_bracket_size(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "도리당 닭도리탕", "main"),
        ("2", "[중] 2인", "option"),
        ("3", "뼈", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "line_role": line_role,
            "qty": "1",
            "total_price": "0",
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out["닭유형"]) == {"뼈닭"}
    assert set(out["사이즈"]) == {"중"}
    assert out[out["line_role"].eq("main")].iloc[0]["사용용량"] == "1"
    assert set(out[out["line_role"].eq("option")]["사용용량"]) == {""}
    assert set(out["수익률"]) == {""}


def test_okpos_large_three_person_size_uses_large_usage(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "한우순살곱도리탕", "main"),
        ("2", "철판 셀프 볶음밥 [2인]", "option"),
        ("3", "[대] 3인", "option"),
        ("4", "순살", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "한우 순살 곱도리탕",
            "line_role": line_role,
            "qty": "1",
            "total_price": "0",
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out["닭유형"]) == {"순살"}
    assert set(out["사이즈"]) == {"대"}
    assert out[out["line_role"].eq("main")].iloc[0]["사용용량"] == "1.2"
    assert set(out[out["line_role"].eq("option")]["사용용량"]) == {""}
    assert set(out["수익률"]) == {""}


def _half_menu_rows(type_options, qty="1"):
    rows = []
    option_names = ["[대] 3~6인", *type_options]
    for item_seq, item_name, line_role in [
        ("1", "도리당 반반 닭도리탕", "main"),
        *[(str(idx + 2), name, "option") for idx, name in enumerate(option_names)],
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "도리당 반반 닭도리탕",
            "std_menu_name": "도리당 반반 닭도리탕",
            "line_role": line_role,
            "qty": qty if line_role == "main" else "1",
            "total_price": "34000" if line_role == "main" else "0",
        })
        rows.append(row)
    return rows


def test_half_half_bone_and_boneless_splits_usage_without_extra_rows(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(_half_menu_rows(["뼈", "순살"], qty="2")))
    main = out[out["line_role"].eq("main")].iloc[0]

    assert len(out) == 4
    assert main["닭유형"] == menu_hierarchy.CHICKEN_TYPE_MIXED
    assert main["닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_HALF_SLOT
    assert main[menu_hierarchy.HALF_COMBO_COLUMN] == "뼈+순살"
    assert main["사용용량"] == "1.35"
    assert main[menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "2.7"
    assert main[menu_hierarchy.BONE_USAGE_COLUMN] == "0.75"
    assert main[menu_hierarchy.BONELESS_USAGE_COLUMN] == "0.6"
    assert main[menu_hierarchy.BONE_USAGE_TOTAL_COLUMN] == "1.5"
    assert main[menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN] == "1.2"


def test_half_half_bone_bone_and_boneless_boneless_are_explicit_combos(monkeypatch):
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    bone_out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(_half_menu_rows(["뼈", "뼈"])))
    bone_main = bone_out[bone_out["line_role"].eq("main")].iloc[0]
    assert bone_main["닭유형"] == "뼈닭"
    assert bone_main[menu_hierarchy.HALF_COMBO_COLUMN] == "뼈+뼈"
    assert bone_main[menu_hierarchy.BONE_USAGE_TOTAL_COLUMN] == "1.5"
    assert bone_main[menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN] == "0"

    boneless_out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(_half_menu_rows(["순살", "순살"])))
    boneless_main = boneless_out[boneless_out["line_role"].eq("main")].iloc[0]
    assert boneless_main["닭유형"] == "순살"
    assert boneless_main[menu_hierarchy.HALF_COMBO_COLUMN] == "순살+순살"
    assert boneless_main[menu_hierarchy.BONE_USAGE_TOTAL_COLUMN] == "0"
    assert boneless_main[menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN] == "1.2"


def test_non_chicken_main_does_not_consume_chicken_from_attached_options(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, total_price in [
        ("1", "미나리 비빔칼국수", "main", "9000"),
        ("2", "[중] 2인", "option", "0"),
        ("3", "뼈", "option", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "미나리 비빔칼국수",
            "line_role": line_role,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out["닭유형"]) == {menu_hierarchy.CHICKEN_TYPE_NONE}
    assert set(out["사이즈"]) == {menu_hierarchy.CHICKEN_SIZE_NONE}
    assert set(out["사용용량"]) == {"", "0"}


def test_okpos_one_person_boneless_dakdori_set_infers_usage(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_seq": "1",
        "item_id": "10031034",
        "item_name": "1인 순살 닭도리 정식",
        "std_menu_name": "1인 순살 닭도리 정식",
        "line_role": "main",
        "qty": "1",
        "total_price": "12000",
    })

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == "1인"
    assert out.iloc[0]["사용용량"] == "0.3"


def test_okpos_fixed_one_person_boneless_menu_overrides_bone_size_options(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "1인 순살 닭도리탕(공깃밥 포함)", "main"),
        ("2", "[중] 2인", "option"),
        ("3", "뼈", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "1인 순살 닭도리탕(밥포함)",
            "line_role": line_role,
            "qty": "1",
            "total_price": "9900" if line_role == "main" else "0",
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))
    main = out[out["line_role"].eq("main")].iloc[0]

    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "1인"
    assert main["사용용량"] == "0.3"
    assert main["닭유형_판정"] == "메뉴명"
    assert main["사이즈_판정"] == "메뉴명"
    assert set(out[out["line_role"].eq("option")]["사용용량"]) == {""}


def test_okpos_two_person_or_more_boneless_dakdori_set_infers_two_person(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_seq": "1",
        "item_id": "10030002",
        "item_name": "순살 닭도리 정식(2인이상)",
        "std_menu_name": "[점심] 순살 닭도리 정식(2인이상)",
        "line_role": "main",
        "qty": "1",
        "total_price": "-30000",
    })

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == "2인"
    assert out.iloc[0]["사용용량"] == "0.6"


def test_two_person_boneless_product_keeps_two_person_size(monkeypatch):
    row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_seq": "1",
        "item_id": "MAIN1",
        "item_name": "2인 순살 반반",
        "std_menu_name": "2인 순살 반반",
        "line_role": "main",
        "qty": "1",
        "total_price": "30000",
    })

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))

    assert out.iloc[0]["닭유형"] == "순살"
    assert out.iloc[0]["사이즈"] == "2인"
    assert out.iloc[0]["사용용량"] == "0.6"
    assert out.iloc[0]["수익률"] == ""


def test_delivery_one_person_boneless_name_overrides_stale_manager_size(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, total_price in [
        ("1", "[1인] 순살 닭도리탕 (밥포함) 1인분", "main", "16800"),
        ("2", "기본맛", "option", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
            "line_role": line_role,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    manager = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
        "옵션조합": "기본맛",
        "닭유형_manual": "순살",
        "사이즈_manual": "중",
        "닭사용량_manual": "0.8",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))
    main = out[out["line_role"].eq("main")].iloc[0]

    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "1인"
    assert main["사용용량"] == "0.3"
    assert main["닭유형_판정"] == "메뉴명"
    assert main["사이즈_판정"] == "메뉴명"


def test_delivery_one_person_boneless_name_keeps_explicit_size_option(monkeypatch):
    rows = []
    for item_seq, item_name, line_role, total_price in [
        ("1", "[1인] 순살 닭도리탕 (밥포함) 1인분", "main", "16800"),
        ("2", "[중] 2인", "option", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
            "line_role": line_role,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    main = out[out["line_role"].eq("main")].iloc[0]
    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "1인"
    assert main["사용용량"] == "0.3"
    assert set(out[out["line_role"].eq("option")]["사용용량"]) == {""}


def test_manager_input_table_is_order_group_based_and_preserves_existing_edits(monkeypatch):
    rows = [
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_id": "MAIN1",
            "item_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
            "item_seq": "1",
            "qty": "1",
            "total_price": "20000",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_id": "OPT1",
                "item_name": "[중] 한마리 + 기본제공",
                "std_menu_name": "도리당 닭도리탕",
                "line_role": "option",
                "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
                "item_seq": "2",
                "qty": "1",
                "total_price": "9000",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O2",
            "menu_seq": "1",
            "item_id": "MAIN1",
            "item_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": "main",
            "item_seq": "1",
            "qty": "2",
            "total_price": "40000",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O2",
            "menu_seq": "1",
            "item_id": "OPT1",
                "item_name": "[중] 한마리 + 기본제공",
                "std_menu_name": "도리당 닭도리탕",
                "line_role": "option",
                "option_kind": menu_hierarchy.OPTION_KIND_SIZE,
                "item_seq": "2",
                "qty": "2",
                "total_price": "18000",
        },
    ]
    existing = pd.DataFrame([
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "std_menu_name": "도리당 닭도리탕",
            "옵션조합": "[중] 한마리 + 기본제공",
            "닭유형_manual": "순살",
            "사이즈_manual": "2인",
            "닭사용량_manual": "0.6",
            "수익률_manual": "22%",
            "메모": "유지",
        }
    ])

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: existing)

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows))

    assert len(out) == 2
    row = out[out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN].eq("[중] 한마리 + 기본제공")].iloc[0]
    assert row["주문건수"] == "2"
    assert row["판매수량"] == "3"
    assert row["매출합계"] == "87000"
    assert row[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == "[중] 한마리 + 기본제공"
    assert row["닭사용량_manual"] == "0.6"
    assert row["수익률_manual"] == "22%"


def test_manager_input_table_is_manual_only_and_includes_all_groups(monkeypatch):
    rows = []
    for order_id, item_seq, item_name, std_menu_name, line_role in [
        ("O1", "1", "[한우 대창] 순살 곱도리탕", "[한우 대창] 순살 곱도리탕", "main"),
        ("O1", "2", "기본맛", "[한우 대창] 순살 곱도리탕", "option"),
        ("O2", "1", "흑미 공기밥", "흑미 공기밥", "main"),
    ]:
        row = {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": order_id,
            "menu_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": std_menu_name,
            "line_role": line_role,
            "item_seq": item_seq,
            "qty": "1",
            "total_price": "30000" if line_role == "main" else "0",
        }
        rows.append(row)
    group_attrs = menu_hierarchy._build_order_group_attrs(pd.DataFrame(rows))

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows), group_attrs=group_attrs)

    assert len(out) == 4
    assert "닭유형_llm" not in out.columns
    assert "llm_confidence" not in out.columns
    assert set(out["std_menu_name"]) == {"[한우 대창] 순살 곱도리탕", "흑미 공기밥"}
    assert set(out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN]) == {
        menu_hierarchy.OPTION_COMBO_NONE,
        menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO,
    }


def test_manager_input_preserves_dynamic_material_usage_columns(monkeypatch):
    rows = [{
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "MAIN1",
        "item_name": "1인 미나리 수삼 백숙",
        "std_menu_name": "1인 미나리 수삼 백숙",
        "line_role": "main",
        "item_seq": "1",
        "qty": "1",
        "total_price": "22000",
    }]
    existing = pd.DataFrame([{
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "1인 미나리 수삼 백숙",
        "옵션조합": "",
        "미나리사용량_manual": "0.1",
        "소스사용량_manual": "0.2",
        "메모": "수기 재료",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: existing)

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows))

    assert list(out.columns)[-4:] == ["미나리사용량_manual", "소스사용량_manual", "수익률_manual", "메모"]
    assert out.iloc[0]["미나리사용량_manual"] == "0.1"
    assert out.iloc[0]["소스사용량_manual"] == "0.2"


def test_manager_input_uses_representative_name_when_std_menu_name_is_blank(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "1인 순살 닭도리탕 (밥포함)", "main"),
        ("2", "[소] 반마리", "option"),
    ]:
        rows.append({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "1인 순살 닭도리탕 (밥포함)",
            "std_menu_name": "",
            "line_role": line_role,
            "item_seq": item_seq,
            "qty": "1",
            "total_price": "21500" if line_role == "main" else "0",
        })

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows))

    assert len(out) == 2
    exact = out[out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN].ne(menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO)].iloc[0]
    assert exact["std_menu_name"] == "1인 순살 닭도리탕 (밥포함)"
    assert exact["대표메뉴명"] == "1인 순살 닭도리탕 (밥포함)"


def test_left_joined_orders_expose_option_combo_and_manual_material_usage(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "1인 미나리 수삼 백숙", "main"),
        ("2", "미나리 추가", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "1인 미나리 수삼 백숙",
            "line_role": line_role,
            "qty": "1",
            "total_price": "22000" if line_role == "main" else "0",
        })
        rows.append(row)
    manager = pd.DataFrame([{
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "1인 미나리 수삼 백숙",
        "옵션조합": "미나리 추가",
        "미나리사용량_manual": "0.1",
        "소스사용량_manual": "0.2",
        "메모": "수기 재료",
    }])

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN]) == {menu_hierarchy.OPTION_COMBO_NONE}
    assert set(out["재료사용량"]) == {"미나리=0.1 | 소스=0.2"}


def test_left_joined_orders_fill_blank_std_menu_name_for_review_key(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "갈비찜닭 반반 [3~6인]", "main"),
        ("2", "3~4인", "option"),
        ("3", "닭도리탕 [순살]", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "갈비찜닭 반반 [3~6인]",
            "std_menu_name": "",
            "line_role": line_role,
            "qty": "1",
            "total_price": "47500" if line_role == "main" else "0",
        })
        rows.append(row)

    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame(rows))

    assert set(out["std_menu_name"]) == {"갈비찜닭 반반 [3~6인]"}
    assert set(out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN]) == {menu_hierarchy.OPTION_COMBO_NONE}


def test_manager_input_includes_standalone_side_for_manual_material_entry(monkeypatch):
    row = {
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "SIDE1",
        "item_name": "흑미 공기밥",
        "menu_name": "도리당 닭도리탕",
        "std_menu_name": "흑미 공기밥",
        "line_role": "main",
        "item_seq": "1",
        "qty": "2",
        "total_price": "3000",
    }

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame([row]))

    assert len(out) == 2
    assert set(out["std_menu_name"]) == {"흑미 공기밥"}
    assert menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO in set(out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN])


def test_manager_input_marks_no_option_combo_for_single_main(monkeypatch):
    row = {
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "order_id": "O1",
        "menu_seq": "1",
        "item_id": "DRINK1",
        "item_name": "새로 오미자 360ml",
        "menu_name": "새로 오미자 360ml",
        "std_menu_name": "새로 오미자 360ml",
        "line_role": "main",
        "item_seq": "1",
        "qty": "2",
        "total_price": "8000",
    }

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame([row]))

    assert len(out) == 2
    assert {menu_hierarchy.OPTION_COMBO_NONE, menu_hierarchy.MANAGER_DEFAULT_OPTION_COMBO} == set(
        out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN]
    )


def test_manager_input_includes_non_chicken_main_with_options(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "흑미 공기밥", "main"),
        ("2", "뼈", "option"),
    ]:
        rows.append({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "흑미 공기밥",
            "line_role": line_role,
            "item_seq": item_seq,
            "qty": "1",
            "total_price": "2000" if line_role == "main" else "0",
        })

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows))

    assert len(out) == 2
    exact = out[out[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN].eq(menu_hierarchy.OPTION_COMBO_NONE)].iloc[0]
    assert exact["std_menu_name"] == "흑미 공기밥"


def test_manager_input_keeps_chicken_set_menu_with_chicken_options(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "시그니처 반반", "main"),
        ("2", "3~4인", "option"),
        ("3", "닭도리탕 [순살]", "option"),
    ]:
        rows.append({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_id": item_name,
            "item_name": item_name,
            "std_menu_name": "시그니처 반반",
            "line_role": line_role,
            "item_seq": item_seq,
            "qty": "1",
            "total_price": "60000" if line_role == "main" else "0",
        })

    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._build_manager_input(pd.DataFrame(rows))

    assert len(out) == 2
    assert set(out["std_menu_name"]) == {"시그니처 반반"}


def test_option_material_input_is_option_line_based_and_preserves_existing_edits(monkeypatch):
    rows = []
    for order_id in ["O1", "O2"]:
        for item_seq, item_id, item_name, line_role, qty, total_price in [
            ("1", "MAIN1", "도리당 닭도리탕", "main", "1", "20000"),
            ("2", "OPT_PK", "파김치 반찬 추가", "option", "1", "1000"),
            ("3", "FEE1", "배달팁", "fee", "1", "3000"),
        ]:
            row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
            row.update({
                "source": "쿠팡수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "order_id": order_id,
                "menu_seq": "1",
                "item_seq": item_seq,
                "item_id": item_id,
                "item_name": item_name,
                "menu_name": "도리당 닭도리탕",
                "std_menu_name": "도리당 닭도리탕",
                "line_role": line_role,
                "qty": qty,
                "total_price": total_price,
            })
            rows.append(row)
    existing = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "OPT_PK",
        "item_name": "파김치 반찬 추가",
        "파김치사용량_manual": "0.1",
        "메모": "옵션 원가",
    }])

    monkeypatch.setattr(menu_hierarchy, "_option_material_attrs", lambda: existing)

    out = menu_hierarchy._build_option_material_input(pd.DataFrame(rows))

    assert len(out) == 1
    row = out.iloc[0]
    assert row["item_id"] == "OPT_PK"
    assert row["item_name"] == "파김치 반찬 추가"
    assert row["주문건수"] == "2"
    assert row["판매수량"] == "2"
    assert row["매출합계"] == "2000"
    assert row["파김치사용량_manual"] == "0.1"
    assert row["메모"] == "옵션 원가"


def test_left_joined_orders_attach_option_material_usage_to_option_lines_only(monkeypatch):
    rows = []
    for item_seq, item_id, item_name, line_role, total_price in [
        ("1", "MAIN1", "도리당 닭도리탕", "main", "20000"),
        ("2", "OPT_PK", "파김치 반찬 추가", "option", "1000"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_id,
            "item_name": item_name,
            "menu_name": "도리당 닭도리탕",
            "std_menu_name": "도리당 닭도리탕",
            "line_role": line_role,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    option_input = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "OPT_PK",
        "item_name": "파김치 반찬 추가",
        "파김치사용량_manual": "0.1",
        "대파사용량_manual": "0.2",
    }])

    monkeypatch.setattr(menu_hierarchy, "_option_material_attrs", lambda: option_input)

    out = menu_hierarchy._attach_option_material_usage_columns(pd.DataFrame(rows))

    by_item = out.set_index("item_id")[menu_hierarchy.OPTION_MATERIAL_USAGE_COLUMN].to_dict()
    assert by_item["MAIN1"] == ""
    assert by_item["OPT_PK"] == "파김치=0.1 | 대파=0.2"


def test_menu_weight_input_is_standard_weight_based_and_preserves_edits(monkeypatch):
    rows = []
    for order_id in ["O1", "O2"]:
        for item_seq, item_name, line_role, total_price in [
            ("1", "들깨 우거지 닭도리탕", "main", "32000"),
            ("2", "[대] 3인", "option", "0"),
            ("3", "닭다리살 100% 순살로 변경", "option", "3000"),
        ]:
            row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
            row.update({
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "order_id": order_id,
                "menu_seq": "1",
                "item_seq": item_seq,
                "item_id": item_name,
                "item_name": item_name,
                "menu_name": "들깨 우거지 닭도리탕",
                "std_menu_name": "들깨 우거지 닭도리탕",
                "line_role": line_role,
                "qty": "1",
                "total_price": total_price,
            })
            rows.append(row)
    existing = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "들깨 우거지 닭도리탕",
        "사이즈키": "대",
        "닭유형키": "순살",
        "추가재료키": "우거지 | 순살추가",
        "닭사용량_manual": "1.2",
        "우거지사용량_manual": "0.2",
        "순살추가사용량_manual": "0.1",
        "메모": "실사 비교 기준",
    }])

    monkeypatch.setattr(menu_hierarchy, "_menu_weight_attrs", lambda: existing)

    out = menu_hierarchy._build_menu_weight_input(pd.DataFrame(rows))

    assert len(out) == 1
    row = out.iloc[0]
    assert row["사이즈키"] == "대"
    assert row["닭유형키"] == "순살"
    assert row["추가재료키"] == "우거지 | 순살추가"
    assert row["주문건수"] == "2"
    assert row["판매수량"] == "2"
    assert row["매출합계"] == "70000"
    assert row["닭사용량_manual"] == "1.2"
    assert row["우거지사용량_manual"] == "0.2"
    assert row["순살추가사용량_manual"] == "0.1"


def test_left_joined_orders_attach_menu_weight_usage_for_inventory_loss(monkeypatch):
    rows = []
    for item_seq, item_name, line_role in [
        ("1", "들깨 우거지 닭도리탕", "main"),
        ("2", "[대] 3인", "option"),
        ("3", "닭다리살 100% 순살로 변경", "option"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O1",
            "menu_seq": "1",
            "item_seq": item_seq,
            "item_id": item_name,
            "item_name": item_name,
            "menu_name": "들깨 우거지 닭도리탕",
            "std_menu_name": "들깨 우거지 닭도리탕",
            "line_role": line_role,
            "사이즈": "대",
            "닭유형": "순살",
            "qty": "1",
            "total_price": "32000" if line_role == "main" else "0",
        })
        rows.append(row)
    weight_master = pd.DataFrame([{
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "들깨 우거지 닭도리탕",
        "사이즈": "대",
        "닭유형": "순살",
        "닭사용량_manual": "1.2",
        "우거지사용량_manual": "0.2",
        "순살추가사용량_manual": "0.1",
    }])

    out = menu_hierarchy._attach_menu_weight_usage_columns(pd.DataFrame(rows), menu_weight_master=weight_master)

    by_role = out.set_index("line_role")[menu_hierarchy.MENU_WEIGHT_USAGE_COLUMN].to_dict()
    assert by_role["main"] == "닭=1.2 | 우거지=0.2 | 순살추가=0.1"
    assert by_role["option"] == ""


def test_material_usage_summary_multiplies_standard_weight_by_sales_qty():
    weight_input = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "들깨 우거지 닭도리탕",
        "사이즈키": "대",
        "닭유형키": "순살",
        "추가재료키": "우거지 | 순살추가",
        "주문건수": "2",
        "판매수량": "2",
        "매출합계": "70000",
        "닭사용량_manual": "1.2",
        "우거지사용량_manual": "0.2",
        "순살추가사용량_manual": "0.1",
    }])

    out = menu_hierarchy._build_material_usage_summary(weight_input)

    usage = out.set_index("재료명")["예상사용량"].to_dict()
    assert usage == {"닭": "2.4", "우거지": "0.4", "순살추가": "0.2"}
    assert set(out["메뉴당사용량"]) == {"1.2", "0.2", "0.1"}


def test_write_readme_documents_output_contract(monkeypatch):
    written = {}

    monkeypatch.setattr(menu_hierarchy, "resolve_yms", lambda ym=None: ["2026-04", "2026-06"])
    monkeypatch.setattr(
        menu_hierarchy,
        "_write_text",
        lambda path, text: written.update({"path": path, "text": text}),
    )

    result = menu_hierarchy.write_readme(None)

    assert written["path"] == menu_hierarchy.README_OUTPUT_PATH
    assert written["path"].name == "설명.md"
    assert "10_orders.csv" in written["text"]
    assert "11_hierarchy.csv" in written["text"]
    assert "12_orders_left.csv" in written["text"]
    assert "16_option_material_input.csv" in written["text"]
    assert "17_menu_weight_input.csv" in written["text"]
    assert "18_material_usage_summary.csv" in written["text"]
    assert "옵션재료사용량" in written["text"]
    assert "loss" in written["text"]
    assert "LLM" in written["text"]
    assert "상품표는 읽기만" in written["text"]
    assert "_pk" in written["text"]
    assert "2026-04, 2026-06" in written["text"]
    assert result == "설명.md 저장 완료 | yms=2026-04,2026-06"


def test_write_manual_input_workbook_preserves_unmatched_profit_manual_rows(monkeypatch, tmp_path):
    previous_profit = pd.DataFrame([{
        "수익채널": "홀",
        "수익키": "메뉴|홀|기존메뉴|중",
        "대표품목명": "기존메뉴",
        "판매가": "20000",
        "판매가_manual": "21000",
        "메뉴원가_manual": "9000",
        "상차림비_manual": "500",
        "메모": "기존 입력",
    }])
    current_profit = pd.DataFrame([{
        "수익채널": "홀",
        "수익키": "메뉴|홀|새메뉴|중",
        "대표품목명": "새메뉴",
    }]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    captured = {}
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    def read_sheet(sheet_name):
        if sheet_name == "수익률":
            return previous_profit
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_BACKUP_ROOT", tmp_path / "backups")
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)
    monkeypatch.setattr(menu_hierarchy, "_write_excel_workbook", lambda sheets, path: captured.update({"sheets": sheets, "path": path}))

    menu_hierarchy._write_manual_input_workbook(
        option_kind_master=pd.DataFrame(),
        std_menu_override_input=pd.DataFrame(),
        menu_weight_master=pd.DataFrame(),
        material_price_master=pd.DataFrame(),
        chicken_ratio_master=pd.DataFrame(),
        menu_chicken_profile_master=pd.DataFrame(),
        manual_profit_rate_master=current_profit,
        judgement_option_input=pd.DataFrame(),
        manager_input=pd.DataFrame(),
        option_material_input=pd.DataFrame(),
        order_exception_input=pd.DataFrame(),
    )

    preserved = captured["sheets"][menu_hierarchy.MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME]
    assert captured["path"] == workbook_path
    assert menu_hierarchy.STD_MENU_OVERRIDE_SHEET_NAME in captured["sheets"]
    assert preserved.iloc[0]["수익키"] == "메뉴|홀|기존메뉴|중"
    assert preserved.iloc[0]["판매가"] == ""
    assert preserved.iloc[0]["판매가_manual"] == "21000"
    assert preserved.iloc[0]["판매가기준"] == "수기"
    assert preserved.iloc[0]["상차림포함원가"] == "9500"


def test_std_menu_name_override_on_main_propagates_to_order_group(monkeypatch):
    rows = []
    for seq, line_role, item_id, item_name in [
        ("1", "main", "MAIN_WRONG", "잘못된 메뉴"),
        ("2", "option", "OPT_EGG", "계란찜"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-19",
            "order_id": "O_STD_MAIN",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "item_id": item_id,
            "item_name": item_name,
            "std_menu_name": "잘못된 표준메뉴",
        })
        rows.append(row)
    overrides = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "main",
        "item_id": "MAIN_WRONG",
        "item_name": "잘못된 메뉴",
        "현재_std_menu_name": "잘못된 표준메뉴",
        "std_menu_name_manual": "도리당 닭도리탕",
    }])
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", lambda sheet: overrides if sheet == menu_hierarchy.STD_MENU_OVERRIDE_SHEET_NAME else pd.DataFrame())

    out = menu_hierarchy._apply_std_menu_name_overrides(pd.DataFrame(rows))

    assert set(out["std_menu_name"]) == {"도리당 닭도리탕"}


def test_std_menu_name_override_is_scoped_by_current_std_menu_name(monkeypatch):
    rows = []
    for order_id, std_name in [("O_A", "잘못된 메뉴 A"), ("O_B", "잘못된 메뉴 B")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-19",
            "order_id": order_id,
            "menu_seq": "1",
            "item_seq": "2",
            "parent_item_seq": "1",
            "line_role": "option",
            "item_id": "OPT_EGG",
            "item_name": "계란찜",
            "std_menu_name": std_name,
        })
        rows.append(row)
    overrides = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "option",
        "item_id": "OPT_EGG",
        "item_name": "계란찜",
        "현재_std_menu_name": "잘못된 메뉴 A",
        "std_menu_name_manual": "도리당 닭도리탕",
    }])
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", lambda sheet: overrides if sheet == menu_hierarchy.STD_MENU_OVERRIDE_SHEET_NAME else pd.DataFrame())

    out = menu_hierarchy._apply_std_menu_name_overrides(pd.DataFrame(rows)).set_index("order_id")

    assert out.at["O_A", "std_menu_name"] == "도리당 닭도리탕"
    assert out.at["O_B", "std_menu_name"] == "잘못된 메뉴 B"


def test_build_std_menu_override_input_preserves_manual_values(monkeypatch):
    rows = []
    for order_id, std_name in [("O_A", "잘못된 메뉴 A"), ("O_B", "잘못된 메뉴 B")]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": order_id,
            "line_role": "option",
            "item_id": "OPT_EGG",
            "item_name": "계란찜",
            "std_menu_name": std_name,
            "menu_name": "주문메뉴",
            "qty": "1",
            "total_price": "3000",
        })
        rows.append(row)
    previous = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "line_role": "option",
        "item_id": "OPT_EGG",
        "item_name": "계란찜",
        "현재_std_menu_name": "잘못된 메뉴 A",
        "std_menu_name_manual": "도리당 닭도리탕",
        "메모": "수기 보정",
    }])
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", lambda sheet: previous if sheet == menu_hierarchy.STD_MENU_OVERRIDE_SHEET_NAME else pd.DataFrame())

    out = menu_hierarchy._build_std_menu_override_input(pd.DataFrame(rows))
    by_std = out.set_index("현재_std_menu_name")

    assert by_std.at["잘못된 메뉴 A", "std_menu_name_manual"] == "도리당 닭도리탕"
    assert by_std.at["잘못된 메뉴 A", "메모"] == "수기 보정"
    assert by_std.at["잘못된 메뉴 B", "std_menu_name_manual"] == ""


def test_guard_manual_workbook_loss_blocks_manual_amount_drop(monkeypatch, tmp_path):
    previous_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
        "메뉴원가_manual": "9000",
    }])
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    def read_sheet(sheet_name):
        if sheet_name == "수익률":
            return previous_profit
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    with pytest.raises(RuntimeError, match="수기입력값 유실"):
        menu_hierarchy._guard_manual_workbook_loss({
            "수익률": pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS),
            menu_hierarchy.MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME: pd.DataFrame(
                columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS
            ),
        })


def test_guard_manual_workbook_loss_blocks_same_key_manual_amount_change(monkeypatch, tmp_path):
    previous_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
        "판매가": "20000",
        "메뉴원가_manual": "9000",
        "상차림비_manual": "250",
    }])
    changed_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
        "판매가": "20000",
        "메뉴원가_manual": "9000",
        "상차림비_manual": "710",
    }]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    def read_sheet(sheet_name):
        if sheet_name == "수익률":
            return previous_profit
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    with pytest.raises(RuntimeError, match="수기금액변경"):
        menu_hierarchy._guard_manual_workbook_loss({
            "수익률": changed_profit,
            menu_hierarchy.MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME: pd.DataFrame(
                columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS
            ),
        })


def test_guard_manual_workbook_loss_ignores_dropped_legacy_profit_columns(monkeypatch, tmp_path):
    previous_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
        "상차림포함원가_manual": "9500",
    }])
    current_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
    }]).reindex(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    def read_sheet(sheet_name):
        if sheet_name == "수익률":
            return previous_profit
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    menu_hierarchy._guard_manual_workbook_loss({
        "수익률": current_profit,
        menu_hierarchy.MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME: pd.DataFrame(
            columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS
        ),
    })


def test_guard_manual_workbook_loss_blocks_manual_price_drop(monkeypatch, tmp_path):
    previous_profit = pd.DataFrame([{
        "수익키": "메뉴|홀|기존메뉴|중",
        "판매가_manual": "20000",
    }])
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    def read_sheet(sheet_name):
        if sheet_name == "수익률":
            return previous_profit
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    with pytest.raises(RuntimeError, match="수익률계.판매가_manual"):
        menu_hierarchy._guard_manual_workbook_loss({
            "수익률": pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS),
            menu_hierarchy.MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME: pd.DataFrame(
                columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS
            ),
        })


def test_build_orders_blocks_zero_source_overwriting_manual_workbook(monkeypatch, tmp_path):
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "resolve_yms", lambda ym=None: ["2026-08"])
    monkeypatch.setattr(menu_hierarchy, "_all_source_orders", lambda target_ym: pd.DataFrame())

    with pytest.raises(RuntimeError, match="원본 데이터 0건"):
        menu_hierarchy.build_orders("2026-08", debug_outputs=False, archive_legacy=False)


def test_manual_workbook_guard_ignores_auto_generated_judgement_option_loss(monkeypatch, tmp_path):
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")
    auto_previous = pd.DataFrame(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "std_menu_name": "자동 메뉴",
                menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인",
                "닭유형": "순살",
                "사이즈": "대",
                menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
                "메모": "자동생성 선택",
            }
        ]
    )

    def read_sheet(sheet_name):
        if sheet_name == menu_hierarchy.JUDGEMENT_OPTION_SHEET_NAME:
            return auto_previous
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    menu_hierarchy._guard_manual_workbook_loss({
        menu_hierarchy.JUDGEMENT_OPTION_SHEET_NAME: pd.DataFrame(columns=auto_previous.columns)
    })


def test_manual_workbook_guard_blocks_manual_judgement_option_loss(monkeypatch, tmp_path):
    workbook_path = tmp_path / "01_수기입력.xlsx"
    workbook_path.write_bytes(b"existing")
    manual_previous = pd.DataFrame(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "std_menu_name": "수기 메뉴",
                menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인",
                "닭유형": "순살",
                "사이즈": "대",
                menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
                "메모": "수기 보정",
            }
        ]
    )

    def read_sheet(sheet_name):
        if sheet_name == menu_hierarchy.JUDGEMENT_OPTION_SHEET_NAME:
            return manual_previous
        return pd.DataFrame()

    monkeypatch.setattr(menu_hierarchy, "MANUAL_WORKBOOK_OUTPUT_PATH", workbook_path)
    monkeypatch.setattr(menu_hierarchy, "_read_manual_workbook_sheet", read_sheet)

    with pytest.raises(RuntimeError, match="판정옵션"):
        menu_hierarchy._guard_manual_workbook_loss({
            menu_hierarchy.JUDGEMENT_OPTION_SHEET_NAME: pd.DataFrame(columns=manual_previous.columns)
        })


def test_explicit_large_size_option_overrides_boneless_menu_profile():
    rows = []
    for seq, line_role, option_kind, item_name, total_price in [
        ("1", "option", menu_hierarchy.OPTION_KIND_SPICE, "기본맛", "0"),
        ("2", "option", menu_hierarchy.OPTION_KIND_SIZE, "[대] 3인", "0"),
        ("3", "main", menu_hierarchy.OPTION_KIND_MAIN, "갈비 찜닭 [순살]", "27500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "order_id": "O_LARGE_BONELESS",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "3",
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": "갈비 찜닭 [순살]",
            "item_name": item_name,
            "std_menu_name": "갈비 찜닭 [순살]",
            menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    profile_master = menu_hierarchy._build_menu_chicken_profile_master(base)

    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile_master)

    assert group_attrs.iloc[0]["사이즈_auto"] == "대"
    assert group_attrs.iloc[0]["닭유형_auto"] == "순살"
    assert group_attrs.iloc[0]["사이즈_판정_auto"] == "선택"


def test_codex_auto_manual_size_does_not_override_explicit_large_size(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_name, total_price in [
        ("1", "option", menu_hierarchy.OPTION_KIND_SPICE, "기본맛", "0"),
        ("2", "option", menu_hierarchy.OPTION_KIND_SIZE, "[대] 3인", "0"),
        ("3", "main", menu_hierarchy.OPTION_KIND_MAIN, "갈비 찜닭 [순살]", "27500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O_CODEX_MANUAL_LARGE",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "3",
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": "갈비 찜닭 [순살]",
            "item_name": item_name,
            "std_menu_name": "갈비 찜닭 [순살]",
            menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    manager = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "갈비 찜닭 [순살]",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인",
        "닭유형_manual": "순살",
        "사이즈_manual": "중",
        "닭사용량_manual": "",
        "수익률_manual": "",
        "메모": "Codex profit 0 is temporary",
    }])
    profile_master = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile_master)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: manager)

    out = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    main = out[out["line_role"].eq("main")].iloc[0]

    assert main["사이즈"] == "대"
    assert main["닭유형"] == "순살"
    assert main["사이즈_판정"] == "선택"
    assert main[menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_PRESENT


def test_plain_boneless_menu_without_size_option_defaults_to_mid(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-17",
        "platform": "쿠팡이츠",
        "order_type": "배달",
        "order_id": "O_BONELESS_GALBI",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "300004063",
        "menu_name": "[재주문 1위] 도리당 닭도리탕",
        "item_name": "[단짠단짠] 순살 갈비찜닭",
        "std_menu_name": "순살 갈비찜닭",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: menu_hierarchy.OPTION_COMBO_NONE,
        "qty": "1",
        "total_price": "32200",
    })
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    base = pd.DataFrame([row])
    profile_master = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile_master)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    main = attached.iloc[0]

    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "중"
    assert main["사용용량"] == menu_hierarchy._usage_for("순살", "중")
    assert main["미해결사유"] == ""


def test_okpos_paid_large_size_hides_zero_default_mid_size_from_profit_options(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_name, qty, unit_price, total_price in [
        ("2", "main", menu_hierarchy.OPTION_KIND_MAIN, "실비파김치곱도리탕+미나리새우전", "1", "43000", "43000"),
        ("3", "option", menu_hierarchy.OPTION_KIND_SIZE, "[대] 3인", "1", "13000", "13000"),
        ("4", "option", menu_hierarchy.OPTION_KIND_SIZE, "[중] 2인", "1", "0", "0"),
        ("5", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "순살", "1", "2000", "2000"),
        ("6", "option", menu_hierarchy.OPTION_KIND_SPICE, "실비맛(신라면보다매운)", "1", "0", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-05-07",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "송파삼전점_1-32_18:20:05",
            "menu_seq": "2",
            "item_seq": seq,
            "parent_item_seq": "2",
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": "실비파김치곱도리탕+미나리새우전",
            "item_name": item_name,
            "std_menu_name": "실비파김치곱도리탕+미나리새우전",
            "qty": qty,
            "unit_price": unit_price,
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    profile_master = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile_master)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    monkeypatch.setattr(
        menu_hierarchy,
        "_manual_profit_rate_attrs",
        lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS),
    )

    group = group_attrs.iloc[0]
    main = attached[attached["line_role"].eq("main")].iloc[0]
    profit = menu_hierarchy._build_manual_profit_rate_master(attached)
    target = profit[profit["수익키"].eq("메뉴|홀|실비파김치곱도리탕+미나리새우전|대|순살")].iloc[0]
    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), attached, pd.DataFrame())

    assert profile_master.iloc[0]["허용닭유형_제안"] == "순살"
    assert main["사이즈"] == "대"
    assert main["닭유형"] == "순살"
    assert group[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == "[대] 3인 | 순살"
    assert "[중] 2인" not in group["옵션조합"]
    assert "[중] 2인" not in target["대표옵션조합"]
    assert "parent_child_size_conflict" not in set(issues["issue_type"])


def test_multi_main_order_sequence_splits_attached_bone_and_boneless_options(monkeypatch):
    rows = []
    for seq, menu_seq, parent_seq, line_role, option_kind, item_name, std_name, total_price in [
        ("1", "1", "1", "main", menu_hierarchy.OPTION_KIND_MAIN, "도리당 닭도리탕", "도리당 닭도리탕", "29800"),
        ("2", "2", "2", "main", menu_hierarchy.OPTION_KIND_MAIN, "누룽지 백도리탕", "누룽지 백도리탕", "31500"),
        ("3", "2", "2", "option", menu_hierarchy.OPTION_KIND_SIZE, "[중] 2인", "누룽지 백도리탕", "0"),
        ("4", "2", "2", "option", menu_hierarchy.OPTION_KIND_SIZE, "[중] 2인", "누룽지 백도리탕", "0"),
        ("5", "2", "2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "뼈", "누룽지 백도리탕", "0"),
        ("6", "2", "2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "순살", "누룽지 백도리탕", "2000"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-04-19",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "송파삼전점_1-31_17:53:31",
            "menu_seq": menu_seq,
            "item_seq": seq,
            "parent_item_seq": parent_seq,
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": item_name if line_role == "main" else "누룽지 닭한마리",
            "item_name": item_name,
            "std_menu_name": std_name,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    group_attrs = menu_hierarchy._build_order_group_attrs(base)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    main_by_seq = attached[attached["line_role"].eq("main")].set_index("menu_seq")

    assert main_by_seq.at["1", "닭유형"] == "뼈닭"
    assert main_by_seq.at["1", "사이즈"] == "중"
    assert main_by_seq.at["1", "닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_ORDER_SEQUENCE
    assert main_by_seq.at["1", menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == "[중] 2인 | 뼈"
    assert main_by_seq.at["2", "닭유형"] == "순살"
    assert main_by_seq.at["2", "사이즈"] == "중"
    assert main_by_seq.at["2", "닭유형_판정"] == menu_hierarchy.CHICKEN_METHOD_ORDER_SEQUENCE
    assert main_by_seq.at["2", menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == "[중] 2인 | 순살"
    assert set(main_by_seq[menu_hierarchy.CHICKEN_SIGNAL_COLUMN]) == {menu_hierarchy.CHICKEN_SIGNAL_PRESENT}


def test_multi_main_order_sequence_does_not_split_when_type_count_is_ambiguous():
    rows = []
    for seq, menu_seq, parent_seq, line_role, option_kind, item_name, std_name in [
        ("1", "1", "1", "main", menu_hierarchy.OPTION_KIND_MAIN, "도리당 닭도리탕", "도리당 닭도리탕"),
        ("2", "2", "2", "main", menu_hierarchy.OPTION_KIND_MAIN, "누룽지 백도리탕", "누룽지 백도리탕"),
        ("3", "2", "2", "option", menu_hierarchy.OPTION_KIND_SIZE, "[중] 2인", "누룽지 백도리탕"),
        ("4", "2", "2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "뼈", "누룽지 백도리탕"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-04-19",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O_AMBIGUOUS_MULTI_MAIN",
            "menu_seq": menu_seq,
            "item_seq": seq,
            "parent_item_seq": parent_seq,
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": item_name,
            "item_name": item_name,
            "std_menu_name": std_name,
            "qty": "1",
            "total_price": "0",
        })
        rows.append(row)

    group_attrs = menu_hierarchy._build_order_group_attrs(pd.DataFrame(rows))

    assert menu_hierarchy.CHICKEN_METHOD_ORDER_SEQUENCE not in set(group_attrs["닭유형_판정_auto"])


def test_order_group_attrs_include_sale_date_to_prevent_cross_day_size_bleed(monkeypatch):
    rows = []
    for sale_date, seq, line_role, option_kind, menu_name, item_name, std_name, total_price in [
        ("2026-06-16", "1", "main", menu_hierarchy.OPTION_KIND_MAIN, "1인 순살 닭도리탕(공깃밥 포함)", "1인 순살 닭도리탕(공깃밥 포함)", "1인 순살 닭도리탕(밥포함)", "11900"),
        ("2026-06-16", "2", "option", menu_hierarchy.OPTION_KIND_SPICE, "1인 순살 닭도리탕(공깃밥 포함)", "기본맛", "1인 순살 닭도리탕(밥포함)", "0"),
        ("2026-07-02", "1", "main", menu_hierarchy.OPTION_KIND_MAIN, "한우순살곱도리탕", "한우순살곱도리탕", "한우 순살 곱도리탕", "28900"),
        ("2026-07-02", "2", "option", menu_hierarchy.OPTION_KIND_SIZE, "한우순살곱도리탕", "[중] 2인", "한우 순살 곱도리탕", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": sale_date,
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "송파삼전점_1-18_19:28:32",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": menu_name,
            "item_name": item_name,
            "std_menu_name": std_name,
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    profile_master = menu_hierarchy._build_menu_chicken_profile_master(base)
    group_attrs = menu_hierarchy._build_order_group_attrs(base, menu_chicken_profile_master=profile_master)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    hanwoo_main = attached[
        attached["sale_date"].eq("2026-07-02")
        & attached["line_role"].eq("main")
        & attached["std_menu_name"].eq("한우 순살 곱도리탕")
    ].iloc[0]
    profit = menu_hierarchy._build_manual_profit_rate_master(attached)
    hanwoo_profit = profit[
        profit["수익키"].eq("메뉴|홀|한우 순살 곱도리탕|중|순살")
    ]

    assert hanwoo_main["사이즈"] == "중"
    assert hanwoo_main[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN] == "[중] 2인"
    assert not profit["수익키"].eq("메뉴|홀|한우 순살 곱도리탕|1인|순살").any()
    assert len(hanwoo_profit) == 1


def test_paid_chicken_type_option_is_absorbed_into_parent_profit_key(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_name, total_price in [
        ("1", "option", menu_hierarchy.OPTION_KIND_SIZE, "3~4인", "0"),
        ("2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "닭도리탕 [순살]", "1000"),
        ("3", "main", menu_hierarchy.OPTION_KIND_MAIN, "갈비찜닭 반반 [3~6인]", "47500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-08-02",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O_HALF_SET",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "3",
            "line_role": line_role,
            "option_kind": option_kind,
            "menu_name": "갈비찜닭 반반 [3~6인]",
            "item_name": item_name,
            "std_menu_name": "갈비찜닭 반반세트",
            menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "3~4인 | 닭도리탕 [순살]",
            menu_hierarchy.OPTION_COMBO_COLUMN: "3~4인 | 닭도리탕 [순살]",
            "닭유형": menu_hierarchy.CHICKEN_TYPE_MIXED,
            "사이즈": "중",
            "닭유형_판정": menu_hierarchy.CHICKEN_METHOD_HALF_SLOT,
            "사이즈_판정": "수기",
            menu_hierarchy.CHICKEN_SIGNAL_COLUMN: menu_hierarchy.CHICKEN_SIGNAL_PRESENT if line_role == "main" else "",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)
    base = pd.DataFrame(rows)
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    profit = menu_hierarchy._build_manual_profit_rate_master(base)
    parent = profit[profit["수익키"].eq("메뉴|홀|갈비찜닭 반반세트|중|혼합")]

    assert not profit["수익키"].eq("메뉴|홀|갈비찜닭 반반세트|중|순살").any()
    assert len(parent) == 1
    assert parent.iloc[0]["매출합계"] == "48500"
    assert parent.iloc[0]["판매수량"] == "1"
    assert parent.iloc[0]["대표품목원문"] == "갈비찜닭 반반 [3~6인]"
    assert parent.iloc[0][menu_hierarchy.CHICKEN_SIGNAL_COLUMN] == menu_hierarchy.CHICKEN_SIGNAL_PRESENT
    assert parent.iloc[0]["원본품목명목록"] == "갈비찜닭 반반 [3~6인]"


def test_paid_chicken_gram_option_is_forced_to_chicken_addon(monkeypatch):
    stale_master = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "ADD_BONELESS_300",
        "item_name": "순살(닭다리살 100%) 300g",
        "option_kind_확정": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
    }]).reindex(columns=[*menu_hierarchy.OPTION_KIND_MASTER_KEY_COLUMNS, *menu_hierarchy.OPTION_KIND_MASTER_EDIT_COLUMNS], fill_value="")
    row = {
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "ADD_BONELESS_300",
        "item_name": "순살(닭다리살 100%) 300g",
        "line_role": "option",
        "std_menu_name": "도리당 닭도리탕",
        "order_id": "O_CHICKEN_ADDON_KIND",
        "qty": "1",
        "total_price": "7500",
    }
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)

    out = menu_hierarchy._attach_option_kind(pd.DataFrame([row]))
    master = menu_hierarchy._build_option_kind_master(out)

    assert out.iloc[0]["option_kind"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert master.iloc[0]["option_kind_제안"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert master.iloc[0]["option_kind_확정"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert "닭가산_manual" in master.columns
    assert "닭가산유형_manual" in master.columns


def test_option_kind_master_preserves_manual_rows_missing_from_current_orders(monkeypatch):
    previous = pd.DataFrame([{
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "100059083",
        "item_name": "닭다리살 100% 순살 300g 추가",
        "line_role": "option",
        "std_menu_name": "1인 순살 닭도리탕(밥포함)",
        "option_kind_제안": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
        "option_kind_확정": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
        "재료명_제안": "",
        "재료명_확정": "",
    }]).reindex(columns=menu_hierarchy.OPTION_KIND_MASTER_COLUMNS, fill_value="")
    current = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    current.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "300004013",
        "item_name": "닭다리살 100% 순살 300g 추가",
        "line_role": "option",
        "std_menu_name": "도리당 닭도리탕",
        "order_id": "O_CURRENT_ADDON",
        "qty": "1",
        "total_price": "7500",
    })
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: previous)

    master = menu_hierarchy._build_option_kind_master(pd.DataFrame([current]))
    preserved = master[master["item_id"].eq("100059083")]

    assert len(preserved) == 1
    assert preserved.iloc[0]["option_kind_확정"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert preserved.iloc[0]["item_name"] == "닭다리살 100% 순살 300g 추가"


def test_chicken_addon_and_change_options_are_separated(monkeypatch):
    rows = [
        {
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "ADD_OKPOS_300_A",
            "item_name": "닭다리살100% 순살 300g",
            "line_role": "option",
            "std_menu_name": "낙곱새 전골",
            "order_id": "O_KIND_SPLIT",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "ADD_BAEMIN_300",
            "item_name": "순살 (100%닭다리살)300g 추가",
            "line_role": "option",
            "std_menu_name": "한우 순살 곱도리탕",
            "order_id": "O_KIND_SPLIT",
        },
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "ADD_COUPANG_150",
            "item_name": "한그릇] 닭다리살 100% 순살 150g 추가",
            "line_role": "option",
            "std_menu_name": "누룽지 1인 순살 백도리당",
            "order_id": "O_KIND_SPLIT",
        },
        {
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "CHANGE_BONELESS",
            "item_name": "닭다리살 100% 순살로 변경",
            "line_role": "option",
            "std_menu_name": "도리당 닭도리탕",
            "order_id": "O_KIND_SPLIT",
        },
        {
            "source": "posfeed",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "CHANGE_BONELESS_POSFEED",
            "item_name": "순살(닭다리살100%)로 변경",
            "line_role": "option",
            "std_menu_name": "도리당 닭도리탕",
            "order_id": "O_KIND_SPLIT",
        },
        {
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "item_id": "COMBO_OPTION",
            "item_name": "[중] 닭다리살 + 우삼겹 300g + 묵은지 600g",
            "line_role": "option",
            "std_menu_name": "도리당 닭도리탕",
            "order_id": "O_KIND_SPLIT",
        },
    ]
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: pd.DataFrame())

    out = menu_hierarchy._attach_option_kind(pd.DataFrame(rows))
    by_item = dict(zip(out["item_id"], out["option_kind"]))

    assert by_item["ADD_OKPOS_300_A"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert by_item["ADD_BAEMIN_300"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert by_item["ADD_COUPANG_150"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert by_item["CHANGE_BONELESS"] == menu_hierarchy.OPTION_KIND_CHICKEN_TYPE
    assert by_item["CHANGE_BONELESS_POSFEED"] == menu_hierarchy.OPTION_KIND_CHICKEN_TYPE
    assert by_item["COMBO_OPTION"] != menu_hierarchy.OPTION_KIND_CHICKEN_ADDON


def test_paid_chicken_gram_addon_splits_profit_and_adds_chicken_usage(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_id, item_name, qty, total_price in [
        ("1", "main", menu_hierarchy.OPTION_KIND_MAIN, "MAIN", "도리당 닭도리탕", "1", "28900"),
        ("2", "option", menu_hierarchy.OPTION_KIND_SIZE, "SIZE_M", "[중] 2인", "1", "0"),
        ("3", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "TYPE_BONE", "뼈", "1", "0"),
        ("4", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "ADD_BONELESS_300", "순살(닭다리살 100%) 300g", "1", "7500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-04-17",
            "platform": "홀",
            "order_type": "홀_테이블",
            "order_id": "O_CHICKEN_ADDON_300",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "option_kind": option_kind,
            "item_id": item_id,
            "menu_name": "도리당 닭도리탕",
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "qty": qty,
            "total_price": total_price,
        })
        rows.append(row)
    stale_master = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "ADD_BONELESS_300",
        "item_name": "순살(닭다리살 100%) 300g",
        "option_kind_확정": menu_hierarchy.OPTION_KIND_CHICKEN_TYPE,
    }]).reindex(columns=[*menu_hierarchy.OPTION_KIND_MASTER_KEY_COLUMNS, *menu_hierarchy.OPTION_KIND_MASTER_EDIT_COLUMNS], fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))
    monkeypatch.setattr(menu_hierarchy, "_chicken_conversion_attrs", lambda: {"순살_1마리_g": 600.0, "뼈닭_1마리_g": 600.0})

    base = menu_hierarchy._attach_option_kind(pd.DataFrame(rows))
    group_attrs = menu_hierarchy._build_order_group_attrs(base)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    attached = menu_hierarchy._attach_chicken_addon_columns(attached)
    profit = menu_hierarchy._build_manual_profit_rate_master(attached)

    main = attached[attached["line_role"].eq("main")].iloc[0]
    addon = attached[attached["item_id"].eq("ADD_BONELESS_300")].iloc[0]
    menu_row = profit[profit["수익키"].eq("메뉴|홀|도리당 닭도리탕|중|뼈닭")].iloc[0]
    addon_row = profit[profit["수익키"].eq("품목|홀|도리당 닭도리탕|중|뼈닭|순살추가300g")].iloc[0]

    assert addon["option_kind"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert "순살(닭다리살 100%) 300g" not in main[menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN]
    assert addon[menu_hierarchy.CHICKEN_ADDON_BONELESS_COLUMN] == "0.5"
    assert main[menu_hierarchy.CHICKEN_ADDON_BONELESS_COLUMN] == "0.5"
    assert menu_row["매출합계"] == "28900"
    assert addon_row["매출합계"] == "7500"
    assert addon_row["대표품목명"] == "순살추가300g"


def test_one_serving_paid_option_does_not_override_parent_size_for_profit(monkeypatch):
    rows = []
    for seq, line_role, item_id, item_name, qty, unit_price, total_price in [
        ("1", "main", "200008035", "[삼계탕] 누룽지 닭한마리", "1", "22500", "22500"),
        ("2", "option", "200008012", "닭다리살 100% 순살로 변경", "1", "2000", "2000"),
        ("3", "option", "200008039", "[소] 반마리", "1", "0", "0"),
        ("4", "option", "200008000", "1인분", "1", "16900", "16900"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "배민수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-12",
            "platform": "배달의민족",
            "order_type": "배달",
            "order_id": "O_NOORUNGJI_ONE_SERVING_ADDON",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "item_id": item_id,
            "menu_name": "[삼계탕] 누룽지 닭한마리",
            "item_name": item_name,
            "std_menu_name": "누룽지 닭한마리",
            "qty": qty,
            "unit_price": unit_price,
            "total_price": total_price,
        })
        rows.append(row)
    stale_master = pd.DataFrame([{
        "source": "배민수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "200008000",
        "item_name": "1인분",
        "line_role": "option",
        "std_menu_name": "누룽지 닭한마리",
        "option_kind_확정": menu_hierarchy.OPTION_KIND_SIZE,
    }]).reindex(columns=[*menu_hierarchy.OPTION_KIND_MASTER_KEY_COLUMNS, *menu_hierarchy.OPTION_KIND_MASTER_EDIT_COLUMNS], fill_value="")
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: stale_master)
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    base = menu_hierarchy._attach_option_kind(pd.DataFrame(rows))
    group_attrs = menu_hierarchy._build_order_group_attrs(base)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    attached = menu_hierarchy._attach_chicken_addon_columns(attached)
    profit = menu_hierarchy._build_manual_profit_rate_master(attached)

    main = attached[attached["line_role"].eq("main")].iloc[0]
    addon = attached[attached["item_id"].eq("200008000")].iloc[0]

    assert main["사이즈"] == "소"
    assert main["닭유형"] == "순살"
    assert addon["option_kind"] == menu_hierarchy.OPTION_KIND_CHICKEN_ADDON
    assert addon[menu_hierarchy.CHICKEN_ADDON_USAGE_COLUMN] == "0.5"
    assert not profit["수익키"].eq("메뉴|배달의민족|누룽지 닭한마리|1인|순살").any()
    menu_row = profit[profit["수익키"].eq("메뉴|배달의민족|누룽지 닭한마리|소|순살")].iloc[0]
    addon_row = profit[profit["수익키"].eq("품목|배달의민족|누룽지 닭한마리|소|순살|1인분 추가")].iloc[0]
    assert menu_row["매출합계"] == "24500"
    assert addon_row["매출합계"] == "16900"


def test_paid_chicken_150g_addon_defaults_to_half_usage(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_id, item_name, qty, total_price in [
        ("1", "main", menu_hierarchy.OPTION_KIND_MAIN, "MAIN", "누룽지 1인 순살 백도리당", "1", "19900"),
        ("2", "option", menu_hierarchy.OPTION_KIND_CHICKEN_ADDON, "ADD_BONELESS_150", "한그릇] 닭다리살 100% 순살 150g 추가", "1", "4000"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-04-17",
            "platform": "쿠팡이츠",
            "order_type": "배달",
            "order_id": "O_CHICKEN_ADDON_150",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "option_kind": option_kind,
            "item_id": item_id,
            "menu_name": "누룽지 1인 순살 백도리당",
            "item_name": item_name,
            "std_menu_name": "누룽지 1인 순살 백도리당",
            "qty": qty,
            "total_price": total_price,
            "닭유형": "순살" if line_role == "main" else "",
            "사이즈": "1인" if line_role == "main" else "",
            menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "0.3" if line_role == "main" else "",
            menu_hierarchy.BONELESS_USAGE_TOTAL_COLUMN: "0.3" if line_role == "main" else "",
        })
        rows.append(row)
    monkeypatch.setattr(menu_hierarchy, "_option_kind_master_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_chicken_conversion_attrs", lambda: {"순살_1마리_g": 300.0, "뼈닭_1마리_g": 300.0})

    attached = menu_hierarchy._attach_chicken_addon_columns(pd.DataFrame(rows))

    main = attached[attached["line_role"].eq("main")].iloc[0]
    addon = attached[attached["line_role"].eq("option")].iloc[0]
    assert addon[menu_hierarchy.CHICKEN_ADDON_BONELESS_COLUMN] == "0.5"
    assert main[menu_hierarchy.CHICKEN_ADDON_BONELESS_COLUMN] == "0.5"
    assert main[menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "0.8"


def test_chicken_addon_price_mismatch_is_warn():
    rows = []
    for item_id, item_name, unit_price, total_price in [
        ("ADD_150_BAD_PRICE", "한그릇] 닭다리살 100% 순살 150g 추가", "3000", "3000"),
        ("ADD_300_OK_PRICE", "순살(닭다리살 100%) 300g", "7500", "7500"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "okpos",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-04-17",
            "ym": "2026-04",
            "order_id": "O_CHICKEN_ADDON_PRICE",
            "item_seq": item_id,
            "item_id": item_id,
            "line_role": "option",
            "option_kind": menu_hierarchy.OPTION_KIND_CHICKEN_ADDON,
            "item_name": item_name,
            "std_menu_name": "도리당 닭도리탕",
            "qty": "1",
            "unit_price": unit_price,
            "total_price": total_price,
            "수익키": "품목|홀|도리당 닭도리탕|중|뼈닭|순살추가",
            "공헌이익": "1",
            "수기수익": "1",
        })
        rows.append(row)

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame(rows), pd.DataFrame())
    price_issues = issues[issues["issue_type"].eq("chicken_addon_price_mismatch")]

    assert len(price_issues) == 1
    assert price_issues.iloc[0]["severity"] == "WARN"
    assert price_issues.iloc[0]["item_id"] == "ADD_150_BAD_PRICE"


def test_classification_audit_warns_when_menu_name_chicken_signal_conflicts_final_type():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-07-15",
        "ym": "2026-07",
        "order_id": "O_MENU_SIGNAL_CONFLICT",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "10030905",
        "menu_name": "순살 닭한마리 칼국수 정식 (2인이상)",
        "item_name": "닭한마리 칼국수 정식 (2인이상)",
        "std_menu_name": "[점심] 닭한마리 칼국수 정식 (2인이상)",
        "qty": "1",
        "total_price": "21000",
        "닭유형": "뼈닭",
        "사이즈": "중",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1",
    })

    audit = menu_hierarchy._build_classification_audit(pd.DataFrame([row]), pd.DataFrame(), pd.DataFrame())
    conflict = audit[audit["audit_type"].eq("main_menu_name_chicken_type_conflict")]

    assert len(conflict) == 1
    assert conflict.iloc[0]["severity"] == "WARN"
    assert "메뉴명보정/메뉴닭프로필 확인" in conflict.iloc[0]["detail"]


def test_non_main_tmp_drink_product_gap_is_warn_not_blocking_tmp_item():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-19",
        "ym": "2026-08",
        "order_id": "O_TMP_DRINK",
        "item_seq": "7",
        "item_id": "TMP_DRINK",
        "item_name": "쿨피스 350ml (캔)",
        "std_menu_name": "1인 순살 닭도리탕(밥포함)",
        "line_role": "option",
        "option_kind": menu_hierarchy.OPTION_KIND_DRINK,
        "total_price": "2000",
    })
    gap = pd.DataFrame([{
        "item_id": "TMP_DRINK",
        "source": "posfeed",
        "item_name": "쿨피스 350ml (캔)",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "side",
    }]).reindex(columns=menu_hierarchy.GAP_COLUMNS, fill_value="")

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([row]), gap)

    assert not issues["issue_type"].eq("tmp_item").any()
    product_gap = issues[issues["issue_type"].eq("product_gap")]
    assert len(product_gap) == 1
    assert product_gap.iloc[0]["severity"] == "WARN"


def test_resolved_tmp_main_product_gap_is_warn_not_blocking_tmp_item():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-20",
        "ym": "2026-08",
        "order_id": "O_RESOLVED_TMP",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "TMP_RESOLVED",
        "item_name": "[단짠단짠] 순살 갈비찜닭",
        "std_menu_name": "순살 갈비찜닭",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "total_price": "29500",
        "닭유형": "순살",
        "사이즈": "중",
        "수익키": "메뉴|쿠팡이츠|순살 갈비찜닭|중|순살",
    })
    gap = pd.DataFrame([{
        "item_id": "TMP_RESOLVED",
        "source": "posfeed",
        "item_name": "[단짠단짠] 순살 갈비찜닭",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "main",
    }]).reindex(columns=menu_hierarchy.GAP_COLUMNS, fill_value="")

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([row]), gap)

    assert not issues["issue_type"].eq("tmp_item").any()
    product_gap = issues[issues["issue_type"].eq("product_gap")]
    assert len(product_gap) == 1
    assert product_gap.iloc[0]["severity"] == "WARN"


def test_unresolved_tmp_main_product_gap_stays_blocking():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "posfeed",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-08-20",
        "ym": "2026-08",
        "order_id": "O_UNRESOLVED_TMP",
        "item_seq": "1",
        "parent_item_seq": "1",
        "menu_seq": "1",
        "item_id": "TMP_UNRESOLVED",
        "item_name": "신규 닭도리탕",
        "std_menu_name": "신규 닭도리탕",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "total_price": "29500",
    })
    gap = pd.DataFrame([{
        "item_id": "TMP_UNRESOLVED",
        "source": "posfeed",
        "item_name": "신규 닭도리탕",
        "주문건수": "1",
        "현재상태": "상품표_없음",
        "추정_수동분류": "main",
    }]).reindex(columns=menu_hierarchy.GAP_COLUMNS, fill_value="")

    issues = menu_hierarchy._build_validation_issues(pd.DataFrame(), pd.DataFrame([row]), gap)

    severity_by_type = issues.drop_duplicates("issue_type").set_index("issue_type")["severity"].to_dict()
    assert severity_by_type["product_gap"] == "ERROR"
    assert severity_by_type["tmp_item"] == "ERROR"


def test_stale_mixed_judgement_option_is_constrained_by_current_chicken_key():
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-04-18",
        "ym": "2026-04",
        "platform": "홀",
        "order_type": "홀_테이블",
        "order_id": "O_STALE_JUDGEMENT",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "menu_name": "묵은지 닭도리탕",
        "item_name": "묵은지 닭도리탕",
        "std_menu_name": "묵은지 닭도리탕",
        "qty": "1",
        "total_price": "32500",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인 | 뼈",
        "닭유형": menu_hierarchy.CHICKEN_TYPE_MIXED,
        "사이즈": "대",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1.3614",
        menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN: "1.3614",
        menu_hierarchy.CHICKEN_CONFIDENCE_COLUMN: "입력필요",
    })
    judgement = pd.DataFrame([{
        "source": "okpos",
        "brand": "도리당",
        "store": "송파삼전점",
        "std_menu_name": "묵은지 닭도리탕",
        "조건": "자동보정",
        menu_hierarchy.CHICKEN_OPTION_KEY_COLUMN: "[대] 3인 | 뼈",
        "닭유형": menu_hierarchy.CHICKEN_TYPE_MIXED,
        "사이즈": "대",
        menu_hierarchy.CHICKEN_USAGE_COLUMN: "1.3614",
    }]).reindex(columns=menu_hierarchy.JUDGEMENT_OPTION_COLUMNS, fill_value="")

    out, result = menu_hierarchy._apply_judgement_options(pd.DataFrame([row]), judgement)

    main = out.iloc[0]
    assert main["닭유형"] == "뼈닭"
    assert main[menu_hierarchy.CHICKEN_USAGE_COLUMN] == "1.5"
    assert main[menu_hierarchy.CHICKEN_USAGE_TOTAL_COLUMN] == "1.5"
    assert not result.empty


def test_product_manual_chicken_requires_item_name_match(monkeypatch):
    rows = []
    for seq, line_role, option_kind, item_id, item_name, total_price in [
        ("1", "main", menu_hierarchy.OPTION_KIND_MAIN, "300004066", "[삼계탕] 누룽지 닭한마리", "42100"),
        ("2", "option", menu_hierarchy.OPTION_KIND_SIZE, "300004017", "[대] 한마리반 + 기본제공", "0"),
        ("3", "option", menu_hierarchy.OPTION_KIND_CHICKEN_TYPE, "300004014", "닭다리살 100% 순살로 변경", "0"),
    ]:
        row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
        row.update({
            "source": "쿠팡수동",
            "brand": "도리당",
            "store": "송파삼전점",
            "sale_date": "2026-07-05",
            "platform": "쿠팡이츠",
            "order_type": "배달",
            "order_id": "O_COUPANG_NOODLE",
            "menu_seq": "1",
            "item_seq": seq,
            "parent_item_seq": "1",
            "line_role": line_role,
            "option_kind": option_kind,
            "item_id": item_id,
            "menu_name": "[재주문 1위] 도리당 닭도리탕",
            "item_name": item_name,
            "std_menu_name": "누룽지 닭한마리",
            "qty": "1",
            "total_price": total_price,
        })
        rows.append(row)

    stale_review = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "300004066",
        "item_name": "[후.참] 통 가래떡",
        "닭유형_manual": "순살",
        "사이즈_manual": "1인",
        "닭사용량_manual": "0.3",
    }]).reindex(
        columns=["source", "brand", "store", "item_id", "item_name", *menu_hierarchy._MANUAL_CHICKEN_COLUMNS, "수익률_manual"],
        fill_value="",
    )
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: stale_review)
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())
    monkeypatch.setattr(menu_hierarchy, "_manual_profit_rate_attrs", lambda: pd.DataFrame(columns=menu_hierarchy.MANUAL_PROFIT_RATE_MASTER_COLUMNS))

    base = pd.DataFrame(rows)
    group_attrs = menu_hierarchy._build_order_group_attrs(base)
    attached = menu_hierarchy._attach_manual_chicken_columns(base, group_attrs=group_attrs)
    profit = menu_hierarchy._build_manual_profit_rate_master(attached)
    main = attached[attached["line_role"].eq("main")].iloc[0]

    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "대"
    assert main["닭유형_판정"] == "변경"
    assert main["사이즈_판정"] == "선택"
    assert not profit["수익키"].eq("메뉴|쿠팡이츠|누룽지 닭한마리|1인|순살").any()
    assert profit["수익키"].eq("메뉴|쿠팡이츠|누룽지 닭한마리|대|순살").any()


def test_product_manual_chicken_still_applies_when_item_name_matches(monkeypatch):
    row = {col: "" for col in menu_hierarchy.LEFT_JOINED_OUTPUT_COLUMNS}
    row.update({
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "sale_date": "2026-07-05",
        "platform": "쿠팡이츠",
        "order_type": "배달",
        "order_id": "O_COUPANG_EXACT",
        "menu_seq": "1",
        "item_seq": "1",
        "parent_item_seq": "1",
        "line_role": "main",
        "option_kind": menu_hierarchy.OPTION_KIND_MAIN,
        "item_id": "300004066",
        "menu_name": "[재주문 1위] 도리당 닭도리탕",
        "item_name": "[삼계탕] 누룽지 닭한마리",
        "std_menu_name": "누룽지 닭한마리",
        "qty": "1",
        "total_price": "42100",
    })
    review = pd.DataFrame([{
        "source": "쿠팡수동",
        "brand": "도리당",
        "store": "송파삼전점",
        "item_id": "300004066",
        "item_name": "[삼계탕] 누룽지 닭한마리",
        "닭유형_manual": "순살",
        "사이즈_manual": "1인",
        "닭사용량_manual": "0.3",
    }]).reindex(
        columns=["source", "brand", "store", "item_id", "item_name", *menu_hierarchy._MANUAL_CHICKEN_COLUMNS, "수익률_manual"],
        fill_value="",
    )
    monkeypatch.setattr(menu_hierarchy, "_manual_chicken_attrs", lambda: review)
    monkeypatch.setattr(menu_hierarchy, "_manager_input_attrs", lambda: pd.DataFrame())

    attached = menu_hierarchy._attach_manual_chicken_columns(pd.DataFrame([row]))
    main = attached.iloc[0]

    assert main["닭유형"] == "순살"
    assert main["사이즈"] == "1인"
    assert main["닭유형_판정"] == "수기"
    assert main["사이즈_판정"] == "수기"
