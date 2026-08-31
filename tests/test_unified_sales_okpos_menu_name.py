import pandas as pd
import sys
import types

from modules.transform.pipelines.db import DB_UnifiedSales_okpos as okpos


def _empty_fin_product() -> pd.DataFrame:
    return pd.DataFrame(columns=["상품코드", "상품명", "is_main_candidate", "exclude_check"])


def _okpos_order_lines(amounts, discounts) -> pd.DataFrame:
    names = [
        "라면사리 1개",
        "흑미 공기밥",
        "모짜렐라 체다 치즈 추가",
        "시그니처 반반 [3~6인]",
        "3~4인",
        "닭도리탕 [뼈]",
        "닭한마리 [뼈]",
    ]
    rows = []
    for i, name in enumerate(names[: len(amounts)], start=1):
        rows.append(
            {
                "_order_key": "2026-07-01|도리당 송파삼전점|1|1",
                "상품명": name,
                "상품코드": f"10030{i:03d}",
                "item_seq": str(i),
                "수량": 1,
                "_item_amt": amounts[i - 1],
                "discount_amount": discounts[i - 1],
                "order_type": "홀_테이블",
                "테이블명": "1",
            }
        )
    return pd.DataFrame(rows)


def test_okpos_hall_zero_amount_uses_largest_discount_as_menu_name(monkeypatch):
    monkeypatch.setattr(okpos, "_load_fin_product", _empty_fin_product)

    merged = _okpos_order_lines(
        amounts=[0, 0, 0, 0, 0, 0, 0],
        discounts=[1000, 1000, 6000, 38500, 0, 0, 0],
    )

    out = okpos._resolve_menu_name(merged)

    assert out.tolist() == ["시그니처 반반 [3~6인]"] * len(merged)


def test_okpos_hall_positive_amount_keeps_existing_highest_price_rule(monkeypatch):
    monkeypatch.setattr(okpos, "_load_fin_product", _empty_fin_product)

    merged = _okpos_order_lines(
        amounts=[0, 0, 20000, 0],
        discounts=[50000, 1000, 0, 0],
    )

    out = okpos._resolve_menu_name(merged)

    assert out.tolist() == ["모짜렐라 체다 치즈 추가"] * len(merged)


def test_okpos_sales_import_is_lazy_for_daily_gate(monkeypatch):
    module_name = "modules.transform.pipelines.db.DB_OKPOS_Sales"
    fake_module = types.SimpleNamespace(
        _sum_csv_amount_for_date=lambda csv_path, sale_date, amount_col: (123, None)
    )
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    assert okpos._sum_okpos_csv_amount_for_date("daily.csv", "2026-08-01", "실매출액") == (123, None)


def test_transform_okpos_df_keeps_actual_default_and_supports_gross_basis(monkeypatch):
    monkeypatch.setattr(okpos, "_load_store_map", lambda: {})
    monkeypatch.setattr(okpos, "_lookup_store_meta", lambda store_map, store, key: "")
    monkeypatch.setattr(okpos, "allocate_manual_item_ids", lambda df, persist=True: df["item_id"].astype(str))
    monkeypatch.setattr(okpos, "_apply_fin_item_name", lambda df: df)

    order_df = pd.DataFrame(
        [
            {
                "_pk": "ORDER1",
                "sale_date": "2026-08-01",
                "매장명": "도리당 송파삼전점",
                "포스번호": "1",
                "영수번호": "10",
                "주문채널": "포스",
                "주문유형": "홀",
                "테이블명": "1",
                "최초주문시각": "12:00:00",
                "총매출액": "10000",
                "총할인액": "1000",
                "실매출액": "9000",
                "구분": "정상",
            }
        ]
    )
    item_df = pd.DataFrame(
        [
            {
                "_pk": "ITEM1",
                "sale_date": "2026-08-01",
                "매장명": "도리당 송파삼전점",
                "포스번호": "1",
                "영수증번호": "10",
                "상품코드": "P1",
                "상품명": "테스트 메뉴",
                "수량": "1",
                "총매출액": "10000",
                "할인액": "1000",
                "실매출액": "9000",
                "구분": "정상",
            }
        ]
    )

    actual = okpos._transform_okpos_df(order_df, item_df)
    gross = okpos._transform_okpos_df(order_df, item_df, amount_basis="gross")

    assert int(actual["total_price"].sum()) == 9000
    assert int(gross["total_price"].sum()) == 10000
    assert int(gross["discount_amount"].sum()) == 1000
