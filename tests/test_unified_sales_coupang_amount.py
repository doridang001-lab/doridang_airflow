import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_coupang as coupang


def _raw(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "sale_date": "2026-07-20",
        "order_time": "00:03:00",
        "order_date": "2026.07.20 00:03:00",
        "delivery_type": "배달",
        "order_status": "",
        "order_summary": "테스트메뉴",
        "total_price": "0",
        "is_cancelled": "N",
        "menu_name": "테스트메뉴",
        "menu_qty": 1,
        "menu_price": 0,
        "menu_options": "테스트옵션",
        "매출액": 0,
        "취소금액": 0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def _patch_allocator(monkeypatch) -> None:
    monkeypatch.setattr(
        coupang,
        "allocate_manual_item_ids",
        lambda df: pd.Series(
            [f"9000000{i}" for i in range(1, len(df) + 1)],
            index=df.index,
        ),
    )


def test_full_cancel_books_zero(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([
        {
            "order_id": "CANCEL1",
            "is_cancelled": "Y",
            "매출액": 0,
            "취소금액": 22_400,
            "menu_options": "취소 메뉴",
        },
        {
            "order_id": "CANCEL1",
            "is_cancelled": "Y",
            "매출액": 0,
            "취소금액": 22_400,
            "menu_options": "취소 옵션",
        },
    ])

    out = coupang._transform_to_unified(raw, "전주전북대점", "도리당", {})

    assert out["total_price"].sum() == 0
    assert out["order_cnt"].sum() == 0


def test_partial_cancel_uses_net_sales(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([{
        "order_id": "PARTIAL1",
        "is_cancelled": "N",
        "매출액": 9_000,
        "취소금액": 56_300,
    }])

    out = coupang._transform_to_unified(raw, "기흥테라타워점", "도리당", {})

    assert out["total_price"].sum() == 9_000
    assert out["order_cnt"].sum() == 1


def test_mixed_day_matches_toorder(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([
        {"order_id": "NORMAL1", "매출액": 25_400, "total_price": "25400"},
        {"order_id": "NORMAL2", "매출액": 16_000, "total_price": "16000"},
        {
            "order_id": "CANCEL1",
            "is_cancelled": "Y",
            "매출액": 0,
            "취소금액": 22_400,
            "total_price": "22400",
        },
        {
            "order_id": "CANCEL2",
            "is_cancelled": "Y",
            "매출액": 0,
            "취소금액": 36_600,
            "total_price": "36600",
        },
    ])

    out = coupang._transform_to_unified(raw, "전주전북대점", "도리당", {})

    assert out["total_price"].sum() == 41_400
    assert out["order_cnt"].sum() == 2


def test_missing_settlement_falls_back_to_total_price(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([{
        "order_id": "MISSING1",
        "매출액": None,
        "total_price": "35800",
        "is_cancelled": "N",
    }])

    out = coupang._transform_to_unified(raw, "동탄영천점", "도리당", {})

    assert out["total_price"].sum() == 35_800
    assert out["order_cnt"].sum() == 1


def test_priced_coupang_row_uses_parent_menu_not_option(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([
        {
            "order_id": "OPTION_SALES",
            "order_summary": "메인메뉴 외 1건",
            "menu_name": "실제 부모 메뉴",
            "menu_price": "30000",
            "menu_options": "꼬치오뎅 2개 추가",
            "매출액": None,
            "total_price": "30000",
        },
        {
            "order_id": "OPTION_SALES",
            "order_summary": "메인메뉴 외 1건",
            "menu_name": "실제 부모 메뉴",
            "menu_price": "",
            "menu_options": "기본맛",
            "매출액": None,
            "total_price": "30000",
        },
    ])

    out = coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert out["menu_name"].tolist() == ["실제 부모 메뉴", "실제 부모 메뉴"]
    assert out["item_name"].tolist() == ["실제 부모 메뉴", "기본맛"]
    assert out["total_price"].astype(int).tolist() == [30000, 0]
    assert out["order_cnt"].astype(int).tolist() == [1, 0]


def test_multi_menu_order_allocates_total_to_priced_menu_rows(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([
        {
            "order_id": "MULTI1",
            "order_summary": "닭도리탕 외 1건",
            "menu_name": "닭도리탕",
            "menu_price": "16900",
            "menu_options": "기본 구성",
            "매출액": None,
            "total_price": "17900",
        },
        {
            "order_id": "MULTI1",
            "order_summary": "닭도리탕 외 1건",
            "menu_name": "닭도리탕",
            "menu_price": "",
            "menu_options": "기본맛",
            "매출액": None,
            "total_price": "17900",
        },
        {
            "order_id": "MULTI1",
            "order_summary": "닭도리탕 외 1건",
            "menu_name": "흑미 공기밥",
            "menu_price": "1000",
            "menu_options": "기본",
            "매출액": None,
            "total_price": "17900",
        },
    ])

    out = coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert out["item_name"].tolist() == ["닭도리탕", "기본맛", "흑미 공기밥"]
    assert out["total_price"].astype(int).tolist() == [16900, 0, 1000]
    assert int(out["total_price"].sum()) == 17900
    assert out["order_cnt"].astype(int).sum() == 1


def test_discounted_order_allocates_rounding_difference_to_largest_menu(monkeypatch) -> None:
    _patch_allocator(monkeypatch)
    raw = _raw([
        {
            "order_id": "DISCOUNT1",
            "menu_name": "메뉴A",
            "menu_price": "10000",
            "menu_options": "옵션A",
            "매출액": None,
            "total_price": "15001",
        },
        {
            "order_id": "DISCOUNT1",
            "menu_name": "메뉴B",
            "menu_price": "10000",
            "menu_options": "옵션B",
            "매출액": None,
            "total_price": "15001",
        },
    ])

    out = coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert int(out["total_price"].sum()) == 15001
    assert sorted(out["total_price"].astype(int).tolist()) == [7500, 7501]
    assert out["order_cnt"].astype(int).sum() == 1


def test_raw_dedup_normalizes_numeric_and_null_text_variants() -> None:
    raw = _raw([
        {
            "_src_path": "orders_2026-06.parquet",
            "order_id": "DUP1",
            "order_status": None,
            "total_price": 10_000,
            "menu_qty": 1,
            "menu_price": 10_000,
            "매출액": 10_000,
        },
        {
            "_src_path": "orders_2026-06.parquet",
            "order_id": "DUP1",
            "order_status": "nan",
            "total_price": "10000.0",
            "menu_qty": "1.0",
            "menu_price": "10000.0",
            "매출액": 10_000,
        },
    ])

    out = coupang._deduplicate_raw(raw, "부산서면점", "2026-06")

    assert len(out) == 1
