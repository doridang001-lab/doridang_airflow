from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_DeliveryCommission as delivery


def _partition(root: Path, brand: str, store: str) -> Path:
    path = root / f"brand={brand}" / f"store={store}" / "ym=2026-07"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_baemin_order(
    root: Path,
    brand: str,
    store: str,
    total: int,
    deposit: int | str,
    *,
    order_id: str = "공통주문번호",
    order_time: str = "2026. 07. 19. (일) 오후 12:00:00",
    status: str = "배달완료",
    payment: int | str | None = None,
    partner_instant_discount: int | str = 0,
    instant_discount: int | str | None = None,
    cash_amount: int | str = 0,
) -> None:
    path = _partition(root, brand, store) / "orders_2026-07.parquet"
    new = pd.DataFrame(
        [{
            "주문번호": order_id,
            "주문시각": order_time,
            "주문상태": status,
            "결제금액": total if payment is None else payment,
            "총결제금액": total,
            "즉시할인": partner_instant_discount if instant_discount is None else instant_discount,
            "즉시할인_파트너부담": partner_instant_discount,
            "만나서결제금액": cash_amount,
            "입금예정금액": deposit,
        }]
    )
    if path.exists():
        new = pd.concat([pd.read_parquet(path), new], ignore_index=True)
    new["즉시할인"] = new["즉시할인"].astype(str)
    new["즉시할인_파트너부담"] = new["즉시할인_파트너부담"].astype(str)
    new["만나서결제금액"] = new["만나서결제금액"].astype(str)
    new["입금예정금액"] = new["입금예정금액"].astype(str)
    new.to_parquet(path, index=False)


def _write_baemin_ad(
    root: Path,
    brand: str,
    store_name: str,
    spend: int,
    *,
    ad_date: str = "2026-07-19",
    impressions: int = 1_000,
    clicks: int = 50,
    orders: int = 5,
) -> None:
    path = _partition(root, brand, "광고") / "metrics.csv"
    new = pd.DataFrame(
        [{
            "날짜": ad_date,
            "store_name": store_name,
            "광고지출": spend,
            "노출수": impressions,
            "클릭수": clicks,
            "주문수": orders,
        }]
    )
    if path.exists():
        new = pd.concat([pd.read_csv(path, encoding="utf-8-sig", dtype=str), new], ignore_index=True)
    new.to_csv(path, index=False, encoding="utf-8-sig")


def _write_coupang_rows(
    root: Path,
    brand: str,
    store: str,
    rows: list[dict],
) -> None:
    defaults = {
        "order_id": "공통주문번호",
        "order_date": "2026.07.19 12:00:00",
        "매출액": 0,
        "정산_예정_금액": 0,
        "취소금액": 0,
        "is_cancelled": "N",
        "total_price": 0,
    }
    pd.DataFrame([{**defaults, **row} for row in rows]).to_parquet(
        _partition(root, brand, store) / "orders_2026-07.parquet",
        index=False,
    )


def _write_coupang_order(
    root: Path,
    brand: str,
    store: str,
    total: int,
    settlement: int,
    *,
    is_cancelled: str = "N",
    cancel_amount: int = 0,
) -> None:
    _write_coupang_rows(
        root,
        brand,
        store,
        [{
            "order_id": "공통주문번호",
            "order_date": "2026.07.19 12:00:00",
            "매출액": total,
            "정산_예정_금액": settlement,
            "취소금액": cancel_amount,
            "is_cancelled": is_cancelled,
        }],
    )


def _write_coupang_cmg(
    root: Path,
    brand: str,
    store: str,
    ratio: str,
    *,
    query_date: str = "2026-07-19",
    store_name: str | None = None,
    ad_cost: int = 1_000,
    new_customers: int = 2,
    impressions: int = 100,
    clicks: int = 10,
) -> None:
    path = _partition(root, brand, store) / "cmg.csv"
    display_store = store_name or f"{brand} {store}"
    new = pd.DataFrame(
        [{
            "조회일자": query_date,
            "매장명": display_store,
            "광고비율": ratio,
            "광고비용": ad_cost,
            "신규고객": new_customers,
            "광고노출수": impressions,
            "광고클릭수": clicks,
        }]
    )
    if path.exists():
        new = pd.concat([pd.read_csv(path, encoding="utf-8-sig", dtype=str), new], ignore_index=True)
    new.to_csv(path, index=False, encoding="utf-8-sig")


def _configure_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    output = tmp_path / "mart" / "delivery_commission.parquet"
    revenue_output = tmp_path / "mart" / "delivery_revenue.parquet"
    cost_output = tmp_path / "mart" / "store_cost_allocation.parquet"
    unified_root = tmp_path / "mart" / "unified_sales_grp"
    onedrive_root = tmp_path / "Repository"
    unified_root.mkdir(parents=True, exist_ok=True)
    onedrive_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(delivery, "BAEMIN_ORDERS_DB", tmp_path / "baemin_orders")
    monkeypatch.setattr(delivery, "BAEMIN_OUR_STORE_CLICKS_DB", tmp_path / "baemin_ads")
    monkeypatch.setattr(delivery, "COUPANG_ORDERS_DB", tmp_path / "coupang_orders")
    monkeypatch.setattr(delivery, "COUPANG_CMG_DB", tmp_path / "coupang_cmg")
    monkeypatch.setattr(delivery, "DELIVERY_COMMISSION_PATH", output)
    monkeypatch.setattr(delivery, "DELIVERY_REVENUE_PATH", revenue_output)
    monkeypatch.setattr(delivery, "STORE_COST_ALLOCATION_PATH", cost_output)
    monkeypatch.setattr(delivery, "ONEDRIVE_DB", onedrive_root)
    monkeypatch.setattr(
        delivery,
        "YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH",
        tmp_path
        / "analytics"
        / "Yogiyo"
        / "Yogiyo_Settlement"
        / "yogiyo_settlement_monthly_fee.csv",
    )
    monkeypatch.setattr(delivery, "UNIFIED_SALES_ROOT", unified_root)
    monkeypatch.setattr(
        delivery,
        "DDANGYO_FEE_RATIO_MONTHLY_CSV",
        tmp_path / "mart" / "ddangyo_fee_ratio_monthly.csv",
    )
    monkeypatch.setattr(
        delivery,
        "DDANGYO_FEE_RATIO_BASELINE_CSV",
        tmp_path / "mart" / "ddangyo_fee_ratio_baseline.csv",
    )
    _write_yogiyo_monthly_fee(delivery.YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH, [])
    return output


def _write_sales_employee(root: Path, rows: list[dict]) -> None:
    defaults = {
        "오픈순서": "1",
        "호점": "1",
        "매장명": "도리당 부산대신점",
        "플랫폼": "배달의 민족",
        "임대료": "",
        "collected_at": "2026-07-19 00:00",
    }
    pd.DataFrame([{**defaults, **row} for row in rows]).to_csv(
        root / "sales_employee.csv",
        index=False,
        encoding="utf-8-sig",
    )


def _write_unified_sales(root: Path, rows: list[dict]) -> None:
    defaults = {
        "sale_date": "2026-07-19",
        "ym": "2026-07",
        "source": "posfeed",
        "brand": "도리당",
        "store": "부산대신점",
        "platform": "땡겨요",
        "total_price": 0,
    }
    path = root / "unified_sales_260719.parquet"
    pd.DataFrame([{**defaults, **row} for row in rows]).to_parquet(path, index=False)


def _write_ddangyo_fee_ratios(monthly_path: Path, baseline_path: Path) -> None:
    monthly_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ym": "2026_07", "store": "도리당 부산대신점", "fee_ratio": "0.05"},
        ]
    ).to_csv(monthly_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"ym": "2026_07", "fee_ratio": "0.07"},
        ]
    ).to_csv(baseline_path, index=False, encoding="utf-8-sig")


def _write_yogiyo_monthly_fee(path: Path, rows: list[dict]) -> None:
    defaults = {
        "ym": "2026_07",
        "brand": "도리당",
        "store": "부산대신점",
        "sales_tot": 0,
        "settlement_tot": 0,
        "settlement_gap_rate": 0,
        "fee_tot": 0,
        "fee_rate": 0,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [{**defaults, **row} for row in rows]
    frame = pd.DataFrame(records) if records else pd.DataFrame(columns=defaults)
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )


def test_build_combines_both_brands_and_brand_aware_store_aliases(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        partner_instant_discount=1_000,
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        5_000,
        4_000,
        partner_instant_discount=300,
    )
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "부산대신점",
        "전체 15%",
        store_name="닭도리탕 전문 도리당 부산대신점",
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "나홀로",
        "대신점",
        "신규 12%, 재주문 7%",
        store_name="나홀로 1인 곱도리탕 대신점",
    )

    delivery.build_delivery_commission()

    result = pd.read_parquet(output).sort_values("platform").reset_index(drop=True)
    assert result.attrs[delivery.BAEMIN_DISCOUNT_INCLUDED_ATTR] is True
    assert list(result.columns) == delivery.OUTPUT_COLUMNS
    assert not result.duplicated(["sale_date", "store", "platform", "brand"]).any()
    assert set(result["store"]) == {"부산대신점"}
    assert set(result["brand"]) == {"도리당", "나홀로"}

    keyed = result.set_index(["platform", "brand"])
    assert keyed.loc[("배달의민족", "도리당"), "total_amt"] == 11_000
    assert keyed.loc[("배달의민족", "도리당"), "settlement_amount"] == 7_900
    assert keyed.loc[("배달의민족", "도리당"), "diff_amt"] == 3_100
    assert keyed.loc[("배달의민족", "도리당"), "diff_amt"] / keyed.loc[
        ("배달의민족", "도리당"), "total_amt"
    ] == pytest.approx(3_100 / 11_000)
    assert keyed.loc[("배달의민족", "도리당"), "배민_즉시할인"] == 1_000
    assert keyed.loc[("배달의민족", "도리당"), "우가클_평균비용"] == 2.0
    assert keyed.loc[("배달의민족", "도리당"), "우가클_주문수"] == 5
    assert keyed.loc[("배달의민족", "도리당"), "우가클_클릭율"] == 0.05
    assert keyed.loc[("배달의민족", "나홀로"), "total_amt"] == 5_300
    assert keyed.loc[("배달의민족", "나홀로"), "settlement_amount"] == 3_800
    assert keyed.loc[("배달의민족", "나홀로"), "diff_amt"] == 1_500
    assert keyed.loc[("쿠팡이츠", "도리당"), "total_amt"] == 7_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "settlement_amount"] == 5_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "diff_amt"] == 2_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_신규비율"] == 0.15
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_재주문비율"] == 0.15
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_광고비용"] == 1_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_신규고객"] == 2
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_광고노출수"] == 100
    assert keyed.loc[("쿠팡이츠", "도리당"), "쿠팡_광고클릭수"] == 10
    assert keyed.loc[("쿠팡이츠", "나홀로"), "total_amt"] == 3_000
    assert keyed.loc[("쿠팡이츠", "나홀로"), "settlement_amount"] == 2_000
    assert keyed.loc[("쿠팡이츠", "나홀로"), "diff_amt"] == 1_000
    assert keyed.loc[("쿠팡이츠", "나홀로"), "쿠팡_신규비율"] == 0.12
    assert keyed.loc[("쿠팡이츠", "나홀로"), "쿠팡_재주문비율"] == 0.07
    assert pd.isna(keyed.loc[("쿠팡이츠", "도리당"), "배민_즉시할인"])
    assert pd.isna(keyed.loc[("쿠팡이츠", "도리당"), "우가클_평균비용"])
    assert pd.isna(keyed.loc[("쿠팡이츠", "도리당"), "우가클_주문수"])
    assert pd.isna(keyed.loc[("쿠팡이츠", "도리당"), "우가클_클릭율"])
    assert pd.isna(keyed.loc[("배달의민족", "도리당"), "쿠팡_신규비율"])
    assert pd.isna(keyed.loc[("배달의민족", "도리당"), "쿠팡_재주문비율"])
    assert pd.isna(keyed.loc[("배달의민족", "도리당"), "쿠팡_광고비용"])


def test_build_delivery_revenue_keeps_platform_grain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        partner_instant_discount=1_000,
        instant_discount=4_000,
    )
    order_path = _partition(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점") / "orders_2026-07.parquet"
    order = pd.read_parquet(order_path)
    pd.concat([order, order], ignore_index=True).to_parquet(order_path, index=False)
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 20_000, 0,
        order_id="할인취소주문", status="주문취소", partner_instant_discount=8_000,
    )
    _write_sales_employee(
        delivery.ONEDRIVE_DB,
        [{"매장명": "도리당 부산대신점", "플랫폼": "배달의 민족", "임대료": "3100"}],
    )
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    delivery.build_delivery_commission()
    delivery.build_delivery_revenue()

    result = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    assert list(result.columns) == delivery.REVENUE_OUTPUT_COLUMNS
    assert not result.duplicated(["sale_date", "brand", "store", "platform"]).any()

    keyed = result.set_index(["platform", "brand"])
    assert keyed.loc[("배달의민족", "도리당"), "total_price"] == 11_000
    assert keyed.loc[("배달의민족", "도리당"), "settlement_price"] == 7_900
    assert keyed.loc[("배달의민족", "도리당"), "revenue"] == 3_100
    assert keyed.loc[("쿠팡이츠", "도리당"), "total_price"] == 7_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "settlement_price"] == 5_000
    assert keyed.loc[("쿠팡이츠", "도리당"), "revenue"] == 2_000
    assert keyed.loc[("배달의민족", "도리당"), "rent_cost"] == 59
    assert keyed.loc[("쿠팡이츠", "도리당"), "rent_cost"] == 41
    assert keyed.loc[("배달의민족", "도리당"), "fin_revenue"] == 3_041
    assert keyed.loc[("쿠팡이츠", "도리당"), "fin_revenue"] == 1_959
    assert keyed.loc[("배달의민족", "도리당"), "store_expected_month_fin_revenue"] == 155_000

    commission = pd.read_parquet(delivery.DELIVERY_COMMISSION_PATH)
    delivery.build_delivery_revenue()
    pd.testing.assert_frame_equal(result, pd.read_parquet(delivery.DELIVERY_REVENUE_PATH))
    delivery.build_delivery_commission()
    delivery.build_delivery_revenue()
    pd.testing.assert_frame_equal(commission, pd.read_parquet(delivery.DELIVERY_COMMISSION_PATH))
    pd.testing.assert_frame_equal(result, pd.read_parquet(delivery.DELIVERY_REVENUE_PATH))


def test_build_delivery_revenue_adds_unified_sales_delivery_platforms(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")
    _write_ddangyo_fee_ratios(
        delivery.DDANGYO_FEE_RATIO_MONTHLY_CSV,
        delivery.DDANGYO_FEE_RATIO_BASELINE_CSV,
    )
    _write_unified_sales(
        delivery.UNIFIED_SALES_ROOT,
        [
            {"platform": "땡겨요", "brand": "도리당", "store": "부산대신점", "total_price": 10_000},
            {"platform": "땡배달", "brand": "도리당", "store": "부산대신점", "total_price": 5_000},
            {"platform": "땡겨요", "brand": "도리당", "store": "신규점", "total_price": 20_000},
            {"platform": "먹깨비", "brand": "도리당", "store": "부산대신점", "total_price": 10_000},
            {"platform": "배달특급", "brand": "도리당", "store": "부산대신점", "total_price": 10_000},
            {"platform": "인천이음", "brand": "도리당", "store": "부산대신점", "total_price": 10_000},
        ],
    )

    delivery.build_delivery_commission()
    delivery.build_delivery_revenue()

    result = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    keyed = result.set_index(["platform", "brand", "store"])
    assert ("땡배달", "도리당", "부산대신점") not in keyed.index
    assert keyed.loc[("땡겨요", "도리당", "부산대신점"), "total_price"] == 15_000
    assert keyed.loc[("땡겨요", "도리당", "부산대신점"), "revenue"] == 750
    assert keyed.loc[("땡겨요", "도리당", "부산대신점"), "settlement_price"] == 14_250
    assert keyed.loc[("땡겨요", "도리당", "신규점"), "revenue"] == 1_400
    assert keyed.loc[("먹깨비", "도리당", "부산대신점"), "revenue"] == 150
    assert keyed.loc[("배달특급", "도리당", "부산대신점"), "revenue"] == 100
    assert keyed.loc[("인천이음", "도리당", "부산대신점"), "revenue"] == 200


def test_build_delivery_revenue_uses_yogiyo_monthly_gap_rate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")
    _write_yogiyo_monthly_fee(
        delivery.YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH,
        [
            {
                "store": "부산대신점",
                "sales_tot": 100_000,
                "settlement_gap_rate": 0.2,
                "fee_rate": 0.99,
            },
            {
                "store": "신규점",
                "sales_tot": 200_000,
                "settlement_gap_rate": 0.4,
            },
            {
                "store": "보정점",
                "sales_tot": 50_000,
                "settlement_gap_rate": -0.1,
            },
        ],
    )
    _write_unified_sales(
        delivery.UNIFIED_SALES_ROOT,
        [
            {"platform": "요기요", "brand": "도리당", "store": "부산대신점", "total_price": 10_000},
            {"platform": "요기배달", "brand": "도리당", "store": "부산대신점", "total_price": 5_000},
            {"platform": "요기요", "brand": "도리당", "store": "미등록점", "total_price": 30_000},
            {"platform": "요기요", "brand": "도리당", "store": "보정점", "total_price": 10_000},
            {"platform": "요기요", "brand": "도리당", "store": "취소점", "total_price": -10_000},
        ],
    )

    delivery.build_delivery_commission()
    delivery.build_delivery_revenue()

    result = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    yogiyo = result[result["platform"].eq("요기요")]
    assert len(yogiyo) == 4
    keyed = yogiyo.set_index("store")
    row = keyed.loc["부산대신점"]
    assert row["total_price"] == 15_000
    assert row["revenue"] == 3_000
    assert row["settlement_price"] == 12_000
    fallback = keyed.loc["미등록점"]
    assert fallback["revenue"] == 8_143
    assert fallback["settlement_price"] == 21_857
    negative_rate = keyed.loc["보정점"]
    assert negative_rate["revenue"] == -1_000
    assert negative_rate["settlement_price"] == 11_000
    negative_sale = keyed.loc["취소점"]
    assert negative_sale["revenue"] == -2_714
    assert negative_sale["settlement_price"] == -7_286


def test_build_store_cost_allocation_allocates_rent_evenly_by_day(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_sales_employee(
        delivery.ONEDRIVE_DB,
        [
            {"매장명": "도리당 부산대신점", "플랫폼": "배달의 민족", "임대료": "3100"},
            {"매장명": "도리당 부산대신점", "플랫폼": "쿠팡이츠", "임대료": "3100"},
            {"매장명": "도리당 부산대신점", "플랫폼": "토더", "임대료": "3100"},
        ],
    )
    _write_unified_sales(
        delivery.UNIFIED_SALES_ROOT,
        [
            {"sale_date": "2026-07-01", "platform": "홀", "total_price": 1_000},
            {"sale_date": "2026-07-01", "platform": "배달의민족", "total_price": 500},
            {"sale_date": "2026-07-02", "platform": "쿠팡이츠", "total_price": 500},
        ],
    )

    delivery.build_store_cost_allocation()

    result = pd.read_parquet(delivery.STORE_COST_ALLOCATION_PATH)
    assert list(result.columns) == delivery.COST_OUTPUT_COLUMNS
    assert not result.duplicated(["sale_date", "brand", "store", "platform"]).any()
    assert result["monthly_rent"].max() == 3_100
    assert result["rent_cost"].sum() == 200

    keyed = result.set_index(["sale_date", "platform"])
    assert keyed.loc[("2026-07-01", "홀"), "rent_cost"] == 67
    assert keyed.loc[("2026-07-01", "배달의민족"), "rent_cost"] == 33
    assert keyed.loc[("2026-07-02", "쿠팡이츠"), "rent_cost"] == 100


@pytest.mark.parametrize("discount", [0, 500, 1_000])
def test_build_delivery_revenue_includes_rent_cost_column(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, discount: int
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_sales_employee(
        delivery.ONEDRIVE_DB,
        [
            {"매장명": "도리당 부산대신점", "플랫폼": "배달의 민족", "임대료": "3100"},
            {"매장명": "도리당 부산대신점", "플랫폼": "쿠팡이츠", "임대료": "3100"},
        ],
    )
    _write_unified_sales(
        delivery.UNIFIED_SALES_ROOT,
        [
            {"sale_date": "2026-07-19", "platform": "홀", "total_price": 1_000},
            {"sale_date": "2026-07-19", "platform": "배달의민족", "total_price": 500},
            {"sale_date": "2026-07-19", "platform": "쿠팡이츠", "total_price": 500},
        ],
    )
    defaults = {column: pd.NA for column in delivery.OUTPUT_COLUMNS}
    rows = [
        {
            **defaults,
            "sale_date": "2026-07-19",
            "brand": "도리당",
            "store": "부산대신점",
            "platform": "배달의민족",
            "total_amt": 500 + discount,
            "settlement_amount": 400,
            "diff_amt": 100 + discount,
            "배민_즉시할인": discount,
        },
        {
            **defaults,
            "sale_date": "2026-07-19",
            "brand": "도리당",
            "store": "부산대신점",
            "platform": "쿠팡이츠",
            "total_amt": 500,
            "settlement_amount": 350,
            "diff_amt": 150,
        },
    ]
    delivery.DELIVERY_COMMISSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    commission = pd.DataFrame(rows, columns=delivery.OUTPUT_COLUMNS)
    commission.attrs[delivery.BAEMIN_DISCOUNT_INCLUDED_ATTR] = True
    commission.to_parquet(
        delivery.DELIVERY_COMMISSION_PATH,
        index=False,
    )

    delivery.build_delivery_revenue()

    result = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    assert list(result.columns) == delivery.REVENUE_OUTPUT_COLUMNS
    keyed = result.set_index("platform")
    assert keyed.loc["배달의민족", "rent_cost"] == 50
    assert keyed.loc["배달의민족", "fin_revenue"] == 50 + discount
    assert keyed.loc["쿠팡이츠", "rent_cost"] == 50
    assert keyed.loc["쿠팡이츠", "fin_revenue"] == 100


def test_delivery_revenue_keeps_legacy_commission_rent_basis(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_sales_employee(
        delivery.ONEDRIVE_DB,
        [{"매장명": "도리당 부산대신점", "플랫폼": "배달의 민족", "임대료": "3100"}],
    )
    base = {column: pd.NA for column in delivery.OUTPUT_COLUMNS}
    base.update(sale_date="2026-07-19", brand="도리당", store="부산대신점",
                total_amt=1_000, settlement_amount=700, diff_amt=300)
    commission = pd.DataFrame([
        {**base, "platform": "배달의민족", "배민_즉시할인": 500},
        {**base, "platform": "쿠팡이츠"},
    ])
    delivery.DELIVERY_COMMISSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    commission.to_parquet(delivery.DELIVERY_COMMISSION_PATH, index=False)
    delivery.build_delivery_revenue()
    result = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    assert result.total_price.tolist() == [1_000, 1_000]
    assert result.rent_cost.tolist() == [50, 50]
    assert result.fin_revenue.tolist() == [250, 250]


def test_delivery_revenue_allocates_full_daily_rent_to_delivery_platforms(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_sales_employee(
        delivery.ONEDRIVE_DB,
        [
            {"매장명": "도리당 부산대신점", "플랫폼": "배달의 민족", "임대료": "3100"},
            {"매장명": "도리당 부산대신점", "플랫폼": "쿠팡이츠", "임대료": "3100"},
        ],
    )
    _write_unified_sales(
        delivery.UNIFIED_SALES_ROOT,
        [
            {"sale_date": "2026-07-19", "platform": "홀", "total_price": 9_000},
            {"sale_date": "2026-07-19", "platform": "배달의민족", "total_price": 1_000},
            {"sale_date": "2026-07-20", "platform": "홀", "total_price": 9_000},
            {"sale_date": "2026-07-20", "platform": "쿠팡이츠", "total_price": 1_000},
        ],
    )
    defaults = {column: pd.NA for column in delivery.OUTPUT_COLUMNS}
    rows = [
        {
            **defaults,
            "sale_date": "2026-07-19",
            "brand": "도리당",
            "store": "부산대신점",
            "platform": "배달의민족",
            "total_amt": 1_000,
            "settlement_amount": 700,
            "diff_amt": 300,
        },
        {
            **defaults,
            "sale_date": "2026-07-20",
            "brand": "도리당",
            "store": "부산대신점",
            "platform": "쿠팡이츠",
            "total_amt": 1_000,
            "settlement_amount": 800,
            "diff_amt": 200,
        },
    ]
    delivery.DELIVERY_COMMISSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=delivery.OUTPUT_COLUMNS).to_parquet(
        delivery.DELIVERY_COMMISSION_PATH,
        index=False,
    )

    delivery.build_store_cost_allocation()
    delivery.build_delivery_revenue()

    cost = pd.read_parquet(delivery.STORE_COST_ALLOCATION_PATH)
    revenue = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    assert cost["rent_cost"].sum() == 200
    assert revenue["rent_cost"].sum() == 200
    assert revenue.set_index("platform").loc["배달의민족", "rent_cost"] == 100
    assert revenue.set_index("platform").loc["쿠팡이츠", "rent_cost"] == 100


def test_fin_revenue_subtracts_all_cost_suffix_columns() -> None:
    revenue = pd.DataFrame(
        [
            {
                "sale_date": "2026-07-19",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "배달의민족",
                "total_price": 500,
                "settlement_price": 400,
                "revenue": 100,
                "rent_cost": 25,
                "extra_cost": 7,
            }
        ]
    )

    result = delivery._apply_fin_revenue(revenue)

    assert result.loc[0, "fin_revenue"] == 68


def test_store_expected_month_fin_revenue_uses_model_for_remaining_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeModel:
        def predict(self, features: pd.DataFrame) -> list[int]:
            return [10] * len(features)

    def fake_build_model(completed_daily: pd.DataFrame):
        return FakeModel(), list(delivery._store_forecast_features(completed_daily).columns)

    monkeypatch.setattr(delivery, "_build_store_month_forecast_model", fake_build_model)
    revenue = pd.DataFrame(
        [
            {
                "sale_date": "2026-08-01",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "배달의민족",
                "total_price": 1000,
                "settlement_price": 800,
                "revenue": 200,
                "fin_revenue": 190,
                "rent_cost": 10,
            },
            {
                "sale_date": "2026-08-02",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "쿠팡이츠",
                "total_price": 1000,
                "settlement_price": 900,
                "revenue": 100,
                "fin_revenue": 95,
                "rent_cost": 5,
            },
            {
                "sale_date": "2026-09-01",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "배달의민족",
                "total_price": 1000,
                "settlement_price": 800,
                "revenue": 200,
                "fin_revenue": 190,
                "rent_cost": 10,
            },
            {
                "sale_date": "2026-09-02",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "쿠팡이츠",
                "total_price": 1000,
                "settlement_price": 900,
                "revenue": 100,
                "fin_revenue": 95,
                "rent_cost": 5,
            },
        ]
    )

    result = delivery._apply_store_expected_month_fin_revenue(revenue)

    keyed = result.set_index(["sale_date", "platform"])
    assert keyed.loc[("2026-08-01", "배달의민족"), "store_expected_month_fin_revenue"] == 285
    assert keyed.loc[("2026-08-02", "쿠팡이츠"), "store_expected_month_fin_revenue"] == 285
    assert keyed.loc[("2026-09-01", "배달의민족"), "store_expected_month_fin_revenue"] == 565
    assert keyed.loc[("2026-09-02", "쿠팡이츠"), "store_expected_month_fin_revenue"] == 565


def test_store_expected_month_fin_revenue_falls_back_when_model_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(delivery, "_build_store_month_forecast_model", lambda completed_daily: None)
    revenue = pd.DataFrame(
        [
            {
                "sale_date": "2026-09-01",
                "brand": "도리당",
                "store": "신규점",
                "platform": "배달의민족",
                "total_price": 1000,
                "settlement_price": 800,
                "revenue": 200,
                "fin_revenue": 100,
                "rent_cost": 10,
            },
            {
                "sale_date": "2026-09-02",
                "brand": "도리당",
                "store": "신규점",
                "platform": "쿠팡이츠",
                "total_price": 1000,
                "settlement_price": 900,
                "revenue": 100,
                "fin_revenue": 200,
                "rent_cost": 5,
            },
        ]
    )

    result = delivery._apply_store_expected_month_fin_revenue(revenue)

    assert result["store_expected_month_fin_revenue"].tolist() == [4500, 4500]


def test_revenue_cost_columns_are_ordered_at_the_right_edge() -> None:
    revenue = pd.DataFrame(
        [
            {
                "sale_date": "2026-09-01",
                "brand": "도리당",
                "store": "부산대신점",
                "platform": "배달의민족",
                "total_price": 1000,
                "settlement_price": 800,
                "revenue": 200,
                "rent_cost": 10,
                "extra_cost": 5,
            }
        ]
    )

    result = delivery._apply_fin_revenue(revenue)
    result = delivery._apply_store_expected_month_fin_revenue(result)

    assert list(result.columns)[-2:] == ["rent_cost", "extra_cost"]
    assert result.loc[0, "fin_revenue"] == 185


def test_read_failure_preserves_existing_mart(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = _configure_paths(monkeypatch, tmp_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"existing mart")

    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    broken = _partition(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점") / "orders_2026-07.parquet"
    broken.write_bytes(b"not parquet")

    with pytest.raises(RuntimeError, match="원천 파일 1개 읽기 실패"):
        delivery.build_delivery_commission()

    assert output.read_bytes() == b"existing mart"


def test_build_adds_zero_row_when_platform_has_no_sales_for_date(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        order_time="2026. 07. 20. (월) 오후 12:00:00",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        5_000,
        4_000,
        order_time="2026. 07. 20. (월) 오후 12:00:00",
    )
    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "도리당",
        "도리당 부산대신점",
        100,
        ad_date="2026-07-20",
    )
    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "나홀로",
        "나홀로 대신점",
        200,
        ad_date="2026-07-20",
    )
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "송파삼전점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "송파삼전점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    delivery.build_delivery_commission()

    result = pd.read_parquet(output)
    row = result[
        result["sale_date"].astype(str).eq("2026-07-19")
        & result["store"].eq("송파삼전점")
        & result["platform"].eq("배달의민족")
        & result["brand"].eq("도리당")
    ].iloc[0]
    assert row["total_amt"] == 0
    assert row["settlement_amount"] == 0
    assert row["diff_amt"] == 0
    assert row["배민_즉시할인"] == 0
    assert row["우가클_평균비용"] == 0
    assert row["우가클_주문수"] == 0
    assert row["우가클_클릭율"] == 0
    assert pd.isna(row["쿠팡_신규비율"])


def test_wgc_metrics_are_computed_from_summed_totals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)

    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "도리당",
        "도리당 부산대신점",
        30_000,
        impressions=1_000,
        clicks=50,
        orders=3,
    )
    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "도리당",
        "도리당 부산대신점",
        10_000,
        impressions=1_000,
        clicks=10,
        orders=1,
    )
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)

    result = delivery._load_baemin_ad_spend_agg()

    row = result[result["brand"].eq("도리당")].iloc[0]
    assert row["ad_spend"] == 40_000
    assert row["wgc_avg_cost"] == 667.0
    assert row["wgc_orders"] == 4
    assert row["wgc_ctr"] == 0.03


def test_wgc_metrics_are_zero_for_baemin_and_null_for_coupang(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "도리당",
        "도리당 부산대신점",
        100,
        impressions=100,
        clicks=0,
        orders=0,
    )
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "부산대신점",
        "전체 15%",
        store_name="닭도리탕 전문 도리당 부산대신점",
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "나홀로",
        "대신점",
        "신규 12%, 재주문 7%",
        store_name="나홀로 1인 곱도리탕 대신점",
    )

    delivery.build_delivery_commission()

    result = pd.read_parquet(output)
    baemin = result[
        result["platform"].eq("배달의민족") & result["brand"].eq("도리당")
    ].iloc[0]
    coupang = result[result["platform"].eq("쿠팡이츠")]
    assert baemin["우가클_평균비용"] == 0
    assert baemin["우가클_주문수"] == 0
    assert baemin["우가클_클릭율"] == 0
    assert coupang[["우가클_평균비용", "우가클_주문수", "우가클_클릭율"]].isna().all().all()


def test_baemin_missing_wgc_rows_keep_settlement_and_zero_metrics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(
        delivery.BAEMIN_OUR_STORE_CLICKS_DB,
        "도리당",
        "도리당 부산대신점",
        100,
        ad_date="2026-07-20",
    )
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    delivery.build_delivery_commission()

    result = pd.read_parquet(output)
    baemin = result[
        result["platform"].eq("배달의민족") & result["brand"].eq("도리당")
    ].iloc[0]
    assert baemin["settlement_amount"] == 8_000
    assert baemin["우가클_평균비용"] == 0
    assert baemin["우가클_주문수"] == 0
    assert baemin["우가클_클릭율"] == 0


def test_baemin_cash_payment_uses_deposit_amount_without_extra_adjustment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        34_700,
        -2_528,
        cash_amount=-34_700,
    )
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 0)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 0)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    delivery.build_delivery_commission()

    result = pd.read_parquet(output)
    baemin = result[
        result["platform"].eq("배달의민족") & result["brand"].eq("도리당")
    ].iloc[0]
    assert baemin["settlement_amount"] == -2_528
    assert baemin["diff_amt"] == 37_228


def test_baemin_partner_instant_discount_is_not_double_counted_by_detail_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        20_000,
        15_000,
        order_id="상세행있는주문",
        partner_instant_discount=1_500,
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        0,
        "",
        order_id="상세행있는주문",
        partner_instant_discount=0,
        payment=0,
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        order_id="다른주문",
        partner_instant_discount=500,
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        5_000,
        4_000,
        partner_instant_discount=300,
    )

    result = delivery._load_baemin_orders_agg()

    doridang = result[result["brand"].eq("도리당")].iloc[0]
    assert doridang["total_amt"] == 30_000
    assert doridang["baemin_partner_instant_discount"] == 2_000


def test_baemin_instant_discount_uses_partner_only_when_partner_is_blank(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        instant_discount=2_000,
        partner_instant_discount="",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        5_000,
        4_000,
        order_id="할인없는주문",
        instant_discount="",
        partner_instant_discount="",
    )
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)

    result = delivery._load_baemin_orders_agg()

    doridang = result[result["brand"].eq("도리당")].iloc[0]
    assert doridang["baemin_partner_instant_discount"] == 0


def test_baemin_settlement_is_null_when_any_order_is_unsettled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        order_id="정상주문",
        order_time="2026. 07. 19. (일) 오후 12:00:00",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        5_000,
        "",
        order_id="정산미수집주문",
        order_time="2026. 07. 19. (일) 오후 12:10:00",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        7_000,
        5_500,
        order_id="다음날정상주문",
        order_time="2026. 07. 20. (월) 오후 12:00:00",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        3_000,
        2_000,
        order_id="브랜드검증용주문",
        order_time="2026. 07. 21. (화) 오후 12:00:00",
    )

    result = delivery._load_baemin_orders_agg()

    day_19 = result[result["date"].eq("2026-07-19")]
    assert day_19["total_amt"].sum() == 15_000
    assert set(day_19["brand"]) == {"도리당"}
    assert pd.isna(day_19["baemin_deposit_amt"].iloc[0])
    day_20 = result[result["date"].eq("2026-07-20")]
    assert day_20["total_amt"].sum() == 7_000
    assert day_20["baemin_deposit_amt"].sum() == 5_500


def test_build_writes_with_nulls_when_baemin_settlement_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        "",
        order_id="정산미수집주문",
        partner_instant_discount=1_000,
    )
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    delivery.build_delivery_commission()

    assert output.exists()
    result = pd.read_parquet(output)
    missing = result[
        result["platform"].eq("배달의민족")
        & result["brand"].eq("도리당")
        & result["store"].eq("부산대신점")
    ].iloc[0]
    normal = result[
        result["platform"].eq("배달의민족")
        & result["brand"].eq("나홀로")
        & result["store"].eq("부산대신점")
    ].iloc[0]
    coupang = result[result["platform"].eq("쿠팡이츠")]

    assert pd.isna(missing["settlement_amount"])
    assert pd.isna(missing["diff_amt"])
    assert missing["total_amt"] == 11_000
    assert normal["settlement_amount"] == 3_800
    assert len(coupang) == 2

    delivery.build_delivery_revenue()
    revenue = pd.read_parquet(delivery.DELIVERY_REVENUE_PATH)
    revenue_missing = revenue[
        revenue["platform"].eq("배달의민족")
        & revenue["brand"].eq("도리당")
        & revenue["store"].eq("부산대신점")
    ].iloc[0]
    assert pd.isna(revenue_missing["settlement_price"])
    assert pd.isna(revenue_missing["revenue"])
    assert revenue_missing["total_price"] == 11_000


def test_find_baemin_settlement_missing_targets_groups_recollect_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        delivery,
        "load_automation_account_df",
        lambda **_: pd.DataFrame(
            {
                "매장명": ["도리당 부산대신점", "나홀로 대신점"],
                "계정ID": ["top", "bottom"],
                "계정PW": ["pw", "pw"],
                "플랫폼": ["배달의 민족", "배달의 민족"],
                "비고": ["자동화 연결", "자동화 연결"],
            }
        ),
    )

    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        "",
        order_id="정산미수집1",
        order_time="2026. 07. 19. (일) 오후 12:00:00",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        7_000,
        "",
        order_id="정산미수집2",
        order_time="2026. 07. 20. (월) 오후 12:00:00",
    )
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)

    targets = delivery.find_baemin_settlement_missing_targets()

    assert list(targets.columns) == delivery.BAEMIN_SETTLEMENT_MISSING_COLUMNS
    assert len(targets) == 1
    row = targets.iloc[0]
    assert row["scope"] == "하위"
    assert row["store"] == "부산대신점"
    assert row["brand"] == "도리당"
    assert row["missing_days"] == 2
    assert row["total_amt_sum"] == 17_000
    assert row["first_date"] == "2026-07-19"
    assert row["last_date"] == "2026-07-20"
    assert row["missing_periods"] == "2026-07-19~2026-07-20"
    assert row["missing_dates"] == "2026-07-19|2026-07-20"

    confs = delivery.build_baemin_orders_only_recollect_confs(targets)
    assert confs == [
        {
            "dag_id": "DB_Beamin_Macro_Backfill_Dags",
            "conf": {
                "orders_only": True,
                "target_date": "2026-07-19",
                "stores": ["부산대신점"],
                "collect_range": "하위",
                "run_all_batches": False,
                "stability_profile": "bulk_70",
            },
        },
        {
            "dag_id": "DB_Beamin_Macro_Backfill_Dags",
            "conf": {
                "orders_only": True,
                "target_date": "2026-07-20",
                "stores": ["부산대신점"],
                "collect_range": "하위",
                "run_all_batches": False,
                "stability_profile": "bulk_70",
            },
        },
    ]


def test_monitor_baemin_settlement_missing_pushes_xcom(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(delivery, "BAEMIN_SETTLEMENT_MISSING_LATEST_CSV", tmp_path / "missing.csv")
    monkeypatch.setattr(
        delivery,
        "load_automation_account_df",
        lambda **_: pd.DataFrame({"매장명": ["도리당 부산대신점"]}),
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        "",
        order_id="정산미수집",
    )
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)

    class FakeTaskInstance:
        def __init__(self) -> None:
            self.values = {}

        def xcom_push(self, *, key, value) -> None:
            self.values[key] = value

    ti = FakeTaskInstance()
    result = delivery.monitor_baemin_settlement_missing(ti=ti)

    assert (tmp_path / "missing.csv").exists()
    saved = pd.read_csv(tmp_path / "missing.csv", encoding="utf-8-sig")
    assert len(saved) == 1
    assert "orders_only 재수집 필요" in result
    assert ti.values["baemin_settlement_missing_targets"][0]["store"] == "부산대신점"
    assert ti.values["orders_only_recollect_confs"][0]["conf"]["orders_only"] is True


def test_trigger_baemin_orders_only_recollect_triggers_and_allows_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import types

    triggered = []
    airflow_mod = types.ModuleType("airflow")
    api_mod = types.ModuleType("airflow.api")
    common_mod = types.ModuleType("airflow.api.common")
    trigger_mod = types.ModuleType("airflow.api.common.trigger_dag")
    exceptions_mod = types.ModuleType("airflow.exceptions")

    class DagRunAlreadyExists(Exception):
        pass

    trigger_mod.trigger_dag = lambda **kwargs: triggered.append(kwargs)
    exceptions_mod.DagRunAlreadyExists = DagRunAlreadyExists
    monkeypatch.setitem(sys.modules, "airflow", airflow_mod)
    monkeypatch.setitem(sys.modules, "airflow.api", api_mod)
    monkeypatch.setitem(sys.modules, "airflow.api.common", common_mod)
    monkeypatch.setitem(sys.modules, "airflow.api.common.trigger_dag", trigger_mod)
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_mod)

    class FakeTaskInstance:
        def xcom_pull(self, *, task_ids, key):
            assert task_ids == "monitor_baemin_settlement_missing"
            assert key == "orders_only_recollect_confs"
            return [
                {
                    "dag_id": "DB_Beamin_Macro_Dags",
                    "conf": {
                        "orders_only": True,
                        "target_date": "2026-07-19",
                        "stores": ["부산대신점"],
                        "collect_range": "하위",
                        "run_all_batches": False,
                        "stability_profile": "bulk_70",
                    },
                }
            ]

    class FakeDagRun:
        run_id = "scheduled__2026-07-20T03:10:00+00:00"

    result = delivery.trigger_baemin_orders_only_recollect(
        ti=FakeTaskInstance(),
        dag_run=FakeDagRun(),
    )

    assert len(triggered) == 1
    assert "NA 포함 갱신 계속 진행" in result
    assert triggered[0]["dag_id"] == "DB_Beamin_Macro_Dags"
    assert triggered[0]["run_id"].startswith(
        "delivery_commission_settlement_recollect__20260719__"
    )
    assert triggered[0]["conf"]["orders_only"] is True
    assert triggered[0]["conf"]["stability_profile"] == "bulk_70"
    assert triggered[0]["conf"]["source_dag_id"] == "DB_DeliveryCommission_Dags"
    assert triggered[0]["conf"]["source_run_id"] == FakeDagRun.run_id


def test_delivery_commission_dag_monitors_before_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow_home"))

    from dags.db.DB_DeliveryCommission_Dags import dag

    assert set(dag.task_ids) == {
        "monitor_baemin_settlement_missing",
        "trigger_baemin_orders_only_recollect",
        "build_delivery_commission",
        "build_delivery_revenue",
        "build_store_cost_allocation",
    }
    assert dag.get_task("trigger_baemin_orders_only_recollect").upstream_task_ids == {
        "monitor_baemin_settlement_missing"
    }
    assert dag.get_task("build_delivery_commission").upstream_task_ids == {
        "trigger_baemin_orders_only_recollect"
    }
    assert dag.get_task("build_delivery_revenue").upstream_task_ids == {
        "build_delivery_commission"
    }
    assert dag.get_task("build_store_cost_allocation").upstream_task_ids == {
        "build_delivery_revenue"
    }
    monitor = dag.get_task("monitor_baemin_settlement_missing")
    trigger = dag.get_task("trigger_baemin_orders_only_recollect")
    assert monitor.retries == 0
    assert getattr(monitor.on_failure_callback, "__name__", "") == "on_failure_callback"
    assert getattr(trigger.on_failure_callback, "__name__", "") == "on_failure_callback"


def test_baemin_excludes_cancelled_orders(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        10_000,
        8_000,
        order_id="정상주문",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        5_000,
        "",
        order_id="취소주문",
        status="주문취소",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        3_000,
        2_000,
        order_id="다른브랜드정상주문",
    )

    result = delivery._load_baemin_orders_agg()

    assert result["total_amt"].sum() == 13_000
    assert result["baemin_deposit_amt"].sum() == 10_000


def test_baemin_uses_same_payment_amount_as_unified_sales(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "도리당",
        "부산대신점",
        0,
        8_000,
        order_id="부분취소주문",
        payment="부분취소22300",
    )
    _write_baemin_order(
        delivery.BAEMIN_ORDERS_DB,
        "나홀로",
        "대신점",
        10_000,
        8_000,
        order_id="결제금액누락주문",
        payment=0,
    )

    result = delivery._load_baemin_orders_agg()

    assert result["total_amt"].sum() == 22_300


def test_coupang_full_cancel_contributes_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_order(
        delivery.COUPANG_ORDERS_DB,
        "도리당",
        "부산대신점",
        0,
        0,
        is_cancelled="Y",
        cancel_amount=40_000,
    )
    _write_coupang_order(
        delivery.COUPANG_ORDERS_DB,
        "나홀로",
        "대신점",
        0,
        0,
        is_cancelled="Y",
        cancel_amount=40_000,
    )

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == 0


def test_coupang_legacy_negative_refund_is_not_double_counted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    rows = [
        {
            "order_id": "취소주문",
            "매출액": 0,
            "취소금액": 40_000,
            "is_cancelled": "Y",
            "menu_options": "주문행",
        },
        {
            "order_id": "취소주문",
            "매출액": -40_000,
            "취소금액": 0,
            "is_cancelled": "Y",
            "menu_options": "환불행",
        },
    ]
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", rows)
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", rows)

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == -80_000


def test_coupang_partial_cancel_uses_net_sales(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_order(
        delivery.COUPANG_ORDERS_DB,
        "도리당",
        "부산대신점",
        9_000,
        0,
        is_cancelled="N",
        cancel_amount=56_300,
    )
    _write_coupang_order(
        delivery.COUPANG_ORDERS_DB,
        "나홀로",
        "대신점",
        9_000,
        0,
        is_cancelled="N",
        cancel_amount=56_300,
    )

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == 18_000


def test_coupang_missing_settlement_falls_back_to_total_price(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    rows = [{
        "order_id": "정산미파싱주문",
        "매출액": None,
        "total_price": 20_000,
        "is_cancelled": "N",
    }]
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", rows)
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", rows)

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == 40_000


def test_coupang_deduplicates_exact_raw_rows_before_sum(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    duplicated = {
        "order_id": "중복주문",
        "매출액": 10_000,
        "정산_예정_금액": 8_000,
        "menu_options": "동일행",
    }
    _write_coupang_rows(
        delivery.COUPANG_ORDERS_DB,
        "도리당",
        "부산대신점",
        [duplicated, duplicated],
    )
    _write_coupang_rows(
        delivery.COUPANG_ORDERS_DB,
        "나홀로",
        "대신점",
        [duplicated, duplicated],
    )

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == 20_000
    assert result["coupang_settlement_amt"].sum() == 16_000


def test_coupang_deduplicates_same_order_across_normalized_store_partitions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    duplicated = [{
        "order_id": "개명중복주문",
        "매출액": 10_000,
        "정산_예정_금액": 8_000,
        "total_price": 10_000,
        "order_status": None,
        "menu_qty": 1,
        "menu_price": 10_000,
        "menu_options": "동일행",
    }]
    duplicated_float_text = [{
        **duplicated[0],
        "total_price": "10000.0",
        "order_status": "nan",
        "menu_qty": "1.0",
        "menu_price": "10000.0",
    }]
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "도리당", "구로디지털점", duplicated)
    _write_coupang_rows(
        delivery.COUPANG_ORDERS_DB,
        "도리당",
        "구로디지털단지점",
        duplicated_float_text,
    )
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)

    result = delivery._load_coupang_orders_agg()
    doridang = result[result["brand"].eq("도리당")].iloc[0]

    assert doridang["store"] == "구로디지털점"
    assert doridang["total_amt"] == 10_000
    assert doridang["coupang_settlement_amt"] == 8_000
    assert result["total_amt"].sum() == 13_000


def test_coupang_ignores_noncanonical_orders_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    rows = [{
        "order_id": "중복파일주문",
        "매출액": 10_000,
        "정산_예정_금액": 8_000,
        "menu_options": "동일행",
    }]
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", rows)
    _write_coupang_rows(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", [])
    pd.DataFrame(rows).to_parquet(
        _partition(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점")
        / "orders_2026-07-DESKTOP-HG136JL.parquet",
        index=False,
    )

    result = delivery._load_coupang_orders_agg()

    assert result["total_amt"].sum() == 10_000
    assert result["coupang_settlement_amt"].sum() == 8_000


def test_coupang_cmg_ratios_and_metrics_use_partition_store_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "송파삼전점",
        "전체 15%",
        store_name="닭도리탕 전문 도리당 송파삼전점",
        ad_cost=30_480,
        new_customers=4,
        impressions=429,
        clicks=30,
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "나홀로",
        "대신점",
        "신규 12%, 재주문 7%",
        store_name="나홀로 1인 곱도리탕 대신점",
        ad_cost=10_000,
        new_customers=3,
        impressions=200,
        clicks=20,
    )

    result = delivery._load_coupang_cmg_agg()
    keyed = result.set_index(["brand", "store"])

    assert keyed.loc[("도리당", "송파삼전점"), "coupang_new_ratio"] == 0.15
    assert keyed.loc[("도리당", "송파삼전점"), "coupang_reorder_ratio"] == 0.15
    assert keyed.loc[("도리당", "송파삼전점"), "coupang_ad_cost"] == 30_480
    assert keyed.loc[("도리당", "송파삼전점"), "coupang_new_customers"] == 4
    assert keyed.loc[("도리당", "송파삼전점"), "coupang_ad_impressions"] == 429
    assert keyed.loc[("도리당", "송파삼전점"), "coupang_ad_clicks"] == 30
    assert keyed.loc[("나홀로", "부산대신점"), "coupang_new_ratio"] == 0.12
    assert keyed.loc[("나홀로", "부산대신점"), "coupang_reorder_ratio"] == 0.07


def test_coupang_cmg_reorder_ratio_is_null_when_only_new_ratio_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "신규 10%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    result = delivery._load_coupang_cmg_agg()
    row = result[result["brand"].eq("도리당")].iloc[0]

    assert row["coupang_new_ratio"] == 0.10
    assert pd.isna(row["coupang_reorder_ratio"])


def test_coupang_cmg_prefers_canonical_partition_and_deduplicates_exact_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "구로디지털단지점",
        "전체 10%",
        ad_cost=33_450,
        new_customers=11,
        impressions=1_689,
        clicks=116,
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "구로디지털단지점",
        "전체 10%",
        ad_cost=33_450,
        new_customers=11,
        impressions=1_689,
        clicks=116,
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "구로디지털점",
        "전체 10%",
        ad_cost=40_000,
        new_customers=12,
        impressions=1_700,
        clicks=120,
    )
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    result = delivery._load_coupang_cmg_agg()
    row = result[result["brand"].eq("도리당")].iloc[0]

    assert row["store"] == "구로디지털점"
    assert row["coupang_ad_cost"] == 40_000
    assert row["coupang_new_customers"] == 12
    assert row["coupang_ad_impressions"] == 1_700
    assert row["coupang_ad_clicks"] == 120


def test_coupang_cmg_metrics_are_null_when_order_has_no_matching_cmg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "도리당",
        "부산대신점",
        "전체 15%",
        query_date="2026-07-18",
    )
    _write_coupang_cmg(
        delivery.COUPANG_CMG_DB,
        "나홀로",
        "대신점",
        "전체 10%",
        query_date="2026-07-18",
    )

    delivery.build_delivery_commission()

    result = pd.read_parquet(output)
    coupang = result[result["platform"].eq("쿠팡이츠")]
    assert coupang[
        [
            "쿠팡_신규비율",
            "쿠팡_재주문비율",
            "쿠팡_광고비용",
            "쿠팡_신규고객",
            "쿠팡_광고노출수",
            "쿠팡_광고클릭수",
        ]
    ].isna().all().all()


def test_coupang_cmg_invalid_ratio_fails_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "광고 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    with pytest.raises(RuntimeError, match="coupang cmg 원천 파일 1개 읽기 실패"):
        delivery._load_coupang_cmg_agg()


def test_coupang_cmg_ratio_conflict_fails_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 15%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "도리당", "부산대신점", "전체 10%")
    _write_coupang_cmg(delivery.COUPANG_CMG_DB, "나홀로", "대신점", "전체 10%")

    with pytest.raises(RuntimeError, match="coupang cmg 광고비율 집계 실패"):
        delivery._load_coupang_cmg_agg()
