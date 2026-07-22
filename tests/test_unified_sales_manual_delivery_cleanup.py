import datetime

import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_unionpos as unionpos
from modules.transform.pipelines.db import DB_UnifiedSales_common as common
from modules.transform.pipelines.db import DB_UnifiedSales_baemin as baemin
from modules.transform.pipelines.db import DB_UnifiedSales_coupang as coupang
from modules.transform.pipelines.db import DB_UnifiedSales_posfeed as posfeed
from modules.transform.pipelines.db import DB_UnifiedSales_validate as validate


def _unified_rows(rows):
    return pd.DataFrame(rows).reindex(columns=common.UNIFIED_COLUMNS, fill_value="")


def test_baemin_transform_falls_back_to_menu_for_price_option_names(monkeypatch):
    monkeypatch.setattr(
        baemin,
        "allocate_manual_item_ids",
        lambda df: pd.Series(["8100001", "8100002", "8100003"], index=df.index),
    )
    raw = pd.DataFrame(
        [
            {
                "주문상태": "배달완료",
                "주문번호": "A",
                "수령방법": "배달",
                "주문내역": "[복날한정] 1인 미나리 수삼 백숙 외 2건",
                "주문옵션상세": "16900",
                "주문수량": "1",
                "주문옵션금액": "12000",
                "결제금액": "12000",
                "sale_date": "2026-07-08",
                "order_time": "12:00:00",
            },
            {
                "주문상태": "배달완료",
                "주문번호": "B",
                "수령방법": "배달",
                "주문내역": "[복날한정] 미나리 수삼 백숙",
                "주문옵션상세": "23,500",
                "주문수량": "1",
                "주문옵션금액": "18000",
                "결제금액": "18000",
                "sale_date": "2026-07-08",
                "order_time": "12:01:00",
            },
            {
                "주문상태": "배달완료",
                "주문번호": "C",
                "수령방법": "배달",
                "주문내역": "정상 메뉴",
                "주문옵션상세": "맵기 보통",
                "주문수량": "1",
                "주문옵션금액": "10000",
                "결제금액": "10000",
                "sale_date": "2026-07-08",
                "order_time": "12:02:00",
            },
        ]
    )

    out = baemin._transform_to_unified(raw, "송파삼전점", "도리당", {})

    assert out["item_name"].tolist() == [
        "[복날한정] 1인 미나리 수삼 백숙",
        "[복날한정] 미나리 수삼 백숙",
        "맵기 보통",
    ]
    assert out["unit_price"].astype(int).tolist() == [12000, 18000, 10000]
    assert out["total_price"].astype(int).tolist() == [12000, 18000, 10000]


def test_filter_manual_delivery_sources_for_non_test_stores_removes_only_non_test_manual_rows():
    df = _unified_rows(
        [
            {"sale_date": "2026-06-01", "store": "법흥리점", "source": "배민수동", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-06-01", "store": "동탄영천점", "source": "배민수동", "platform": "배달의민족", "total_price": 2000},
            {"sale_date": "2026-06-01", "store": "동탄영천점", "source": "쿠팡수동", "platform": "쿠팡이츠", "total_price": 3000},
            {"sale_date": "2026-06-01", "store": "동탄영천점", "source": "posfeed", "platform": "배달의민족", "total_price": 4000},
        ]
    )

    out = common.filter_manual_delivery_sources_for_non_test_stores(df, stores=["법흥리점"])

    assert set(out["total_price"].astype(int)) == {1000, 4000}
    assert out[out["source"].eq("배민수동")]["store"].tolist() == ["법흥리점"]


def test_purge_manual_delivery_sources_for_non_test_stores_rewrites_existing_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "UNIFIED_ROOT", tmp_path)
    path = tmp_path / "unified_sales_260601.parquet"
    df = _unified_rows(
        [
            {"sale_date": "2026-06-01", "store": "법흥리점", "source": "배민수동", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-06-01", "store": "동탄영천점", "source": "쿠팡수동", "platform": "쿠팡이츠", "total_price": 3000},
            {"sale_date": "2026-06-01", "store": "동탄영천점", "source": "okpos", "platform": "쿠팡이츠", "total_price": 4000},
        ]
    )
    df.to_parquet(path, index=False, engine="pyarrow")

    result = common.purge_manual_delivery_sources_for_non_test_stores(stores=["법흥리점"])

    out = pd.read_parquet(path)
    assert "파일=1 제거=1" in result
    assert set(out["total_price"].astype(int)) == {1000, 4000}
    assert not ((out["store"] == "동탄영천점") & out["source"].isin(["배민수동", "쿠팡수동"])).any()


def test_filter_manual_delivery_sources_for_test_stores_treats_today_as_non_test(monkeypatch):
    monkeypatch.setattr(common, "_kst_today_str", lambda: "2026-07-08")
    df = _unified_rows(
        [
            {"sale_date": "2026-07-08", "store": "법흥리점", "source": "posfeed", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-07-08", "store": "법흥리점", "source": "배민수동", "platform": "배달의민족", "total_price": 2000},
            {"sale_date": "2026-07-08", "store": "법흥리점", "source": "posfeed", "platform": "쿠팡이츠", "total_price": 3000},
        ]
    )

    out = common.filter_manual_delivery_sources_for_test_stores(df, stores=["법흥리점"])

    assert set(out["total_price"].astype(int)) == {1000, 3000}


def test_filter_manual_delivery_sources_for_test_stores_removes_past_posfeed_even_without_manual(monkeypatch):
    monkeypatch.setattr(common, "_kst_today_str", lambda: "2026-07-08")
    df = _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "쿠팡이츠", "total_price": 2000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "요기요", "total_price": 3000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "홀", "total_price": 4000},
        ]
    )

    out = common.filter_manual_delivery_sources_for_test_stores(df, stores=["법흥리점"])

    assert set(out["total_price"].astype(int)) == {1000, 2000, 3000, 4000}


def test_filter_manual_delivery_sources_for_test_stores_keeps_past_manual_only(monkeypatch):
    monkeypatch.setattr(common, "_kst_today_str", lambda: "2026-07-08")
    df = _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "배민수동", "platform": "배달의민족", "total_price": 2000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "쿠팡이츠", "total_price": 3000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "쿠팡수동", "platform": "쿠팡이츠", "total_price": 4000},
        ]
    )

    out = common.filter_manual_delivery_sources_for_test_stores(df, stores=["법흥리점"])

    assert set(out["total_price"].astype(int)) == {2000, 4000}


def test_empty_manual_baemin_upsert_removes_past_test_store_posfeed(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "UNIFIED_ROOT", tmp_path)
    monkeypatch.setattr(baemin, "UNIFIED_ROOT", tmp_path)
    path = tmp_path / "unified_sales_260707.parquet"
    _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "배달의민족", "total_price": 1000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "홀", "total_price": 2000},
        ]
    ).to_parquet(path, index=False, engine="pyarrow")

    removed, added = baemin._upsert_daily(pd.DataFrame(columns=common.UNIFIED_COLUMNS), "2026-07-07", "법흥리점")

    out = pd.read_parquet(path)
    assert (removed, added) == (1, 0)
    assert set(out["total_price"].astype(int)) == {2000}


def test_empty_manual_coupang_upsert_removes_past_test_store_posfeed(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "UNIFIED_ROOT", tmp_path)
    monkeypatch.setattr(coupang, "UNIFIED_ROOT", tmp_path)
    path = tmp_path / "unified_sales_260707.parquet"
    _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "쿠팡이츠", "total_price": 1000},
            {"sale_date": "2026-07-07", "store": "법흥리점", "source": "posfeed", "platform": "홀", "total_price": 2000},
        ]
    ).to_parquet(path, index=False, engine="pyarrow")

    removed, added = coupang._upsert_daily(pd.DataFrame(columns=common.UNIFIED_COLUMNS), "2026-07-07", "법흥리점")

    out = pd.read_parquet(path)
    assert (removed, added) == (1, 0)
    assert set(out["total_price"].astype(int)) == {2000}


def test_validation_excludes_only_missing_test_store_manual_delivery_family():
    df = pd.DataFrame(
        [
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배달의민족", "price": 1000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배민1", "price": 2000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "쿠팡이츠", "price": 3000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "홀", "price": 4000},
            {"date": "2000-01-01", "store": "일반점", "platform": "배달의민족", "price": 5000},
        ]
    )
    unified_family_keys = {("2000-01-01", "법흥리점", "배민수동")}

    out = validate._exclude_manual_delivery_test_store_platforms(
        df,
        date_col="date",
        unified_platform_keys=unified_family_keys,
    )

    assert set(out["price"].astype(int)) == {1000, 2000, 3000, 4000, 5000}


def test_validation_keeps_past_test_store_delivery_rows_when_no_manual_key():
    df = pd.DataFrame(
        [
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배달의민족", "source": "배민수동", "price": 1000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배달의민족", "source": "posfeed", "price": 2000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "쿠팡이츠", "source": "posfeed", "price": 3000},
            {"date": "2000-01-01", "store": "다른매장", "platform": "배달의민족", "source": "posfeed", "price": 4000},
        ]
    )
    unified_family_keys: set[tuple[str, str, str]] = set()

    out = validate._exclude_manual_delivery_test_store_platforms(
        df,
        date_col="date",
        unified_platform_keys=unified_family_keys,
    )

    assert set(out["price"].astype(int)) == {1000, 2000, 3000, 4000}


def test_validation_excludes_non_manual_delivery_family_when_manual_exists():
    df = pd.DataFrame(
        [
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배달의민족", "source": "배민수동", "price": 1000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "배달의민족", "source": "posfeed", "price": 2000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "쿠팡이츠", "source": "posfeed", "price": 3000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "홀", "source": "포장", "price": 4000},
            {"date": "2000-01-01", "store": "법흥리점", "platform": "쿠팡이츠", "source": "쿠팡수동", "price": 5000},
            {"date": "2000-01-01", "store": "일반점", "platform": "배달의민족", "source": "posfeed", "price": 6000},
        ]
    )
    unified_family_keys = {("2000-01-01", "법흥리점", "배민수동"), ("2000-01-01", "법흥리점", "쿠팡수동")}

    out = validate._exclude_manual_delivery_test_store_platforms(
        df,
        date_col="date",
        unified_platform_keys=unified_family_keys,
    )

    assert set(out["price"].astype(int)) == {1000, 5000, 4000, 6000}


def test_validation_excludes_today_manual_delivery_source(monkeypatch):
    df = pd.DataFrame(
        [
            {"date": "2000-01-02", "store": "법흥리점", "platform": "배달의민족", "source": "배민수동", "price": 1000},
            {"date": "2000-01-02", "store": "법흥리점", "platform": "배달의민족", "source": "posfeed", "price": 2000},
            {"date": "2000-01-02", "store": "일반점", "platform": "배달의민족", "source": "배민수동", "price": 3000},
        ]
    )
    unified_family_keys = {("2000-01-02", "법흥리점", "배민수동")}

    class _FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            base = cls(2000, 1, 2, 12, 0, 0)
            return base if tz is None else base.replace(tzinfo=tz)

    monkeypatch.setattr(validate, "datetime", _FixedDateTime)

    out = validate._exclude_manual_delivery_test_store_platforms(
        df,
        date_col="date",
        unified_platform_keys=unified_family_keys,
    )

    assert set(out["price"].astype(int)) == {2000, 3000}


def test_validation_excludes_toorder_hall_for_pos_hall_stores(tmp_path, monkeypatch):
    okpos_root = tmp_path / "okpos"
    unionpos_root = tmp_path / "unionpos"
    (okpos_root / "brand=도리당" / "store=송파삼전점").mkdir(parents=True)
    (unionpos_root / "brand=도리당" / "store=동탄영천점").mkdir(parents=True)

    monkeypatch.setattr(validate, "RAW_OKPOS_SALES", okpos_root)
    monkeypatch.setattr(validate, "RAW_UNIONPOS_SALES", unionpos_root)

    df = pd.DataFrame(
        [
            {"date": "2026-07-07", "store": "송파삼전점", "platform": "홀", "price": 1000},
            {"date": "2026-07-07", "store": "송파삼전점", "platform": "홀 포장", "price": 2000},
            {"date": "2026-07-07", "store": "송파삼전점", "platform": "홀 배달", "price": 2500},
            {"date": "2026-07-07", "store": "송파삼전점", "platform": "배민1", "price": 3000},
            {"date": "2026-07-07", "store": "동탄영천점", "platform": "홀", "price": 4000},
            {"date": "2026-07-07", "store": "일반점", "platform": "홀", "price": 5000},
        ]
    )

    out = validate._exclude_pos_hall_platforms(df)

    assert set(out["price"].astype(int)) == {3000, 5000}


def test_load_unified_platform_keys_uses_manual_source_filter(tmp_path, monkeypatch):
    path = tmp_path / "unified_sales_260701.parquet"
    pd.DataFrame(
        [
            {
                "sale_date": "2026-07-01",
                "store": "법흥리점",
                "platform": "배달의민족",
                "source": "배민수동",
                "total_price": 1000,
            },
            {
                "sale_date": "2026-07-01",
                "store": "법흥리점",
                "platform": "배달의민족",
                "source": "posfeed",
                "total_price": 2000,
            },
            {
                "sale_date": "2026-07-01",
                "store": "법흥리점",
                "platform": "쿠팡이츠",
                "source": "쿠팡수동",
                "total_price": 3000,
            },
        ]
    ).to_parquet(path, index=False, engine="pyarrow")

    monkeypatch.setattr(validate, "iter_unified_sales_files", lambda: [path])

    out = validate._load_unified_platform_keys("2026-07")

    assert out == {("2026-07-01", "법흥리점", "배민수동"), ("2026-07-01", "법흥리점", "쿠팡수동")}


def test_validation_pos_hall_store_names_include_easypos(tmp_path, monkeypatch):
    monkeypatch.setattr(validate, "RAW_OKPOS_SALES", tmp_path / "okpos")
    monkeypatch.setattr(validate, "RAW_UNIONPOS_SALES", tmp_path / "unionpos")

    stores = validate._pos_hall_store_names()

    assert "송파점" in stores


def test_validation_loads_unified_pos_hall_totals(tmp_path, monkeypatch):
    path = tmp_path / "unified_sales_260707.parquet"
    _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "송파삼전점", "source": "okpos", "platform": "홀", "total_price": 1000},
            {"sale_date": "2026-07-07", "store": "동탄영천점", "source": "unionpos", "platform": "홀", "total_price": 2000},
            {"sale_date": "2026-07-07", "store": "일반점", "source": "posfeed", "platform": "홀", "total_price": 3000},
            {"sale_date": "2026-07-07", "store": "송파삼전점", "source": "okpos", "platform": "배달의민족", "total_price": 4000},
        ]
    ).to_parquet(path, index=False, engine="pyarrow")
    monkeypatch.setattr(validate, "iter_unified_sales_files", lambda: [path])

    out = validate._load_unified_pos_hall_totals(target_date="2026-07-07")

    assert out.sort_values("store").reset_index(drop=True).to_dict("records") == [
        {"sale_date": "2026-07-07", "store": "동탄영천점", "excel_total": 2000},
        {"sale_date": "2026-07-07", "store": "송파삼전점", "excel_total": 1000},
    ]


def test_validation_load_excel_totals_adds_pos_hall_baseline(tmp_path, monkeypatch):
    analytics_root = tmp_path / "analytics" / "toorder_daily_store_platform"
    analytics_root.mkdir(parents=True)
    toorder_path = analytics_root / "toorder_store_platform_daily.parquet"
    pd.DataFrame(
        [
            {
                "date": "2026-07-07",
                "store": "송파삼전점",
                "platform": "홀",
                "price": 3000,
            },
            {
                "date": "2026-07-07",
                "store": "송파삼전점",
                "platform": "배달의민족",
                "price": 5000,
            },
            {
                "date": "2026-07-07",
                "store": "해운대중동점",
                "platform": "홀",
                "price": 7000,
            },
        ]
    ).to_parquet(toorder_path, index=False, engine="pyarrow")

    okpos_root = tmp_path / "okpos" / "brand=도리당"
    unionpos_root = tmp_path / "unionpos" / "brand=도리당"
    (okpos_root / "store=송파삼전점").mkdir(parents=True, exist_ok=True)
    (unionpos_root / "store=해운대중동점").mkdir(parents=True, exist_ok=True)
    (tmp_path / "easypos").mkdir(parents=True, exist_ok=True)

    unified_path = tmp_path / "unified_sales_260707.parquet"
    _unified_rows(
        [
            {"sale_date": "2026-07-07", "store": "송파삼전점", "source": "okpos", "platform": "홀", "total_price": 1100},
            {"sale_date": "2026-07-07", "store": "해운대중동점", "source": "unionpos", "platform": "홀", "total_price": 1200},
        ]
    ).to_parquet(unified_path, index=False, engine="pyarrow")

    monkeypatch.setattr(validate, "ANALYTICS_DB", tmp_path / "analytics")
    monkeypatch.setattr(validate, "RAW_OKPOS_SALES", tmp_path / "okpos")
    monkeypatch.setattr(validate, "RAW_UNIONPOS_SALES", tmp_path / "unionpos")
    monkeypatch.setattr(validate, "iter_unified_sales_files", lambda: [unified_path])

    out = validate._load_excel_totals(
        target_date="2026-07-07",
        unified_platform_keys=set(),
    )

    expected = {
        ("2026-07-07", "송파삼전점", 6100),
        ("2026-07-07", "해운대중동점", 1200),
    }
    assert set(zip(out["sale_date"], out["store"], out["excel_total"])) == expected


def test_validation_load_excel_monthly_totals_adds_pos_hall_baseline(tmp_path, monkeypatch):
    analytics_root = tmp_path / "analytics" / "toorder_daily_store_platform"
    analytics_root.mkdir(parents=True)
    toorder_path = analytics_root / "toorder_store_platform_daily.parquet"
    pd.DataFrame(
        [
            {
                "date": "2026-07-10",
                "store": "송파삼전점",
                "platform": "홀",
                "price": 1000,
            },
            {
                "date": "2026-07-10",
                "store": "송파삼전점",
                "platform": "배달의민족",
                "price": 4000,
            },
            {
                "date": "2026-07-10",
                "store": "동탄영천점",
                "platform": "홀",
                "price": 2000,
            },
        ]
    ).to_parquet(toorder_path, index=False, engine="pyarrow")

    okpos_root = tmp_path / "okpos" / "brand=도리당"
    unionpos_root = tmp_path / "unionpos" / "brand=도리당"
    (okpos_root / "store=송파삼전점").mkdir(parents=True, exist_ok=True)
    (unionpos_root / "store=동탄영천점").mkdir(parents=True, exist_ok=True)

    unified_path = tmp_path / "unified_sales_260707.parquet"
    _unified_rows(
        [
            {"sale_date": "2026-07-10", "store": "송파삼전점", "source": "okpos", "platform": "홀", "total_price": 900},
            {"sale_date": "2026-07-10", "store": "동탄영천점", "source": "unionpos", "platform": "홀", "total_price": 800},
            {"sale_date": "2026-07-10", "store": "동탄영천점", "source": "easypos", "platform": "홀", "total_price": 500},
        ]
    ).to_parquet(unified_path, index=False, engine="pyarrow")

    monkeypatch.setattr(validate, "ANALYTICS_DB", tmp_path / "analytics")
    monkeypatch.setattr(validate, "RAW_OKPOS_SALES", tmp_path / "okpos")
    monkeypatch.setattr(validate, "RAW_UNIONPOS_SALES", tmp_path / "unionpos")
    monkeypatch.setattr(validate, "iter_unified_sales_files", lambda: [unified_path])

    out = validate._load_excel_monthly_totals(
        target_ym="2026-07",
        max_date="2026-07-31",
        unified_platform_keys=set(),
    )

    expected = {
        ("2026-07", "송파삼전점", 4900),
        ("2026-07", "동탄영천점", 1300),
    }
    assert set(zip(out["ym"], out["store"], out["excel_total"])) == expected


def test_daily_summary_columns_match_powerbi_legacy_schema():
    assert validate.DAILY_SUMMARY_COLUMNS == [
        "sale_date", "ym", "store", "brand", "region", "담당자", "실오픈일",
        "order_type", "platform", "total_price", "order_cnt",
        "expected_month_sales", "expected_month_order_cnt",
        "prev_expected_month_sales", "avg_3m_expected_sales",
        "prev_month_order_cnt", "avg_3m_order_cnt",
        "status",
        "llm_summary", "llm_reason", "llm_action",
        "week_start",
        "store_expected_month_sales", "store_expected_month_order_cnt",
        "store_prev_expected_month_sales", "store_prev_expected_month_order_cnt",
        "llm_total_summary", "llm_total_reason", "llm_total_action",
        "llm_brand_summary", "llm_brand_reason", "llm_brand_action",
        "expected_week_sales", "expected_week_order_cnt",
        "prev_expected_week_sales", "avg_3w_expected_sales",
        "prev_week_order_cnt", "avg_3w_order_cnt",
        "store_expected_week_sales", "store_expected_week_order_cnt",
    ]


def test_posfeed_keeps_receiving_orders_for_past_dates(monkeypatch):
    monkeypatch.setattr(posfeed, "_load_store_map", lambda: {})
    monkeypatch.setattr(posfeed, "_lookup_store_meta", lambda store_map, key, col: "")
    monkeypatch.setattr(posfeed, "_apply_posfeed_blacklist", lambda df: df)
    monkeypatch.setattr(
        posfeed,
        "allocate_manual_item_ids",
        lambda df, persist=True: pd.Series(["9000001"] * len(df), index=df.index),
    )

    orders = pd.DataFrame(
        [
            {
                "등록날짜": "2000-01-01",
                "주문상태": "접수",
                "주문타입": "배달주문",
                "주문경로": "배민1",
                "총 주문금액": "10000",
                "배달비": "0",
                "할인": "0",
                "결제금액": "10000",
                "주문 코드": "123",
                "외부 주문 번호": "ORD123",
                "주문등록 시각": "2000-01-01 12:00:00",
                "brand": "도리당",
                "store": "법흥리점",
                "메뉴명": "테스트메뉴",
            }
        ]
    )
    items = pd.DataFrame(
        [
            {
                "주문코드": "123",
                "상품명": "테스트메뉴",
                "수량": "1",
                "단품가격": "10000",
                "합계": "10000",
            }
        ]
    )

    out = posfeed._transform_df(orders, items, "2000-01-01", persist_item_ids=False)

    assert len(out) == 1
    assert out.iloc[0]["sale_type"] == "정상"
    assert int(out.iloc[0]["total_price"]) == 10000
    assert int(out.iloc[0]["order_cnt"]) == 1


def test_run_lookback_posfeed_uses_yesterday_as_start(monkeypatch):
    captured: list[str] = []

    def _capture(d: str, overwrite: bool = False, persist_item_ids: bool = True):
        captured.append(d)
        return f"posfeed {d}: 1행 저장"

    monkeypatch.setattr(posfeed, "run_posfeed", _capture)
    monkeypatch.setattr(posfeed, "_kst_today_str", lambda: "2026-07-08")

    result = posfeed.run_lookback_posfeed(days=3)

    assert captured == ["2026-07-07", "2026-07-06", "2026-07-05"]
    assert result == "posfeed lookback(3일): 3행 저장"


def test_unionpos_fin_product_write_permission_error_keeps_pending_file(tmp_path, monkeypatch):
    target = tmp_path / "fin_product_grp.csv"
    monkeypatch.setattr(unionpos, "FIN_PRODUCT_CSV_PATH", target)

    real_replace = unionpos.os.replace

    def fake_replace(src, dst):
        if str(dst) == str(target):
            raise PermissionError("locked")
        return real_replace(src, dst)

    def fake_write_bytes(data):
        raise PermissionError("locked")

    monkeypatch.setattr(unionpos.os, "replace", fake_replace)
    monkeypatch.setattr(type(target), "write_bytes", lambda self, data: fake_write_bytes(data))

    saved, path = unionpos._write_fin_product_grp_csv_from_unionpos(
        pd.DataFrame(
            [
                {
                    "source": "unipos",
                    "brand": "도리당",
                    "store": "동탄영천점",
                    "상품코드": "2000001",
                    "상품명": "테스트",
                    "item_key": "테스트",
                    "store_seq": "0",
                    "item_seq": "1",
                }
            ]
        )
    )

    assert saved is False
    assert "unionpos_pending" in path
    assert pd.read_csv(path, dtype=str, encoding="utf-8-sig").iloc[0]["상품코드"] == "2000001"


def test_unionpos_changed_product_rows_keep_allocator_identity(tmp_path, monkeypatch):
    target = tmp_path / "fin_product_grp.csv"
    monkeypatch.setattr(unionpos, "FIN_PRODUCT_CSV_PATH", target)

    df = pd.DataFrame(
        [
            {
                "source": "unipos",
                "brand": "도리당",
                "store": "동탄영천점",
                "상품코드": "2000191",
                "상품명": "한우순살곱도리탕2인",
                "중메뉴": "메인메뉴",
                "판매단가": "28900",
                "item_key": "한우순살곱도리탕2인",
                "store_seq": "0",
                "item_seq": "191",
                "is_latest": "N",
            },
            {
                "source": "unipos",
                "brand": "도리당",
                "store": "동탄영천점",
                "상품코드": "2000191",
                "상품명": "한우순살곱도리탕2인",
                "중메뉴": "곱도리탕",
                "판매단가": "28900",
                "item_key": "한우순살곱도리탕2인",
                "store_seq": "0",
                "item_seq": "191",
                "is_latest": "Y",
            },
        ]
    )

    saved, path = unionpos._write_fin_product_grp_csv_from_unionpos(df)

    assert saved is True
    out = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    assert out["item_seq"].tolist() == ["191", "191"]


def test_unionpos_upsert_change_preserves_master_allocator_columns(tmp_path, monkeypatch):
    source = tmp_path / "fin_product_grp_input.csv"
    target = tmp_path / "fin_product_grp_output.csv"
    pd.DataFrame(
        [
            {
                "source": "unipos",
                "brand": "도리당",
                "store": "동탄영천점",
                "상품코드": "2000191",
                "상품명": "한우순살곱도리탕2인",
                "중메뉴": "메인메뉴",
                "판매단가": "28900",
                "item_key": "한우순살곱도리탕2인",
                "store_seq": "0",
                "item_seq": "191",
                "llm_check": "N",
                "exclude_check": "N",
                "is_latest": "Y",
                "updated_at": "2026-07-01 00:00:00",
            }
        ]
    ).to_csv(source, index=False, encoding="utf-8-sig")

    monkeypatch.setattr(unionpos, "existing_fin_product_csv_path", lambda: source)
    monkeypatch.setattr(unionpos, "FIN_PRODUCT_CSV_PATH", target)
    monkeypatch.setattr(
        unionpos,
        "_load_unionpos_month",
        lambda ym: (
            pd.DataFrame([{"sale_date": "2026-07-08"}]),
            pd.DataFrame([{"상품코드": "2000191", "분류명": "곱도리탕", "단가": "28900"}]),
        ),
    )
    monkeypatch.setattr(
        unionpos,
        "_transform_unionpos_df",
        lambda list_df, item_df: pd.DataFrame(
            [{"store": "동탄영천점", "item_id": "2000191", "item_name": "한우순살곱도리탕2인"}]
        ),
    )

    result = unionpos.upsert_fin_product_grp_from_unionpos(ym="2026-07", dry_run=False)

    assert "+1행(변경)" in result
    out = pd.read_csv(target, dtype=str, encoding="utf-8-sig").fillna("")
    changed = out[out["is_latest"].eq("Y")].iloc[0]
    assert changed["중메뉴"] == "곱도리탕"
    assert changed["item_key"] == "한우순살곱도리탕2인"
    assert changed["store_seq"] == "0"
    assert changed["item_seq"] == "191"
