import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.extract import croling_coupang
from modules.transform.pipelines.db import DB_Coupang_01_orders as orders_pipeline
from modules.transform.pipelines.db import DB_CoupangMacro_load as macro_load
from modules.transform.pipelines.db import DB_UnifiedSales_common as unified_common
from modules.transform.pipelines.db import DB_UnifiedSales_coupang as unified_coupang


def _coupang_unified_raw(rows: list[dict]) -> pd.DataFrame:
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


def _patch_coupang_allocator(monkeypatch) -> None:
    monkeypatch.setattr(
        unified_coupang,
        "allocate_manual_item_ids",
        lambda df: pd.Series(
            [f"9000000{i}" for i in range(1, len(df) + 1)],
            index=df.index,
        ),
    )
    monkeypatch.setattr(unified_coupang, "_load_posfeed_order_price_lines", lambda *args, **kwargs: {})
    monkeypatch.setattr(unified_coupang, "_lookup_observed_price", lambda *args, **kwargs: 0)


def test_single_day_range_label_matches_target():
    assert croling_coupang._is_single_day_range_label(
        "2026.05.27 - 2026.05.27",
        "2026-05-27",
    )
    assert croling_coupang._is_single_day_range_label(
        "2026.5.27-2026.5.27",
        "2026-05-27",
    )
    assert not croling_coupang._is_single_day_range_label(
        "2026.05.27 - 2026.05.28",
        "2026-05-27",
    )


def test_calendar_popup_detector_rejects_minimum_order_menu_popup():
    snapshot = {
        "text": "최소주문 없는 메뉴 설정",
        "html": "<div class='dialog-modal-wrapper'>최소주문 없는 메뉴 설정</div>",
        "className": "dialog-modal-wrapper",
        "headerText": "",
        "dayCellCount": 0,
        "hasDayPicker": False,
        "hasGridcell": False,
    }
    assert not croling_coupang._looks_like_calendar_popup(snapshot)


def test_calendar_popup_detector_accepts_daypicker_popup():
    snapshot = {
        "text": "2026년 5월 어제 오늘",
        "html": "<div class='DayPicker'><button>어제</button><div class='DayPicker-Day'>27</div></div>",
        "className": "DayPicker-root",
        "headerText": "2026년 5월",
        "dayCellCount": 35,
        "hasDayPicker": True,
        "hasGridcell": True,
    }
    assert croling_coupang._looks_like_calendar_popup(snapshot)


def test_collect_orders_for_driver_raises_when_date_filter_fails(monkeypatch):
    monkeypatch.setattr(orders_pipeline, "navigate_to_orders_with_date", lambda *args: False)
    monkeypatch.setattr(orders_pipeline, "get_expected_order_count", lambda *args: 0)
    monkeypatch.setattr(
        orders_pipeline,
        "collect_orders_all_pages",
        lambda *args: {
            "rows": [],
            "page_count": 1,
            "session_lost": False,
            "incomplete_reason": None,
        },
    )

    with pytest.raises(RuntimeError, match="order API date filter failed"):
        orders_pipeline.collect_orders_for_driver(
            MagicMock(),
            {"store_id": "1", "store_name": "store", "account_id": "acct"},
            "2026-05-28",
        )


def test_collect_orders_for_driver_raises_when_count_mismatches(monkeypatch):
    monkeypatch.setattr(orders_pipeline, "navigate_to_orders_with_date", lambda *args: True)
    monkeypatch.setattr(orders_pipeline, "get_expected_order_count", lambda *args: 3)
    monkeypatch.setattr(
        orders_pipeline,
        "collect_orders_all_pages",
        lambda *args: {
            "rows": [{"order_id": "A"}, {"order_id": "B"}],
            "page_count": 1,
            "session_lost": False,
            "incomplete_reason": None,
        },
    )

    with pytest.raises(RuntimeError, match="count mismatch"):
        orders_pipeline.collect_orders_for_driver(
            MagicMock(),
            {"store_id": "1", "store_name": "store", "account_id": "acct"},
            "2026-05-28",
        )


def test_collect_orders_for_driver_raises_when_session_is_lost(monkeypatch):
    monkeypatch.setattr(orders_pipeline, "navigate_to_orders_with_date", lambda *args: True)
    monkeypatch.setattr(orders_pipeline, "get_expected_order_count", lambda *args: 2)
    monkeypatch.setattr(
        orders_pipeline,
        "collect_orders_all_pages",
        lambda *args: {
            "rows": [{"order_id": "A"}, {"order_id": "B"}],
            "page_count": 2,
            "session_lost": True,
            "incomplete_reason": "session_lost_during_page_transition",
        },
    )

    with pytest.raises(RuntimeError, match="session lost"):
        orders_pipeline.collect_orders_for_driver(
            MagicMock(),
            {"store_id": "1", "store_name": "store", "account_id": "acct"},
            "2026-05-28",
        )


def test_collect_orders_for_driver_uses_api_expected_count(monkeypatch):
    monkeypatch.setattr(orders_pipeline, "navigate_to_orders_with_date", lambda *args: True)
    monkeypatch.setattr(orders_pipeline, "get_expected_order_count", lambda *args: 999)
    monkeypatch.setattr(
        orders_pipeline,
        "collect_orders_all_pages",
        lambda *args: {
            "rows": [{"order_id": "A"}, {"order_id": "B"}],
            "page_count": 1,
            "session_lost": False,
            "incomplete_reason": None,
            "expected": 2,
            "api_filtered": True,
        },
    )

    _, validation = orders_pipeline.collect_orders_for_driver(
        MagicMock(),
        {"store_id": "1", "store_name": "store", "account_id": "acct"},
        "2026-05-28",
    )

    assert validation["expected"] == 2
    assert validation["collected"] == 2
    assert validation["matched"] is True
    assert validation["api_filtered"] is True


def test_collect_orders_for_driver_accepts_dom_target_date_fallback(monkeypatch):
    monkeypatch.setattr(orders_pipeline, "navigate_to_orders_with_date", lambda *args: False)
    monkeypatch.setattr(orders_pipeline, "get_expected_order_count", lambda *args: 2)
    monkeypatch.setattr(
        orders_pipeline,
        "collect_orders_all_pages",
        lambda *args: {
            "rows": [
                {"order_id": "A", "order_date": "2026.05.28 11:01"},
                {"order_id": "B", "order_date": "2026.05.28 12:15"},
            ],
            "page_count": 1,
            "session_lost": False,
            "incomplete_reason": "next_button_missing",
            "api_filtered": False,
        },
    )

    _, validation = orders_pipeline.collect_orders_for_driver(
        MagicMock(),
        {"store_id": "1", "store_name": "store", "account_id": "acct"},
        "2026-05-28",
    )

    assert validation["date_filter_ok"] is True
    assert validation["dom_target_date_ok"] is True
    assert validation["matched"] is True
    assert validation["incomplete_reason"] is None


def test_coupang_manual_load_replaces_covered_range(tmp_path, monkeypatch):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    recorded = []
    monkeypatch.setattr(
        macro_load,
        "record_manual_reingest_marker",
        lambda source, store, date, meta: recorded.append((source, store, date, meta)) or True,
    )

    out_dir = orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "orders_2026-07.parquet"
    pd.DataFrame(
        [
            {"order_date": "2026.07.01 10:00", "order_id": "OUT", "total_price": "9000"},
            {"order_date": "2026.07.12 10:00", "order_id": "GHOST", "total_price": "22000"},
        ]
    ).to_parquet(out_path, index=False)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(
        [
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.12 11:00",
                "order_id": "B",
                "total_price": "50000",
            }
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = macro_load._load_orders([{"path": csv_path, "source": "collect"}])

    out = pd.read_parquet(out_path)
    assert result["rows_loaded"] == 1
    assert set(out["order_id"]) == {"OUT", "B"}
    assert recorded == [
        (
            "쿠팡수동",
            "테스트매장",
            "2026-07-12",
            {"rows": 1, "removed": 1},
        )
    ]


def test_coupang_manual_load_append_only_when_shrinking_reingest_is_duplicate(
    tmp_path,
    monkeypatch,
):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    recorded = []
    monkeypatch.setattr(
        macro_load,
        "record_manual_reingest_marker",
        lambda *args, **kwargs: recorded.append((args, kwargs)) or True,
    )

    out_dir = orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "orders_2026-07.parquet"
    existing_rows = [
        {
            "store_name": "테스트매장",
            "order_date": "2026.07.29 12:00",
            "order_id": f"OLD{i:02d}",
            "total_price": "10000",
        }
        for i in range(39)
    ]
    pd.DataFrame(existing_rows).to_parquet(out_path, index=False)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(existing_rows[:10]).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = macro_load._load_orders([{"path": csv_path, "source": "down"}])

    out = pd.read_parquet(out_path)
    assert set(out["order_id"]) == {f"OLD{i:02d}" for i in range(39)}
    assert result["rows_loaded"] == 10
    assert result["rows_blocked"] == 0
    assert result["blocked_files"] == []
    assert result["blocked_outputs"] == []
    assert result["append_only_outputs"][0]["dates"] == ["2026-07-29"]
    assert result["append_only_outputs"][0]["details"] == {
        "2026-07-29": {"existing_orders": 39, "new_orders": 10}
    }
    assert recorded == [
        (
            (
                "쿠팡수동",
                "테스트매장",
                "2026-07-29",
                {"rows": 10, "removed": 0},
            ),
            {},
        )
    ]


def test_coupang_manual_load_append_only_adds_new_orders_when_shrinking_reingest(
    tmp_path,
    monkeypatch,
):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    monkeypatch.setattr(macro_load, "record_manual_reingest_marker", lambda *args, **kwargs: True)

    out_dir = orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "orders_2026-07.parquet"
    pd.DataFrame(
        [
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.29 12:00",
                "order_id": f"OLD{i:02d}",
                "total_price": "10000",
            }
            for i in range(39)
        ]
    ).to_parquet(out_path, index=False)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(
        [
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.29 22:00",
                "order_id": f"NEW{i:02d}",
                "total_price": "10000",
            }
            for i in range(10)
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = macro_load._load_orders([{"path": csv_path, "source": "down"}])

    out = pd.read_parquet(out_path)
    assert set(out["order_id"]) == {
        *{f"OLD{i:02d}" for i in range(39)},
        *{f"NEW{i:02d}" for i in range(10)},
    }
    assert result["rows_loaded"] == 10
    assert result["rows_blocked"] == 0
    assert result["blocked_files"] == []
    assert result["blocked_outputs"] == []
    assert result["append_only_outputs"][0]["dates"] == ["2026-07-29"]
    assert result["append_only_outputs"][0]["details"] == {
        "2026-07-29": {"existing_orders": 39, "new_orders": 10}
    }


def test_coupang_manual_load_only_preserves_dates_that_shrink(tmp_path, monkeypatch):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    monkeypatch.setattr(macro_load, "record_manual_reingest_marker", lambda *args, **kwargs: True)

    out_dir = orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "orders_2026-07.parquet"
    pd.DataFrame(
        [
            *[
                {
                    "store_name": "테스트매장",
                    "order_date": "2026.07.29 12:00",
                    "order_id": f"OLD{i:02d}",
                    "total_price": "10000",
                }
                for i in range(39)
            ],
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.30 12:00",
                "order_id": "GHOST",
                "total_price": "10000",
            },
        ]
    ).to_parquet(out_path, index=False)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(
        [
            *[
                {
                    "store_name": "테스트매장",
                    "order_date": "2026.07.29 22:00",
                    "order_id": f"NEW{i:02d}",
                    "total_price": "10000",
                }
                for i in range(10)
            ],
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.30 13:00",
                "order_id": "B",
                "total_price": "50000",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = macro_load._load_orders([{"path": csv_path, "source": "down"}])

    out = pd.read_parquet(out_path)
    assert "GHOST" not in set(out["order_id"])
    assert "B" in set(out["order_id"])
    assert {f"OLD{i:02d}" for i in range(39)}.issubset(set(out["order_id"]))
    assert {f"NEW{i:02d}" for i in range(10)}.issubset(set(out["order_id"]))
    assert result["append_only_outputs"][0]["dates"] == ["2026-07-29"]


def test_coupang_manual_load_deduplicates_numeric_and_null_text_variants(tmp_path, monkeypatch):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    monkeypatch.setattr(macro_load, "record_manual_reingest_marker", lambda *args, **kwargs: True)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(
        [
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.12 11:00",
                "order_id": "DUP",
                "order_status": None,
                "total_price": "10000",
                "menu_qty": "1",
                "menu_price": "10000",
                "menu_options": "동일행",
            },
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.12 11:00",
                "order_id": "DUP",
                "order_status": "nan",
                "total_price": "10000.0",
                "menu_qty": "1.0",
                "menu_price": "10000.0",
                "menu_options": "동일행",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    macro_load._load_orders([{"path": csv_path, "source": "collect"}])

    out = pd.read_parquet(
        orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07" / "orders_2026-07.parquet"
    )
    assert len(out) == 1
    assert out["order_id"].tolist() == ["DUP"]


def test_coupang_manual_load_backfills_item_menu_from_menu_name(tmp_path, monkeypatch):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "_resolve_brand_store", lambda value: ("도리당", "테스트매장"))
    monkeypatch.setattr(macro_load, "record_manual_reingest_marker", lambda *args, **kwargs: True)

    csv_path = tmp_path / "coupangeats_orders_test.csv"
    pd.DataFrame(
        [
            {
                "store_name": "테스트매장",
                "order_date": "2026.07.12 11:00",
                "order_id": "ITEM_MENU",
                "total_price": "10000",
                "menu_name": "부모메뉴",
                "menu_price": "10000",
                "menu_options": "옵션명",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    macro_load._load_orders([{"path": csv_path, "source": "collect"}])

    out = pd.read_parquet(
        orders_root / "brand=도리당" / "store=테스트매장" / "ym=2026-07" / "orders_2026-07.parquet"
    )
    assert out["item_menu"].tolist() == ["부모메뉴"]
    assert out["menu_name"].tolist() == ["부모메뉴"]


def test_coupang_unified_priced_row_uses_parent_menu_not_option(monkeypatch):
    _patch_coupang_allocator(monkeypatch)
    raw = _coupang_unified_raw([
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

    out = unified_coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert out["menu_name"].tolist() == ["실제 부모 메뉴", "실제 부모 메뉴"]
    assert out["item_name"].tolist() == ["실제 부모 메뉴", "기본맛"]
    assert out["total_price"].astype(int).tolist() == [30000, 0]
    assert out["order_cnt"].astype(int).tolist() == [1, 0]


def test_coupang_unified_multi_menu_order_allocates_total_to_priced_rows(monkeypatch):
    _patch_coupang_allocator(monkeypatch)
    raw = _coupang_unified_raw([
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

    out = unified_coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert out["item_name"].tolist() == ["닭도리탕", "기본맛", "흑미 공기밥"]
    assert out["total_price"].astype(int).tolist() == [16900, 0, 1000]
    assert int(out["total_price"].sum()) == 17900
    assert out["order_cnt"].astype(int).sum() == 1


def test_coupang_unified_discounted_order_preserves_total_after_rounding(monkeypatch):
    _patch_coupang_allocator(monkeypatch)
    raw = _coupang_unified_raw([
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

    out = unified_coupang._transform_to_unified(raw, "강동점", "나홀로", {})

    assert int(out["total_price"].sum()) == 15001
    assert sorted(out["total_price"].astype(int).tolist()) == [7500, 7501]
    assert out["order_cnt"].astype(int).sum() == 1


def test_coupang_load_lock_blocks_concurrent_loader(tmp_path, monkeypatch):
    lock_dir = tmp_path / "coupang_macro_load.lock"
    monkeypatch.setattr(macro_load, "COUPANG_LOAD_LOCK_DIR", lock_dir)

    with macro_load._coupang_load_lock(timeout_sec=1, wait_interval_sec=0.01):
        assert lock_dir.exists()
        with pytest.raises(TimeoutError):
            with macro_load._coupang_load_lock(timeout_sec=0.02, wait_interval_sec=0.01):
                pass

    assert not lock_dir.exists()


def test_coupang_iter_source_files_includes_extra_download_dirs(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    downloads_dir = tmp_path / "downloads"
    for path in (down_dir, collect_dir, downloads_dir):
        path.mkdir()

    (down_dir / "coupangeats_orders_down_20260818.csv").write_text("x", encoding="utf-8")
    (collect_dir / "coupangeats_orders_collect_20260818.csv").write_text("x", encoding="utf-8")
    (downloads_dir / "coupangeats_orders_downloads_20260818.csv").write_text("x", encoding="utf-8")

    monkeypatch.setattr(macro_load, "DOWN_DIR", down_dir)
    monkeypatch.setattr(macro_load, "COLLECT_SRC", collect_dir)
    monkeypatch.setenv(macro_load.COUPANG_EXTRA_DOWNLOAD_DIRS_ENV, str(downloads_dir))

    found = macro_load._iter_source_files("orders")

    assert {item["path"].name for item in found} == {
        "coupangeats_orders_down_20260818.csv",
        "coupangeats_orders_collect_20260818.csv",
        "coupangeats_orders_downloads_20260818.csv",
    }
    assert {item["source"] for item in found} == {"down", "collect", "downloads"}


def test_move_coupang_down_to_collect_moves_extra_download_dirs(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    downloads_dir = tmp_path / "downloads"
    for path in (down_dir, collect_dir, downloads_dir):
        path.mkdir()

    (down_dir / "coupangeats_orders_down_20260818.csv").write_text("x", encoding="utf-8")
    (downloads_dir / "coupangeats_orders_downloads_20260818.csv").write_text(
        "x",
        encoding="utf-8",
    )
    (collect_dir / "coupangeats_orders_existing_20260818.csv").write_text("x", encoding="utf-8")

    monkeypatch.setattr(macro_load, "DOWN_DIR", down_dir)
    monkeypatch.setattr(macro_load, "COLLECT_SRC", collect_dir)
    monkeypatch.setenv(macro_load.COUPANG_EXTRA_DOWNLOAD_DIRS_ENV, str(downloads_dir))

    result = macro_load.move_coupang_down_to_collect()

    assert result == "이동 완료: 2개"
    assert not list(down_dir.glob("coupangeats_*"))
    assert not list(downloads_dir.glob("coupangeats_*"))
    assert (collect_dir / "coupangeats_orders_down_20260818.csv").exists()
    assert (collect_dir / "coupangeats_orders_downloads_20260818.csv").exists()
    assert (collect_dir / "coupangeats_orders_existing_20260818.csv").exists()


def test_misplaced_coupang_marketing_file_moves_to_collect(tmp_path, monkeypatch):
    collect_dir = tmp_path / "collect"
    marketing_dir = tmp_path / "marketing"
    collect_dir.mkdir()
    marketing_dir.mkdir()

    misplaced = marketing_dir / "coupangeats_orders_store_20260818.csv"
    misplaced.write_text("x", encoding="utf-8")

    monkeypatch.setattr(macro_load, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(macro_load, "MISPLACED_MARKETING_SRC", marketing_dir)

    moved = macro_load.move_misplaced_coupang_marketing_to_collect()

    assert moved == [{"source": str(misplaced), "dest": str(collect_dir / misplaced.name)}]
    assert not misplaced.exists()
    assert (collect_dir / misplaced.name).exists()


def test_load_coupang_partition_ingests_misplaced_marketing_file(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    marketing_dir = tmp_path / "marketing"
    orders_root = tmp_path / "orders"
    cmg_root = tmp_path / "cmg"
    options_root = tmp_path / "options"
    for path in (down_dir, collect_dir, marketing_dir):
        path.mkdir()

    csv_path = marketing_dir / "coupangeats_orders_닭도리탕_전문_도리당_삼송점_932290_20260901.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-09-01T11:14:57",
                "store_id": "932290",
                "store_name": "닭도리탕 전문 도리당 삼송점",
                "order_date": "2026.05.06 19:29",
                "order_id": "ORDER1",
                "delivery_type": "배달",
                "order_status": "",
                "order_summary": "테스트메뉴",
                "total_price": "12000",
                "is_cancelled": "N",
                "menu_name": "테스트메뉴",
                "menu_qty": "1",
                "menu_price": "12000",
                "menu_options": "",
            }
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    monkeypatch.setattr(macro_load, "DOWN_DIR", down_dir)
    monkeypatch.setattr(macro_load, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(macro_load, "MISPLACED_MARKETING_SRC", marketing_dir)
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    monkeypatch.setattr(macro_load, "CMG_DIR", cmg_root)
    monkeypatch.setattr(macro_load, "OPTIONS_DIR", options_root)
    monkeypatch.delenv(macro_load.COUPANG_EXTRA_DOWNLOAD_DIRS_ENV, raising=False)
    monkeypatch.setattr(macro_load, "record_manual_reingest_marker", lambda *args, **kwargs: None)

    result = json.loads(macro_load._load_coupang_macro_partition_unlocked())

    assert result["orders"]["files_found"] == 1
    assert result["orders"]["files_loaded"] == 1
    assert result["orders"]["rows_loaded"] == 1
    assert result["misplaced_moved_count"] == 1
    assert result["misplaced_moved_files"][0]["source"] == str(csv_path)
    assert not csv_path.exists()
    assert not list(collect_dir.glob("coupangeats_orders_*.csv"))
    assert (orders_root / "brand=도리당" / "store=삼송점" / "ym=2026-05" / "orders_2026-05.parquet").exists()


def test_repair_coupang_orders_duplicates_removes_normalized_store_partition_duplicate(
    tmp_path,
    monkeypatch,
):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(macro_load, "COUPANG_ORDERS_DB", orders_root)
    base_row = {
        "order_date": "2026.06.14 16:31",
        "order_id": "02MV31",
        "delivery_type": "배달",
        "order_status": None,
        "order_summary": "테스트메뉴",
        "total_price": "21000.0",
        "is_cancelled": "N",
        "menu_name": "테스트메뉴",
        "menu_qty": "1.0",
        "menu_price": "21000.0",
        "menu_options": "동일행",
    }
    old_path = (
        orders_root
        / "brand=도리당"
        / "store=구로디지털단지점"
        / "ym=2026-06"
        / "orders_2026-06.parquet"
    )
    new_path = (
        orders_root
        / "brand=도리당"
        / "store=구로디지털점"
        / "ym=2026-06"
        / "orders_2026-06.parquet"
    )
    old_path.parent.mkdir(parents=True, exist_ok=True)
    new_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([base_row]).to_parquet(old_path, index=False)
    pd.DataFrame([
        {
            **base_row,
            "order_status": "nan",
            "total_price": "21000",
            "menu_qty": "1",
            "menu_price": "21000",
        }
    ]).to_parquet(new_path, index=False)

    result = macro_load.repair_coupang_orders_duplicates()

    assert "제거=1" in result
    assert not old_path.exists()
    out = pd.read_parquet(new_path)
    assert len(out) == 1
    assert out["order_id"].tolist() == ["02MV31"]


def test_coupang_reingest_marker_is_total_only_target(tmp_path, monkeypatch):
    marker_root = tmp_path / "markers"
    unified_root = tmp_path / "unified"
    unified_root.mkdir()
    monkeypatch.setattr(unified_common, "MANUAL_REINGEST_MARKER_ROOT", marker_root)
    monkeypatch.setattr(unified_common, "UNIFIED_ROOT", unified_root)
    monkeypatch.setattr(unified_coupang, "UNIFIED_ROOT", unified_root)

    date = "2000-01-01"
    store = "테스트매장"
    assert unified_common.record_manual_reingest_marker("쿠팡수동", store, date, {}) is True
    base_dates = unified_coupang._resolve_target_dates([store], None, 14)

    assert date not in base_dates
    assert date in unified_coupang._target_dates_for_store(
        store,
        base_dates,
        None,
        14,
        include_reingest_markers=True,
    )
