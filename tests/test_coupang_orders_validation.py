import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.extract import croling_coupang
from modules.transform.pipelines.db import DB_Coupang_01_orders as orders_pipeline


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
