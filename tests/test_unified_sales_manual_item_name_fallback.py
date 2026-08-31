import json

import numpy as np
import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_baemin as baemin
from modules.transform.pipelines.db import DB_UnifiedSales_common as common
from modules.transform.pipelines.db import DB_UnifiedSales_coupang as coupang
from modules.transform.utility import notifier


def _allocator_spy(rows: pd.DataFrame, **kwargs) -> pd.Series:
    assert rows["item_name"].fillna("").astype(str).str.strip().ne("").all()
    return pd.Series(["90000001"] * len(rows), index=rows.index)


def test_fill_missing_manual_item_name_normalizes_and_notifies_once(
    tmp_path,
    monkeypatch,
    caplog,
):
    sent: list[str] = []
    monkeypatch.setattr(common, "MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT", tmp_path / "markers")
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    values = pd.Series(["정상메뉴", None, " ", np.nan, "NaN"])
    dates = pd.Series(["2026-07-19"] * len(values))
    order_ids = pd.Series(["NORMAL", "ORD1", "ORD2", "ORD3", "ORD4"])

    first = common.fill_missing_manual_item_name(
        values,
        source="배민수동",
        label="배민",
        store="테스트점",
        sale_date=dates,
        order_id=order_ids,
    )
    second = common.fill_missing_manual_item_name(
        values,
        source="배민수동",
        label="배민",
        store="테스트점",
        sale_date=dates,
        order_id=order_ids,
    )

    assert first.tolist() == ["정상메뉴", "메뉴미상(배민)", "메뉴미상(배민)", "메뉴미상(배민)", "메뉴미상(배민)"]
    assert second.equals(first)
    assert len(sent) == 1
    assert "결손: 4건" in sent[0]
    assert "ORD1,ORD2,ORD3,ORD4" in sent[0]
    assert "플레이스홀더 처리" in caplog.text

    marker = tmp_path / "markers" / "배민수동" / "테스트점" / "2026-07-19.json"
    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["missing_count"] == 4
    assert payload["order_ids"] == ["ORD1", "ORD2", "ORD3", "ORD4"]


def test_marker_write_failure_does_not_block_placeholder(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    sent: list[str] = []
    monkeypatch.setattr(common, "MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT", blocked)
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    out = common.fill_missing_manual_item_name(
        pd.Series([""]),
        source="쿠팡수동",
        label="쿠팡",
        store="테스트점",
        sale_date="2026-07-19",
        order_id=pd.Series(["COUPANG1"]),
    )

    assert out.tolist() == ["메뉴미상(쿠팡)"]
    assert sent == []


def test_notification_failure_does_not_block_placeholder(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT", tmp_path / "markers")

    def _raise_notification_error(message: str):
        raise RuntimeError("notification unavailable")

    monkeypatch.setattr(notifier, "send_telegram", _raise_notification_error)

    out = common.fill_missing_manual_item_name(
        pd.Series([""]),
        source="배민수동",
        label="배민",
        store="테스트점",
        sale_date="2026-07-19",
        order_id=pd.Series(["BAEMIN1"]),
    )

    assert out.tolist() == ["메뉴미상(배민)"]
    assert (tmp_path / "markers" / "배민수동" / "테스트점" / "2026-07-19.json").exists()


def test_baemin_transform_preserves_missing_detail_order_amount(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(common, "MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT", tmp_path / "markers")
    monkeypatch.setattr(notifier, "send_telegram", lambda message: True)
    monkeypatch.setattr(baemin, "allocate_manual_item_ids", _allocator_spy)

    raw = pd.DataFrame(
        [
            {
                "주문시각": "2026. 07. 19. (일) 오후 1:00:00",
                "주문번호": "NORMAL1",
                "주문상태": "배달완료",
                "수령방법": "배달",
                "주문내역": "정상메뉴",
                "주문옵션상세": "정상메뉴",
                "주문수량": "1",
                "주문옵션금액": "10000",
                "결제금액": "10000",
                "sale_date": "2026-07-19",
                "order_time": "13:00:00",
                "_source_brand": "도리당",
            },
            {
                "주문시각": "2026. 07. 19. (일) 오후 2:00:00",
                "주문번호": "B2EP00E792",
                "주문상태": "배달완료",
                "수령방법": "배달",
                "주문내역": "",
                "주문옵션상세": "",
                "주문수량": "",
                "주문옵션금액": "",
                "결제금액": "22300",
                "sale_date": "2026-07-19",
                "order_time": "14:00:00",
                "_source_brand": "도리당",
            },
        ]
    )

    out = baemin._transform_to_unified(raw, "경북상주점", "도리당", {})
    target = out[out["order_id"].eq("B2EP00E792")]

    assert len(out) == 2
    assert target["item_name"].tolist() == ["메뉴미상(배민)"]
    assert target["item_id"].tolist() == ["90000001"]
    assert int(target["qty"].iloc[0]) == 1
    assert int(target["unit_price"].iloc[0]) == 0
    assert int(target["total_price"].sum()) == 22300


def test_coupang_transform_fills_missing_detail_before_allocator(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(common, "MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT", tmp_path / "markers")
    monkeypatch.setattr(notifier, "send_telegram", lambda message: True)
    monkeypatch.setattr(coupang, "allocate_manual_item_ids", _allocator_spy)

    raw = pd.DataFrame(
        [
            {
                "order_id": "COUPANG1",
                "order_time": "14:00:00",
                "sale_date": "2026-07-19",
                "order_summary": "",
                "menu_options": "",
                "menu_name": "",
                "menu_qty": "",
                "menu_price": "",
                "delivery_type": "배달",
                "is_cancelled": "N",
                "total_price": "17500",
            }
        ]
    )

    out = coupang._transform_to_unified(raw, "경북상주점", "도리당", {})

    assert out["item_name"].tolist() == ["메뉴미상(쿠팡)"]
    assert out["item_id"].tolist() == ["90000001"]
    assert int(out["qty"].iloc[0]) == 1
    assert int(out["unit_price"].iloc[0]) == 0
    assert int(out["total_price"].sum()) == 17500
