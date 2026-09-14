import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_ItemIdAllocator as allocator


def _master(rows):
    columns = [
        "source",
        "brand",
        "store",
        "상품코드",
        "상품명",
        "item_key",
        "store_seq",
        "item_seq",
        "is_latest",
    ]
    return pd.DataFrame(rows).reindex(columns=columns, fill_value="").fillna("")


def test_item_seq_token_series_vectorized():
    values = pd.Series(["0", "5", "abc", "", "99999999", "-1"])

    assert allocator._item_seq_token_series(values).tolist() == ["0", "5", "", "", "", ""]


def test_allocate_manual_item_ids_keeps_existing_semantics(monkeypatch):
    master = _master(
        [
            {
                "source": "unipos",
                "brand": "도리당",
                "store": "동탄영천점",
                "상품코드": "2000005",
                "상품명": "기존메뉴",
                "item_key": "기존메뉴",
                "store_seq": "0",
                "item_seq": "5",
                "is_latest": "Y",
            },
            {
                "source": "unipos",
                "상품코드": "2000008",
                "상품명": "레거시",
                "item_key": "레거시",
                "store_seq": "0",
                "item_seq": "8",
                "is_latest": "Y",
            },
            {
                "source": "easypos",
                "brand": "도리당",
                "store": "이지점",
                "상품코드": "1000002",
                "상품명": "이지기존",
                "item_key": "이지기존",
                "store_seq": "0",
                "item_seq": "2",
                "is_latest": "Y",
            },
        ]
    )
    monkeypatch.setattr(allocator, "_read_master", lambda: master.copy())

    rows = pd.DataFrame(
        [
            {"source": "unionpos", "brand": "도리당", "store": "동탄영천점", "item_id": "2000005", "item_name": "기존메뉴"},
            {"source": "unionpos", "brand": "도리당", "store": "동탄영천점", "item_id": "2000008", "item_name": "신규충돌"},
            {"source": "unionpos", "brand": "도리당", "store": "신규점", "item_id": "", "item_name": "새매장메뉴"},
            {"source": "unionpos", "brand": "도리당", "store": "신규점", "item_id": "", "item_name": "새매장메뉴"},
            {"source": "easypos", "brand": "도리당", "store": "이지점", "item_id": "1000002", "item_name": "이지기존"},
            {"source": "easypos", "brand": "도리당", "store": "이지점", "item_id": "", "item_name": "이지신규"},
        ]
    )

    result = allocator.allocate_manual_item_ids(rows, persist=False)

    assert result.tolist() == ["2000005", "2000009", "2001000", "2001000", "1000002", "1000003"]


def test_allocate_manual_item_ids_raises_when_block_is_full(monkeypatch):
    master = _master(
        [
            {
                "source": "okpos",
                "brand": "도리당",
                "store": "만석점",
                "상품코드": str(10_000_000 + seq),
                "상품명": f"상품{seq}",
                "item_key": f"상품{seq}",
                "store_seq": "0",
                "item_seq": str(seq),
                "is_latest": "Y",
            }
            for seq in range(10_000)
        ]
    )
    monkeypatch.setattr(allocator, "_read_master", lambda: master.copy())
    rows = pd.DataFrame(
        [{"source": "okpos", "brand": "도리당", "store": "만석점", "item_id": "", "item_name": "추가상품"}]
    )

    with pytest.raises(ValueError, match="상품 슬롯 초과"):
        allocator.allocate_manual_item_ids(rows, persist=False)
