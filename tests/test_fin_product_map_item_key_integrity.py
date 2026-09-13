from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_FinProduct_Map as M


# --- Fix A: item_key는 항상 item_name으로부터 재계산되어야 한다 -----------------

def test_fill_item_identity_columns_recomputes_mismatched_item_key():
    """디스크에 저장된 item_key가 item_name과 어긋나 있어도 항상 바로잡는다.

    실제로 발견된 사례(2026-09-11): item_id=300004078의 item_name은
    "[1인] 순살 닭도리탕 (밥포함) 1인분"인데 item_key는 엉뚱하게 "참이슬후레쉬355ml"로
    저장되어 있었다.
    """
    df = pd.DataFrame([{
        "item_id": "300004078",
        "item_key": "참이슬후레쉬355ml",  # item_name과 무관한 잘못된 값
        "store_seq": "",
        "item_seq": "",
        "store": "송파삼전점",
        "source": "posfeed",
        "brand": "도리당",
        "item_name": "[1인] 순살 닭도리탕 (밥포함) 1인분",
        "unitprice": "0",
    }])

    result = M._fill_item_identity_columns(df, persist=False)

    assert result.loc[0, "item_key"] == M.normalize_item_key("[1인] 순살 닭도리탕 (밥포함) 1인분")
    assert result.loc[0, "item_key"] != "참이슬후레쉬355ml"


def test_scan_target_items_item_key_matches_picked_item_name(monkeypatch):
    """item_id 재사용으로 과거/현재 item_name이 섞여 있어도, 선택된 대표 item_name과
    item_key가 항상 일치해야 한다 (독립적으로 최빈값을 뽑으면 서로 어긋날 수 있었음).
    """
    raw = pd.DataFrame(
        [
            {
                "item_id": "300009999",
                "store": "송파삼전점",
                "source": "posfeed",
                "brand": "도리당",
                "item_name": "새 메뉴 A",
                "unit_price": "10000",
                "menu_name": "새 메뉴 A",
            }
        ]
        * 3
        + [
            {
                "item_id": "300009999",
                "store": "송파삼전점",
                "source": "posfeed",
                "brand": "도리당",
                "item_name": "예전 메뉴 B",
                "unit_price": "9000",
                "menu_name": "예전 메뉴 B",
            }
        ]
        * 2
    )
    monkeypatch.setattr(M, "iter_unified_sales_files", lambda: [Path("sample.parquet")])
    monkeypatch.setattr(M.pd, "read_parquet", lambda *a, **k: raw.copy())

    result = M.scan_target_items(persist_identity=False)

    # source가 posfeed면 item_id가 재할당되므로(allocate_manual_item_ids), item_name으로 찾는다.
    assert len(result) == 1
    row = result.iloc[0]
    assert row["item_name"] == "새 메뉴 A"
    assert row["item_key"] == M.normalize_item_key("새 메뉴 A")
    assert row["item_key"] != M.normalize_item_key("예전 메뉴 B")


# --- Fix B: item_id 재사용(실제 상품 변경) 감지 → 분류 초기화 ------------------

def test_reset_reused_item_classifications_clears_stale_label_when_product_changed():
    existing = pd.DataFrame([
        {
            "item_id": "300004078",
            "item_name": "참이슬후레쉬 355ml",  # 예전(재사용 전) 상품명
            "item_name_current": "[1인] 순살 닭도리탕 (밥포함) 1인분",  # 새로 스캔된 현재 상품명
            "표준_메뉴명_edit": "참이슬 후레쉬",
            "수동분류_edit": "음료",
            "classified_by": "human",
            M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED,
            "updated_at": "2026-08-01",
        },
        {
            "item_id": "300004095",
            "item_name": "참이슬후레쉬 355ml",
            "item_name_current": "참이슬후레쉬 355ml",  # 상품 그대로 - 재사용 아님
            "표준_메뉴명_edit": "참이슬 후레쉬",
            "수동분류_edit": "주류",
            "classified_by": "human",
            M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED,
            "updated_at": "2026-08-01",
        },
    ])

    result, reused_count = M._reset_reused_item_classifications(existing)

    assert reused_count == 1
    changed = result.set_index("item_id").loc["300004078"]
    assert changed["표준_메뉴명_edit"] == ""
    assert changed["수동분류_edit"] == ""
    assert changed["classified_by"] == ""
    assert changed[M.REVIEW_STATUS_COLUMN] == M.REVIEW_PENDING

    unchanged = result.set_index("item_id").loc["300004095"]
    assert unchanged["표준_메뉴명_edit"] == "참이슬 후레쉬"
    assert unchanged["수동분류_edit"] == "주류"
    assert unchanged[M.REVIEW_STATUS_COLUMN] == M.REVIEW_APPROVED


def test_reset_reused_item_classifications_ignores_cosmetic_name_changes():
    """공백 차이 등 사소한 표기 차이는 상품이 바뀐 것으로 보지 않는다."""
    existing = pd.DataFrame([{
        "item_id": "300004095",
        "item_name": "참이슬후레쉬355ml",
        "item_name_current": "참이슬후레쉬 355ml",  # 공백만 다름
        "표준_메뉴명_edit": "참이슬 후레쉬",
        "수동분류_edit": "주류",
        "classified_by": "human",
        M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED,
        "updated_at": "2026-08-01",
    }])

    result, reused_count = M._reset_reused_item_classifications(existing)

    assert reused_count == 0
    assert result.loc[0, "수동분류_edit"] == "주류"


def test_reset_reused_item_classifications_noop_without_current_column():
    existing = pd.DataFrame([{"item_id": "1", "item_name": "a", "수동분류_edit": "메인"}])
    result, reused_count = M._reset_reused_item_classifications(existing)
    assert reused_count == 0
    assert result.loc[0, "수동분류_edit"] == "메인"


# --- Fix C: 형제(item_key 동일, item_id 다름) 분류 불일치 플래그 ---------------

def _review_row(item_id: str, item_key: str, std_name: str, label: str, **overrides) -> dict:
    row = {
        "item_id": item_id,
        "item_key": item_key,
        "store": "송파삼전점",
        "source": "쿠팡수동",
        "brand": "도리당",
        "item_name": f"item-{item_id}",
        "unitprice": "4000",
        "표준_메뉴명_edit": std_name,
        "수동분류_edit": label,
        M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED,
        "검수사유": "",
    }
    row.update(overrides)
    return row


def test_mark_sibling_conflicts_flags_disagreeing_group_only():
    df = pd.DataFrame([
        _review_row("300004078", "참이슬후레쉬355ml", "참이슬 후레쉬", "음료"),
        _review_row("300004095", "참이슬후레쉬355ml", "참이슬 후레쉬", "주류"),
        _review_row("100059120", "동일상품키", "동일 상품", "메인"),
        _review_row("100059121", "동일상품키", "동일 상품", "메인"),
    ])

    result = M._mark_sibling_conflicts(df)

    flagged = result.set_index("item_id")[M.SIBLING_CONFLICT_COLUMN]
    assert flagged["300004078"] == "Y"
    assert flagged["300004095"] == "Y"
    assert flagged["100059120"] == "N"
    assert flagged["100059121"] == "N"


def test_mark_sibling_conflicts_ignores_single_item_id_groups():
    df = pd.DataFrame([_review_row("9001", "단독상품", "단독 상품", "메인")])
    result = M._mark_sibling_conflicts(df)
    assert result.loc[0, M.SIBLING_CONFLICT_COLUMN] == "N"


def test_notify_sibling_conflicts_counts_distinct_groups(monkeypatch):
    sent = []
    monkeypatch.setattr(M, "send_telegram", lambda msg: sent.append(msg))
    df = M._mark_sibling_conflicts(pd.DataFrame([
        _review_row("300004078", "참이슬후레쉬355ml", "참이슬 후레쉬", "음료"),
        _review_row("300004095", "참이슬후레쉬355ml", "참이슬 후레쉬", "주류"),
    ]))

    count = M._notify_sibling_conflicts(df)

    assert count == 1
    assert len(sent) == 1
    assert "형제 상품" in sent[0]
