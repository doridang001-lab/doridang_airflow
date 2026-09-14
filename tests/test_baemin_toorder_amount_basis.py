"""ToOrder 교차검증의 금액 기준(총결제금액)과 재수집 안전장치 회귀 테스트.

배경:
- ToOrder price / 배민 페이지 TotalSummary 는 총결제금액 기준이다.
  결제금액만 합산하면 만나서결제 주문(결제금액 공란)이 0원 처리되어
  해당 매장이 매일 영구 불일치로 남는다.
- 그 불일치가 삭제→재수집을 유발하고, 재수집이 0건을 성공으로 처리하면
  삭제한 원본이 복원되지 않고 소실된다.
"""

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_Beamin_Macro_validate as macro_validate
from modules.transform.pipelines.db import DB_Beamin_04_orders as orders

DORIDANG = "도리당"
NAHOLLO = "나홀로"
STORE = "부산광안점"
TARGET_DATE = "2026-08-23"
DATE_PREFIX = "2026. 08. 23."


def _orders_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for col in ("주문상태", "주문번호", "주문시각"):
        if col not in frame.columns:
            frame[col] = ""
    return frame.astype(str)


def _write_orders(base, brand: str, store: str, frame: pd.DataFrame):
    ym = TARGET_DATE[:7]
    path = base / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


# ---------------------------------------------------------------------------
# 금액 기준
# ---------------------------------------------------------------------------

def test_order_amounts_prefers_total_payment_over_blank_payment():
    """만나서결제 주문은 결제금액이 공란이라 총결제금액으로 집계돼야 한다."""
    frame = _orders_frame(
        [
            {"주문번호": "A", "결제금액": "10,000", "총결제금액": "10,000"},
            {"주문번호": "A", "결제금액": "", "총결제금액": ""},
            {"주문번호": "B", "결제금액": "", "총결제금액": "13,900"},
        ]
    )

    result = macro_validate._order_amounts(frame)

    assert dict(zip(result["주문번호"], result["amount"])) == {"A": 10000, "B": 13900}


def test_order_amounts_falls_back_to_payment_when_total_missing():
    """수동 CSV처럼 총결제금액 컬럼이 없으면 결제금액으로 폴백한다."""
    frame = _orders_frame([{"주문번호": "A", "결제금액": "7,500"}])

    result = macro_validate._order_amounts(frame)

    assert result["amount"].tolist() == [7500]


def test_baemin_orders_stats_counts_cash_on_delivery_orders(monkeypatch, tmp_path):
    """결제금액이 빈 주문도 건수/금액에 포함돼야 ToOrder와 맞는다."""
    monkeypatch.setattr(macro_validate, "BAEMIN_ORDERS_DB", tmp_path)
    _write_orders(
        tmp_path,
        DORIDANG,
        STORE,
        _orders_frame(
            [
                {
                    "주문상태": "배달완료",
                    "주문번호": "A",
                    "주문시각": f"{DATE_PREFIX} 12:00",
                    "결제금액": "20,000",
                    "총결제금액": "20,000",
                },
                {
                    "주문상태": "배달완료",
                    "주문번호": "B",
                    "주문시각": f"{DATE_PREFIX} 13:00",
                    "결제금액": "",
                    "총결제금액": "13,900",
                },
            ]
        ),
    )

    stats = macro_validate._baemin_orders_stats_by_store(TARGET_DATE)

    assert stats[STORE] == {"amount": 33900, "count": 2}


# ---------------------------------------------------------------------------
# 불일치 분류
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "toorder_receipts, baemin_count, expected",
    [
        (68, 68, True),    # 건수 동일·금액만 차이 → 재수집 제외
        (30, 32, True),    # 배민 건수가 더 많음 → 배민 누락 아님, 재수집 제외
        (27, 26, False),   # ToOrder 건수가 더 많음(배민 누락) → 재수집 대상
        (6, 0, False),     # 배민 데이터 없음 → 재수집 대상
        (0, 14, False),    # ToOrder 건수 정보 없음 → 판단 근거 없음, 재수집 대상
    ],
)
def test_is_amount_only_mismatch(toorder_receipts, baemin_count, expected):
    toorder_stats = {STORE: {"amount": 1523300, "receipts": toorder_receipts}}
    baemin_stats = {STORE: {"amount": 1509400, "count": baemin_count}}

    assert macro_validate._is_amount_only_mismatch(STORE, toorder_stats, baemin_stats) is expected


def test_amount_only_mismatch_is_excluded_from_recollect(monkeypatch):
    """건수가 같고 금액만 다르면 삭제·재수집하지 않는다."""
    deleted = []
    monkeypatch.setattr(macro_validate, "_toorder_baemin_by_store", lambda _d: {STORE: 1523300})
    monkeypatch.setattr(macro_validate, "_baemin_orders_by_store", lambda _d: {STORE: 1509400})
    monkeypatch.setattr(
        macro_validate,
        "_toorder_baemin_stats_by_store",
        lambda _d: {STORE: {"amount": 1523300, "receipts": 68}},
    )
    monkeypatch.setattr(
        macro_validate,
        "_baemin_orders_stats_by_store",
        lambda _d: {STORE: {"amount": 1509400, "count": 68}},
    )
    monkeypatch.setattr(macro_validate, "_baemin_orders_brand_totals_by_store", lambda _d: {})
    monkeypatch.setattr(
        macro_validate,
        "_inspect_brand_coverage",
        lambda *args, **kwargs: {STORE: {"expected_brands": [DORIDANG], "issue_type": None}},
    )
    monkeypatch.setattr(
        macro_validate,
        "_delete_orders_for_stores",
        lambda _d, stores: deleted.extend(stores),
    )
    monkeypatch.setattr(macro_validate, "_recollect_stores", lambda *a, **k: set())

    result = macro_validate.validate_toorder_orders(
        account_list=[{"account_id": "acct", "password": "pw"}],
        store_info_per_account=[
            {"account_id": "acct", "stores": [{"brand": DORIDANG, "store": STORE, "store_id": "1"}]}
        ],
        target_date=TARGET_DATE,
    )

    assert result["amount_only_mismatch_stores"] == [STORE]
    assert result["retried_stores"] == []
    assert deleted == []


# ---------------------------------------------------------------------------
# 재수집 0건 → 삭제 전 데이터 복원
# ---------------------------------------------------------------------------

def test_emptied_partition_is_restored_even_when_recollect_reports_success(monkeypatch, tmp_path):
    """재수집이 성공으로 보고돼도 삭제 전 데이터가 사라졌으면 되돌린다."""
    monkeypatch.setattr(macro_validate, "BAEMIN_ORDERS_DB", tmp_path)
    frame = _orders_frame(
        [
            {
                "주문상태": "배달완료",
                "주문번호": "A",
                "주문시각": f"{DATE_PREFIX} 12:00",
                "결제금액": "341,800",
                "총결제금액": "341,800",
            }
        ]
    )
    _write_orders(tmp_path, DORIDANG, STORE, frame)

    snapshot = macro_validate._snapshot_orders_for_stores(TARGET_DATE, [STORE])
    assert snapshot, "스냅샷이 잡혀야 한다"

    # 삭제 후 재수집이 0건으로 끝난 상황을 재현한다.
    macro_validate._delete_orders_for_stores(TARGET_DATE, [STORE])
    assert macro_validate._baemin_orders_stats_by_store(TARGET_DATE) == {}

    emptied = macro_validate._partitions_emptied_after_recollect(TARGET_DATE, snapshot)
    assert emptied == {(DORIDANG, STORE)}

    restored = macro_validate._restore_orders_snapshot(TARGET_DATE, snapshot, emptied)

    assert restored == 1
    assert macro_validate._baemin_orders_stats_by_store(TARGET_DATE)[STORE]["amount"] == 341800


def test_restore_snapshot_targets_single_brand_only(monkeypatch, tmp_path):
    """같은 매장이라도 브랜드별로 성패가 갈리므로 실패한 브랜드만 복원한다."""
    monkeypatch.setattr(macro_validate, "BAEMIN_ORDERS_DB", tmp_path)
    for brand, order_id, amount in ((DORIDANG, "A", "341,800"), (NAHOLLO, "B", "224,400")):
        _write_orders(
            tmp_path,
            brand,
            STORE,
            _orders_frame(
                [
                    {
                        "주문상태": "배달완료",
                        "주문번호": order_id,
                        "주문시각": f"{DATE_PREFIX} 12:00",
                        "결제금액": amount,
                        "총결제금액": amount,
                    }
                ]
            ),
        )

    snapshot = macro_validate._snapshot_orders_for_stores(TARGET_DATE, [STORE])
    macro_validate._delete_orders_for_stores(TARGET_DATE, [STORE])

    macro_validate._restore_orders_snapshot(TARGET_DATE, snapshot, {(DORIDANG, STORE)})

    stats = macro_validate._baemin_orders_stats_by_store(TARGET_DATE)
    assert stats[STORE]["amount"] == 341800  # 나홀로는 복원되지 않음


# ---------------------------------------------------------------------------
# suspect_zero 마커
# ---------------------------------------------------------------------------

def test_suspect_zero_marker_is_not_treated_as_normal_empty(monkeypatch, tmp_path):
    """이력이 있는데 0건으로 읽힌 마커는 정상 빈값으로 인정하지 않는다."""
    monkeypatch.setattr(orders, "BAEMIN_ORDERS_DB", tmp_path)
    monkeypatch.setattr(macro_validate, "BAEMIN_ORDERS_DB", tmp_path)
    orders._record_orders_no_data_marker(
        DORIDANG, STORE, "14793699", TARGET_DATE, reason=orders.SUSPECT_ZERO_REASON
    )

    coverage = macro_validate._inspect_brand_coverage(
        TARGET_DATE, {STORE}, {STORE: {DORIDANG}}
    )

    assert coverage[STORE]["issue_type"] == "suspect_no_data"
    assert coverage[STORE]["suspect_zero_brands"] == [DORIDANG]
    assert coverage[STORE]["active_brands"] == []


def test_total_summary_zero_marker_still_counts_as_normal_empty(monkeypatch, tmp_path):
    """이력이 없어 진짜 빈값인 케이스는 기존대로 정상 처리한다."""
    monkeypatch.setattr(orders, "BAEMIN_ORDERS_DB", tmp_path)
    monkeypatch.setattr(macro_validate, "BAEMIN_ORDERS_DB", tmp_path)
    orders._record_orders_no_data_marker(DORIDANG, STORE, "14793699", TARGET_DATE)

    coverage = macro_validate._inspect_brand_coverage(
        TARGET_DATE, {STORE}, {STORE: {DORIDANG}}
    )

    assert coverage[STORE]["issue_type"] is None
    assert coverage[STORE]["active_brands"] == [DORIDANG]


def test_zero_orders_with_recent_history_is_reported_as_failure(monkeypatch, tmp_path):
    """직전 영업일 데이터가 있으면 0건 조회를 성공으로 처리하지 않는다."""
    monkeypatch.setattr(orders, "BAEMIN_ORDERS_DB", tmp_path)
    ym = TARGET_DATE[:7]
    path = tmp_path / f"brand={DORIDANG}" / f"store={STORE}" / f"ym={ym}" / f"orders_{ym}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    _orders_frame(
        [
            {
                "주문상태": "배달완료",
                "주문번호": str(index),
                "주문시각": f"2026. 08. {20 + index}. 12:00",
                "결제금액": "10,000",
            }
            for index in range(3)
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    assert orders.has_recent_orders_history(DORIDANG, STORE, TARGET_DATE) is True
    assert orders._handle_zero_orders(DORIDANG, STORE, "14793699", TARGET_DATE) is False
    assert (
        orders.orders_no_data_marker_reason(DORIDANG, STORE, TARGET_DATE)
        == orders.SUSPECT_ZERO_REASON
    )


def test_zero_orders_without_history_records_normal_marker(monkeypatch, tmp_path):
    monkeypatch.setattr(orders, "BAEMIN_ORDERS_DB", tmp_path)

    assert orders.has_recent_orders_history(DORIDANG, STORE, TARGET_DATE) is False
    assert orders._handle_zero_orders(DORIDANG, STORE, "14793699", TARGET_DATE) is True
    assert orders.orders_no_data_marker_reason(DORIDANG, STORE, TARGET_DATE) == "total_summary_zero"
