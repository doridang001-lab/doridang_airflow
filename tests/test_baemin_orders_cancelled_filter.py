"""배민 orders가 주문취소를 신규 수집하지 않는지 검증한다."""

from pathlib import Path

from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    _block_low_settle_rate,
    _validate_collected,
)

SOURCE = Path("modules/transform/pipelines/db/DB_Beamin_04_orders.py")


class TestValidateCollected:
    def test_empty_rows_no_summary(self):
        result = _validate_collected([], None)
        assert result["matched"] is None
        assert result["actual_count"] == 0
        assert result["actual_amount"] == 0
        assert result["settle_rate"] is None

    def test_zero_rows_zero_summary(self):
        result = _validate_collected([], {"count": 0, "amount": 0})
        assert result["matched"] is True
        assert result["settle_rate"] is None

    def test_match_success(self):
        rows = [
            {"주문번호": "A001", "결제금액": "10,000", "입금예정금액": "8,000"},
            {"주문번호": "A001", "결제금액": ""},
            {"주문번호": "A002", "결제금액": "5,000", "입금예정금액": "4,000"},
        ]
        result = _validate_collected(rows, {"count": 2, "amount": 15000})
        assert result["matched"] is True
        assert result["actual_count"] == 2
        assert result["actual_amount"] == 15000
        assert result["settle_rate"] == 1.0

    def test_match_success_tracks_low_settle_rate(self):
        rows = [
            {"주문번호": "A001", "결제금액": "10,000", "입금예정금액": "8,000"},
            {"주문번호": "A002", "결제금액": "5,000", "입금예정금액": ""},
        ]
        result = _validate_collected(rows, {"count": 2, "amount": 15000})
        assert result["matched"] is True
        assert result["settle_count"] == 1
        assert result["settle_denominator"] == 2
        assert result["settle_rate"] == 0.5

    def test_low_settle_rate_marks_suspect_but_keeps_save(self):
        # 배민 정산정보(입금예정금액)는 09:00 KST 전후에 게시되므로, 그 전 수집은
        # 합계가 맞아도 정산 수집률이 낮은 게 정상이다. settlement_suspect로
        # 표시만 하고 matched=True를 유지해 주문 저장을 막지 않는다
        # (정산 컬럼은 이후 DB_DeliveryCommission 정산 미게시 감지 재수집이 채운다).
        rows = [
            {"주문번호": "A001", "결제금액": "10,000", "입금예정금액": "8,000"},
            {"주문번호": "A002", "결제금액": "5,000", "입금예정금액": ""},
        ]
        result = _validate_collected(rows, {"count": 2, "amount": 15000})

        blocked = _block_low_settle_rate(result)

        assert blocked["matched"] is True
        assert blocked["settlement_suspect"] is True
        assert blocked["reason"] == "low_settle_rate"

    def test_match_fail_count_mismatch(self):
        rows = [{"주문번호": "A001", "결제금액": "10,000"}]
        result = _validate_collected(rows, {"count": 2, "amount": 10000})
        assert result["matched"] is False

    def test_duplicate_order_ids_counted_once(self):
        rows = [
            {"주문번호": "X999", "결제금액": "20,000"},
            {"주문번호": "X999", "결제금액": ""},
            {"주문번호": "X999", "결제금액": ""},
        ]
        result = _validate_collected(rows, {"count": 1, "amount": 20000})
        assert result["matched"] is True
        assert result["actual_count"] == 1


def test_cancelled_collection_code_removed():
    source = SOURCE.read_text(encoding="utf-8")
    forbidden = [
        "_select_status_cancelled",
        "_setup_cancel_filter",
        "주문취소 수집 시작",
        "저장 완료(취소)",
        "CANCELLED",
    ]
    for token in forbidden:
        assert token not in source


def test_delivered_collection_still_present():
    source = SOURCE.read_text(encoding="utf-8")
    assert '"배달완료"' in source
    assert "저장 완료(정상)" in source
    assert "_collect_with_retry_on_mismatch" in source
