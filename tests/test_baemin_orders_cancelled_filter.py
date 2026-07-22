"""배민 orders가 주문취소를 신규 수집하지 않는지 검증한다."""

from pathlib import Path

from modules.transform.pipelines.db.DB_Beamin_04_orders import _validate_collected

SOURCE = Path("modules/transform/pipelines/db/DB_Beamin_04_orders.py")


class TestValidateCollected:
    def test_empty_rows_no_summary(self):
        result = _validate_collected([], None)
        assert result["matched"] is None
        assert result["actual_count"] == 0
        assert result["actual_amount"] == 0

    def test_zero_rows_zero_summary(self):
        result = _validate_collected([], {"count": 0, "amount": 0})
        assert result["matched"] is True

    def test_match_success(self):
        rows = [
            {"주문번호": "A001", "결제금액": "10,000"},
            {"주문번호": "A001", "결제금액": ""},
            {"주문번호": "A002", "결제금액": "5,000"},
        ]
        result = _validate_collected(rows, {"count": 2, "amount": 15000})
        assert result["matched"] is True
        assert result["actual_count"] == 2
        assert result["actual_amount"] == 15000

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
