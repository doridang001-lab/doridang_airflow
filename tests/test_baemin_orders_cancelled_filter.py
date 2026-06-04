"""배민 orders 주문취소 필터 적용 검증 테스트.

버그 재현 이력 (2026-05-28):
  _select_status_cancelled 에서 wait_for_page(timeout=20) 호출 시,
  취소건 0건이면 테이블 행 없음 → 20s 타임아웃 → driver.refresh() 실행
  → CANCELLED 필터 소실(배달완료 상태 복귀) → True 반환 → 배달완료 데이터를 주문취소 슬롯에 저장

수정 내용:
  - 적용 버튼: human_click(ActionChains) → JS btn.click()
  - 팝업 닫힘 확인: wait_for_page(table rows) → WebDriverWait(invisibility of CANCELLED radio)
  - timeout 시 return False (이전: return True)

검증 항목:
  1. _validate_collected 단위 테스트
  2. mock driver로 _select_status_cancelled 동작 검증 (refresh 미호출, 실패 시 False 반환)
  3. 수집된 CSV에서 오수집 감지 (배달완료 == 주문취소 건수·금액 동일 케이스)
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from selenium.common.exceptions import TimeoutException as SeleniumTimeoutException

from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    _select_status_cancelled,
    _validate_collected,
)

ORDERS_DB = Path("analytics/baemin_macro/orders")


# ---------------------------------------------------------------------------
# _validate_collected 단위 테스트
# ---------------------------------------------------------------------------

class TestValidateCollected:
    def test_empty_rows_no_summary(self):
        result = _validate_collected([], None)
        assert result["matched"] is None
        assert result["actual_count"] == 0
        assert result["actual_amount"] == 0

    def test_zero_rows_zero_summary(self):
        """취소건 0건 + 기댓값 0건 → matched True (정상 케이스)."""
        result = _validate_collected([], {"count": 0, "amount": 0})
        assert result["matched"] is True

    def test_match_success(self):
        rows = [
            {"주문번호": "A001", "결제금액": "10,000"},
            {"주문번호": "A001", "결제금액": ""},   # expand 중복행
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

    def test_match_fail_amount_mismatch(self):
        rows = [{"주문번호": "A001", "결제금액": "10,000"}]
        result = _validate_collected(rows, {"count": 1, "amount": 9000})
        assert result["matched"] is False

    def test_duplicate_order_ids_counted_once(self):
        """같은 주문번호 여러 행 → 고유 주문번호 1건으로 집계."""
        rows = [
            {"주문번호": "X999", "결제금액": "20,000"},
            {"주문번호": "X999", "결제금액": ""},
            {"주문번호": "X999", "결제금액": ""},
        ]
        result = _validate_collected(rows, {"count": 1, "amount": 20000})
        assert result["matched"] is True
        assert result["actual_count"] == 1

    def test_no_order_id_rows_excluded(self):
        """주문번호 없는 행(옵션 펼침 상세행 등)은 건수에서 제외."""
        rows = [
            {"주문번호": "B001", "결제금액": "3,000"},
            {"주문번호": "",     "결제금액": ""},
            {"주문번호": None,   "결제금액": ""},
        ]
        result = _validate_collected(rows, {"count": 1, "amount": 3000})
        assert result["matched"] is True
        assert result["actual_count"] == 1


# ---------------------------------------------------------------------------
# _select_status_cancelled mock 동작 검증
# ---------------------------------------------------------------------------

class TestSelectStatusCancelled:
    """Selenium driver를 mock으로 대체해 필터 함수 내부 동작을 검증한다."""

    def _make_driver(self, execute_script_side_effect):
        driver = MagicMock()
        driver.execute_script.side_effect = execute_script_side_effect
        return driver

    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.time.sleep")
    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.WebDriverWait")
    def test_success_no_refresh_called(self, mock_wdwait, mock_sleep):
        """정상 흐름: 팝업 닫힘 확인 후 True 반환, refresh 미호출."""
        driver = self._make_driver([
            "가게/상태/결제방법",  # 1. 통합 필터 버튼 클릭 반환값
            "label_clicked",       # 2. CANCELLED 라디오 클릭
            True,                  # 3. 적용 버튼 JS 클릭
        ])
        # WebDriverWait().until() 두 번 모두 성공
        mock_wdwait.return_value.until.return_value = MagicMock()

        result = _select_status_cancelled(driver)

        assert result is True
        driver.refresh.assert_not_called()

    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.time.sleep")
    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.WebDriverWait")
    def test_popup_timeout_returns_false_no_refresh(self, mock_wdwait, mock_sleep):
        """팝업 닫힘 타임아웃 → False 반환 (이전: refresh → True로 오반환).

        핵심 회귀 테스트: 이 케이스가 True를 반환하면 수정 전 버그가 재발한 것.
        """
        driver = self._make_driver([
            "가게/상태/결제방법",
            "label_clicked",
            True,
        ])
        mock_instance = MagicMock()
        mock_wdwait.return_value = mock_instance
        # 1st until: CANCELLED radio presence_of → 성공
        # 2nd until: invisibility_of popup → TimeoutException (팝업 안 닫힘)
        mock_instance.until.side_effect = [
            MagicMock(),
            SeleniumTimeoutException("popup not closed"),
        ]

        result = _select_status_cancelled(driver)

        assert result is False, (
            "팝업 닫힘 실패 시 반드시 False를 반환해야 한다. "
            "True 반환은 수정 전 버그(refresh 후 배달완료 데이터 오수집)와 동일한 상태."
        )
        driver.refresh.assert_not_called()

    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.time.sleep")
    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.WebDriverWait")
    def test_filter_button_not_found_returns_false(self, mock_wdwait, mock_sleep):
        """통합 필터 버튼 미발견 → False 반환."""
        driver = self._make_driver([False])  # 버튼 없음

        result = _select_status_cancelled(driver)

        assert result is False
        driver.refresh.assert_not_called()

    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.time.sleep")
    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.WebDriverWait")
    def test_apply_button_not_found_returns_false(self, mock_wdwait, mock_sleep):
        """적용 버튼 JS 클릭 실패(False) → False 반환."""
        driver = self._make_driver([
            "가게/상태/결제방법",
            "label_clicked",
            False,   # 적용 버튼 미발견
        ])
        mock_wdwait.return_value.until.return_value = MagicMock()

        result = _select_status_cancelled(driver)

        assert result is False
        driver.refresh.assert_not_called()

    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.time.sleep")
    @patch("modules.transform.pipelines.db.DB_Beamin_04_orders.WebDriverWait")
    def test_zero_cancellations_still_returns_true(self, mock_wdwait, mock_sleep):
        """취소건 0건(팝업 정상 닫힘) → True 반환. 0건은 유효 결과."""
        driver = self._make_driver([
            "가게/상태/결제방법",
            "label_clicked",
            True,
        ])
        # 팝업이 정상 닫힘 (invisibility 성공) — 테이블 행은 0개일 수 있음
        mock_wdwait.return_value.until.return_value = MagicMock()

        result = _select_status_cancelled(driver)

        assert result is True
        driver.refresh.assert_not_called()


# ---------------------------------------------------------------------------
# 수집된 CSV 오수집 감지 (통합 검증)
# ---------------------------------------------------------------------------

def _find_all_order_csvs():
    if not ORDERS_DB.exists():
        return []
    return sorted(ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.csv"))


@pytest.mark.skipif(not ORDERS_DB.exists(), reason="orders CSV 없음 (ORDERS_DB 미존재)")
class TestOrdersCsvIntegrity:
    """수집된 orders CSV에서 주문취소 오수집(배달완료 데이터 중복 저장) 감지."""

    def test_no_order_id_in_both_normal_and_cancelled(self):
        """같은 주문번호가 배달완료·주문취소에 동시에 존재하면 필터 오수집."""
        violations = []
        for csv_path in _find_all_order_csvs():
            df = pd.read_csv(csv_path, dtype=str)
            if "주문상태" not in df.columns or "주문번호" not in df.columns:
                continue
            normal_ids = set(df[df["주문상태"] == "배달완료"]["주문번호"].dropna())
            cancel_ids = set(df[df["주문상태"] == "주문취소"]["주문번호"].dropna())
            overlap = normal_ids & cancel_ids
            if overlap:
                violations.append(
                    f"{csv_path.relative_to(ORDERS_DB)}: {len(overlap)}건 중복"
                )

        assert not violations, (
            "배달완료·주문취소 주문번호 중복 (필터 오수집 의심):\n"
            + "\n".join(f"  {v}" for v in violations)
        )

    def test_no_identical_normal_vs_cancelled_count_and_amount(self):
        """배달완료 건수·금액 == 주문취소 건수·금액이면 CANCELLED 필터 미적용 의심.

        단, 두 값이 모두 0인 경우는 제외 (정상적으로 둘 다 없는 경우).
        """
        def _unique_amount(sub_df: pd.DataFrame) -> int:
            total = 0
            seen = set()
            for _, row in sub_df.iterrows():
                oid = row.get("주문번호", "")
                if oid in seen:
                    continue
                seen.add(oid)
                raw = row.get("결제금액", "")
                if raw and str(raw).strip():
                    try:
                        total += int(str(raw).replace(",", "").strip())
                    except (ValueError, TypeError):
                        pass
            return total

        suspicious = []
        for csv_path in _find_all_order_csvs():
            df = pd.read_csv(csv_path, dtype=str)
            if "주문상태" not in df.columns:
                continue
            normal = df[df["주문상태"] == "배달완료"]
            cancelled = df[df["주문상태"] == "주문취소"]
            if normal.empty or cancelled.empty:
                continue

            n_cnt = len(normal["주문번호"].dropna().unique())
            c_cnt = len(cancelled["주문번호"].dropna().unique())
            n_amt = _unique_amount(normal)
            c_amt = _unique_amount(cancelled)

            if n_cnt == c_cnt and n_amt == c_amt and n_cnt > 0:
                suspicious.append(
                    f"{csv_path.relative_to(ORDERS_DB)}: "
                    f"배달완료={n_cnt}건/{n_amt}원 == 주문취소={c_cnt}건/{c_amt}원"
                )

        assert not suspicious, (
            "배달완료 == 주문취소 (건수·금액 동일) — CANCELLED 필터 미적용 의심:\n"
            + "\n".join(f"  {s}" for s in suspicious)
        )
