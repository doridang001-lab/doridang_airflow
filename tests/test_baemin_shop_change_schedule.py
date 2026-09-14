from unittest.mock import MagicMock, patch

from modules.transform.pipelines.db import DB_Beamin_combined as combined


def test_shop_operation_stage_runs_without_store_hint():
    account = {
        "account_id": "acct1",
        "password": "pw",
    }
    driver = MagicMock()
    driver.current_url = "https://self.baemin.com/"
    profile = {
        "name": "test",
        "max_session_recovery_per_account": 2,
        "driver_restart_every_stores": 999,
        "account_wait_range": (0, 0),
    }

    with (
        patch.object(combined, "resolve_stability_profile", return_value=profile),
        patch.object(combined, "_build_dashboard_session", return_value=driver),
        patch.object(combined, "_ensure_dashboard_store_select", return_value=driver),
        patch.object(
            combined,
            "get_store_options",
            return_value=[{"store_id": "1", "text": "도리당 송파삼전점"}],
        ),
        patch.object(combined, "is_on_main_dashboard", return_value=True),
        patch.object(combined, "wait_for_page", return_value=True),
        patch.object(combined, "collect_now_for_driver"),
        patch.object(combined, "collect_woori_for_driver"),
        patch.object(combined, "collect_shop_operation_for_driver") as collect_shop_operation,
        patch.object(combined, "logout_baemin"),
        patch.object(combined, "quit_driver_safely"),
        patch.object(combined.random, "uniform", return_value=0),
        patch.object(combined.time, "sleep"),
    ):
        result = combined.collect_now_and_woori([account], stability_profile="test", woori_only=True)

    assert result["summary"] == (
        "계정 루프 완료 1/1 계정 "
        "(완전성공 1, 부분실패 0, 계정실패 0, orders 실패 0, ads 실패 0, stages 실패 0)"
    )
    collect_shop_operation.assert_called_once()
