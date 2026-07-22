from unittest.mock import MagicMock, patch

import pendulum

from modules.transform.pipelines.db import DB_Beamin_combined as combined


KST = pendulum.timezone("Asia/Seoul")


def test_shop_change_collection_runs_on_saturday():
    saturday = pendulum.datetime(2026, 7, 25, 3, 15, tz=KST)

    assert combined._should_collect_shop_change(saturday)


def test_shop_change_collection_skips_other_weekdays():
    friday = pendulum.datetime(2026, 7, 24, 3, 15, tz=KST)
    sunday = pendulum.datetime(2026, 7, 26, 3, 15, tz=KST)

    assert not combined._should_collect_shop_change(friday)
    assert not combined._should_collect_shop_change(sunday)


def _run_collection_with_shop_change_day(enabled: bool):
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
        patch.object(combined, "_should_collect_shop_change", return_value=enabled),
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
        patch.object(combined, "collect_shop_change_for_driver") as collect_shop_change,
        patch.object(combined, "logout_baemin"),
        patch.object(combined, "quit_driver_safely"),
        patch.object(combined.random, "uniform", return_value=0),
        patch.object(combined.time, "sleep"),
    ):
        result = combined.collect_now_and_woori([account], stability_profile="test", woori_only=True)

    assert result["summary"] == "성공 1/1 계정"
    return collect_shop_change


def test_shop_change_stage_runs_when_saturday_gate_is_enabled():
    collect_shop_change = _run_collection_with_shop_change_day(True)

    collect_shop_change.assert_called_once()


def test_shop_change_stage_skips_when_saturday_gate_is_disabled():
    collect_shop_change = _run_collection_with_shop_change_day(False)

    collect_shop_change.assert_not_called()
