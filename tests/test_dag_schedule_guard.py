import pendulum

from modules.transform.utility import dag_schedule_guard as guard


def test_has_daily_run_accepts_scheduled_running_run():
    runs = [{"run_id": "scheduled__2026-07-08T23:37:00+00:00", "state": "running"}]

    assert guard.has_daily_unified_sales_run(runs, recovery_prefix="schedule_guard__") is True


def test_has_daily_run_ignores_today_mode_trigger_runs():
    runs = [{"run_id": "today__20260708__1250__scheduled", "state": "success"}]

    assert guard.has_daily_unified_sales_run(runs, recovery_prefix="schedule_guard__") is False


def test_has_daily_run_accepts_existing_guard_recovery_run():
    runs = [{"run_id": "schedule_guard__20260708__scheduled__2026", "state": "success"}]

    assert guard.has_daily_unified_sales_run(runs, recovery_prefix="schedule_guard__") is True


def test_has_daily_run_rejects_failed_run():
    runs = [{"run_id": "manual__2026-07-08T00:49:07", "state": "failed"}]

    assert guard.has_daily_unified_sales_run(runs, recovery_prefix="schedule_guard__") is False


def test_kst_day_bounds_returns_yyyymmdd_target():
    now = pendulum.datetime(2026, 7, 8, 9, 5, tz="Asia/Seoul")

    start_utc, end_utc, target_date = guard.kst_day_bounds(now)

    assert target_date == "20260708"
    assert start_utc.to_iso8601_string() == "2026-07-07T15:00:00Z"
    assert end_utc.to_iso8601_string() == "2026-07-08T15:00:00Z"


def test_build_guard_recovery_run_id_is_deterministic_and_safe():
    run_id = guard.build_guard_recovery_run_id(
        target_date="20260708",
        parent_run_id="scheduled__2026-07-08T00:05:00+00:00",
    )

    assert run_id == "schedule_guard__20260708__scheduled__2026-07-08T00_05_00_00_00"
