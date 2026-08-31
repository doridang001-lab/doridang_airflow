import pendulum

from dags.strategy import Strategy_ScheduleGuard_01_Overdue_Dags as overdue_dag
from modules.transform.utility import dag_schedule_guard as guard
from modules.transform.utility.dag_schedule_guard import OverdueSchedule


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


def test_build_overdue_recovery_run_id_includes_logical_date_and_dag_id():
    run_id = guard.build_overdue_recovery_run_id(
        dag_id="demo.dag",
        logical_date=pendulum.datetime(2026, 7, 30, 0, 0, tz="UTC"),
    )

    assert run_id == "schedule_guard__20260730T000000__demo.dag"


def test_overdue_schedule_blocked_defaults_false():
    item = OverdueSchedule(
        dag_id="demo_dag",
        next_dagrun="2026-07-30T00:00:00+00:00",
        next_dagrun_create_after=None,
        lag_minutes=5,
    )

    assert item.blocked is False


def test_collect_overdue_schedules_marks_null_create_after_as_blocked():
    class DummyColumn:
        def __eq__(self, _other):
            return self

        def __le__(self, _other):
            return self

        def isnot(self, _other):
            return self

        def in_(self, _other):
            return self

        def __invert__(self):
            return self

    class DummyDagModel:
        dag_id = DummyColumn()
        is_active = DummyColumn()
        is_paused = DummyColumn()
        next_dagrun = DummyColumn()
        next_dagrun_create_after = DummyColumn()

    class DummyModel:
        dag_id = "blocked_dag"
        next_dagrun = pendulum.datetime(2026, 7, 30, 0, 0, tz="UTC")
        next_dagrun_create_after = None

    class DummyQuery:
        def filter(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def all(self):
            return [DummyModel()]

    class DummySession:
        def query(self, _model):
            return DummyQuery()

    overdue = guard.collect_overdue_schedules(
        DummySession(),
        DummyDagModel,
        now=pendulum.datetime(2026, 7, 30, 1, 0, tz="UTC"),
        grace_minutes=10,
    )

    assert len(overdue) == 1
    assert overdue[0].dag_id == "blocked_dag"
    assert overdue[0].blocked is True
    assert overdue[0].lag_minutes == 60


def test_format_overdue_schedule_alert_marks_blocked_status():
    body = guard.format_overdue_schedule_alert(
        [
            OverdueSchedule(
                dag_id="blocked_dag",
                next_dagrun="2026-07-30T00:00:00+00:00",
                next_dagrun_create_after=None,
                lag_minutes=60,
                blocked=True,
            )
        ],
        grace_minutes=10,
    )

    assert "max_active_runs 봉쇄 의심" in body
    assert "blocked_dag" in body


def test_check_overdue_schedules_triggers_missing_overdue_without_telegram(monkeypatch):
    class DummySession:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    overdue = [OverdueSchedule(dag_id="demo_dag", next_dagrun="2026-07-30T00:00:00+00:00", next_dagrun_create_after="2026-07-29T23:00:00+00:00", lag_minutes=60)]
    triggered = []

    monkeypatch.setattr(overdue_dag, "create_session", lambda: DummySession())
    monkeypatch.setattr(overdue_dag, "collect_overdue_schedules", lambda *args, **kwargs: overdue)
    monkeypatch.setattr(overdue_dag, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = overdue_dag.check_overdue_schedules(run_id="scheduled__parent")

    assert "스케줄 생성 지연 보정" in result
    assert "demo_dag" in result
    assert triggered == [
        {
            "dag_id": "demo_dag",
            "run_id": "schedule_guard__20260730T000000__demo_dag",
            "conf": {
                "recovered_by": "Strategy_ScheduleGuard_01_Overdue_Dags",
                "parent_run_id": "scheduled__parent",
                "scheduled_logical_date": "2026-07-30T00:00:00Z",
            },
            "execution_date": pendulum.datetime(2026, 7, 30, 0, 0, tz="UTC"),
        }
    ]


def test_trigger_recovery_runs_skips_blocked_items(monkeypatch):
    overdue = [
        OverdueSchedule(
            dag_id="blocked_dag",
            next_dagrun="2026-07-30T00:00:00+00:00",
            next_dagrun_create_after=None,
            lag_minutes=60,
            blocked=True,
        )
    ]
    triggered = []

    monkeypatch.setattr(overdue_dag, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = overdue_dag.trigger_recovery_runs(overdue, parent_run_id="scheduled__parent")

    assert result == ([], ["blocked_dag:blocked"])
    assert triggered == []
