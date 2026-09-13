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
    monkeypatch.setattr(overdue_dag, "find_zeroed_pools", lambda *args, **kwargs: [])
    monkeypatch.setattr(overdue_dag, "find_stale_scheduler", lambda *args, **kwargs: None)
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


def _blocked_item(*, start, run_id="scheduled__2026-07-29T00:00:00+00:00"):
    return OverdueSchedule(
        dag_id="blocked_dag",
        next_dagrun="2026-07-30T00:00:00+00:00",
        next_dagrun_create_after=None,
        lag_minutes=60,
        blocked=True,
        blocking_run_id=run_id,
        blocking_run_start=start,
    )


def test_is_stale_blocking_run_true_when_run_older_than_threshold():
    item = _blocked_item(start=pendulum.datetime(2026, 7, 29, 12, 0, tz="UTC"))

    assert guard.is_stale_blocking_run(
        item,
        stale_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    ) is True


def test_is_stale_blocking_run_false_for_recent_run():
    item = _blocked_item(start=pendulum.datetime(2026, 7, 29, 18, 0, tz="UTC"))

    assert guard.is_stale_blocking_run(
        item,
        stale_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    ) is False


def test_is_stale_blocking_run_false_without_start_date():
    """start_date를 모르면 실행 중인지 판단할 수 없으므로 죽이지 않는다."""
    item = _blocked_item(start=None)

    assert guard.is_stale_blocking_run(item, stale_hours=6) is False


def test_is_stale_blocking_run_false_when_not_blocked():
    item = OverdueSchedule(
        dag_id="demo_dag",
        next_dagrun="2026-07-30T00:00:00+00:00",
        next_dagrun_create_after="2026-07-29T23:00:00+00:00",
        lag_minutes=60,
    )

    assert guard.is_stale_blocking_run(item, stale_hours=6) is False


class _RecordingQuery:
    def __init__(self, sink, label, first_result=None):
        self._sink = sink
        self._label = label
        self._first_result = first_result

    def filter(self, *_args):
        return self

    def first(self):
        return self._first_result

    def update(self, values, **_kwargs):
        self._sink.append((self._label, values))
        return 1


class _RecordingSession:
    """idle 판정의 first()는 기본 None(=멈춘 런)으로 돌려준다."""

    def __init__(self, first_result=None):
        self.updates = []
        self._first_result = first_result

    def query(self, model):
        return _RecordingQuery(self.updates, model.__name__, self._first_result)


class DagRunStub:
    dag_id = None
    run_id = None
    state = None
    end_date = None


class TaskInstanceStub:
    class _State:
        @staticmethod
        def in_(_values):
            return True

    class _EndDate:
        @staticmethod
        def isnot(_value):
            return True

        def __gt__(self, _other):
            return True

    dag_id = None
    run_id = None
    state = _State()
    end_date = _EndDate()


def test_expire_stale_blocking_runs_fails_run_and_clears_task_instances():
    session = _RecordingSession()
    stale = _blocked_item(start=pendulum.datetime(2026, 7, 29, 0, 0, tz="UTC"))
    fresh = OverdueSchedule(
        dag_id="fresh_dag",
        next_dagrun="2026-07-30T00:00:00+00:00",
        next_dagrun_create_after=None,
        lag_minutes=20,
        blocked=True,
        blocking_run_id="scheduled__2026-07-29T19:30:00+00:00",
        blocking_run_start=pendulum.datetime(2026, 7, 29, 19, 30, tz="UTC"),
    )

    unblocked = guard.expire_stale_blocking_runs(
        session,
        DagRunStub,
        TaskInstanceStub,
        [stale, fresh],
        stale_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    )

    assert unblocked == ["blocked_dag"]
    labels = [label for label, _ in session.updates]
    assert labels == ["TaskInstanceStub", "DagRunStub"]
    assert session.updates[0][1] == {"state": None}
    assert session.updates[1][1]["state"] == "failed"


def test_trigger_recovery_runs_triggers_unblocked_dag(monkeypatch):
    """좀비 런을 마감해 봉쇄가 풀린 DAG는 skip하지 않고 보정 트리거한다."""
    overdue = [_blocked_item(start=pendulum.datetime(2026, 7, 29, 0, 0, tz="UTC"))]
    triggered = []

    monkeypatch.setattr(overdue_dag, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = overdue_dag.trigger_recovery_runs(
        overdue,
        parent_run_id="scheduled__parent",
        unblocked_dag_ids={"blocked_dag"},
    )

    assert result[1] == []
    assert len(triggered) == 1
    assert triggered[0]["dag_id"] == "blocked_dag"


def test_format_overdue_schedule_alert_includes_blocking_run_id():
    body = guard.format_overdue_schedule_alert(
        [_blocked_item(start=pendulum.datetime(2026, 7, 29, 0, 0, tz="UTC"))],
        grace_minutes=10,
    )

    assert "blocked_by=scheduled__2026-07-29T00:00:00+00:00" in body


def test_expire_stale_blocking_runs_keeps_run_with_live_task_instance():
    """오래됐어도 아직 움직이는 런은 죽이지 않는다 (긴 수집 보호)."""
    session = _RecordingSession(first_result=object())
    stale = _blocked_item(start=pendulum.datetime(2026, 7, 29, 0, 0, tz="UTC"))

    unblocked = guard.expire_stale_blocking_runs(
        session,
        DagRunStub,
        TaskInstanceStub,
        [stale],
        stale_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    )

    assert unblocked == []
    assert session.updates == []


def test_blocking_run_is_idle_false_when_active_task_instance_exists():
    session = _RecordingSession(first_result=object())

    assert guard.blocking_run_is_idle(
        session,
        TaskInstanceStub,
        dag_id="blocked_dag",
        run_id="scheduled__x",
        idle_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    ) is False


def test_blocking_run_is_idle_true_when_nothing_moving():
    session = _RecordingSession(first_result=None)

    assert guard.blocking_run_is_idle(
        session,
        TaskInstanceStub,
        dag_id="blocked_dag",
        run_id="scheduled__x",
        idle_hours=6,
        now=pendulum.datetime(2026, 7, 29, 20, 0, tz="UTC"),
    ) is True


def test_find_zeroed_pools_flags_zero_slot_pools():
    """풀 슬롯 0은 신규 태스크 큐잉 자체를 막는다(2026-09-11 사고)."""
    class FakePool:
        def __init__(self, pool, slots, description=None):
            self.pool = pool
            self.slots = slots
            self.description = description

    class FakeQuery:
        def __init__(self, items):
            self._items = items

        def all(self):
            return self._items

    class FakeSession:
        def __init__(self, items):
            self._items = items

        def query(self, _model):
            return FakeQuery(self._items)

    session = FakeSession([
        FakePool("default_pool", 0, "Default pool"),
        FakePool("selenium_pool", 8, "Selenium pool"),
        FakePool("baemin_selenium_pool", 0, "Baemin pool"),
    ])

    zeroed = guard.find_zeroed_pools(session, object)

    assert [z["pool"] for z in zeroed] == ["default_pool", "baemin_selenium_pool"]


def test_find_zeroed_pools_empty_when_all_have_capacity():
    class FakePool:
        def __init__(self, pool, slots):
            self.pool = pool
            self.slots = slots
            self.description = None

    class FakeQuery:
        def __init__(self, items):
            self._items = items

        def all(self):
            return self._items

    class FakeSession:
        def __init__(self, items):
            self._items = items

        def query(self, _model):
            return FakeQuery(self._items)

    session = FakeSession([FakePool("default_pool", 128), FakePool("selenium_pool", 8)])

    assert guard.find_zeroed_pools(session, object) == []


class _JobModelStub:
    class _Col:
        def __eq__(self, _other):
            return self

        def desc(self):
            return self

    job_type = _Col()
    state = _Col()
    latest_heartbeat = _Col()


class _JobRow:
    def __init__(self, job_type, state, latest_heartbeat, hostname="host-a"):
        self.job_type = job_type
        self.state = state
        self.latest_heartbeat = latest_heartbeat
        self.hostname = hostname


class _JobQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def first(self):
        return self._result


class _JobSession:
    def __init__(self, result):
        self._result = result

    def query(self, _model):
        return _JobQuery(self._result)


def test_find_stale_scheduler_flags_old_heartbeat():
    """2026-09-11: 스케줄러가 7분간 하트비트 없이 멈춘 사고의 재발 감지."""
    row = _JobRow("SchedulerJob", "running", pendulum.datetime(2026, 9, 11, 5, 17, 10, tz="UTC"))
    session = _JobSession(row)

    result = guard.find_stale_scheduler(
        session, _JobModelStub, stale_minutes=5, now=pendulum.datetime(2026, 9, 11, 5, 24, 40, tz="UTC")
    )

    assert result is not None
    assert result["age_minutes"] == 7.5
    assert result["hostname"] == "host-a"


def test_find_stale_scheduler_none_when_fresh():
    row = _JobRow("SchedulerJob", "running", pendulum.datetime(2026, 9, 11, 5, 24, 38, tz="UTC"))
    session = _JobSession(row)

    result = guard.find_stale_scheduler(
        session, _JobModelStub, stale_minutes=5, now=pendulum.datetime(2026, 9, 11, 5, 24, 40, tz="UTC")
    )

    assert result is None


def test_find_stale_scheduler_none_when_no_job_row():
    session = _JobSession(None)

    assert guard.find_stale_scheduler(session, _JobModelStub, stale_minutes=5) is None
