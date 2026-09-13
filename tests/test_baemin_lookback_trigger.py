import os
from pathlib import Path

import pendulum
import pytest

os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))

from dags.db import DB_Beamin_Macro_Lookback_Trigger_Dags as lookback


@pytest.fixture(autouse=True)
def route_without_database(monkeypatch):
    monkeypatch.setattr(lookback, "route_trigger", lambda trigger, **kw: trigger(**kw))


def test_date_gap_marks_store_level_zero_baemin_as_missing(monkeypatch):
    monkeypatch.setattr(
        lookback,
        "_toorder_baemin_by_store",
        lambda _date: {"매장A": 10000, "매장B": 20000, "매장C": 0},
    )
    monkeypatch.setattr(
        lookback,
        "_baemin_orders_by_store",
        lambda _date: {"매장A": 10000, "매장B": 0},
    )

    gap = lookback._date_gap("2026-09-06")

    assert gap["toorder_total"] == 30000
    assert gap["baemin_total"] == 10000
    assert gap["diff"] == 20000
    assert gap["missing_store_count"] == 1
    assert gap["missing_stores"] == ["매장B"]


def test_lookback_candidates_uses_60_days_and_sorts_large_store_gaps(monkeypatch):
    calls = []

    def fake_gap(target_date):
        calls.append(target_date)
        count = {"2026-09-04": 2, "2026-09-05": 5, "2026-09-06": 5}.get(target_date, 0)
        return {
            "target_date": target_date,
            "toorder_total": 100000 if count else 0,
            "baemin_total": 0,
            "diff": 1000 * count,
            "gap_rate": 1.0 if count else 0.0,
            "missing_store_count": count,
            "missing_stores": [f"매장{i}" for i in range(count)],
        }

    monkeypatch.setattr(lookback, "_date_gap", fake_gap)
    today = pendulum.date(2026, 9, 7)

    candidates = lookback._lookback_candidates(today=today)

    assert len(calls) == 60
    assert [item["target_date"] for item in candidates] == [
        "2026-09-06",
        "2026-09-05",
        "2026-09-04",
    ]


def test_trigger_missing_baemin_orders_limits_to_two_and_passes_store_scope(monkeypatch):
    targets = [
        {
            "target_date": f"2026-09-{day:02d}",
            "toorder_total": 100000 + day,
            "baemin_total": 0,
            "diff": 100000 + day,
            "gap_rate": 1.0,
            "missing_store_count": day,
            "missing_stores": [f"매장{day}"],
        }
        for day in range(1, 8)
    ]
    triggered = []

    monkeypatch.setattr(lookback, "_lookback_candidates", lambda: targets)
    monkeypatch.setattr(lookback, "_active_or_recent_lookback_run_exists", lambda _date: False)
    monkeypatch.setattr(lookback, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = lookback.trigger_missing_baemin_orders(
        ds_nodash="20260907",
        ts_nodash="20260907T010500",
        logical_date=pendulum.datetime(2026, 9, 7, 1, 5, tz="UTC"),
    )

    assert result.startswith("배민 원천 lookback 트리거:")
    assert len(triggered) == 2
    assert triggered[0]["dag_id"] == lookback.TARGET_DAG_ID
    assert triggered[0]["run_id"] == "lookback_recovery__20260901__20260907T010500"
    assert [item["execution_date"].second for item in triggered] == [0, 1]
    assert all(item["replace_microseconds"] is False for item in triggered)
    assert triggered[0]["conf"]["orders_only"] is True
    assert triggered[0]["conf"]["run_all_batches"] is False
    assert triggered[0]["conf"]["collect_range"] is None
    assert triggered[0]["conf"]["stores"] == ["매장1"]
    assert triggered[0]["conf"]["reason"] == "baemin_orders_store_lookback_gap"


def test_trigger_missing_baemin_orders_skips_recent_runs_and_uses_next_candidates(monkeypatch):
    targets = [
        {
            "target_date": "2026-09-06",
            "toorder_total": 100000,
            "baemin_total": 0,
            "diff": 100000,
            "gap_rate": 1.0,
            "missing_store_count": 10,
            "missing_stores": ["최근실행점"],
        },
        {
            "target_date": "2026-09-05",
            "toorder_total": 90000,
            "baemin_total": 0,
            "diff": 90000,
            "gap_rate": 1.0,
            "missing_store_count": 9,
            "missing_stores": ["신규점"],
        },
    ]
    triggered = []

    monkeypatch.setattr(lookback, "_lookback_candidates", lambda: targets)
    monkeypatch.setattr(
        lookback,
        "_active_or_recent_lookback_run_exists",
        lambda target_date: target_date == "2026-09-06",
    )
    monkeypatch.setattr(lookback, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = lookback.trigger_missing_baemin_orders(ds_nodash="20260907")

    assert "2026-09-05" in result
    assert len(triggered) == 1
    assert triggered[0]["conf"]["stores"] == ["신규점"]


def test_trigger_missing_baemin_orders_does_not_block_on_other_active_target_run(monkeypatch):
    targets = [
        {
            "target_date": "2026-09-04",
            "toorder_total": 100000,
            "baemin_total": 0,
            "diff": 100000,
            "gap_rate": 1.0,
            "missing_store_count": 8,
            "missing_stores": ["4일누락점"],
        }
    ]
    triggered = []

    monkeypatch.setattr(
        lookback,
        "_has_active_target_run",
        lambda: (_ for _ in ()).throw(AssertionError("전역 active run 가드를 호출하면 안 됨")),
    )
    monkeypatch.setattr(lookback, "_lookback_candidates", lambda: targets)
    monkeypatch.setattr(lookback, "_active_or_recent_lookback_run_exists", lambda _date: False)
    monkeypatch.setattr(lookback, "trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = lookback.trigger_missing_baemin_orders(ds_nodash="20260907")

    assert "2026-09-04" in result
    assert len(triggered) == 1


def test_lookback_dag_schedule_and_limits():
    assert lookback.LOOKBACK_DAYS == 60
    assert lookback.MAX_REQUESTS_PER_RUN == 2


def test_deferred_requests_count_toward_per_run_limit(monkeypatch):
    monkeypatch.setattr(lookback, "_lookback_candidates", lambda: [
        {"target_date": f"2026-09-{day:02d}", "missing_stores": ["매장"],
         "toorder_total": 100, "baemin_total": 0, "diff": 100,
         "gap_rate": 1, "missing_store_count": 1} for day in range(1, 9)
    ])
    monkeypatch.setattr(lookback, "_active_or_recent_lookback_run_exists", lambda _: False)
    calls = []
    def route(*args, **kwargs):
        calls.append(kwargs)
        return "deferred"
    monkeypatch.setattr(lookback, "route_trigger", route)
    lookback.trigger_missing_baemin_orders(ts_nodash="test")
    assert len(calls) == 2
    assert lookback.dag.schedule_interval == "5 */2 * * *"
