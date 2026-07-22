import os
from pathlib import Path
from types import SimpleNamespace

import pendulum
import pytest

from dags.strategy import Strategy_SusamReport_01_FlowUpload_Dags as susam_dag


def test_resolve_targets_defaults_to_previous_kst_day():
    args, label, dates, default_mode = susam_dag._resolve_targets(
        {},
        data_interval_end=pendulum.datetime(2026, 7, 21, 9, 0, tz="Asia/Seoul"),
    )

    assert args == ["--date", "2026-07-20"]
    assert label == "2026-07-20"
    assert dates == ["2026-07-20"]
    assert default_mode is True


def test_resolve_targets_preserves_explicit_date_and_range():
    assert susam_dag._resolve_targets({"date": "2026-07-03"}) == (
        ["--date", "2026-07-03"],
        "2026-07-03",
        ["2026-07-03"],
        False,
    )
    assert susam_dag._resolve_targets(
        {"start": "2026-07-01", "end": "2026-07-03"}
    ) == (
        ["--start", "2026-07-01", "--end", "2026-07-03"],
        "2026-07-01~2026-07-03",
        ["2026-07-01", "2026-07-02", "2026-07-03"],
        False,
    )


def test_resolve_targets_rejects_partial_or_mixed_conf():
    with pytest.raises(Exception, match="함께"):
        susam_dag._resolve_targets({"start": "2026-07-01"})
    with pytest.raises(Exception, match="함께"):
        susam_dag._resolve_targets(
            {"date": "2026-07-01", "start": "2026-07-01", "end": "2026-07-02"}
        )


def test_daily_runs_ready_requires_terminal_daily_run_and_no_active_run():
    success = SimpleNamespace(run_id="scheduled__daily", state="success")
    failed = SimpleNamespace(run_id="schedule_guard__failed", state="failed")
    running = SimpleNamespace(run_id="schedule_guard__daily", state="running")
    today = SimpleNamespace(run_id="today__20260721__0845", state="running")

    assert susam_dag._daily_runs_ready([success, today]) is True
    assert susam_dag._daily_runs_ready([failed, today]) is True
    assert susam_dag._daily_runs_ready([success, running]) is False
    assert susam_dag._daily_runs_ready([today]) is False


def test_source_updated_today_rejects_previous_kst_day(tmp_path):
    source = tmp_path / "unified_sales_260720.parquet"
    source.touch()
    now = pendulum.datetime(2026, 7, 21, 9, 0, tz="Asia/Seoul")

    os.utime(source, (now.subtract(days=1).timestamp(), now.subtract(days=1).timestamp()))
    assert susam_dag._source_updated_today(source, now=now) is False

    os.utime(source, (now.subtract(minutes=20).timestamp(), now.subtract(minutes=20).timestamp()))
    assert susam_dag._source_updated_today(source, now=now) is True


def test_explicit_source_sensor_only_requires_target_files(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "unified_sales_260720.parquet").touch()
    monkeypatch.setattr(susam_dag, "UNIFIED_ROOT", source)
    context = {
        "dag_run": SimpleNamespace(conf={"date": "2026-07-20"}),
        "data_interval_end": pendulum.datetime(2026, 7, 21, 9, 0, tz="Asia/Seoul"),
    }

    assert susam_dag._wait_for_source_ready(**context) is True
    Path(source / "unified_sales_260720.parquet").unlink()
    assert susam_dag._wait_for_source_ready(**context) is False


def test_dag_runs_daily_at_nine_and_orders_sensors_before_upload():
    assert str(susam_dag.dag.schedule_interval) == "0 9 * * *"
    assert susam_dag.dag.timezone.name == "Asia/Seoul"
    assert susam_dag.dag.catchup is False
    assert susam_dag.dag.max_active_runs == 1
    assert susam_dag.dag.get_task("wait_for_source_ready").downstream_task_ids == {
        "wait_for_flow_chrome"
    }
    assert susam_dag.dag.get_task("wait_for_flow_chrome").downstream_task_ids == {
        "upload_daily_report"
    }
    assert susam_dag.dag.get_task("upload_daily_report").downstream_task_ids == {
        "release_flow_chrome"
    }
