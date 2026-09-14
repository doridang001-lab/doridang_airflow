import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import airflow_scheduler_watchdog as scheduler
import docker_airflow_watchdog as host


def test_docker_timeout_is_reported_not_raised():
    with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 15)):
        result = scheduler.run_cmd(["docker", "info"], timeout=15)
    assert result.returncode == 124


def test_hidden_runner_does_not_inherit_invalid_stdin():
    with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as call:
        scheduler.run_cmd(["docker", "info"])
    assert call.call_args.kwargs["stdin"] == subprocess.DEVNULL


def test_ping_output_does_not_hide_cli_timeout():
    output = b"pong\n1 node online.\n"
    with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 45, output=output)):
        assert not host.worker_responds()


def test_worker_requires_successful_complete_ping():
    for code, output, expected in (
        (0, "pong\n1 node online.\n", True),
        (1, "pong\n1 node online.\n", False),
        (0, "pong\n", False),
    ):
        with patch.object(host, "run_cmd", return_value=subprocess.CompletedProcess([], code, output, "")):
            assert host.worker_responds() is expected


def test_worker_no_response_is_unhealthy():
    with patch.object(host, "run_cmd", return_value=subprocess.CompletedProcess([], 124, "", "timeout")):
        assert not host.worker_responds()


def test_restart_requires_three_failures_and_cooldown():
    state = {}
    assert host.decision(state, False, 50, 10_000) == "observe"
    assert host.decision(state, False, 50, 10_300) == "observe"
    assert host.decision(state, False, 50, 10_600) == "recover"
    state["last_restart"] = 10_600
    assert host.decision(state, False, 50, 10_900) == "cooldown"
    assert host.decision(state, False, 50, 12_400) == "recover"
    assert host.decision(state, True, 50, 12_500) == "healthy"
    assert state["failures"] == 0


def test_disk_pressure_blocks_restart():
    assert host.decision({"failures": 10}, False, 4, 10_000) == "disk_blocked"
    assert host.decision({}, True, 25, 10_000) == "disk_warning"


def test_invalid_heartbeat_is_not_healthy():
    for value in ("invalid", "2026-09-08T00:00:00", 123):
        assert scheduler.heartbeat_age_seconds({"scheduler": {"latest_scheduler_heartbeat": value}}) is None


def test_compose_json_lines_and_array():
    row = '{"Service":"postgres","State":"running","Health":"healthy"}'
    for output in (row, "[" + row + "]"):
        with patch.object(host, "run_cmd", return_value=subprocess.CompletedProcess([], 0, output)):
            assert host.container_states()["postgres"]["Health"] == "healthy"


def test_start_stopped_service_does_not_run_init_dependencies():
    with patch.object(host, "run_cmd", return_value=subprocess.CompletedProcess([], 0, "")) as call:
        assert host.recover_services(True, {"airflow-webserver": {"State": "exited"}},
                                     ["airflow-webserver"], False, {}) == 0
    assert call.call_args.args[0] == [
        "docker", "compose", "up", "--no-deps", "--no-build", "-d", "airflow-webserver"]
