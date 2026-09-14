from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = REPO_ROOT / "scripts" / "flow_susam_host_chrome_window.ps1"
REGISTER_PATH = REPO_ROOT / "scripts" / "register_flow_susam_chrome_task.ps1"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_runner_has_bounded_restart_defaults_and_completion_grace():
    source = _read(RUNNER_PATH)

    assert "[int]$MaxRestarts = 3" in source
    assert "[int]$RestartDelaySeconds = 10" in source
    assert "[int]$CompletionGraceSeconds = 30" in source
    assert "Wait-CompletionGrace -TimeoutSeconds $CompletionGraceSeconds" in source
    assert "$restartCount -ge $MaxRestarts" in source
    assert "Flow Chrome restart limit exceeded" in source


def test_runner_checks_completion_marker_before_restarting():
    source = _read(RUNNER_PATH)
    loop_start = source.index("while ((Get-Date) -lt $deadline)")
    loop_body = source[loop_start:]

    marker_check = loop_body.index("if (Test-NewCompletionMarker)")
    endpoint_check = loop_body.index("if (-not (Test-DevToolsEndpoint")
    restart = loop_body.index("Restarting Flow Chrome")

    assert marker_check < endpoint_check < restart
    assert "Airflow completion marker detected during grace period" in loop_body


def test_runner_fails_when_no_completion_marker_arrives():
    source = _read(RUNNER_PATH)

    assert "if (-not $completed)" in source
    assert "completion marker was not detected before deadline" in source
    assert "finally {" in source
    assert "Stop-FlowChromeProcesses -TargetPort $Port" in source


def test_runner_removes_only_dedicated_profile_processes_before_restart():
    source = _read(RUNNER_PATH)

    assert "function Get-FlowChromeProcesses" in source
    assert '"--user-data-dir=$UserDataDir"' in source
    assert '"--user-data-dir=`"$UserDataDir`""' in source
    restart_log = source.index('Write-FlowLog "Restarting Flow Chrome')
    cleanup = source.index(
        "Stop-FlowChromeProcesses -TargetPort $Port",
        restart_log,
    )
    restart = source.index(
        'Start-FlowChrome -TargetPort $Port -Reason "restart-$restartCount"',
        cleanup,
    )
    assert cleanup < restart


def test_runner_enables_dedicated_chrome_diagnostics():
    source = _read(RUNNER_PATH)

    assert '"--enable-logging"' in source
    assert '"--log-file=$chromeLogPath"' in source
    assert '"chrome_debug_{0:yyyyMMdd}.log"' in source


def test_scheduled_task_registration_pins_recovery_policy():
    source = _read(REGISTER_PATH)

    assert "-MaxRestarts 3" in source
    assert "-RestartDelaySeconds 10" in source
    assert "-CompletionGraceSeconds 30" in source
    assert "Open and recover dedicated Flow Chrome" in source
