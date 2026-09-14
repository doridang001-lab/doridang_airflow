import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import watch_heal_queue as watcher


@pytest.mark.parametrize("platform", ["nt", "posix"])
def test_window_options_are_platform_specific(monkeypatch, platform):
    monkeypatch.setattr(watcher, "os", SimpleNamespace(name=platform))
    fake = SimpleNamespace(
        STARTUPINFO=lambda: SimpleNamespace(dwFlags=0, wShowWindow=1),
        STARTF_USESHOWWINDOW=1,
        SW_HIDE=0,
        CREATE_NO_WINDOW=0x08000000,
    )
    monkeypatch.setattr(watcher, "subprocess", fake)
    options = watcher._subprocess_window_kwargs()
    if platform == "posix":
        assert options == {}
    else:
        assert options["creationflags"] == fake.CREATE_NO_WINDOW
        assert options["startupinfo"].dwFlags & fake.STARTF_USESHOWWINDOW
        assert options["startupinfo"].wShowWindow == fake.SW_HIDE


@pytest.mark.parametrize("platform", ["nt", "posix"])
@pytest.mark.parametrize("returncode", [0, 7])
def test_codex_preserves_prompt_exit_and_process_group(monkeypatch, platform, returncode):
    stream = Mock()
    proc = SimpleNamespace(stdin=stream, poll=lambda: returncode, args=["codex", "exec", "-"])
    popen = Mock(return_value=proc)
    monkeypatch.setattr(watcher, "os", SimpleNamespace(name=platform, environ={}))
    monkeypatch.setattr(watcher, "_prepare_codex_workdir", lambda: "head")
    monkeypatch.setattr(watcher, "_codex_args", lambda prompt: proc.args)
    monkeypatch.setattr(watcher, "_subprocess_window_kwargs", lambda: {"creationflags": 8} if platform == "nt" else {})
    monkeypatch.setattr(watcher.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False)
    monkeypatch.setattr(watcher.subprocess, "Popen", popen)
    result = watcher._run_codex("prompt\n")
    assert result.returncode == returncode
    stream.write.assert_called_once_with("prompt\n")
    stream.close.assert_called_once()
    options = popen.call_args.kwargs
    assert options["stdin"] == subprocess.PIPE
    assert "stdout" not in options and "stderr" not in options
    if platform == "nt":
        assert options["creationflags"] == 8 | 512
    else:
        assert options["start_new_session"] is True
        assert "creationflags" not in options


def test_codex_timeout_still_terminates(monkeypatch):
    proc = SimpleNamespace(stdin=Mock(), poll=lambda: None, args=["codex", "exec", "-"])
    terminate = Mock()
    ticks = iter([0, watcher.CODEX_TIMEOUT_SECONDS + 1])
    monkeypatch.setattr(watcher, "_prepare_codex_workdir", lambda: "head")
    monkeypatch.setattr(watcher.subprocess, "Popen", lambda *a, **kw: proc)
    monkeypatch.setattr(watcher, "_terminate_process_tree", terminate)
    monkeypatch.setattr(watcher.time, "monotonic", lambda: next(ticks))
    with pytest.raises(subprocess.TimeoutExpired):
        watcher._run_codex("prompt")
    terminate.assert_called_once_with(proc)


@pytest.mark.parametrize("operation", ["git", "variable", "log"])
def test_background_queries_have_hidden_window_and_closed_stdin(monkeypatch, operation):
    run = Mock(return_value=SimpleNamespace(returncode=0, stdout="ok", stderr=""))
    monkeypatch.setattr(watcher.subprocess, "run", run)
    monkeypatch.setattr(watcher, "_subprocess_window_kwargs", lambda: {"creationflags": 8})
    if operation == "git":
        watcher._git_output(".", "status", "--porcelain")
    elif operation == "variable":
        watcher._airflow_variable_get("test")
    else:
        monkeypatch.setattr(watcher, "_request", Mock(side_effect=OSError("offline")))
        assert watcher.get_task_log("sample", "run", "task", 1) == "ok"
    assert run.call_args.kwargs["creationflags"] == 8
    assert run.call_args.kwargs["stdin"] == subprocess.DEVNULL


@pytest.mark.skipif(os.name != "nt", reason="Windows console integration")
@pytest.mark.parametrize("attempt", range(3))
def test_real_cmd_descendant_has_no_console_and_keeps_stdin(tmp_path, attempt):
    command = tmp_path / "hidden_probe.cmd"
    command.write_text(
        '@echo off\n"' + sys.executable + '" -c "import ctypes,sys; '
        'print(ctypes.windll.kernel32.GetConsoleWindow()); '
        'print(sys.stdin.read()); sys.exit(7)"\n', encoding="utf-8"
    )
    options = watcher._subprocess_window_kwargs()
    options["creationflags"] |= subprocess.CREATE_NEW_PROCESS_GROUP
    result = subprocess.run(
        [str(command)], input=f"probe-{attempt}", capture_output=True,
        text=True, timeout=15, **options,
    )
    assert result.returncode == 7, result.stderr
    assert result.stdout.splitlines() == ["0", f"probe-{attempt}"]


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher integration")
@pytest.mark.parametrize("process_name,expected", [("python", "alive"), ("pythonw", "alive"), ("dead", "stale")])
def test_launcher_checks_process_not_only_recent_heartbeat(tmp_path, process_name, expected):
    import json
    from datetime import datetime, timezone

    heartbeat = tmp_path / "heartbeat.json"
    heartbeat.write_text(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                                     "pid": 12345, "backend": "windows"}), encoding="utf-8")
    root = Path(watcher.__file__).parent
    env = os.environ.copy()
    env.update(PROBE_HEARTBEAT=str(heartbeat), PROBE_PROCESS=process_name,
               PROBE_SCRIPT=str(root / "scripts" / "start_codex_autoheal_hidden.ps1"))
    code = r'''
$ErrorActionPreference = 'Stop'
$ast = [System.Management.Automation.Language.Parser]::ParseFile($env:PROBE_SCRIPT, [ref]$null, [ref]$null)
$fn = $ast.Find({param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-FreshHeartbeat'}, $true)
. ([scriptblock]::Create($fn.Extent.Text))
function Write-AutohealLog([string]$Message) {}
function Get-Process { if ($env:PROBE_PROCESS -ne 'dead') { [pscustomobject]@{ProcessName=$env:PROBE_PROCESS} } }
$heartbeatPath = $env:PROBE_HEARTBEAT
$heartbeatMaxAgeSeconds = 180
if (Get-FreshHeartbeat) { 'alive' } else { 'stale' }
'''
    result = subprocess.run(["powershell.exe", "-NoProfile", "-Command", code],
                            env=env, capture_output=True, text=True, timeout=15,
                            stdin=subprocess.DEVNULL, **watcher._subprocess_window_kwargs())
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
    launcher = (root / "scripts" / "start_codex_autoheal_wsl_hidden.vbs").read_text(encoding="utf-8")
    assert "DateLastModified" not in launcher
    assert '0, True)' in launcher
