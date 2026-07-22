$ErrorActionPreference = "Stop"

$root = "C:\airflow"
$worktree = if ($env:CODEX_WORKDIR) { $env:CODEX_WORKDIR } else { "C:\tmp\airflow-autoheal" }
$logPath = if ($env:CODEX_AUTOHEAL_WINDOWS_LOG) { $env:CODEX_AUTOHEAL_WINDOWS_LOG } else { "C:\airflow\logs\codex_autoheal_windows.log" }

$env:PYTHONIOENCODING = "utf-8"
$env:HEAL_QUEUE_PATH = if ($env:HEAL_QUEUE_PATH) { $env:HEAL_QUEUE_PATH } else { "C:\airflow\logs\heal_queue.jsonl" }
$env:HEAL_TASK_STATE_PATH = if ($env:HEAL_TASK_STATE_PATH) { $env:HEAL_TASK_STATE_PATH } else { "C:\airflow\logs\heal_task_state.json" }
$env:AUTOHEAL_HEARTBEAT_PATH = if ($env:AUTOHEAL_HEARTBEAT_PATH) { $env:AUTOHEAL_HEARTBEAT_PATH } else { "C:\airflow\logs\autoheal_heartbeat.json" }
$env:AIRFLOW_RUNTIME_WORKDIR = if ($env:AIRFLOW_RUNTIME_WORKDIR) { $env:AIRFLOW_RUNTIME_WORKDIR } else { $root }
$env:CODEX_WORKDIR = $worktree

if (-not (Test-Path $worktree)) {
    throw "Codex auto-heal worktree not found: $worktree"
}

New-Item -ItemType Directory -Force -Path (Split-Path $logPath -Parent) | Out-Null
Set-Location $root

$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$startedAt] start Windows autoheal fallback CODEX_WORKDIR=$worktree" | Add-Content -Path $logPath -Encoding utf8

& python -X utf8 "$root\watch_heal_queue.py" *>> $logPath

$exitCode = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
$finishedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$finishedAt] Windows autoheal fallback exited exit_code=$exitCode" | Add-Content -Path $logPath -Encoding utf8
exit $exitCode
