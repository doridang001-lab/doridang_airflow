$ErrorActionPreference = "Stop"

$root = "C:\airflow"
$logPath = if ($env:CODEX_AUTOHEAL_WINDOWS_LOG) { $env:CODEX_AUTOHEAL_WINDOWS_LOG } else { "C:\airflow\logs\codex_autoheal_windows.log" }
$heartbeatPath = if ($env:AUTOHEAL_HEARTBEAT_PATH) { $env:AUTOHEAL_HEARTBEAT_PATH } else { "C:\airflow\logs\autoheal_heartbeat.json" }
$heartbeatMaxAgeSeconds = if ($env:CODEX_AUTOHEAL_HEARTBEAT_MAX_AGE_SECONDS) { [int]$env:CODEX_AUTOHEAL_HEARTBEAT_MAX_AGE_SECONDS } else { 1200 }
$tmuxSession = if ($env:DORIDANG_OPS_TMUX_SESSION) { $env:DORIDANG_OPS_TMUX_SESSION } else { "doridang_ops" }
$tmuxWindow = if ($env:CODEX_AUTOHEAL_TMUX_WINDOW) { $env:CODEX_AUTOHEAL_TMUX_WINDOW } else { "codex-autoheal" }
$wslScript = "bash /mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh"

New-Item -ItemType Directory -Force -Path (Split-Path $logPath -Parent) | Out-Null
Set-Location $root

$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$startedAt] check hidden WSL autoheal launcher" | Add-Content -Path $logPath -Encoding utf8

if (Test-Path $heartbeatPath) {
    $heartbeatAgeSeconds = ((Get-Date) - (Get-Item $heartbeatPath).LastWriteTime).TotalSeconds
    if ($heartbeatAgeSeconds -lt $heartbeatMaxAgeSeconds) {
        & wsl.exe -u myuser bash -lc "tmux list-windows -t '$tmuxSession' -F '#W' 2>/dev/null | grep -Fxq '$tmuxWindow'" 2>$null
        if ($LASTEXITCODE -eq 0) {
            "[$startedAt] skip WSL autoheal launcher heartbeat_age_seconds=$([int]$heartbeatAgeSeconds) tmux_window=$tmuxSession`:$tmuxWindow" | Add-Content -Path $logPath -Encoding utf8
            exit 0
        }

        "[$startedAt] heartbeat fresh but tmux window missing heartbeat_age_seconds=$([int]$heartbeatAgeSeconds) tmux_window=$tmuxSession`:$tmuxWindow" | Add-Content -Path $logPath -Encoding utf8
    }
}

"[$startedAt] start hidden WSL autoheal launcher heartbeat_stale_or_missing=$heartbeatPath" | Add-Content -Path $logPath -Encoding utf8

& wsl.exe -u myuser bash -lc $wslScript *>> $logPath

$exitCode = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
$finishedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
"[$finishedAt] hidden WSL autoheal launcher exited exit_code=$exitCode" | Add-Content -Path $logPath -Encoding utf8
exit $exitCode
