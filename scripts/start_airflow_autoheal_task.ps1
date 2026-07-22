$ErrorActionPreference = "Stop"

$env:PYTHONIOENCODING = "utf-8"
$env:CODEX_AUTOHEAL_LOG = if ($env:CODEX_AUTOHEAL_LOG) { $env:CODEX_AUTOHEAL_LOG } else { "/tmp/codex_autoheal_queue.log" }

$logPath = "C:\airflow\scripts\wsl_autoheal_start.log"
$script = "/mnt/c/airflow/scripts/start_telegram_wsl.sh"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"

"[$timestamp] starting AirflowCodexTelegramAutoHeal" | Add-Content -Path $logPath -Encoding utf8

Set-Location "C:\airflow"

try {
    & wsl.exe -- bash $script 2>&1 |
        ForEach-Object { $_.ToString() } |
        Add-Content -Path $logPath -Encoding utf8

    $exitCode = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
    $doneAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
    "[$doneAt] finished exit_code=$exitCode" | Add-Content -Path $logPath -Encoding utf8
    exit $exitCode
}
catch {
    $failedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
    "[$failedAt] failed: $($_.Exception.Message)" | Add-Content -Path $logPath -Encoding utf8
    exit 1
}
