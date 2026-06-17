$taskName = "telegram_claude_loop"
$scriptPath = "C:\airflow\scripts\start_telegram_loop.vbs"

$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$scriptPath`""
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Set-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings
} else {
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description "Start WSL tmux Telegram loop and Codex DAG auto-heal on logon" `
        -Force | Out-Null
}

Write-Host "Scheduled task ready: $taskName -> $scriptPath"
