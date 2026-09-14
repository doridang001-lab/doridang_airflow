$ErrorActionPreference = "Stop"

$taskName = "AirflowCodexTelegramAutoHeal"
$launcher = "bash /mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh"

$action = New-ScheduledTaskAction `
    -Execute "wsl.exe" `
    -Argument "-u myuser bash -lc `"$launcher`""
$triggers = @()
$triggers += New-ScheduledTaskTrigger -AtLogOn
$triggers += New-ScheduledTaskTrigger -AtStartup
$triggers += New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1)) -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -Hidden `
    -MultipleInstances IgnoreNew

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Set-ScheduledTask -TaskName $taskName -Action $action -Trigger $triggers -Settings $settings
} else {
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $triggers `
        -Settings $settings `
        -Description "Airflow errors trigger a WSL tmux Codex auto-heal loop and Telegram completion replies" `
        -Force | Out-Null
}

Write-Host "Scheduled task ready: $taskName -> WSL autoheal"
