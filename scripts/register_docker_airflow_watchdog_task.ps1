param(
    [string]$TaskName = "DoridangDockerAirflowWatchdog",
    [string]$StartTime = "01:00"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LauncherScript = Join-Path $ProjectRoot "scripts\run_docker_airflow_watchdog_hidden.vbs"

if (-not (Test-Path $LauncherScript)) {
    throw "Launcher script not found: $LauncherScript"
}

$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$LauncherScript`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 4) `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Set-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings | Out-Null
} else {
    $principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal | Out-Null
}
Get-ScheduledTaskInfo -TaskName $TaskName
