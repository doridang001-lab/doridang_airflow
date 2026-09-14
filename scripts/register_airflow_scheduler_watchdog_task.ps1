param(
    [string]$TaskName = "DoridangAirflowSchedulerWatchdog",
    [int]$IntervalMinutes = 5
)

$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LauncherScript = Join-Path $ProjectRoot "scripts\run_airflow_scheduler_watchdog_hidden.vbs"
$WatchdogScript = Join-Path $ProjectRoot "scripts\airflow_scheduler_watchdog.py"
$TaskRun = "wscript.exe `"$LauncherScript`""

$Exists = $false
try {
    & schtasks.exe /Query /TN $TaskName *> $null
    $Exists = ($LASTEXITCODE -eq 0)
} catch {
    $Exists = $false
}

if ($Exists) {
    & schtasks.exe /Delete /TN $TaskName /F | Out-Null
}

& schtasks.exe /Create `
    /TN $TaskName `
    /SC MINUTE `
    /MO $IntervalMinutes `
    /TR $TaskRun `
    /F | Out-Null

& schtasks.exe /Query /TN $TaskName /FO LIST
