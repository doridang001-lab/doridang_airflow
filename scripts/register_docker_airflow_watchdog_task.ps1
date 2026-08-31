param(
    [string]$TaskName = "DoridangDockerAirflowWatchdog",
    [string]$StartTime = "01:00"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LauncherScript = Join-Path $ProjectRoot "scripts\run_docker_airflow_watchdog_hidden.vbs"
$TaskRun = "wscript.exe `"$LauncherScript`""

if (-not (Test-Path $LauncherScript)) {
    throw "Launcher script not found: $LauncherScript"
}

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
    /SC DAILY `
    /ST $StartTime `
    /TR $TaskRun `
    /F | Out-Null

& schtasks.exe /Query /TN $TaskName /FO LIST
