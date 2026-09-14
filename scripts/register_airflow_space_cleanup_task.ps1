param(
    [string]$TaskName = "DoridangAirflowSpaceCleanup",
    [int]$IntervalMinutes = 30
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1) {
    throw "IntervalMinutes must be 1 or greater."
}

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LauncherScript = Join-Path $ProjectRoot "scripts\run_airflow_space_cleanup_hidden.vbs"
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
