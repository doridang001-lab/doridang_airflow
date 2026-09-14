param(
    [string]$TaskName = "SaleslabPowerBIKakaoDaily",
    [string]$UserId = $env:USERNAME,
    [string]$At = "08:30",
    [int]$StartupDelaySeconds = 0,
    [int]$ExecutionTimeLimitMinutes = 30,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$scriptPath = Join-Path $repoRoot "scripts\saleslab_powerbi_kakao_daily.ps1"

if (-not (Test-Path $scriptPath)) {
    Write-Error "자동 실행 스크립트를 찾을 수 없습니다: $scriptPath"
    exit 1
}

$argument = @(
    "-NoProfile",
    "-ExecutionPolicy Bypass",
    "-WindowStyle Hidden",
    "-File `"$scriptPath`"",
    "-StartupDelaySeconds $StartupDelaySeconds",
    "-Send",
    "-SendMode enter"
) -join " "

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $argument `
    -WorkingDirectory $repoRoot

$triggerTime = [datetime]::ParseExact($At, "HH:mm", [Globalization.CultureInfo]::InvariantCulture)
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $triggerTime

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes $ExecutionTimeLimitMinutes)

$principal = New-ScheduledTaskPrincipal `
    -UserId $UserId `
    -LogonType Interactive `
    -RunLevel $RunLevel

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Weekdays 08:30 Saleslab Power BI capture and KakaoTalk send" `
    -Force | Out-Null

Write-Host "Scheduled task ready: $TaskName"
Write-Host "User: $UserId"
Write-Host "Schedule: Monday-Friday $At"
Write-Host "Action: powershell.exe $argument"
Write-Host "ExecutionTimeLimitMinutes: $ExecutionTimeLimitMinutes"
Write-Host "RunLevel: $RunLevel"
