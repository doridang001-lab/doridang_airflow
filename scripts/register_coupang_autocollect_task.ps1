param(
    [string]$TaskName = "CoupangAutoCollect",
    [string]$UserId = $env:USERNAME,
    [int]$LogonDelaySeconds = 60,
    [int]$ExecutionTimeLimitMinutes = 45,
    [int]$StartupDelaySeconds = 60,
    [int]$CollectionWaitSeconds = 1200,
    [int]$CollectionStableSeconds = 60,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$scriptPath = Join-Path $repoRoot "scripts\coupang_boot_autostart.ps1"

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
    "-CollectionWaitSeconds $CollectionWaitSeconds",
    "-CollectionStableSeconds $CollectionStableSeconds"
) -join " "

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $argument `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserId
$trigger.Delay = "PT${LogonDelaySeconds}S"

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
    -Description "로그온 후 쿠팡이츠 수집용 Chrome 확장 runner 자동 클릭 및 적재 DAG 트리거" `
    -Force | Out-Null

Write-Host "Scheduled task ready: $TaskName"
Write-Host "User: $UserId"
Write-Host "Action: powershell.exe $argument"
Write-Host "ExecutionTimeLimitMinutes: $ExecutionTimeLimitMinutes"
Write-Host "RunLevel: $RunLevel"



