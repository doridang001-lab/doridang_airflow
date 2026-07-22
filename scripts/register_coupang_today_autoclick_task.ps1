param(
    [string]$TaskName = "CoupangTodayTopHalfCollect",
    [string]$UserId = $env:USERNAME,
    [int[]]$Hours = @(13, 15, 17, 19, 21),
    [int]$ExecutionTimeLimitMinutes = 90,
    [int]$StartupDelaySeconds = 0,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) {
    $chrome = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
$runnerUrl = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1&mode=top50&date=today"

if (-not (Test-Path $chrome)) {
    Write-Error "chrome.exe 를 찾을 수 없습니다."
    exit 1
}

$argument = "--profile-directory=`"Default`" --new-window $runnerUrl"

$action = New-ScheduledTaskAction `
    -Execute $chrome `
    -Argument $argument `
    -WorkingDirectory (Split-Path $chrome)

$triggers = foreach ($hour in $Hours) {
    New-ScheduledTaskTrigger -Daily -At ([datetime]::Today.AddHours($hour))
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances Parallel `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

$principal = New-ScheduledTaskPrincipal `
    -UserId $UserId `
    -LogonType Interactive `
    -RunLevel $RunLevel

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Principal $principal `
    -Description "매일 13/15/17/19/21시에 쿠팡이츠 확장 runner의 topHalfTodayBtn 자동 클릭" `
    -Force | Out-Null

Write-Host "Scheduled task ready: $TaskName"
Write-Host "User: $UserId"
Write-Host "Action: $chrome $argument"
Write-Host ("Hours: {0}" -f (($Hours | ForEach-Object { "{0:D2}:00" -f $_ }) -join ", "))
Write-Host "ExecutionTimeLimit: unlimited"
Write-Host "RunLevel: $RunLevel"
