param(
    [string]$TaskName = "MulryudamLoginConfirm",
    [string]$UserId = $env:USERNAME,
    [datetime]$DailyAt = ([datetime]::Today.AddHours(3)),
    [int]$TimeoutSeconds = 60,
    [int]$ExecutionTimeLimitMinutes = 10,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$scriptPath = Join-Path $repoRoot "scripts\mulryudam_login_confirm.py"

if (-not (Test-Path -LiteralPath $python)) {
    Write-Error "Python 경로가 존재하지 않습니다: $python"
    exit 1
}

if (-not (Test-Path -LiteralPath $scriptPath)) {
    Write-Error "물류담 자동 확인 스크립트를 찾을 수 없습니다: $scriptPath"
    exit 1
}

$argument = @(
    "`"$scriptPath`"",
    "--timeout-seconds $TimeoutSeconds"
) -join " "

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument $argument `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -Daily -At $DailyAt

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
    -Description "Launch Mulryudam, export yesterday order details to daily parquet, close Excel and app" `
    -Force | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Scheduled task ready: $($task.TaskName)"
Write-Host "User: $UserId"
Write-Host ("Trigger: daily {0}" -f $DailyAt.ToString("HH:mm"))
Write-Host "NextRunTime: $($info.NextRunTime)"
Write-Host "Action: $python $argument"
Write-Host "ExecutionTimeLimitMinutes: $ExecutionTimeLimitMinutes"
Write-Host "RunLevel: $RunLevel"
