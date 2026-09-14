param(
    [string]$TaskName = "DoridangBotServer",
    [string]$UserId = $env:USERNAME,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 로그온 시 도리당봇(8788)을 기동한다. 이미 떠 있으면 start 스크립트가 그대로 두고 끝난다.
# 사내 공유를 하려면 Windows 방화벽 인바운드 8788 허용이 별도로 필요하다(코드 아님).

$runner = "C:\airflow\scripts\start_doridang_bot.ps1"
$powershell = "C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe"
if (-not (Test-Path -LiteralPath $runner)) {
    throw "Doridang bot start script was not found: $runner"
}

$argument = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runner`""

$action = New-ScheduledTaskAction `
    -Execute $powershell `
    -Argument $argument `
    -WorkingDirectory "C:\airflow"
$triggerLogon = New-ScheduledTaskTrigger -AtLogOn -User $UserId
# 봇이 죽어도 다음 점검에서 되살아나도록 10분마다 재확인한다.
$triggerRepeat = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(5) `
    -RepetitionInterval (New-TimeSpan -Minutes 10)
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$principal = New-ScheduledTaskPrincipal `
    -UserId $UserId `
    -LogonType Interactive `
    -RunLevel $RunLevel

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($triggerLogon, $triggerRepeat) `
    -Settings $settings `
    -Principal $principal `
    -Description "Keep Doridang Flow assistant (port 8788) running" `
    -Force | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Scheduled task ready: $($task.TaskName)"
Write-Host "User: $UserId"
Write-Host "Triggers: at logon + every 10 minutes"
Write-Host "NextRunTime: $($info.NextRunTime)"
Write-Host "Action: $powershell $argument"
