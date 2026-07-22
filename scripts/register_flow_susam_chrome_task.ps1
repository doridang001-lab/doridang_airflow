param(
    [string]$TaskName = "AirflowFlowSusamChromeWindow",
    [string]$UserId = $env:USERNAME,
    [ValidateSet("Limited", "Highest")]
    [string]$RunLevel = "Limited"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$runner = "C:\airflow\scripts\flow_susam_host_chrome_window.ps1"
$powershell = "C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe"
if (-not (Test-Path -LiteralPath $runner)) {
    throw "Flow Chrome runner was not found: $runner"
}

$argument = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runner`""
$action = New-ScheduledTaskAction `
    -Execute $powershell `
    -Argument $argument `
    -WorkingDirectory "C:\airflow"
$trigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::Today.AddHours(8).AddMinutes(55))
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3)
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
    -Description "Open dedicated Flow Chrome daily from 08:55 to 11:10 for Docker Airflow" `
    -Force | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Scheduled task ready: $($task.TaskName)"
Write-Host "User: $UserId"
Write-Host "Trigger: daily 08:55 KST"
Write-Host "NextRunTime: $($info.NextRunTime)"
Write-Host "Action: $powershell $argument"
Write-Host "RunLevel: $RunLevel"
