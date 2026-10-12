param(
    [Parameter(Mandatory=$true)][string]$Config,
    [switch]$Enable
)
$ErrorActionPreference = 'Stop'
$configPath = (Resolve-Path -LiteralPath $Config).Path
$startScript = Join-Path $PSScriptRoot 'start_samjeon_material_agent.ps1'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ($configPath.Contains('"')) { throw '설정 경로에 따옴표를 사용할 수 없습니다.' }
$arguments = '-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $startScript + '" -Config "' + $configPath + '"'
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arguments -WorkingDirectory $repoRoot
$account = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $account
$principal = New-ScheduledTaskPrincipal -UserId $account -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings
$task.Settings.Enabled = [bool]$Enable
Register-ScheduledTask -TaskName 'DoridangSamjeonMaterialAgent' -InputObject $task
