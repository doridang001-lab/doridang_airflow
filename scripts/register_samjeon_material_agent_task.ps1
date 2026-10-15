param(
    [Parameter(Mandatory=$true)][string]$Config,
    [switch]$Enable
)
$ErrorActionPreference = 'Stop'
$configPath = (Resolve-Path -LiteralPath $Config).Path
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$pythonw = Join-Path $repoRoot '.venv\Scripts\pythonw.exe'
if (-not (Test-Path -LiteralPath $pythonw)) { throw '창 없는 전용 Python 실행 파일을 찾을 수 없습니다.' }
Push-Location -LiteralPath $repoRoot
try {
    $configData = Get-Content -LiteralPath $configPath -Encoding utf8 -Raw | ConvertFrom-Json
    $readinessFlag = if ($configData.conversation_engine -eq 'appserver') { '--require-data-query-ready' } else { '--require-ready' }
    & $python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath $readinessFlag
    if ($LASTEXITCODE -ne 0) { throw '전체 실행 준비 점검을 통과해야 자동 실행 작업을 등록할 수 있습니다.' }
} finally { Pop-Location }
if ($configPath.Contains('"')) { throw '설정 경로에 따옴표를 사용할 수 없습니다.' }
$watchScript = Join-Path $PSScriptRoot 'watch_samjeon_material_agent.py'
if (-not (Test-Path -LiteralPath $watchScript)) { throw '서비스 감시 스크립트를 찾을 수 없습니다.' }
$arguments = '-B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service service --config "' + $configPath + '"'
$action = New-ScheduledTaskAction -Execute $pythonw -Argument $arguments -WorkingDirectory $repoRoot
$account = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $account
# 비정상 종료 뒤에도 5분 이내에 재시작한다. 실행 중인 작업은 IgnoreNew로 중복 실행하지 않는다.
$watchdogTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $account -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$task = New-ScheduledTask -Action $action -Trigger @($logonTrigger, $watchdogTrigger) -Principal $principal -Settings $settings
$task.Settings.Enabled = [bool]$Enable
Register-ScheduledTask -TaskName 'DoridangSamjeonMaterialAgent' -InputObject $task -Force

$watchArguments = '-B -X utf8 "' + $watchScript + '" --config "' + $configPath + '"'
$watchAction = New-ScheduledTaskAction -Execute $pythonw -Argument $watchArguments -WorkingDirectory $repoRoot
$watchTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration (New-TimeSpan -Days 3650)
$watchSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 1) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$watchTask = New-ScheduledTask -Action $watchAction -Trigger $watchTrigger -Principal $principal -Settings $watchSettings
$watchTask.Settings.Enabled = [bool]$Enable
Register-ScheduledTask -TaskName 'DoridangSamjeonMaterialWatchdog' -InputObject $watchTask -Force
