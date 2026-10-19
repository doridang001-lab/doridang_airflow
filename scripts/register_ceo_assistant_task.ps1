param([Parameter(Mandatory=$true)][string]$Config, [switch]$Enable)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$configPath = (Resolve-Path -LiteralPath $Config).Path
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$pythonw = Join-Path $repoRoot '.venv\Scripts\pythonw.exe'
$data = Get-Content -LiteralPath $configPath -Encoding utf8 -Raw | ConvertFrom-Json
if ($data.assistant_profile -ne 'ceo_data' -or $data.token_env -ne 'CEO_TELEGRAM_BOT_TOKEN') { throw '대표님 전용 설정이 필요합니다.' }
if ($configPath.Contains('"') -or -not (Test-Path -LiteralPath $pythonw)) { throw '실행 경로 또는 가상환경을 확인하세요.' }
Push-Location -LiteralPath $repoRoot
try {
    & $python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath --require-ready
    if ($LASTEXITCODE -ne 0) { throw '대표님 비서 연결 진단이 필요합니다.' }
} finally { Pop-Location }
$account = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $account -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$logon = New-ScheduledTaskTrigger -AtLogOn -User $account
$repeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)
$action = New-ScheduledTaskAction -Execute $pythonw -Argument ('-B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service service --config "' + $configPath + '"') -WorkingDirectory $repoRoot
$task = New-ScheduledTask -Action $action -Trigger @($logon, $repeat) -Principal $principal -Settings $settings
$task.Settings.Enabled = [bool]$Enable
Register-ScheduledTask -TaskName 'DoridangCeoAssistant' -InputObject $task -Force
$watch = Join-Path $PSScriptRoot 'watch_ceo_assistant.py'
$watchAction = New-ScheduledTaskAction -Execute $pythonw -Argument ('-B -X utf8 "' + $watch + '" --config "' + $configPath + '"') -WorkingDirectory $repoRoot
$watchTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration (New-TimeSpan -Days 3650)
$watchTask = New-ScheduledTask -Action $watchAction -Trigger $watchTrigger -Principal $principal -Settings $settings
$watchTask.Settings.Enabled = [bool]$Enable
Register-ScheduledTask -TaskName 'DoridangCeoAssistantWatchdog' -InputObject $watchTask -Force
