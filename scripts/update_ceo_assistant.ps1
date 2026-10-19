param([string]$Config = 'C:\Local_DB\ceo_assistant\service.json', [switch]$Rollback)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$configPath = (Resolve-Path -LiteralPath $Config).Path
$data = Get-Content -LiteralPath $configPath -Encoding utf8 -Raw | ConvertFrom-Json
if ($data.assistant_profile -ne 'ceo_data') { throw '대표님 전용 설정이 필요합니다.' }
$state = $data.state_root
if (-not [IO.Path]::IsPathRooted($state)) { $state = Join-Path ([IO.Path]::GetDirectoryName($configPath)) $state }
$state = [IO.Path]::GetFullPath($state)
if ($state -match '(?i)onedrive') { throw '상태 폴더는 로컬에 있어야 합니다.' }
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$receipt = Join-Path $state 'ceo-deployment.json'
function GitChecked([string[]]$Arguments) {
    $result = @(& git @Arguments)
    if ($LASTEXITCODE -ne 0) { throw 'Git 작업 실패. 배포 기록을 확인하세요.' }
    return $result
}
Push-Location -LiteralPath $repoRoot
$tasksStopped = $false
try {
    if ((GitChecked -Arguments @('branch','--show-current')) -ne 'main') { throw '운영 노트북의 깨끗한 main에서 실행하세요.' }
    if (@(GitChecked -Arguments @('status','--porcelain')).Count) { throw '로컬 변경이 있어 업데이트를 중단합니다.' }
    $previous = (GitChecked -Arguments @('rev-parse','HEAD')) -join ''
    if ($Rollback) {
        $record = Get-Content -LiteralPath $receipt -Encoding utf8 -Raw | ConvertFrom-Json
        $target = $record.previous_commit
        if ($target -notmatch '^[0-9a-f]{40}$') { throw '복구 커밋 오류' }
        GitChecked -Arguments @('cat-file','-e',($target + '^{commit}')) | Out-Null
    } else {
        GitChecked -Arguments @('fetch','origin','main') | Out-Null
        GitChecked -Arguments @('merge-base','--is-ancestor','HEAD','origin/main') | Out-Null
        $target = (GitChecked -Arguments @('rev-parse','origin/main')) -join ''
    }
    $service = Get-ScheduledTask -TaskName 'DoridangCeoAssistant' -ErrorAction SilentlyContinue
    $watch = Get-ScheduledTask -TaskName 'DoridangCeoAssistantWatchdog' -ErrorAction SilentlyContinue
    $resumeService = $service -and $service.State -ne 'Disabled'
    $resumeWatch = $watch -and $watch.State -ne 'Disabled'
    if (-not $Rollback) {
        [IO.Directory]::CreateDirectory($state) | Out-Null
        $record = @{previous_commit=$previous; target_commit=$target; service_enabled=[bool]$resumeService; watchdog_enabled=[bool]$resumeWatch; status='prepared'}
        [IO.File]::WriteAllText($receipt, ($record | ConvertTo-Json), [Text.UTF8Encoding]::new($false))
    } else {
        $resumeService = $record.service_enabled
        $resumeWatch = $record.watchdog_enabled
    }
    foreach ($task in @($watch, $service)) {
        if ($task) { Disable-ScheduledTask -TaskName $task.TaskName | Out-Null; Stop-ScheduledTask -TaskName $task.TaskName }
    }
    $tasksStopped = $true
    & $python -B -X utf8 -c 'import sys; from pathlib import Path; from modules.transform.pipelines.db.samjeon_material_service.__main__ import instance_lock; lock=instance_lock(Path(sys.argv[1])); lock.__enter__(); lock.__exit__(None,None,None)' $state
    if ($LASTEXITCODE -ne 0) { throw '직접 실행한 대표님 서비스를 먼저 종료하세요.' }
    if ($Rollback) { GitChecked -Arguments @('reset','--keep',$target) | Out-Null }
    else { GitChecked -Arguments @('merge','--ff-only',$target) | Out-Null }
    & $python -m pip install -r (Join-Path $repoRoot 'requirements-samjeon.txt')
    if ($LASTEXITCODE -ne 0) { throw '의존성 갱신 실패' }
    & $python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath --require-ready
    if ($LASTEXITCODE -ne 0) { throw '진단 실패. 대표님 서비스는 중지 상태입니다.' }
    if ($service -and $resumeService) { Enable-ScheduledTask -TaskName $service.TaskName | Out-Null; Start-ScheduledTask -TaskName $service.TaskName }
    if ($watch -and $resumeWatch) { Enable-ScheduledTask -TaskName $watch.TaskName | Out-Null; Start-ScheduledTask -TaskName $watch.TaskName }
    $record.status = if ($Rollback) { 'rolled_back' } else { 'updated' }
    [IO.File]::WriteAllText($receipt, ($record | ConvertTo-Json), [Text.UTF8Encoding]::new($false))
    Write-Output '대표님 서비스 코드 갱신 완료. 설정·대화 기록·OneDrive 자료는 보존됩니다.'
} catch {
    if ($tasksStopped) {
        foreach ($name in @('DoridangCeoAssistantWatchdog', 'DoridangCeoAssistant')) {
            Disable-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue | Out-Null
            Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
        }
    }
    throw
} finally { Pop-Location }
