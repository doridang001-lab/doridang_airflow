param(
    [string]$Config = 'C:\Local_DB\samjeon_material_agent\service.json',
    [switch]$Rollback
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$taskName = 'DoridangSamjeonMaterialAgent'
function Git-Checked {
    param([string[]]$Arguments)
    $output = @(& git @Arguments)
    if ($LASTEXITCODE -ne 0) { throw ('Git 실패: ' + ($Arguments -join ' ')) }
    return $output
}
Push-Location -LiteralPath $repoRoot
try {
    if ((Git-Checked -Arguments @('branch','--show-current')) -ne 'main') { throw '노트북 업데이트는 main 브랜치에서만 실행하세요.' }
    if (@(Git-Checked -Arguments @('status','--porcelain')).Count -ne 0) { throw '로컬 변경이 있어 업데이트를 중단합니다.' }
    if (-not (Test-Path -LiteralPath $python)) { throw '먼저 설치 스크립트를 실행하세요.' }
    $resolvedConfig = (Resolve-Path -LiteralPath $Config).Path
    # 설정 검증 후 저장소 밖 로컬 상태 경로만 사용한다.
    $stateRoot = & $python -B -X utf8 -c 'import sys; from modules.transform.pipelines.db.samjeon_material_service.config import Settings; print(Settings.load(sys.argv[1]).state_root)' $resolvedConfig
    if ($LASTEXITCODE -ne 0) { throw '운영 설정 검증 실패' }
    $statePath = [IO.Path]::GetFullPath($stateRoot)
    if ($statePath -eq $repoRoot -or $statePath.StartsWith($repoRoot + '\', [StringComparison]::OrdinalIgnoreCase)) { throw '업데이트 상태 경로는 저장소 밖으로 지정하세요.' }
    $receipt = Join-Path $statePath 'deployment.json'
    $previous = (Git-Checked -Arguments @('rev-parse','HEAD')) -join ''
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    $restart = $false
    if ($Rollback) {
        if (-not (Test-Path -LiteralPath $receipt)) { throw '복구할 배포 기록이 없습니다.' }
        $record = Get-Content -LiteralPath $receipt -Encoding utf8 -Raw | ConvertFrom-Json
        $target = $record.previous_commit
        if ($target -notmatch '^[0-9a-f]{40}$') { throw '이전 커밋 기록 오류' }
        $restart = [bool]$record.restart
        # 상태 기록의 해시가 실제 커밋이며 대상이 로컬에 존재하는지 확인한다.
        Git-Checked -Arguments @('cat-file','-e',($target + '^{commit}')) | Out-Null
    } else {
        Git-Checked -Arguments @('fetch','origin','main') | Out-Null
        Git-Checked -Arguments @('merge-base','--is-ancestor','HEAD','origin/main') | Out-Null
        $target = (Git-Checked -Arguments @('rev-parse','origin/main')) -join ''
        $restart = $task -and $task.State -ne 'Disabled'
        New-Item -ItemType Directory -Path $statePath -Force | Out-Null
        @{previous_commit=$previous; target_commit=$target; restart=[bool]$restart; status='prepared'} |
            ConvertTo-Json | Set-Content -LiteralPath $receipt -Encoding utf8
    }
    if ($task) {
        Disable-ScheduledTask -TaskName $taskName | Out-Null
        Stop-ScheduledTask -TaskName $taskName
    }
    # 직접 실행한 서비스도 중지됐는지 OS 파일 잠금으로 확인한다.
    & $python -B -X utf8 -c 'import sys; from modules.transform.pipelines.db.samjeon_material_service.__main__ import instance_lock; from pathlib import Path; lock=instance_lock(Path(sys.argv[1])); lock.__enter__(); lock.__exit__(None,None,None)' $statePath
    if ($LASTEXITCODE -ne 0) { throw '실행 중인 삼전점 서비스를 중지한 뒤 다시 실행하세요.' }
    try {
        if ($Rollback) {
            # 깨끗한 전용 main 체크아웃만 이전 커밋으로 이동한다. --hard/clean은 사용하지 않는다.
            Git-Checked -Arguments @('reset','--keep',$target) | Out-Null
        } else {
            Git-Checked -Arguments @('pull','--ff-only','origin','main') | Out-Null
        }
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $repoRoot 'scripts\set_samjeon_sparse.ps1') -Profile Runtime
        if ($LASTEXITCODE -ne 0) { throw '부분 설치 갱신 실패' }
        & $python -m pip install -r (Join-Path $repoRoot 'requirements-samjeon.txt')
        if ($LASTEXITCODE -ne 0) { throw '의존성 갱신 실패' }
        & $python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $resolvedConfig --require-ready
        if ($LASTEXITCODE -ne 0) { throw '실행 준비 점검 실패' }
        if ($task -and $restart) {
            Enable-ScheduledTask -TaskName $taskName | Out-Null
            Start-ScheduledTask -TaskName $taskName
        }
        $record = Get-Content -LiteralPath $receipt -Encoding utf8 -Raw | ConvertFrom-Json
        $record.status = if ($Rollback) { 'rolled_back' } else { 'updated' }
        $record | ConvertTo-Json | Set-Content -LiteralPath $receipt -Encoding utf8
        Write-Output '코드·의존성·진단 완료. 설정과 대화·발송 이력을 유지했습니다.'
    } catch {
        Write-Error "배포 중단. 서비스는 비활성 상태입니다. 이전 커밋: $previous. 복구: .\scripts\update_samjeon_material_agent.ps1 -Config '$resolvedConfig' -Rollback. 원인: $_"
    }
} finally { Pop-Location }
