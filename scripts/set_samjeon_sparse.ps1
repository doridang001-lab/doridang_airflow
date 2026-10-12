param([ValidateSet('Runtime','Development')][string]$Profile = 'Runtime')
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
Push-Location -LiteralPath $repoRoot
try {
    $changes = @(& git status --porcelain)
    if ($LASTEXITCODE -ne 0 -or $changes.Count -gt 0) { throw '변경 파일을 먼저 확인하세요. 부분 설치는 깨끗한 작업 폴더에서만 적용합니다.' }
    $patterns = @(& git show HEAD:config/samjeon-runtime.paths)
    if ($LASTEXITCODE -ne 0) { throw '실행 파일 목록을 읽지 못했습니다.' }
    if ($Profile -eq 'Development') {
        $patterns += @(& git show HEAD:config/samjeon-development.paths)
        if ($LASTEXITCODE -ne 0) { throw '개발 파일 목록을 읽지 못했습니다.' }
    }
    # 목록은 ASCII 경로 패턴만 사용한다. 한국어 문서를 셸 파이프로 전달하지 않는다.
    $patterns | & git sparse-checkout set --no-cone --stdin
    if ($LASTEXITCODE -ne 0) { throw '부분 설치 설정에 실패했습니다.' }
    & git config --worktree samjeon.profile $Profile
    if ($LASTEXITCODE -ne 0) { throw '작업 폴더별 프로필 기록에 실패했습니다.' }
    Write-Output "부분 설치 적용 완료: $Profile"
} finally { Pop-Location }
