param(
    [string]$Config = 'C:\Local_DB\samjeon_material_agent\service.json',
    [switch]$CheckOnly,
    [switch]$InstallPrerequisites,
    [ValidateSet('windows','wsl')][string]$CodexBackend = 'wsl'
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ($CheckOnly -and $InstallPrerequisites) { throw '-CheckOnly와 -InstallPrerequisites를 함께 사용할 수 없습니다.' }
if ($InstallPrerequisites) {
    $packages = @(@('git','Git.Git'), @('py','Python.Python.3.12'))
    if ($CodexBackend -eq 'windows') { $packages += ,@('npm','OpenJS.NodeJS.LTS') }
    foreach ($package in $packages) {
        $needed = -not (Get-Command $package[0] -ErrorAction SilentlyContinue)
        if ($package[0] -eq 'py' -and -not $needed) {
            & py -3.12 -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 1)'
            $needed = $LASTEXITCODE -ne 0
        }
        if ($needed) {
            & winget install --id $package[1] --exact --source winget --accept-package-agreements --accept-source-agreements
            if ($LASTEXITCODE -ne 0) { throw "설치 실패: $($package[1])" }
        }
    }
    $env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [Environment]::GetEnvironmentVariable('Path','User')
    if ($CodexBackend -eq 'windows') {
        $npm = Get-Command npm.cmd -ErrorAction SilentlyContinue
        if (-not $npm) { throw 'Node.js 설치 후 새 PowerShell에서 다시 실행하세요.' }
        & $npm.Source install --global '@openai/codex@0.158.0'
        if ($LASTEXITCODE -ne 0) { throw 'Codex CLI 설치 실패' }
    }
}
$configPath = [IO.Path]::GetFullPath($Config)
if ($configPath -match '(?i)onedrive') { throw '운영 설정은 OneDrive 밖 로컬 폴더에 두세요.' }
if (-not (Test-Path -LiteralPath $configPath)) {
    if ($CheckOnly) { throw '운영 설정이 없습니다. 설치 스크립트를 -CheckOnly 없이 실행하세요.' }
    $example = if ($CodexBackend -eq 'wsl') { 'config\samjeon_material_agent.wsl.example.json' } else { 'config\samjeon_material_agent.example.json' }
    $settings = Get-Content -LiteralPath (Join-Path $repoRoot $example) -Encoding utf8 -Raw | ConvertFrom-Json
    $settings.state_root = Join-Path (Split-Path -Parent $configPath) 'state'
    New-Item -ItemType Directory -Path (Split-Path -Parent $configPath) -Force | Out-Null
    [IO.File]::WriteAllText($configPath, ($settings | ConvertTo-Json -Depth 8), (New-Object Text.UTF8Encoding($false)))
    Write-Output "비활성 운영 설정 생성: $configPath"
}
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $python)) {
    if ($CheckOnly) { throw '전용 Python 환경이 없습니다. -CheckOnly 없이 다시 실행하세요.' }
    $launcher = Get-Command py -ErrorAction SilentlyContinue
    if ($launcher) {
        & py -3.12 -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 1)'
        if ($LASTEXITCODE -ne 0) { throw 'Python 3.12를 설치하세요.' }
        & py -3.12 -m venv (Join-Path $repoRoot '.venv')
    } else {
        $launcher = Get-Command python -ErrorAction SilentlyContinue
        if (-not $launcher) { throw 'Python 3.12를 설치하세요.' }
        & python -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 1)'
        if ($LASTEXITCODE -ne 0) { throw 'Python 3.12를 설치하세요.' }
        & python -m venv (Join-Path $repoRoot '.venv')
    }
    if ($LASTEXITCODE -ne 0) { throw '전용 Python 환경 생성에 실패했습니다.' }
}

& $python -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 1)'
if ($LASTEXITCODE -ne 0) { throw '전용 Python 환경은 3.12여야 합니다.' }

Push-Location -LiteralPath $repoRoot
try {
    if (-not $CheckOnly) {
        & $python -m pip install -r (Join-Path $repoRoot 'requirements-samjeon.txt')
        if ($LASTEXITCODE -ne 0) { throw '필수 패키지 설치에 실패했습니다.' }
    }
    & $python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath
    if ($LASTEXITCODE -ne 0) { throw '삼전점 서비스 점검에 실패했습니다.' }
    Write-Output '진단의 ready와 blocked_reasons를 확인하세요. 기존 봇 토큰·허용 ID·WSL 설정은 유지하세요. CEO_DB 선택 조회는 기존 설정의 ceo_data_root에 경로를 추가하고 --require-ready로 연결을 확인합니다. 자료 본문은 질문 시 조회합니다.'
} finally {
    Pop-Location
}
