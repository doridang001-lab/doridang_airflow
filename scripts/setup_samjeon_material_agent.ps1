param(
    [string]$Config,
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not $Config) {
    $Config = Join-Path $repoRoot 'config\samjeon_material_agent.example.json'
}
$configPath = (Resolve-Path -LiteralPath $Config).Path
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
} finally {
    Pop-Location
}
