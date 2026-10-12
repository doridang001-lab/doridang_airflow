param(
    [Parameter(Mandatory=$true)][string]$Config,
    [string]$Python = (Join-Path $PSScriptRoot '..\.venv\Scripts\python.exe')
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$configPath = (Resolve-Path -LiteralPath $Config).Path
if (-not (Test-Path -LiteralPath $Python)) { throw '전용 Python 환경을 먼저 준비하세요.' }
Set-Location -LiteralPath $repoRoot
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONUTF8 = '1'
& $Python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service service --config $configPath
exit $LASTEXITCODE
