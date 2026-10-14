param(
    [Parameter(Mandatory=$true)][string]$Config,
    [string]$Python = '',
    [switch]$Background
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not $Python) { $Python = Join-Path $repoRoot '.venv\Scripts\python.exe' }
$configPath = (Resolve-Path -LiteralPath $Config).Path
if (-not (Test-Path -LiteralPath $Python)) { throw '전용 Python 환경을 먼저 준비하세요.' }
if ($Background) {
    if ($configPath.Contains('"') -or $Python.Contains('"')) { throw '실행 경로에 따옴표를 사용할 수 없습니다.' }
    $arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $PSCommandPath + '" -Config "' + $configPath + '" -Python "' + $Python + '"'
    Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -WorkingDirectory $repoRoot -ArgumentList $arguments | Out-Null
    Write-Output '백그라운드 시작을 요청했습니다. doctor와 health.json에서 실제 상태를 확인하세요.'
    exit 0
}
Set-Location -LiteralPath $repoRoot
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONUTF8 = '1'
& $Python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service service --config $configPath
exit $LASTEXITCODE
