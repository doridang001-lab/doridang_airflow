param(
    [Parameter(Mandatory=$true)][string]$DataRoot,
    [string]$Config = 'C:\Local_DB\ceo_assistant\service.json',
    [switch]$CheckOnly
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$configPath = [IO.Path]::GetFullPath($Config)
$dataPath = (Resolve-Path -LiteralPath $DataRoot).Path
if ($configPath -match '(?i)onedrive' -or $configPath.StartsWith($dataPath.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) {
    throw '설정 파일은 OneDrive와 CEO_DB 밖의 로컬 경로에 두세요.'
}
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
if ($CheckOnly) {
    [pscustomobject]@{PythonExists=(Test-Path -LiteralPath $python); ConfigExists=(Test-Path -LiteralPath $configPath); DataRootExists=$true; FilesWritten=$false}
    exit 0
}
if (-not (Test-Path -LiteralPath $python)) {
    & py -3.12 -m venv (Join-Path $repoRoot '.venv')
    if ($LASTEXITCODE -ne 0) { throw 'Python 3.12 가상환경 생성 실패' }
}
& $python -m pip install -r (Join-Path $repoRoot 'requirements-samjeon.txt')
if ($LASTEXITCODE -ne 0) { throw '의존성 설치 실패' }
if (-not (Test-Path -LiteralPath $configPath)) {
    $data = Get-Content -LiteralPath (Join-Path $repoRoot 'config\ceo_assistant.example.json') -Encoding utf8 -Raw | ConvertFrom-Json
    $data.ceo_data_root = $dataPath
    $data.state_root = Join-Path ([IO.Path]::GetDirectoryName($configPath)) 'state'
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($configPath)) | Out-Null
    [IO.File]::WriteAllText($configPath, ($data | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
}
Write-Output '대표님 비서 환경을 준비했습니다. 기존 설정은 보존됩니다. 전용 봇과 WSL 실행 계정 설정 후 진단하세요.'
