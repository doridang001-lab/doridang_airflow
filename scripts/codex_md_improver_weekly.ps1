param(
    [string]$RepoRoot = "C:\airflow",
    [string]$LogRoot = "C:\airflow\.tmp\codex-md-improver"
)

$ErrorActionPreference = "Stop"
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null

$reportPath = Join-Path $LogRoot "report_$timestamp.md"
$promptPath = Join-Path $LogRoot "prompt_$timestamp.txt"
$stdoutPath = Join-Path $LogRoot "stdout_$timestamp.log"
$stderrPath = Join-Path $LogRoot "stderr_$timestamp.log"

$codex = Get-Command codex.cmd -ErrorAction SilentlyContinue
$codexPath = if ($codex) { $codex.Source } else { Join-Path $env:APPDATA "npm\codex.cmd" }
if (-not (Test-Path -LiteralPath $codexPath)) {
    throw "codex.cmd not found. Install Codex CLI or ensure npm global bin is on PATH."
}

$prompt = @"
`$codex-md-improver

C:\airflow 저장소의 Codex guidance 파일을 주기 점검하세요.

규칙:
- 파일을 수정하지 마세요.
- 커밋, git add, 포맷터, 코드 생성, PR 생성은 하지 마세요.
- AGENTS.md, CODEX.md, CLAUDE.md, 중첩 AGENTS.md/CLAUDE.md를 감사하세요.
- Codex Guidance Quality Report 형식으로 결과를 작성하세요.
- 필요한 경우 targeted update proposal/diff만 제안하세요.
- 항상 한글로만 답하세요.
- OneDrive는 수정하지 마세요.
"@

Set-Content -Path $promptPath -Value $prompt -Encoding utf8

Push-Location $RepoRoot
try {
    $promptText = Get-Content -Path $promptPath -Raw -Encoding utf8
    $promptText | & $codexPath exec `
        -C $RepoRoot `
        -s read-only `
        -a never `
        -o $reportPath `
        - `
        > $stdoutPath `
        2> $stderrPath

    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        Write-Host "Codex MD improver failed with exit code $exitCode"
        Write-Host "stdout: $stdoutPath"
        Write-Host "stderr: $stderrPath"
        exit $exitCode
    }

    Write-Host "Codex MD improver report created: $reportPath"
    Write-Host "stdout: $stdoutPath"
    Write-Host "stderr: $stderrPath"
}
finally {
    Pop-Location
}
