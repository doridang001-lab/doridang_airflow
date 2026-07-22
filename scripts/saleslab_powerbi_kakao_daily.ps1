param(
    [int]$StartupDelaySeconds = 0,
    [switch]$Send,
    [ValidateSet("enter", "click")]
    [string]$SendMode = "enter"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$runner = Join-Path $repoRoot "scripts\saleslab_powerbi_kakao_daily.py"
$logRoot = Join-Path $repoRoot ".tmp\saleslab_powerbi_kakao_daily"
$logPath = $null

function Initialize-RunLog {
    if (-not (Test-Path $logRoot)) {
        New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    }
    $script:logPath = Join-Path $logRoot ("{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    Start-Transcript -Path $script:logPath -Append | Out-Null
    Write-Host "로그 파일: $script:logPath"
}

function Write-Step {
    param([string]$Message)
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
}

Initialize-RunLog
try {
    Set-Location $repoRoot
    Write-Step "세일즈랩 Power BI 카카오톡 자동 준비 시작"
    Write-Step ("StartupDelaySeconds={0} Send={1} SendMode={2}" -f $StartupDelaySeconds, $Send.IsPresent, $SendMode)

    if (-not (Test-Path $runner)) {
        throw "실행 스크립트를 찾을 수 없습니다: $runner"
    }

    $pythonExe = $venvPython
    if (-not (Test-Path $pythonExe)) {
        $pythonExe = "python"
    }

    if ($StartupDelaySeconds -gt 0) {
        Write-Step ("시작 지연 대기: {0}s" -f $StartupDelaySeconds)
        Start-Sleep -Seconds $StartupDelaySeconds
    }

    $arguments = @("-X", "utf8", $runner, "--send-mode", $SendMode)
    if (-not $Send.IsPresent) {
        $arguments += "--dry-run"
    }

    Write-Step ("Python 실행: {0} {1}" -f $pythonExe, ($arguments -join " "))
    & $pythonExe @arguments
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "자동 발송 실패 exitCode=$exitCode"
    }

    Write-Step "세일즈랩 Power BI 카카오톡 자동 준비 정상 종료"
    exit 0
} catch {
    Write-Error ("세일즈랩 Power BI 카카오톡 자동 준비 실패: {0}" -f $_.Exception.Message)
    exit 1
} finally {
    if ($script:logPath) {
        Stop-Transcript | Out-Null
    }
}
