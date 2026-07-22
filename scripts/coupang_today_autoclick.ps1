param(
    [int]$StartupDelaySeconds = 0,
    [string]$ButtonId = "topHalfTodayBtn"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$hostScript = Join-Path $repoRoot "scripts\coupang_host_chrome.ps1"
$autoClickScript = Join-Path $repoRoot "scripts\coupang_runner_autoclick.py"
$logRoot = Join-Path $repoRoot ".tmp\coupang_today_autoclick"
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

function Wait-DevToolsEndpoint {
    param(
        [string]$Endpoint = "http://127.0.0.1:9222/json",
        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Endpoint -TimeoutSec 3 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Step "Chrome DevTools endpoint ready: $Endpoint"
                return
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    throw "Chrome DevTools endpoint not ready within ${TimeoutSeconds}s: $Endpoint"
}

Initialize-RunLog
try {
    Write-Step "Coupang today autoclick 시작"
    Write-Step ("ButtonId={0} StartupDelaySeconds={1}" -f $ButtonId, $StartupDelaySeconds)

    if (-not (Test-Path $venvPython)) {
        Write-Error "Python 경로가 존재하지 않습니다: $venvPython"
        exit 1
    }
    if (-not (Test-Path $hostScript)) {
        Write-Error "Chrome 실행 스크립트를 찾을 수 없습니다: $hostScript"
        exit 1
    }
    if (-not (Test-Path $autoClickScript)) {
        Write-Error "자동 클릭 스크립트를 찾을 수 없습니다: $autoClickScript"
        exit 1
    }

    Set-Location $repoRoot
    if ($StartupDelaySeconds -gt 0) {
        Write-Step ("시작 지연 대기: {0}s" -f $StartupDelaySeconds)
        Start-Sleep -Seconds $StartupDelaySeconds
    }

    Write-Step ("host chrome 실행 확인: {0}" -f $hostScript)
    & powershell -NoProfile -ExecutionPolicy Bypass -File $hostScript
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne $null) {
        Write-Error ("coupang_host_chrome.ps1 실행 실패: {0}" -f $LASTEXITCODE)
        exit 1
    }
    Wait-DevToolsEndpoint

    Write-Step ("runner Today 버튼 자동 클릭 실행: {0} #{1}" -f $autoClickScript, $ButtonId)
    & $venvPython $autoClickScript --button-id $ButtonId
    $clickExit = $LASTEXITCODE
    if ($clickExit -ne 0) {
        Write-Error ("coupang_runner_autoclick.py 실행 실패: {0}" -f $clickExit)
        exit $clickExit
    }

    Write-Step "Coupang today autoclick 정상 종료"
    exit 0
} catch {
    Write-Error ("Coupang today autoclick 실패: {0}" -f $_.Exception.Message)
    exit 1
} finally {
    if ($script:logPath) {
        Stop-Transcript | Out-Null
    }
}
