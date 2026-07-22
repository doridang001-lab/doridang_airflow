<#
쿠팡 수집용 "진짜 크롬" 띄우기 (호스트, 1회 준비)

Docker/Xvfb 크롬은 Akamai 봇탐지에 걸려 로그인이 거부됩니다("권한이 존재하지 않습니다").
수동 크롬은 통과하므로, 자동화가 이 실제 크롬에 attach 하도록 디버그 포트로 띄웁니다.

사용법:
  1) 이 스크립트 실행:  powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1
  2) doridang 계정으로 로그인된 기본 Chrome 프로필(Default)을 사용
  3) 창은 최소화해도 됨 — 수집은 백그라운드로 진행, 다른 작업 가능
  4) 닫지 말 것 (닫으면 수집 시 재로그인 필요)

Docker Airflow에서는 Chrome DevTools Host 헤더 제한 때문에 Docker Desktop 호스트 IP(예: 192.168.65.254:9222)로 이 크롬에 attach합니다.
doridang 계정(doridang001@gmail.com)이 로그인된 기본 Chrome 프로필(Default)만 사용합니다.
#>

$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) {
    $chrome = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path $chrome)) {
    Write-Error "chrome.exe 를 찾을 수 없습니다. 경로를 직접 지정하세요."
    exit 1
}

$userDataDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data"
$profileDirectory = "Default"
$port = 9222
$extDir = "C:\airflow\coupang_extension_build"
$requiredExtFiles = @("manifest.json", "runner.html", "runner.js")

if (-not (Test-Path $extDir)) {
    Write-Error "확장 경로가 존재하지 않습니다: $extDir"
    exit 1
}

if (-not (Test-Path (Join-Path $userDataDir $profileDirectory))) {
    Write-Error "doridang Chrome 프로필을 찾을 수 없습니다: $userDataDir\$profileDirectory"
    exit 1
}

$localStatePath = Join-Path $userDataDir "Local State"
if (Test-Path $localStatePath) {
    try {
        $localState = Get-Content -Path $localStatePath -Encoding utf8 -Raw | ConvertFrom-Json
        $profileInfo = $localState.profile.info_cache.$profileDirectory
        if ($profileInfo.user_name -and $profileInfo.user_name -ne "doridang001@gmail.com") {
            Write-Error "Default Chrome 프로필이 doridang 계정이 아닙니다: $($profileInfo.user_name)"
            exit 1
        }
    } catch {
        Write-Warning "Chrome Local State 프로필 확인을 건너뜁니다: $($_.Exception.Message)"
    }
}
foreach ($file in $requiredExtFiles) {
    $path = Join-Path $extDir $file
    if (-not (Test-Path $path)) {
        Write-Error "확장 필수 파일이 없습니다: $path"
        exit 1
    }
}

function Wait-DevToolsEndpoint {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 30
    )

    $endpoint = "http://127.0.0.1:$Port/json"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $endpoint -TimeoutSec 3 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Host "DevTools endpoint ready: $endpoint" -ForegroundColor Green
                return
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    Write-Error "DevTools endpoint가 ${TimeoutSeconds}초 안에 준비되지 않았습니다: $endpoint"
    exit 1
}

# 이미 같은 포트로 떠 있으면 중복 실행 방지
$inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $port 로 크롬이 떠 있습니다. 새로 띄우지 않습니다." -ForegroundColor Yellow
    Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 10
    exit 0
}

$chromeArgs = @(
    "--remote-debugging-port=$port",
    "--remote-debugging-address=0.0.0.0",
    "--remote-allow-origins=*",
    "--user-data-dir=$userDataDir",
    "--profile-directory=$profileDirectory",
    "--no-first-run",
    "--no-default-browser-check"
)
$chromeArgs += "--load-extension=$extDir"
Write-Host "확장 로드: $extDir" -ForegroundColor Cyan
$chromeArgs += "about:blank"

Write-Host "doridang Chrome 실행 (debug port $port, profile $profileDirectory, user-data-dir $userDataDir)" -ForegroundColor Green
Start-Process -FilePath $chrome -WindowStyle Normal -ArgumentList $chromeArgs
Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 30
Write-Host "완료. 확장이 실제 ID의 runner.html을 열고 topHalfBtn 자동 클릭 후 수집이 시작됩니다." -ForegroundColor Green
