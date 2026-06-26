<#
Notion Calendar 수집용 브라우저 실행

사용법:
  1) powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\notion_calendar_host_chrome.ps1 -Browser Edge -UseDefaultProfile
  2) 열린 브라우저에서 Notion Calendar에 1회 로그인
  3) 창을 닫지 말고 최소화해 둠
  4) 로컬 수집 스크립트는 NOTION_CHROME_DEBUGGER=127.0.0.1:9223 으로 이 브라우저에 붙음

같은 브라우저 창/프로필이 계속 살아 있으면 매 실행마다 재로그인하지 않습니다.

평소 쓰는 Edge 로그인 세션을 쓰려면 Edge를 모두 닫은 뒤 아래처럼 실행합니다.
  powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\notion_calendar_host_chrome.ps1 -Browser Edge -UseDefaultProfile
#>

param(
    [ValidateSet("Chrome", "Whale", "Edge")]
    [string]$Browser = "Edge",
    [switch]$UseDefaultProfile,
    [string]$ProfileDir = "C:\notion_calendar_edge_profile",
    [string]$ProfileDirectory = "",
    [int]$Port = 9223,
    [string]$RemoteDebugAddress = "127.0.0.1"
)

[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
try {
    chcp 65001 | Out-Null
} catch {
}

$browserExe = $null
$processName = $null
$defaultUserData = $null

if ($Browser -eq "Whale") {
    $processName = "whale"
    $defaultUserData = Join-Path $env:LOCALAPPDATA "Naver\Naver Whale\User Data"
    $candidates = @(
        "C:\Program Files\Naver\Naver Whale\Application\whale.exe",
        "C:\Program Files (x86)\Naver\Naver Whale\Application\whale.exe",
        (Join-Path $env:LOCALAPPDATA "Naver\Naver Whale\Application\whale.exe")
    )
} elseif ($Browser -eq "Edge") {
    $processName = "msedge"
    $defaultUserData = Join-Path $env:LOCALAPPDATA "Microsoft\Edge\User Data"
    $candidates = @(
        "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        "C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        (Join-Path $env:LOCALAPPDATA "Microsoft\Edge\Application\msedge.exe")
    )
} else {
    $processName = "chrome"
    $defaultUserData = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data"
    $candidates = @(
        "C:\Program Files\Google\Chrome\Application\chrome.exe",
        "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    )
}

foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path $candidate)) {
        $browserExe = $candidate
        break
    }
}
if (-not $browserExe) {
    Write-Error "$Browser 실행 파일을 찾을 수 없습니다."
    exit 1
}

if ($UseDefaultProfile) {
    $ProfileDir = $defaultUserData
    if (-not $ProfileDirectory) {
        $ProfileDirectory = "Default"
    }
}

$inUse = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $Port 로 Notion Calendar 브라우저가 떠 있습니다." -ForegroundColor Yellow
    exit 0
}

if ($UseDefaultProfile) {
    $browserRunning = Get-Process -Name $processName -ErrorAction SilentlyContinue
    if ($browserRunning) {
        Write-Error "평소 쓰는 $Browser 프로필을 쓰려면 먼저 모든 $Browser 창을 닫고 다시 실행하세요. 이미 실행 중이면 디버그 포트 옵션을 무시할 수 있습니다."
        exit 1
    }
}

$chromeArgs = @(
    "--remote-debugging-port=$Port",
    "--remote-debugging-address=$RemoteDebugAddress",
    "--remote-allow-origins=*",
    "--user-data-dir=$ProfileDir",
    "--no-first-run",
    "--no-default-browser-check",
    "--new-window",
    "https://calendar.notion.so/$((Get-Date).Year)/$((Get-Date).Month)"
)
if ($ProfileDirectory) {
    $chromeArgs = @("--profile-directory=$ProfileDirectory") + $chromeArgs
}

Write-Host "Notion Calendar $Browser 실행 (debug port $Port, profile $ProfileDir)" -ForegroundColor Green
if ($ProfileDirectory) {
    Write-Host "$Browser profile directory: $ProfileDirectory" -ForegroundColor Cyan
}
Start-Process -FilePath $browserExe -WindowStyle Normal -ArgumentList $chromeArgs
Write-Host "열린 $Browser 에서 Notion Calendar에 로그인한 뒤 창을 닫지 마세요." -ForegroundColor Green
