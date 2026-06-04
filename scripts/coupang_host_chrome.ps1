<#
쿠팡 수집용 "진짜 크롬" 띄우기 (호스트, 1회 준비)

Docker/Xvfb 크롬은 Akamai 봇탐지에 걸려 로그인이 거부됩니다("권한이 존재하지 않습니다").
수동 크롬은 통과하므로, 자동화가 이 실제 크롬에 attach 하도록 디버그 포트로 띄웁니다.

사용법:
  1) 이 스크립트 실행:  powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1
  2) 뜬 크롬 창에서 쿠팡이츠에 1회 로그인 (세션은 전용 프로필에 유지됨)
  3) 창은 최소화해도 됨 — 수집은 백그라운드로 진행, 다른 작업 가능
  4) 닫지 말 것 (닫으면 수집 시 재로그인 필요)

Docker Airflow에서는 Chrome DevTools Host 헤더 제한 때문에 Docker Desktop 호스트 IP(예: 192.168.65.254:9222)로 이 크롬에 attach합니다.
전용 프로필(C:\coupang_chrome_profile)을 쓰므로 평소 쓰는 크롬과 분리됩니다.
#>

$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) {
    $chrome = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path $chrome)) {
    Write-Error "chrome.exe 를 찾을 수 없습니다. 경로를 직접 지정하세요."
    exit 1
}

$profileDir = "C:\coupang_chrome_profile"
$port = 9222

# 이미 같은 포트로 떠 있으면 중복 실행 방지
$inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $port 로 크롬이 떠 있습니다. 새로 띄우지 않습니다." -ForegroundColor Yellow
    exit 0
}

Write-Host "전용 크롬 실행 (debug port $port, profile $profileDir)" -ForegroundColor Green
Start-Process -FilePath $chrome -WindowStyle Minimized -ArgumentList @(
    "--remote-debugging-port=$port",
    "--remote-debugging-address=0.0.0.0",
    "--remote-allow-origins=*",
    "--user-data-dir=$profileDir",
    "--no-first-run",
    "--no-default-browser-check",
    "https://store.coupangeats.com/merchant/login"
)
Write-Host "완료. 뜬 크롬에서 쿠팡 로그인 1회 후 최소화해 두세요. (닫지 마세요)" -ForegroundColor Green
