param(
    [string]$ChromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe",
    [string]$ProfileDirectory = "Default",
    [string]$RunnerUrl = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1&mode=top50&date=today",
    [switch]$NewWindow = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $ChromePath)) {
    $ChromePath = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path $ChromePath)) {
    Write-Error "chrome.exe 를 찾을 수 없습니다."
    exit 1
}

$separator = if ($RunnerUrl.Contains("?")) { "&" } else { "?" }
$openUrl = "{0}{1}runTs={2}" -f $RunnerUrl, $separator, (Get-Date -Format "yyyyMMddHHmmss")

$args = @("--profile-directory=`"$ProfileDirectory`"")
if ($NewWindow) {
    $args += "--new-window"
}
$args += $openUrl

Start-Process -FilePath $ChromePath -ArgumentList $args -WindowStyle Normal
exit 0
