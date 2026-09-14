param(
    [string]$RepoRoot = "C:\airflow",
    [int]$Port = 8788,
    [int]$HealthTimeoutSeconds = 40,
    [switch]$Restart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 도리당봇(8788)은 지금까지 수동 실행에만 의존했다. 이 스크립트가 기동 진입점이다.
# Ollama가 127.0.0.1에만 바인딩되므로 반드시 Ollama와 같은 PC에서 실행해야 한다.

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    $python = "python"
}

$listening = @()
try {
    $listening = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop)
} catch {
    $listening = @()
}

if ($listening.Count -gt 0) {
    if (-not $Restart) {
        Write-Host "Doridang bot is already listening on port $Port (PID $($listening[0].OwningProcess))."
        exit 0
    }
    foreach ($conn in $listening) {
        Write-Host "Stopping existing listener PID $($conn.OwningProcess)"
        try { Stop-Process -Id $conn.OwningProcess -Force -ErrorAction Stop } catch {}
    }
    Start-Sleep -Seconds 2
}

$logDir = Join-Path $RepoRoot "logs"
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}
$stamp = Get-Date -Format "yyyyMMdd"
$stdout = Join-Path $logDir "doridang_bot_$stamp.log"
$stderr = Join-Path $logDir "doridang_bot_$stamp.err.log"

Write-Host "Starting Doridang bot: $python -m modules.transform.doridang_bot.server"
Start-Process `
    -FilePath $python `
    -ArgumentList "-m", "modules.transform.doridang_bot.server" `
    -WorkingDirectory $RepoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr | Out-Null

$healthUrl = "http://127.0.0.1:$Port/health"
$deadline = (Get-Date).AddSeconds($HealthTimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 2
    try {
        $response = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -eq 200) {
            Write-Host "Doridang bot is healthy: $healthUrl"
            Write-Host "Log: $stdout"
            exit 0
        }
    } catch {
        continue
    }
}

Write-Warning "Health check did not pass within $HealthTimeoutSeconds seconds. Check $stderr"
exit 1
