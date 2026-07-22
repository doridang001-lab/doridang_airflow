param(
    [int]$Port = 9223,
    [string]$UserDataDir = "C:\tmp\flow_susam_profile",
    [string]$ProfileDirectory = "Default",
    [string]$PostUrl = "https://flow.team/l/QAa7L",
    [int]$StopHour = 11,
    [int]$StopMinute = 10,
    [int]$MaxRunMinutes = 135,
    [switch]$IgnoreDailyCutoff
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path -LiteralPath $chrome)) {
    $chrome = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path -LiteralPath $chrome)) {
    throw "chrome.exe was not found."
}
if (-not (Test-Path -LiteralPath (Join-Path $UserDataDir $ProfileDirectory))) {
    throw "Flow Chrome profile was not found: $UserDataDir\$ProfileDirectory"
}

$logDir = "C:\airflow\.tmp\flow_susam_chrome"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logPath = Join-Path $logDir ("flow_chrome_{0:yyyyMMdd}.log" -f (Get-Date))
$releaseMarker = "C:\Local_DB\flow_susam_chrome_release.json"
$windowStartUtc = [datetime]::UtcNow
$markerBaselineUtc = if (Test-Path -LiteralPath $releaseMarker) {
    (Get-Item -LiteralPath $releaseMarker).LastWriteTimeUtc
} else {
    [datetime]::MinValue
}

function Write-FlowLog {
    param([string]$Message)
    $line = "[{0:yyyy-MM-dd HH:mm:ss}] {1}" -f (Get-Date), $Message
    Add-Content -LiteralPath $logPath -Encoding utf8 -Value $line
    Write-Host $line
}

function Test-DevToolsEndpoint {
    param([int]$TargetPort)
    try {
        $response = Invoke-WebRequest `
            -Uri "http://127.0.0.1:$TargetPort/json/version" `
            -TimeoutSec 3 `
            -UseBasicParsing
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Wait-DevToolsEndpoint {
    param(
        [int]$TargetPort,
        [int]$TimeoutSeconds = 30
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-DevToolsEndpoint -TargetPort $TargetPort) {
            return
        }
        Start-Sleep -Seconds 1
    }
    throw "Chrome DevTools did not become ready in ${TimeoutSeconds}s: 127.0.0.1:$TargetPort"
}

function Stop-FlowChromeListener {
    param([int]$TargetPort)
    $listeners = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue
    foreach ($listener in $listeners) {
        $processId = [int]$listener.OwningProcess
        $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
        $commandLine = if ($null -ne $processInfo) { [string]$processInfo.CommandLine } else { "" }
        $isExpected = (
            $null -ne $processInfo -and
            $processInfo.Name -eq "chrome.exe" -and
            $commandLine.Contains("--remote-debugging-port=$TargetPort") -and
            $commandLine.Contains("--user-data-dir=$UserDataDir")
        )
        if ($isExpected) {
            Write-FlowLog "Stopping Flow Chrome: pid=$processId port=$TargetPort"
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        } else {
            Write-FlowLog "Unexpected port owner was not stopped: pid=$processId port=$TargetPort"
        }
    }
}

$now = Get-Date
$deadline = $now.AddMinutes($MaxRunMinutes)
$dailyCutoff = [datetime]::Today.AddHours($StopHour).AddMinutes($StopMinute)
if (-not $IgnoreDailyCutoff) {
    if ($now -ge $dailyCutoff) {
        Write-FlowLog "Daily cutoff already passed; Chrome will not start: cutoff=$dailyCutoff"
        exit 0
    }
    if ($dailyCutoff -lt $deadline) {
        $deadline = $dailyCutoff
    }
}

try {
    if (Test-DevToolsEndpoint -TargetPort $Port) {
        Write-FlowLog "Reusing existing Flow Chrome: port=$Port deadline=$deadline"
    } else {
        $chromeArgs = @(
            "--remote-debugging-port=$Port",
            "--remote-debugging-address=0.0.0.0",
            "--remote-allow-origins=*",
            "--user-data-dir=$UserDataDir",
            "--profile-directory=$ProfileDirectory",
            "--no-first-run",
            "--no-default-browser-check",
            "--start-minimized",
            $PostUrl
        )
        Start-Process -FilePath $chrome -ArgumentList $chromeArgs -WindowStyle Minimized
        Wait-DevToolsEndpoint -TargetPort $Port -TimeoutSeconds 30
        Write-FlowLog "Flow Chrome ready: port=$Port deadline=$deadline"
    }

    while ((Get-Date) -lt $deadline) {
        if (-not (Test-DevToolsEndpoint -TargetPort $Port)) {
            Write-FlowLog "DevTools closed: Airflow upload completed or Chrome exited"
            break
        }
        if (Test-Path -LiteralPath $releaseMarker) {
            $markerTimeUtc = (Get-Item -LiteralPath $releaseMarker).LastWriteTimeUtc
            if ($markerTimeUtc -gt $markerBaselineUtc -and $markerTimeUtc -ge $windowStartUtc) {
                Write-FlowLog "Airflow completion marker detected: $releaseMarker"
                break
            }
        }
        Start-Sleep -Seconds 15
    }
} finally {
    Stop-FlowChromeListener -TargetPort $Port
    Start-Sleep -Seconds 1
    if (Test-DevToolsEndpoint -TargetPort $Port) {
        Write-FlowLog "WARNING: Flow Chrome DevTools is still open: $Port"
    } else {
        Write-FlowLog "Flow Chrome window closed: port=$Port"
    }
}
