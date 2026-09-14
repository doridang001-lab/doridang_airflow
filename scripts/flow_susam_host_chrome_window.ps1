param(
    [int]$Port = 9223,
    [string]$UserDataDir = "C:\tmp\flow_susam_profile",
    [string]$ProfileDirectory = "Default",
    [string]$PostUrl = "https://flow.team/l/QAa7L",
    [int]$StopHour = 11,
    [int]$StopMinute = 10,
    [int]$MaxRunMinutes = 135,
    [int]$MaxRestarts = 3,
    [int]$RestartDelaySeconds = 10,
    [int]$CompletionGraceSeconds = 30,
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
$chromeLogPath = Join-Path $logDir ("chrome_debug_{0:yyyyMMdd}.log" -f (Get-Date))
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

function Test-NewCompletionMarker {
    if (-not (Test-Path -LiteralPath $releaseMarker)) {
        return $false
    }
    $markerTimeUtc = (Get-Item -LiteralPath $releaseMarker).LastWriteTimeUtc
    return $markerTimeUtc -gt $markerBaselineUtc -and $markerTimeUtc -ge $windowStartUtc
}

function Start-FlowChrome {
    param(
        [int]$TargetPort,
        [string]$Reason
    )
    $chromeArgs = @(
        "--remote-debugging-port=$TargetPort",
        "--remote-debugging-address=0.0.0.0",
        "--remote-allow-origins=*",
        "--user-data-dir=$UserDataDir",
        "--profile-directory=$ProfileDirectory",
        "--no-first-run",
        "--no-default-browser-check",
        "--start-minimized",
        "--enable-logging",
        "--log-file=$chromeLogPath",
        $PostUrl
    )
    $process = Start-Process `
        -FilePath $chrome `
        -ArgumentList $chromeArgs `
        -WindowStyle Minimized `
        -PassThru
    Wait-DevToolsEndpoint -TargetPort $TargetPort -TimeoutSeconds 30
    Write-FlowLog "Flow Chrome ready: pid=$($process.Id) port=$TargetPort reason=$Reason deadline=$deadline"
}

function Wait-CompletionGrace {
    param([int]$TimeoutSeconds)
    $graceDeadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $graceDeadline) {
        if (Test-NewCompletionMarker) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Get-FlowChromeProcesses {
    $plainProfileArg = "--user-data-dir=$UserDataDir"
    $quotedProfileArg = "--user-data-dir=`"$UserDataDir`""
    return @(
        Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" -ErrorAction SilentlyContinue |
            Where-Object {
                $commandLine = [string]$_.CommandLine
                $commandLine.Contains($plainProfileArg) -or $commandLine.Contains($quotedProfileArg)
            }
    )
}

function Stop-FlowChromeProcesses {
    param([int]$TargetPort)
    $processes = @(Get-FlowChromeProcesses)
    if ($processes.Count -eq 0) {
        return
    }
    Write-FlowLog "Stopping dedicated Flow Chrome processes: count=$($processes.Count) port=$TargetPort"
    foreach ($processInfo in ($processes | Sort-Object ParentProcessId -Descending)) {
        Write-FlowLog "Stopping Flow Chrome process: pid=$($processInfo.ProcessId) ppid=$($processInfo.ParentProcessId)"
        Stop-Process -Id ([int]$processInfo.ProcessId) -Force -ErrorAction SilentlyContinue
    }
    $stopDeadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $stopDeadline) {
        if (@(Get-FlowChromeProcesses).Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Dedicated Flow Chrome processes did not stop within 10s: $UserDataDir"
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

$completed = $false
$restartCount = 0
try {
    if (Test-DevToolsEndpoint -TargetPort $Port) {
        Write-FlowLog "Reusing existing Flow Chrome: port=$Port deadline=$deadline"
    } else {
        Stop-FlowChromeProcesses -TargetPort $Port
        Start-FlowChrome -TargetPort $Port -Reason "initial"
    }

    while ((Get-Date) -lt $deadline) {
        if (Test-NewCompletionMarker) {
            $completed = $true
            Write-FlowLog "Airflow completion marker detected: $releaseMarker"
            break
        }

        if (-not (Test-DevToolsEndpoint -TargetPort $Port)) {
            Write-FlowLog "DevTools closed unexpectedly; waiting ${CompletionGraceSeconds}s for completion marker"
            if (Wait-CompletionGrace -TimeoutSeconds $CompletionGraceSeconds) {
                $completed = $true
                Write-FlowLog "Airflow completion marker detected during grace period: $releaseMarker"
                break
            }

            if ($restartCount -ge $MaxRestarts) {
                throw "Flow Chrome restart limit exceeded: restarts=$restartCount max=$MaxRestarts"
            }
            $restartCount++
            Write-FlowLog "Restarting Flow Chrome: attempt=$restartCount/$MaxRestarts delay=${RestartDelaySeconds}s"
            if ($RestartDelaySeconds -gt 0) {
                Start-Sleep -Seconds $RestartDelaySeconds
            }
            try {
                Stop-FlowChromeProcesses -TargetPort $Port
                Start-FlowChrome -TargetPort $Port -Reason "restart-$restartCount"
            } catch {
                Write-FlowLog "Flow Chrome restart failed: attempt=$restartCount error=$($_.Exception.Message)"
            }
            continue
        }
        Start-Sleep -Seconds 15
    }
    if (-not $completed) {
        throw "Flow Chrome completion marker was not detected before deadline: $deadline"
    }
} finally {
    Stop-FlowChromeProcesses -TargetPort $Port
    Start-Sleep -Seconds 1
    if (Test-DevToolsEndpoint -TargetPort $Port) {
        Write-FlowLog "WARNING: Flow Chrome DevTools is still open: $Port"
    } else {
        Write-FlowLog "Flow Chrome window closed: port=$Port"
    }
}
