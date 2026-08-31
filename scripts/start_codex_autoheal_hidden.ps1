$ErrorActionPreference = "Stop"

$root = "C:\airflow"
$logPath = if ($env:CODEX_AUTOHEAL_WINDOWS_LOG) { $env:CODEX_AUTOHEAL_WINDOWS_LOG } else { "$root\logs\codex_autoheal_windows.log" }
$heartbeatPath = if ($env:AUTOHEAL_HEARTBEAT_PATH) { $env:AUTOHEAL_HEARTBEAT_PATH } else { "$root\logs\autoheal_heartbeat.json" }
$heartbeatMaxAgeSeconds = if ($env:CODEX_AUTOHEAL_HEARTBEAT_MAX_AGE_SECONDS) { [int]$env:CODEX_AUTOHEAL_HEARTBEAT_MAX_AGE_SECONDS } else { 180 }
$launchTimeoutSeconds = if ($env:CODEX_AUTOHEAL_LAUNCH_TIMEOUT_SECONDS) { [int]$env:CODEX_AUTOHEAL_LAUNCH_TIMEOUT_SECONDS } else { 60 }
$tmpDir = "$root\.tmp\autoheal_supervisor"
$wslScript = "bash /mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh"

New-Item -ItemType Directory -Force -Path (Split-Path $logPath -Parent) | Out-Null
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
Set-Location $root

function Write-AutohealLog([string]$Message) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
    "[$timestamp] $Message" | Add-Content -Path $logPath -Encoding utf8
}

function Get-FreshHeartbeat {
    if (-not (Test-Path -LiteralPath $heartbeatPath)) {
        return $null
    }
    try {
        $payload = Get-Content -LiteralPath $heartbeatPath -Encoding utf8 -Raw | ConvertFrom-Json
        $ts = [DateTimeOffset]::Parse($payload.ts)
        $age = ([DateTimeOffset]::UtcNow - $ts.ToUniversalTime()).TotalSeconds
        if ($age -lt $heartbeatMaxAgeSeconds) {
            if ($payload.backend -eq "windows") {
                $watcherProcess = Get-Process -Id ([int]$payload.pid) -ErrorAction SilentlyContinue
                if (-not $watcherProcess -or $watcherProcess.ProcessName -ne "python") {
                    Write-AutohealLog "heartbeat references dead Windows watcher pid=$($payload.pid)"
                    return $null
                }
            }
            return [pscustomobject]@{ Payload = $payload; Age = [int]$age }
        }
    }
    catch {
        Write-AutohealLog "heartbeat parse failed: $($_.Exception.Message)"
    }
    return $null
}

function Append-ProcessOutput([string]$Path, [string]$Label) {
    if (Test-Path -LiteralPath $Path) {
        $content = Get-Content -LiteralPath $Path -Encoding utf8 -Raw -ErrorAction SilentlyContinue
        if ($content) {
            Write-AutohealLog "$Label $($content.Trim())"
        }
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
    }
}

function Wait-ForBackend([string]$Backend) {
    $deadline = (Get-Date).AddSeconds($launchTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $heartbeat = Get-FreshHeartbeat
        if ($heartbeat -and $heartbeat.Payload.backend -eq $Backend) {
            Write-AutohealLog "watcher healthy backend=$Backend pid=$($heartbeat.Payload.pid) age_seconds=$($heartbeat.Age)"
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

$mutex = New-Object System.Threading.Mutex($false, "Local\AirflowCodexAutoHealSupervisor")
$acquired = $false
try {
    $acquired = $mutex.WaitOne(0)
    if (-not $acquired) {
        Write-AutohealLog "skip supervisor: another launcher is active"
        exit 0
    }

    $heartbeat = Get-FreshHeartbeat
    if ($heartbeat) {
        Write-AutohealLog "skip launcher backend=$($heartbeat.Payload.backend) heartbeat_age_seconds=$($heartbeat.Age)"
        exit 0
    }

    Write-AutohealLog "heartbeat stale; trying WSL watcher"
    $stdoutPath = Join-Path $tmpDir "wsl_stdout_$PID.log"
    $stderrPath = Join-Path $tmpDir "wsl_stderr_$PID.log"
    $wsl = Start-Process -FilePath "$env:WINDIR\System32\wsl.exe" `
        -ArgumentList @("-d", "Ubuntu", "-u", "myuser", "bash", "-lc", $wslScript) `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $wslCompleted = $wsl.WaitForExit($launchTimeoutSeconds * 1000)
    if (-not $wslCompleted) {
        Stop-Process -Id $wsl.Id -Force -ErrorAction SilentlyContinue
        Write-AutohealLog "WSL launcher timeout seconds=$launchTimeoutSeconds"
    }
    else {
        Write-AutohealLog "WSL launcher exited exit_code=$($wsl.ExitCode)"
    }
    Append-ProcessOutput $stdoutPath "WSL stdout:"
    Append-ProcessOutput $stderrPath "WSL stderr:"

    if ($wslCompleted -and $wsl.ExitCode -eq 0) {
        if (Wait-ForBackend "wsl") {
            exit 0
        }
    }

    Write-AutohealLog "WSL watcher unavailable; starting Windows fallback"
    $env:CODEX_AUTOHEAL_BACKEND = "windows"
    $env:CODEX_COMMAND = "$env:APPDATA\npm\codex.cmd"
    $env:HEAL_QUEUE_PATH = "$root\logs\heal_queue.jsonl"
    $env:HEAL_TASK_STATE_PATH = "$root\logs\heal_task_state.json"
    $env:AUTOHEAL_HEARTBEAT_PATH = $heartbeatPath
    $env:AIRFLOW_RUNTIME_WORKDIR = $root
    $env:CODEX_WORKDIR = "C:\tmp\airflow-autoheal"
    $pythonPath = "$root\.venv\Scripts\python.exe"
    Write-AutohealLog "Windows fallback running in foreground python=$pythonPath"
    $watcherStdout = Join-Path $tmpDir "windows_stdout_$PID.log"
    $watcherStderr = Join-Path $tmpDir "windows_stderr_$PID.log"
    $windowsWatcher = Start-Process -FilePath $pythonPath `
        -ArgumentList @("-X", "utf8", "$root\watch_heal_queue.py") `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $watcherStdout -RedirectStandardError $watcherStderr
    Write-AutohealLog "Windows fallback watcher pid=$($windowsWatcher.Id)"
    $windowsWatcher.WaitForExit()
    Append-ProcessOutput $watcherStdout "Windows stdout:"
    Append-ProcessOutput $watcherStderr "Windows stderr:"
    $watcherExitCode = $windowsWatcher.ExitCode
    if ($watcherExitCode -eq 0) {
        Write-AutohealLog "Windows fallback exited gracefully"
        exit 0
    }
    throw "Windows fallback exited exit_code=$watcherExitCode"
}
catch {
    Write-AutohealLog "supervisor failed: $($_.Exception.Message)"
    try {
        & python -X utf8 "$root\scripts\notify_autoheal_supervisor_failure.py" --reason $_.Exception.Message
    }
    catch {
        Write-AutohealLog "supervisor Telegram fallback failed: $($_.Exception.Message)"
    }
    exit 1
}
finally {
    if ($acquired) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
