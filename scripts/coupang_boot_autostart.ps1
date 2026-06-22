param(
    [int]$StartupDelaySeconds = 60,
    [int]$CollectionWaitSeconds = 1200,
    [int]$CollectionStableSeconds = 60,
    [string]$AirflowDag = "DB_CoupangMacro_Load_Dags",
    [string]$AirflowSchedulerContainer = "airflow-airflow-scheduler-1"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\airflow"
$venvPython = "C:\airflow\.venv\Scripts\python.exe"
$hostScript = Join-Path $repoRoot "scripts\coupang_host_chrome.ps1"
$autoClickScript = Join-Path $repoRoot "scripts\coupang_runner_autoclick.py"
$logFile = Join-Path $repoRoot ".tmp\coupang_boot_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

$null = New-Item -ItemType Directory -Force -Path (Split-Path $logFile)
function Log([string]$msg) {
    $line = "$(Get-Date -Format 'HH:mm:ss') $msg"
    Write-Host $line
    Add-Content -Path $logFile -Value $line -Encoding utf8
}

Log "=== coupang_boot_autostart START ==="

if (-not (Test-Path $venvPython)) {
    Log "[ERROR] Python not found: $venvPython"
    exit 1
}

function Get-CollectDirs {
    $dirs = [System.Collections.Generic.List[string]]::new()
    $down = "E:\down"
    if (Test-Path $down) { $dirs.Add($down) }

    if ($env:COLLECT_DB -and (Test-Path $env:COLLECT_DB)) {
        $dirs.Add((Join-Path $env:COLLECT_DB "collect"))
        return $dirs.ToArray()
    }

    $userHome = [Environment]::GetFolderPath("UserProfile")
    foreach ($suffix in @("OneDrive - ã¹¹ã¹¹", "OneDrive")) {
        $base = Join-Path $userHome $suffix
        if (Test-Path $base) {
            $collect = Join-Path $base "Collect_Data\collect"
            if (Test-Path $collect) { $dirs.Add($collect) }
        }
    }
    return $dirs.ToArray()
}

function Get-RawCounts {
    param([string[]]$Paths)
    $patterns = @("coupangeats_orders_*.csv","coupangeats_cmg_*.csv","coupangeats_options_*.csv")
    $total = 0
    foreach ($dir in $Paths) {
        foreach ($pattern in $patterns) {
            $total += @(Get-ChildItem -Path $dir -Filter $pattern -File -ErrorAction SilentlyContinue).Count
        }
    }
    return $total
}

function Trigger-CoupangMacroDag {
    param([string]$DagId, [string]$SchedulerContainer)
    $hasDocker = Get-Command docker -ErrorAction SilentlyContinue
    if ($hasDocker) {
        $running = docker ps --filter "name=$SchedulerContainer" --filter "status=running" --format "{{.Names}}"
        if ($running -and $running.Trim()) {
            Log "Triggering DAG via Docker: $DagId"
            docker exec $SchedulerContainer airflow dags trigger $DagId
            return
        }
    }
    $hasAirflow = Get-Command airflow -ErrorAction SilentlyContinue
    if (-not $hasAirflow -and (Test-Path $venvPython)) {
        & $venvPython -m airflow dags trigger $DagId; return
    }
    if ($hasAirflow) { airflow dags trigger $DagId; return }
    Log "[WARNING] Airflow CLI not found, skipping DAG trigger"
}

Set-Location $repoRoot
if ($StartupDelaySeconds -gt 0) {
    Log "Waiting ${StartupDelaySeconds}s for boot..."
    Start-Sleep -Seconds $StartupDelaySeconds
}

Log "Starting Chrome..."
$chromeOut = & powershell -ExecutionPolicy Bypass -File $hostScript 2>&1
foreach ($line in $chromeOut) { Log "$line" }
if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne $null) {
    Log "[ERROR] coupang_host_chrome.ps1 failed: $LASTEXITCODE"
    exit 1
}

Log "Waiting for Chrome port 9222..."
$portReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    $conn = Get-NetTCPConnection -LocalPort 9222 -State Listen -ErrorAction SilentlyContinue
    if ($conn) { $portReady = $true; Log "Port 9222 ready (attempt $i)"; break }
}
if (-not $portReady) { Log "[ERROR] Port 9222 not open after 60s"; exit 1 }

$collectDirs = @(Get-CollectDirs)
$before = Get-RawCounts -Paths $collectDirs
Log "Files before collection: $before"

Log "Running autoclick..."
$ErrorActionPreference = "Continue"
$acOut = & $venvPython $autoClickScript 2>&1
$clickExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
foreach ($line in $acOut) { Log "$line" }
if ($clickExit -ne 0) { Log "[ERROR] autoclick failed: $clickExit"; exit $clickExit }
Log "topHalfBtn clicked OK"

if ($CollectionWaitSeconds -gt 0) {
    $deadline = (Get-Date).AddSeconds($CollectionWaitSeconds)
    $lastChanged = (Get-Date)
    while ((Get-Date) -lt $deadline) {
        $nowCount = Get-RawCounts -Paths $collectDirs
        if ($nowCount -gt $before) {
            Log "Files increased: $before -> $nowCount"
            $before = $nowCount
            $lastChanged = Get-Date
        } else {
            $stableSeconds = ((Get-Date) - $lastChanged).TotalSeconds
            if ($stableSeconds -ge $CollectionStableSeconds) {
                Log "Collection stable for ${CollectionStableSeconds}s, done waiting"
                break
            }
        }
        Start-Sleep -Seconds 5
    }
}

try {
    Trigger-CoupangMacroDag -DagId $AirflowDag -SchedulerContainer $AirflowSchedulerContainer
    Log "DAG triggered: $AirflowDag"
} catch {
    Log "[WARNING] DAG trigger failed: $($_.Exception.Message)"
}
Log "=== DONE ==="
exit 0