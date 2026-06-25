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

function Get-PatternCounts {
    param([string[]]$Paths)
    $patterns = @("coupangeats_orders_*.csv","coupangeats_cmg_*.csv","coupangeats_options_*.csv")
    $parts = [System.Collections.Generic.List[string]]::new()
    foreach ($dir in $Paths) {
        if (-not (Test-Path $dir)) {
            $parts.Add("${dir}:missing")
            continue
        }
        foreach ($pattern in $patterns) {
            $count = @(Get-ChildItem -Path $dir -Filter $pattern -File -ErrorAction SilentlyContinue).Count
            $parts.Add("${dir}\${pattern}=${count}")
        }
    }
    return ($parts -join "; ")
}

function Log-ChromeDownloadDiagnostics {
    param([string]$PythonPath)

    $script = @"
from pathlib import Path
import json
import shutil
import sqlite3
import tempfile

profile = Path(r"C:\coupang_chrome_profile")
prefs = profile / "Default" / "Preferences"
if prefs.exists():
    try:
        data = json.loads(prefs.read_text(encoding="utf-8"))
        download = data.get("download", {})
        print("[DIAG] chrome download prefs: default_directory=%s directory_upgrade=%s" % (
            download.get("default_directory", ""),
            download.get("directory_upgrade", ""),
        ))
    except Exception as exc:
        print("[DIAG] chrome download prefs read failed: %s" % exc)
else:
    print("[DIAG] chrome download prefs missing: %s" % prefs)

history = profile / "Default" / "History"
if not history.exists():
    print("[DIAG] chrome history missing: %s" % history)
    raise SystemExit(0)

tmp = Path(tempfile.gettempdir()) / "coupang_chrome_history_diag_copy"
try:
    shutil.copy2(history, tmp)
    con = sqlite3.connect(str(tmp))
    cur = con.cursor()
    rows = list(cur.execute(
        "select guid, target_path, mime_type, received_bytes from downloads "
        "order by start_time desc limit 10"
    ))
    con.close()
finally:
    try:
        tmp.unlink()
    except Exception:
        pass

bad = [row for row in rows if "playwright-artifacts" in str(row[1])]
if bad:
    print("[DIAG][WARNING] latest downloads include playwright-artifacts paths: %d/10" % len(bad))
for guid, target_path, mime_type, received_bytes in rows[:5]:
    print("[DIAG] recent download: guid=%s path=%s mime=%s bytes=%s" % (
        guid,
        target_path,
        mime_type,
        received_bytes,
    ))
"@

    $ErrorActionPreference = "Continue"
    $diagOut = & $PythonPath -X utf8 -c $script 2>&1
    $diagExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    foreach ($line in $diagOut) { Log "$line" }
    if ($diagExit -ne 0) {
        Log "[WARNING] Chrome download diagnostics failed: $diagExit"
    }
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
Log "Pattern counts before: $(Get-PatternCounts -Paths $collectDirs)"
Log-ChromeDownloadDiagnostics -PythonPath $venvPython

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
            Log "Pattern counts current: $(Get-PatternCounts -Paths $collectDirs)"
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

Log "Pattern counts after: $(Get-PatternCounts -Paths $collectDirs)"
Log-ChromeDownloadDiagnostics -PythonPath $venvPython

try {
    Trigger-CoupangMacroDag -DagId $AirflowDag -SchedulerContainer $AirflowSchedulerContainer
    Log "DAG triggered: $AirflowDag"
} catch {
    Log "[WARNING] DAG trigger failed: $($_.Exception.Message)"
}
Log "=== DONE ==="
exit 0
