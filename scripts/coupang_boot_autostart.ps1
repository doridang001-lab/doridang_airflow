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

if (-not (Test-Path $venvPython)) {
    Write-Error "Python 경로가 존재하지 않습니다: $venvPython"
    exit 1
}

function Get-CollectDirs {
    $dirs = @()
    $down = "E:\down"
    if (Test-Path $down) { $dirs += $down }

    if ($env:COLLECT_DB -and (Test-Path $env:COLLECT_DB)) {
        $dirs += Join-Path $env:COLLECT_DB "영업관리부_수집"
        return ($dirs | Select-Object -Unique)
    }

    $home = [Environment]::GetFolderPath("UserProfile")
    $onedriveCandidates = @(
        (Join-Path $home "OneDrive - 주식회사 도리당"),
        (Join-Path $home "OneDrive - 도리당")
    )
    foreach ($base in $onedriveCandidates) {
        if (Test-Path $base) {
            $collect = Join-Path $base "Collect_Data\영업관리부_수집"
            if (Test-Path $collect) {
                $dirs += $collect
            }
        }
    }
    return ($dirs | Select-Object -Unique)
}

function Get-RawCounts {
    param([string[]]$Paths)
    $patterns = @("coupangeats_orders_*.csv","coupangeats_cmg_*.csv","coupangeats_options_*.csv")
    $total = 0
    foreach ($dir in $Paths) {
        foreach ($pattern in $patterns) {
            $total += (Get-ChildItem -Path $dir -Filter $pattern -File -ErrorAction SilentlyContinue).Count
        }
    }
    return $total
}

function Trigger-CoupangMacroDag {
    param([string]$DagId, [string]$SchedulerContainer)

    # 1) docker scheduler if available
    $hasDocker = Get-Command docker -ErrorAction SilentlyContinue
    if ($hasDocker) {
        $running = docker ps --filter "name=$SchedulerContainer" --filter "status=running" --format "{{.Names}}"
        if ($running -and $running.Trim()) {
            Write-Host "Trigger DAG in Docker scheduler: $DagId ($SchedulerContainer)"
            docker exec $SchedulerContainer airflow dags trigger $DagId
            return
        }
    }

    # 2) fallback local airflow cli
    $hasLocalAirflow = Get-Command airflow -ErrorAction SilentlyContinue
    if (-not $hasLocalAirflow -and (Test-Path $venvPython)) {
        & $venvPython -m airflow dags trigger $DagId
        return
    }

    if ($hasLocalAirflow) {
        airflow dags trigger $DagId
        return
    }

    Write-Warning "Airflow CLI를 찾지 못해 DAG 트리거를 건너뜁니다."
}

Set-Location $repoRoot
if ($StartupDelaySeconds -gt 0) { Start-Sleep -Seconds $StartupDelaySeconds }

& powershell -ExecutionPolicy Bypass -File $hostScript
if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne $null) {
    Write-Error "coupang_host_chrome.ps1 실행 실패: $LASTEXITCODE"
    exit 1
}

$collectDirs = Get-CollectDirs
$before = Get-RawCounts -Paths $collectDirs
Write-Host "시작 시 수집 파일 수: $before개"
& $venvPython $autoClickScript
$clickExit = $LASTEXITCODE
if ($clickExit -ne 0) {
    Write-Error "coupang_runner_autoclick.py 실행 실패: $clickExit"
    exit $clickExit
}

if ($CollectionWaitSeconds -gt 0) {
    $deadline = (Get-Date).AddSeconds($CollectionWaitSeconds)
    $lastChanged = (Get-Date)
    while ((Get-Date) -lt $deadline) {
        $nowCount = Get-RawCounts -Paths $collectDirs
        if ($nowCount -gt $before) {
            Write-Host "수집 파일 증가 감지: $before -> $nowCount"
            $before = $nowCount
            $lastChanged = Get-Date
        } else {
            $stableSeconds = ((Get-Date) - $lastChanged).TotalSeconds
            if ($stableSeconds -ge $CollectionStableSeconds) {
                Write-Host "수집 원본 파일이 $CollectionStableSeconds초 동안 안정 상태입니다."
                break
            }
        }
        Start-Sleep -Seconds 5
    }
}

try {
    Trigger-CoupangMacroDag -DagId $AirflowDag -SchedulerContainer $AirflowSchedulerContainer
    Write-Host "DAG 트리거 완료: $AirflowDag"
} catch {
    Write-Warning "DAG 트리거 실패: $($_.Exception.Message)"
}

exit 0
