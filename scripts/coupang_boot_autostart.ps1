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
$logRoot = Join-Path $repoRoot ".tmp\coupang_boot_autostart"
$logPath = $null

function Initialize-RunLog {
    if (-not (Test-Path $logRoot)) {
        New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    }
    $script:logPath = Join-Path $logRoot ("{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    Start-Transcript -Path $script:logPath -Append | Out-Null
    Write-Host "로그 파일: $script:logPath"
}

function Write-Step {
    param([string]$Message)
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
}

function Wait-DevToolsEndpoint {
    param(
        [string]$Endpoint = "http://127.0.0.1:9222/json",
        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Endpoint -TimeoutSec 3 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Step "Chrome DevTools endpoint ready: $Endpoint"
                return
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    throw "Chrome DevTools endpoint not ready within ${TimeoutSeconds}s: $Endpoint"
}

function Get-CollectDirs {
    $dirs = @()
    $down = "E:\down"
    if (Test-Path $down) { $dirs += $down }

    if ($env:COLLECT_DB -and (Test-Path $env:COLLECT_DB)) {
        $dirs += Join-Path $env:COLLECT_DB "영업관리부_수집"
        return ($dirs | Select-Object -Unique)
    }

    $userProfile = [Environment]::GetFolderPath("UserProfile")
    $onedriveCandidates = @(
        (Join-Path $userProfile "OneDrive - 주식회사 도리당"),
        (Join-Path $userProfile "OneDrive - 도리당")
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
            $total += @(Get-ChildItem -Path $dir -Filter $pattern -File -ErrorAction SilentlyContinue).Count
        }
    }
    return $total
}

function Trigger-CoupangMacroDag {
    param([string]$DagId, [string]$SchedulerContainer)

    # 1) docker scheduler if available
    $hasDocker = Get-Command docker -ErrorAction SilentlyContinue
    if ($hasDocker) {
        $running = docker ps --filter "name=$SchedulerContainer" --filter "status=running" --format '{{.Names}}'
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

Initialize-RunLog
try {
    Write-Step 'Coupang boot autostart 시작'
    Write-Step ('StartupDelaySeconds={0} CollectionWaitSeconds={1} CollectionStableSeconds={2}' -f $StartupDelaySeconds, $CollectionWaitSeconds, $CollectionStableSeconds)

    if (-not (Test-Path $venvPython)) {
        Write-Error "Python 경로가 존재하지 않습니다: $venvPython"
        exit 1
    }

    Set-Location $repoRoot
    if ($StartupDelaySeconds -gt 0) {
        Write-Step ('시작 지연 대기: {0}s' -f $StartupDelaySeconds)
        Start-Sleep -Seconds $StartupDelaySeconds
    }

    Write-Step ('host chrome 실행: {0}' -f $hostScript)
    & powershell -NoProfile -ExecutionPolicy Bypass -File $hostScript
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne $null) {
        Write-Error ('coupang_host_chrome.ps1 실행 실패: {0}' -f $LASTEXITCODE)
        exit 1
    }
    Wait-DevToolsEndpoint

    $collectDirs = Get-CollectDirs
    $collectDirText = ($collectDirs | ForEach-Object { $_ }) -join ', '
    Write-Step ('수집 디렉터리: {0}' -f $collectDirText)
    $before = Get-RawCounts -Paths $collectDirs
    Write-Step ('시작 시 수집 파일 수: {0}개' -f $before)

    Write-Step ('runner 자동 클릭 실행: {0}' -f $autoClickScript)
    & $venvPython $autoClickScript
    $clickExit = $LASTEXITCODE
    if ($clickExit -ne 0) {
        Write-Error ('coupang_runner_autoclick.py 실행 실패: {0}' -f $clickExit)
        exit $clickExit
    }
    Write-Step 'runner 자동 클릭 완료'

    if ($CollectionWaitSeconds -gt 0) {
        Write-Step '수집 파일 안정화 대기 시작'
        $deadline = (Get-Date).AddSeconds($CollectionWaitSeconds)
        $lastChanged = (Get-Date)
        while ((Get-Date) -lt $deadline) {
            $nowCount = Get-RawCounts -Paths $collectDirs
            if ($nowCount -gt $before) {
                Write-Step ('수집 파일 증가 감지: {0} -> {1}' -f $before, $nowCount)
                $before = $nowCount
                $lastChanged = Get-Date
            } else {
                $stableSeconds = ((Get-Date) - $lastChanged).TotalSeconds
                if ($stableSeconds -ge $CollectionStableSeconds) {
                    Write-Step ('수집 원본 파일이 {0}초 동안 안정 상태입니다.' -f $CollectionStableSeconds)
                    break
                }
            }
            Start-Sleep -Seconds 5
        }
    }

    try {
        Write-Step ('DAG 트리거 시작: {0}' -f $AirflowDag)
        Trigger-CoupangMacroDag -DagId $AirflowDag -SchedulerContainer $AirflowSchedulerContainer
        Write-Step ('DAG 트리거 완료: {0}' -f $AirflowDag)
    } catch {
        Write-Warning ('DAG 트리거 실패: {0}' -f $_.Exception.Message)
    }

    Write-Step 'Coupang boot autostart 정상 종료'
    exit 0
} catch {
    Write-Error ('Coupang boot autostart 실패: {0}' -f $_.Exception.Message)
    exit 1
} finally {
    if ($script:logPath) {
        Stop-Transcript | Out-Null
    }
}



