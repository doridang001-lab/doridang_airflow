param(
    [int]$StartupDelaySeconds = 60,
    [int]$CollectionWaitSeconds = 1200,
    [int]$CollectionStableSeconds = 60,
    [string]$RunnerUrl = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1&mode=top50&date=yesterday",
    [string]$ChromeProfileDirectory = "Default",
    [string]$AirflowDag = "DB_CoupangMacro_Load_Dags",
    [string]$AirflowSchedulerContainer = "airflow-airflow-scheduler-1"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $OutputEncoding = [System.Text.Encoding]::UTF8
} catch {
    Write-Warning "콘솔 UTF-8 설정을 건너뜁니다: $($_.Exception.Message)"
}

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

function Resolve-ChromePath {
    $paths = @(
        "C:\Program Files\Google\Chrome\Application\chrome.exe",
        "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    )
    foreach ($path in $paths) {
        if (Test-Path $path) {
            return $path
        }
    }
    throw "chrome.exe 를 찾을 수 없습니다."
}

function Open-RunnerUrl {
    param(
        [string]$Url,
        [string]$ProfileDirectory
    )

    $chrome = Resolve-ChromePath
    $separator = if ($Url.Contains("?")) { "&" } else { "?" }
    $openUrl = "{0}{1}runTs={2}" -f $Url, $separator, (Get-Date -Format "yyyyMMddHHmmss")
    $args = @(
        "--profile-directory=`"$ProfileDirectory`"",
        "--new-window",
        $openUrl
    )
    Write-Step ("runner URL 직접 실행: {0}" -f $openUrl)
    Start-Process -FilePath $chrome -ArgumentList $args -WindowStyle Normal
}

function Resolve-ExpectedCollectDir {
    if ($env:COLLECT_DB -and (Test-Path $env:COLLECT_DB)) {
        $candidate = Join-Path -Path $env:COLLECT_DB -ChildPath "영업관리부_수집"
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    $userProfile = [Environment]::GetFolderPath("UserProfile")
    $onedriveCandidates = @(
        (Join-Path -Path $userProfile -ChildPath "OneDrive - 주식회사 도리당"),
        (Join-Path -Path $userProfile -ChildPath "OneDrive - 도리당")
    )
    foreach ($base in $onedriveCandidates) {
        if (Test-Path $base) {
            $candidate = Join-Path -Path $base -ChildPath "Collect_Data\영업관리부_수집"
            if (Test-Path $candidate) {
                return $candidate
            }
        }
    }

    return $null
}

function Test-ChromeDownloadPref {
    param([string]$ExpectedDir)

    if (-not $ExpectedDir) {
        return $false
    }

    $userDataDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data"
    $prefPath = Join-Path $userDataDir "$ChromeProfileDirectory\Preferences"
    if (-not (Test-Path $prefPath)) {
        return $false
    }

    try {
        $prefs = Get-Content -Path $prefPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch {
        return $false
    }

    $downloadDir = $null
    if ($prefs.PSObject.Properties.Name -contains "download") {
        $downloadDir = $prefs.download.default_directory
    }
    return ($downloadDir -eq $ExpectedDir)
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

    if ($env:COLLECT_DB -and (Test-Path $env:COLLECT_DB)) {
        $dirs += Join-Path -Path $env:COLLECT_DB -ChildPath "영업관리부_수집"
    }
    else {
        $userProfile = [Environment]::GetFolderPath("UserProfile")
        $onedriveCandidates = @(
            (Join-Path -Path $userProfile -ChildPath "OneDrive - 주식회사 도리당"),
            (Join-Path -Path $userProfile -ChildPath "OneDrive - 도리당")
        )
        foreach ($base in $onedriveCandidates) {
            if (Test-Path $base) {
                $collect = Join-Path -Path $base -ChildPath "Collect_Data\영업관리부_수집"
                if (Test-Path $collect) {
                    $dirs += $collect
                }
            }
        }
    }

    $down = "E:\down"
    if (Test-Path $down) { $dirs += $down }

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
    Write-Step ('RunnerUrl={0}' -f $RunnerUrl)

    if (-not (Test-Path $venvPython)) {
        Write-Error "Python 경로가 존재하지 않습니다: $venvPython"
        exit 1
    }

    Set-Location $repoRoot
    if ($StartupDelaySeconds -gt 0) {
        Write-Step ('시작 지연 대기: {0}s' -f $StartupDelaySeconds)
        Start-Sleep -Seconds $StartupDelaySeconds
    }

    $collectDirs = Get-CollectDirs
    $collectDirText = ($collectDirs | ForEach-Object { $_ }) -join ', '
    Write-Step ('수집 디렉터리: {0}' -f $collectDirText)
    $before = Get-RawCounts -Paths $collectDirs
    Write-Step ('시작 시 수집 파일 수: {0}개' -f $before)

    $usedDirectRunner = $false
    Write-Step ('host chrome 실행: {0}' -f $hostScript)
    & powershell -NoProfile -ExecutionPolicy Bypass -File $hostScript
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne $null) {
        $expectedCollectDir = Resolve-ExpectedCollectDir
        if (-not (Test-ChromeDownloadPref -ExpectedDir $expectedCollectDir)) {
            Write-Error ('coupang_host_chrome.ps1 실행 실패: {0}; 다운로드 경로가 영업관리부_수집으로 보장되지 않아 runner 직접 실행을 중단합니다. Chrome을 완전히 종료한 뒤 다시 실행하세요.' -f $LASTEXITCODE)
            exit $LASTEXITCODE
        }
        Write-Warning ('coupang_host_chrome.ps1 실행 실패: {0}; 다운로드 경로 확인 후 runner URL 직접 실행으로 전환합니다.' -f $LASTEXITCODE)
        Open-RunnerUrl -Url $RunnerUrl -ProfileDirectory $ChromeProfileDirectory
        $usedDirectRunner = $true
    }

    if (-not $usedDirectRunner) {
        try {
            Wait-DevToolsEndpoint
        } catch {
            Write-Warning ('Chrome DevTools endpoint 확인 실패: {0}; runner URL 직접 실행으로 전환합니다.' -f $_.Exception.Message)
            Open-RunnerUrl -Url $RunnerUrl -ProfileDirectory $ChromeProfileDirectory
            $usedDirectRunner = $true
        }
    }

    if (-not $usedDirectRunner) {
        Write-Step ('runner 자동 클릭 실행: {0}' -f $autoClickScript)
        & $venvPython $autoClickScript
        $clickExit = $LASTEXITCODE
        if ($clickExit -ne 0) {
            Write-Warning ('coupang_runner_autoclick.py 실행 실패: {0}; runner URL 직접 실행으로 전환합니다.' -f $clickExit)
            Open-RunnerUrl -Url $RunnerUrl -ProfileDirectory $ChromeProfileDirectory
            $usedDirectRunner = $true
        } else {
            Write-Step 'runner 자동 클릭 완료'
        }
    }

    if ($usedDirectRunner) {
        Write-Step 'runner URL 직접 실행 완료'
    }

    if ($CollectionWaitSeconds -gt 0) {
        Write-Step '수집 파일 안정화 대기 시작'
        $deadline = (Get-Date).AddSeconds($CollectionWaitSeconds)
        $lastChanged = (Get-Date)
        $sawIncrease = $false
        while ((Get-Date) -lt $deadline) {
            $nowCount = Get-RawCounts -Paths $collectDirs
            if ($nowCount -gt $before) {
                Write-Step ('수집 파일 증가 감지: {0} -> {1}' -f $before, $nowCount)
                $before = $nowCount
                $lastChanged = Get-Date
                $sawIncrease = $true
            } else {
                $stableSeconds = ((Get-Date) - $lastChanged).TotalSeconds
                if ($sawIncrease -and $stableSeconds -ge $CollectionStableSeconds) {
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



