$ErrorActionPreference = "Continue"

$repo = "C:\airflow"
$dockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
$logDir = Join-Path $repo ".tmp\autostart"
$logFile = Join-Path $logDir "airflow-autostart.log"
$env:AIRFLOW_UID = if ($env:AIRFLOW_UID) { $env:AIRFLOW_UID } else { "50000" }

New-Item -ItemType Directory -Path $logDir -Force | Out-Null

function Write-Log {
    param([string]$Message)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Encoding utf8 -Value "[$stamp] $Message"
}

function Test-AirflowHealth {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8080/health" -TimeoutSec 5
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Invoke-RepoProcess {
    param(
        [string]$FilePath,
        [string]$Arguments,
        [int]$TimeoutSeconds
    )

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $FilePath
    $psi.Arguments = $Arguments
    $psi.WorkingDirectory = $repo
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    $process = [System.Diagnostics.Process]::Start($psi)
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Write-Log "timeout: $FilePath $Arguments"
        try {
            $process.Kill()
        } catch {
            Write-Log "kill failed: $($_.Exception.Message)"
        }
        return 124
    }

    return $process.ExitCode
}

Write-Log "start"

try {
    if (-not (Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue)) {
        if (Test-Path $dockerDesktop) {
            Write-Log "launch Docker Desktop"
            Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
        } else {
            Write-Log "Docker Desktop executable not found: $dockerDesktop"
            exit 1
        }
    } else {
        Write-Log "Docker Desktop already running"
    }

    $deadline = (Get-Date).AddMinutes(8)
    do {
        docker info *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Log "Docker engine ready"
            break
        }
        Start-Sleep -Seconds 10
    } while ((Get-Date) -lt $deadline)

    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Docker engine did not become ready"
        exit 2
    }

    if (Test-AirflowHealth) {
        Write-Log "Airflow webserver already healthy"
        exit 0
    }

    Write-Log "docker compose start"
    $composeExit = Invoke-RepoProcess -FilePath "docker" -Arguments "compose start" -TimeoutSeconds 120
    Write-Log "docker compose start exit=$composeExit"

    if ($composeExit -ne 0) {
        Write-Log "docker compose up -d"
        $composeExit = Invoke-RepoProcess -FilePath "docker" -Arguments "compose up -d" -TimeoutSeconds 180
        Write-Log "docker compose up exit=$composeExit"
        if ($composeExit -ne 0) {
            exit $composeExit
        }
    }

    $healthDeadline = (Get-Date).AddMinutes(5)
    do {
        if (Test-AirflowHealth) {
            Write-Log "Airflow webserver healthy"
            exit 0
        }
        Write-Log "health wait"
        Start-Sleep -Seconds 10
    } while ((Get-Date) -lt $healthDeadline)

    Write-Log "Airflow health check timed out"
    exit 3
} catch {
    Write-Log "fatal: $($_.Exception.Message)"
    exit 99
}
