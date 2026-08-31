param(
    [int]$SchedulerLogRetentionDays = 7,
    [int]$TaskLogRetentionDays = 30,
    [int]$MinFreeGbWarning = 30,
    [switch]$SkipDockerPrune,
    [switch]$SkipNotify,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogRoot = Join-Path $ProjectRoot "logs"
$RunLogDir = Join-Path $ProjectRoot ".tmp\airflow_space_cleanup"
$RunLog = Join-Path $RunLogDir "cleanup.log"

New-Item -ItemType Directory -Force -Path $RunLogDir | Out-Null

function Write-RunLog {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
    "[$timestamp] $Message" | Add-Content -Path $RunLog -Encoding utf8
}

function Get-TreeBytes {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return 0
    }
    $sum = Get-ChildItem -LiteralPath $Path -Recurse -Force -File -ErrorAction SilentlyContinue |
        Measure-Object -Property Length -Sum
    return [int64]($sum.Sum)
}

function Assert-UnderPath {
    param(
        [string]$Path,
        [string]$Root
    )
    $resolvedPath = (Resolve-Path -LiteralPath $Path).Path
    $resolvedRoot = (Resolve-Path -LiteralPath $Root).Path
    if (-not $resolvedPath.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove outside root: $resolvedPath"
    }
}

function Remove-Targets {
    param(
        [object[]]$Targets,
        [string]$Root,
        [string]$Label
    )
    $existing = @($Targets | Where-Object { $_ -and (Test-Path -LiteralPath $_.FullName) })
    foreach ($item in $existing) {
        Assert-UnderPath -Path $item.FullName -Root $Root
    }

    $bytes = 0
    foreach ($item in $existing) {
        if ($item.PSIsContainer) {
            $bytes += Get-TreeBytes -Path $item.FullName
        } else {
            $bytes += [int64]$item.Length
        }
    }

    if ($DryRun) {
        Write-RunLog "dry-run $Label targets=$($existing.Count) bytes=$bytes"
        return $bytes
    }

    foreach ($item in $existing) {
        Remove-Item -LiteralPath $item.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }
    Write-RunLog "removed $Label targets=$($existing.Count) bytes=$bytes"
    return $bytes
}

$startFree = ([System.IO.DriveInfo]::GetDrives() | Where-Object { $_.Name -eq "C:\" }).AvailableFreeSpace
Write-RunLog "start free_gb=$([math]::Round($startFree / 1GB, 2)) dry_run=$DryRun"

$schedulerRoot = Join-Path $LogRoot "scheduler"
if (Test-Path -LiteralPath $schedulerRoot) {
    $schedulerCutoff = (Get-Date).Date.AddDays(-$SchedulerLogRetentionDays)
    $schedulerTargets = Get-ChildItem -LiteralPath $schedulerRoot -Directory -Force |
        Where-Object { $_.Name -match '^\d{4}-\d{2}-\d{2}$' -and ([datetime]$_.Name) -lt $schedulerCutoff }
    Remove-Targets -Targets $schedulerTargets -Root $LogRoot -Label "scheduler logs" | Out-Null
}

$taskCutoff = (Get-Date).AddDays(-$TaskLogRetentionDays)
$taskTargets = Get-ChildItem -LiteralPath $LogRoot -Directory -Force -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "dag_id=*" } |
    ForEach-Object {
        Get-ChildItem -LiteralPath $_.FullName -Recurse -Force -File -ErrorAction SilentlyContinue |
            Where-Object { $_.LastWriteTime -lt $taskCutoff }
    }
Remove-Targets -Targets $taskTargets -Root $LogRoot -Label "task logs" | Out-Null

$dagProcessorRoot = Join-Path $LogRoot "dag_processor_manager"
if (Test-Path -LiteralPath $dagProcessorRoot) {
    $rotatedTargets = Get-ChildItem -LiteralPath $dagProcessorRoot -File -Force |
        Where-Object { $_.Name -match '^dag_processor_manager\.log\.\d+$' }
    Remove-Targets -Targets $rotatedTargets -Root $LogRoot -Label "dag processor rotated logs" | Out-Null
}

$tempRoot = Join-Path $env:LOCALAPPDATA "Temp"
$tempTargets = @()
foreach ($name in @("DiagOutputDir", "wsl-crashes")) {
    $path = Join-Path $tempRoot $name
    if (Test-Path -LiteralPath $path) {
        $tempTargets += Get-Item -LiteralPath $path -Force
    }
}
if ($tempTargets.Count -gt 0) {
    Remove-Targets -Targets $tempTargets -Root $tempRoot -Label "local temp diagnostics" | Out-Null
}

if (-not $SkipDockerPrune) {
    $docker = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($docker) {
        if ($DryRun) {
            Write-RunLog "dry-run docker system prune skipped"
        } else {
            & docker system prune -f *> (Join-Path $RunLogDir "docker-prune.log")
            Write-RunLog "docker system prune exit_code=$LASTEXITCODE"
            & docker builder prune -a -f --filter "until=168h" *> (Join-Path $RunLogDir "docker-builder-prune.log")
            Write-RunLog "docker builder prune exit_code=$LASTEXITCODE"
        }
    } else {
        Write-RunLog "docker.exe not found"
    }
}

$endFree = ([System.IO.DriveInfo]::GetDrives() | Where-Object { $_.Name -eq "C:\" }).AvailableFreeSpace
$endFreeGb = [math]::Round($endFree / 1GB, 2)
Write-RunLog "finish free_gb=$endFreeGb freed_gb=$([math]::Round(($endFree - $startFree) / 1GB, 2))"

if ($endFreeGb -lt $MinFreeGbWarning) {
    Write-RunLog "warning C drive free space below ${MinFreeGbWarning}GB"
    if (-not $DryRun -and -not $SkipNotify) {
        $notifyLog = Join-Path $RunLogDir "space-warning-notify.log"
        $env:PYTHONIOENCODING = "utf-8"
        & python -X utf8 "$ProjectRoot\scripts\notify_airflow_space_warning.py" `
            --free-gb $endFreeGb `
            --threshold-gb $MinFreeGbWarning `
            --log-path $RunLog *> $notifyLog
        Write-RunLog "space warning notify exit_code=$LASTEXITCODE"
    }
}

Get-Content -Path $RunLog -Encoding utf8 -Tail 20
