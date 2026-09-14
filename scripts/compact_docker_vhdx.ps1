param(
    [string]$DockerVhdxPath = "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx",
    [switch]$ShutdownAllWsl,
    [switch]$RestartDockerDesktop
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $DockerVhdxPath)) {
    throw "Docker VHDX not found: $DockerVhdxPath"
}

$before = (Get-Item -LiteralPath $DockerVhdxPath).Length

try {
    & wsl -d docker-desktop -- fstrim -av
} catch {
    Write-Warning "fstrim failed or docker-desktop is not running: $($_.Exception.Message)"
}

Get-Process | Where-Object {
    $_.ProcessName -like "Docker Desktop*" -or $_.ProcessName -like "com.docker*"
} | Stop-Process -Force -ErrorAction SilentlyContinue

if ($ShutdownAllWsl) {
    & wsl --shutdown
} else {
    & wsl --terminate docker-desktop 2>$null
}
Start-Sleep -Seconds 5

$optimize = Get-Command Optimize-VHD -ErrorAction SilentlyContinue
if ($optimize) {
    Optimize-VHD -Path $DockerVhdxPath -Mode Full
} else {
    $scriptPath = Join-Path ([System.IO.Path]::GetTempPath()) "compact_docker_vhdx_diskpart.txt"
    @(
        "select vdisk file=`"$DockerVhdxPath`"",
        "attach vdisk readonly",
        "compact vdisk",
        "detach vdisk"
    ) | Set-Content -Path $scriptPath -Encoding ascii
    & diskpart.exe /s $scriptPath
    Remove-Item -LiteralPath $scriptPath -Force -ErrorAction SilentlyContinue
}

$after = (Get-Item -LiteralPath $DockerVhdxPath).Length
[pscustomobject]@{
    Path = $DockerVhdxPath
    BeforeGB = [math]::Round($before / 1GB, 2)
    AfterGB = [math]::Round($after / 1GB, 2)
    FreedGB = [math]::Round(($before - $after) / 1GB, 2)
}

if ($after -ge $before) {
    Write-Warning "VHDX did not shrink. Move Docker Desktop disk image location off C: if this remains large."
}

if ($RestartDockerDesktop) {
    Start-Process -FilePath "C:\Program Files\Docker\Docker\Docker Desktop.exe" -WindowStyle Hidden
}
