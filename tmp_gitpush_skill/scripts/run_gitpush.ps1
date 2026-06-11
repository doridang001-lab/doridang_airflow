param(
    [string]$Message = ""
)

$scriptPath = "C:\airflow\.claude\scripts\gitpush.sh"

if (-not (Test-Path $scriptPath)) {
    Write-Error "Workspace gitpush helper not found: $scriptPath"
    exit 1
}

if ([string]::IsNullOrWhiteSpace($Message)) {
    bash $scriptPath
} else {
    bash $scriptPath $Message
}
