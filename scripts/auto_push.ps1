# Auto push script for daily backups
param(
    [string]$RepoPath = "C:\airflow"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Set-Location $RepoPath

# Exit quietly if no changes
$changes = git status --porcelain
if (-not $changes) { return }

# Update and push
git pull --rebase
if ($LASTEXITCODE -ne 0) { throw "git pull failed" }

git add .
if ($LASTEXITCODE -ne 0) { throw "git add failed" }

$ts = Get-Date -Format "yyyy-MM-dd HH:mm"
git commit -m "chore: auto backup $ts"
if ($LASTEXITCODE -ne 0) { throw "git commit failed" }

git push
if ($LASTEXITCODE -ne 0) { throw "git push failed" }
