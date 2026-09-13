$ErrorActionPreference = "Stop"

$logDir = "C:\Local_DB\logs"
$queueRoot = "C:\Local_DB\baemin_acl_repair_queue"
$exitPath = Join-Path $logDir "baemin_acl_repair_wsl.exit"
$stdoutPath = Join-Path $logDir "baemin_acl_repair_wsl.stdout.log"
$stderrPath = Join-Path $logDir "baemin_acl_repair_wsl.stderr.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$pendingRequests = @()
if (Test-Path -LiteralPath $queueRoot) {
    $pendingRequests = @(
        Get-ChildItem -LiteralPath $queueRoot -Filter "*.request.json" -File -ErrorAction SilentlyContinue |
            Where-Object {
                $donePath = Join-Path $queueRoot ($_.BaseName + ".done.json")
                -not (Test-Path -LiteralPath $donePath)
            }
    )
}

if ($pendingRequests.Count -eq 0) {
    Set-Content -Path $stdoutPath -Encoding utf8 -Value "baemin ACL queue skipped: requests=0 completed=0"
    Set-Content -Path $stderrPath -Encoding utf8 -Value ""
    Set-Content -Path $exitPath -Encoding ascii -Value "0"
    exit 0
}

$processInfo = [System.Diagnostics.ProcessStartInfo]::new()
$processInfo.FileName = "$env:WINDIR\System32\wsl.exe"
$processInfo.UseShellExecute = $false
$processInfo.CreateNoWindow = $true
$processInfo.RedirectStandardOutput = $true
$processInfo.RedirectStandardError = $true
$processInfo.Arguments = '-d UbuntuCodex --exec bash /mnt/c/airflow/scripts/repair_baemin_acl_queue_wsl.sh'

$process = [System.Diagnostics.Process]::new()
$process.StartInfo = $processInfo

try {
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    Set-Content -Path $stdoutPath -Encoding utf8 -Value $stdout
    Set-Content -Path $stderrPath -Encoding utf8 -Value $stderr
    Set-Content -Path $exitPath -Encoding ascii -Value ([string]$process.ExitCode)
    exit $process.ExitCode
}
catch {
    Set-Content -Path $stderrPath -Encoding utf8 -Value $_.Exception.Message
    Set-Content -Path $exitPath -Encoding ascii -Value "1"
    exit 1
}
finally {
    if ($process) {
        $process.Dispose()
    }
}
