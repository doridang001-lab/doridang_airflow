$logFile = "C:\airflow\scripts\telegram_loop_debug.log"
"[$(Get-Date -Format 'HH:mm:ss.fff')] Script started" | Out-File $logFile -Append

Set-Location C:\airflow

# 이미 루프가 실행 중이면 종료 (정확히 TELEGRAM_CLAUDE_LOOP 제목인 창만 체크)
$existing = Get-Process -Name "powershell" -ErrorAction SilentlyContinue |
    Where-Object { $_.MainWindowTitle -eq "TELEGRAM_CLAUDE_LOOP" }
if ($existing) {
    "[$(Get-Date -Format 'HH:mm:ss.fff')] Loop window already exists (PID $($existing.Id)), exiting" | Out-File $logFile -Append
    exit 0
}

# Win32 API 로드
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class WinFocus {
    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
    [DllImport("user32.dll")]
    public static extern bool BringWindowToTop(IntPtr hWnd);
}
"@

# 새 PowerShell 창으로 Claude 시작 (PassThru로 PID 획득)
"[$(Get-Date -Format 'HH:mm:ss.fff')] Starting inner PowerShell..." | Out-File $logFile -Append
$proc = Start-Process -FilePath "powershell.exe" `
    -ArgumentList "-NoExit -ExecutionPolicy Bypass -File `"C:\airflow\scripts\start_claude_inner.ps1`"" `
    -WindowStyle Normal -PassThru
"[$(Get-Date -Format 'HH:mm:ss.fff')] Inner PS PID: $($proc.Id)" | Out-File $logFile -Append

# .NET Thread.Sleep 사용 (Task Scheduler 환경에서 Start-Sleep 불안정 우회)
"[$(Get-Date -Format 'HH:mm:ss.fff')] Waiting 25s for Claude to start..." | Out-File $logFile -Append
[System.Threading.Thread]::Sleep(25000)
"[$(Get-Date -Format 'HH:mm:ss.fff')] Done waiting" | Out-File $logFile -Append

# 프로세스 핸들 갱신 후 창 핸들(HWND) 획득
$proc.Refresh()
$hwnd = $proc.MainWindowHandle
$title = $proc.MainWindowTitle
"[$(Get-Date -Format 'HH:mm:ss.fff')] Inner PS HWND: $hwnd  Title: '$title'" | Out-File $logFile -Append

Add-Type -AssemblyName System.Windows.Forms

if ($hwnd -ne [IntPtr]::Zero) {
    # 창 복원 + 포커스
    [WinFocus]::ShowWindow($hwnd, 9)   # SW_RESTORE
    [System.Threading.Thread]::Sleep(400)
    [WinFocus]::BringWindowToTop($hwnd)
    [WinFocus]::SetForegroundWindow($hwnd)
    [System.Threading.Thread]::Sleep(800)
    [System.Windows.Forms.SendKeys]::SendWait("/loop 텔레그램 메시지가 오면 답변해줘{ENTER}")
    "[$(Get-Date -Format 'HH:mm:ss.fff')] /loop sent via HWND" | Out-File $logFile -Append
} else {
    # 폴백: WScript.Shell AppActivate (제목 기반)
    "[$(Get-Date -Format 'HH:mm:ss.fff')] HWND=0, trying AppActivate fallback..." | Out-File $logFile -Append
    $wshell = New-Object -ComObject WScript.Shell
    $activated = $false
    foreach ($t in @("TELEGRAM_CLAUDE_LOOP", "Claude Code", "Claude")) {
        if ($wshell.AppActivate($t)) {
            $activated = $true
            "[$(Get-Date -Format 'HH:mm:ss.fff')] AppActivate matched: '$t'" | Out-File $logFile -Append
            break
        }
    }
    if ($activated) {
        [System.Threading.Thread]::Sleep(800)
        $wshell.SendKeys("/loop 텔레그램 메시지가 오면 답변해줘{ENTER}")
        "[$(Get-Date -Format 'HH:mm:ss.fff')] /loop sent via AppActivate" | Out-File $logFile -Append
    } else {
        "[$(Get-Date -Format 'HH:mm:ss.fff')] FAILED: no window found" | Out-File $logFile -Append
    }
}

"[$(Get-Date -Format 'HH:mm:ss.fff')] Script finished" | Out-File $logFile -Append
