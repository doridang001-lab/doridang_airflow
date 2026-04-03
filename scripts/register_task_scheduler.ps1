# Claude Telegram 자동시작 Task Scheduler 등록 스크립트
# 관리자 권한으로 실행 필요
# 사용: 우클릭 → "PowerShell로 실행"

$TaskName = "ClaudeTelegramAutostart"
$WslScript = "/mnt/c/airflow/autostart_telegram.sh"

# 기존 작업 제거
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# 작업 정의
$Action = New-ScheduledTaskAction `
    -Execute "wsl.exe" `
    -Argument "-e bash $WslScript"

# 트리거: 로그인 시 (WSL이 준비된 후 30초 대기)
$Trigger = New-ScheduledTaskTrigger -AtLogOn

# 설정: 창 없이 백그라운드 실행
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

# 현재 사용자로 실행
$Principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -RunLevel Limited

# 등록
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "Claude Code Telegram 채널 자동 연결 (WSL tmux)" `
    -Force

# 딜레이 추가 (로그인 후 30초 대기)
$Task = Get-ScheduledTask -TaskName $TaskName
$Task.Triggers[0].Delay = "PT30S"
$Task | Set-ScheduledTask

Write-Host ""
Write-Host "✅ Task Scheduler 등록 완료: $TaskName" -ForegroundColor Green
Write-Host "   - 트리거: 로그인 시 (30초 후 실행)" -ForegroundColor Cyan
Write-Host "   - 스크립트: $WslScript" -ForegroundColor Cyan
Write-Host ""
Write-Host "로그 확인: wsl cat /tmp/autostart_telegram.log" -ForegroundColor Yellow
