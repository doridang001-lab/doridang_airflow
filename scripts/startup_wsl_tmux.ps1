# WSL 자동시작 + tmux a 작업 스케줄러 등록

$TaskName = "StartupWSLTmux"

# 기존 작업 제거
schtasks /delete /tn $TaskName /f 2>$null

# wsl.exe로 sh 스크립트 실행 (인수 파싱 문제 없음)
schtasks /create /tn $TaskName /tr "wsl.exe bash /mnt/c/airflow/scripts/wsl_tmux_attach.sh" /sc onlogon /f

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "등록 완료: $TaskName" -ForegroundColor Green
    Write-Host "   - 로그인 시 WSL 열고 30초 후 tmux a 자동 실행" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "즉시 테스트하려면 PowerShell에서:" -ForegroundColor Yellow
    Write-Host "  schtasks /run /tn $TaskName" -ForegroundColor Yellow
} else {
    Write-Host "등록 실패" -ForegroundColor Red
}
