@echo off
REM 홀 주간보고 Excel을 Windows 로컬 .venv 로 직접 생성 (Docker/OneDrive 동기화 우회)
REM 더블클릭 또는 PowerShell 에서 실행

cd /d "%~dp0\.."
".venv\Scripts\python.exe" "scripts\run_hall_weekly_report_local.py"

echo.
echo ============================================
echo 완료. 생성 파일: OneDrive\data\mart\hall_sales_target\
echo ============================================
pause
