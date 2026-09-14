@echo off
setlocal

set "TASK_NAME=AirflowBaeminAclRepair"
set "LAUNCHER=C:\airflow\scripts\run_baemin_acl_repair_wsl_hidden.vbs"

if not exist "%LAUNCHER%" (
    echo ACL repair launcher not found: %LAUNCHER%
    exit /b 1
)

schtasks.exe /Create /TN "%TASK_NAME%" /SC MINUTE /MO 1 /TR "wscript.exe //B //Nologo \"%LAUNCHER%\"" /RL LIMITED /F
if errorlevel 1 exit /b %errorlevel%

schtasks.exe /Run /TN "%TASK_NAME%"
if errorlevel 1 exit /b %errorlevel%

echo Scheduled WSL task ready: %TASK_NAME%
