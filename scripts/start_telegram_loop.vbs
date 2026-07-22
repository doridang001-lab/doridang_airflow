Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\start_airflow_autoheal_task.ps1", 0, False
