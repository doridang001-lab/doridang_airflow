Option Explicit

Dim shell
Set shell = CreateObject("WScript.Shell")

shell.CurrentDirectory = "C:\airflow"
shell.Run "cmd.exe /c cd /d ""C:\airflow"" && ""C:\airflow\.venv\Scripts\python.exe"" ""C:\airflow\scripts\airflow_scheduler_watchdog.py""", 0, True
