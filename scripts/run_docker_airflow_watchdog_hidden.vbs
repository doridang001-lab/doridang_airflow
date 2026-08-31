Set shell = CreateObject("WScript.Shell")
shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File ""C:\airflow\scripts\start_airflow_on_login.ps1""", 0, False
