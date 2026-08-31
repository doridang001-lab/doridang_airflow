Set shell = CreateObject("WScript.Shell")
shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File ""C:\airflow\scripts\cleanup_airflow_docker_space.ps1"" -SkipDockerPrune -SkipNotify", 0, True
