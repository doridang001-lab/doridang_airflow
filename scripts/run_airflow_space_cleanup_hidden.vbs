Set shell = CreateObject("WScript.Shell")
shell.Run "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\airflow\scripts\cleanup_airflow_docker_space.ps1"" -SkipDockerPrune", 0, True
