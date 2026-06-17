Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd.exe /c wsl.exe -- bash /mnt/c/airflow/scripts/start_telegram_wsl.sh >> C:\airflow\scripts\wsl_autoheal_start.log 2>&1", 0, False
