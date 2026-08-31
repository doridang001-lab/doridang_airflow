Set WshShell = CreateObject("WScript.Shell")
exitCode = WshShell.Run("powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\airflow\scripts\start_codex_autoheal_hidden.ps1""", 0, True)
WScript.Quit exitCode
