Set shell = CreateObject("WScript.Shell")
WScript.Quit shell.Run("""C:\airflow\.venv\Scripts\pythonw.exe"" -X utf8 ""C:\airflow\scripts\docker_airflow_watchdog.py""", 0, True)
