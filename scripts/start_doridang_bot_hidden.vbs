Option Explicit

Dim shell
Dim fileSystem
Dim pythonPath
Dim command

Set shell = CreateObject("WScript.Shell")
Set fileSystem = CreateObject("Scripting.FileSystemObject")

shell.CurrentDirectory = "C:\airflow"
pythonPath = "C:\airflow\.venv\Scripts\pythonw.exe"
If Not fileSystem.FileExists(pythonPath) Then
    pythonPath = "C:\airflow\.venv\Scripts\python.exe"
End If

command = """" & pythonPath & """ ""C:\airflow\scripts\start_doridang_bot_hidden.py"""
WScript.Quit shell.Run(command, 0, True)
