Option Explicit

Dim shell
Dim fileSystem
Dim command
Dim exitCode
Dim statusFile

Set shell = CreateObject("WScript.Shell")
Set fileSystem = CreateObject("Scripting.FileSystemObject")
command = "wsl.exe -d UbuntuCodex --exec bash /mnt/c/airflow/scripts/repair_baemin_acl_queue_wsl.sh"
exitCode = shell.Run(command, 0, True)
Set statusFile = fileSystem.CreateTextFile("C:\Local_DB\logs\baemin_acl_repair_wsl.exit", True)
statusFile.Write CStr(exitCode)
statusFile.Close
WScript.Quit exitCode
