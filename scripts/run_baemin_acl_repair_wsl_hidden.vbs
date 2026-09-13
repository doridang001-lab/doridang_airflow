Option Explicit

Dim shell
Dim fileSystem
Dim command
Dim exitCode
Dim queueRoot
Dim logDir
Dim hasPending
Dim folder
Dim file
Dim donePath

Set shell = CreateObject("WScript.Shell")
Set fileSystem = CreateObject("Scripting.FileSystemObject")

queueRoot = "C:\Local_DB\baemin_acl_repair_queue"
logDir = "C:\Local_DB\logs"
hasPending = False

If Not fileSystem.FolderExists(logDir) Then
    fileSystem.CreateFolder(logDir)
End If

If fileSystem.FolderExists(queueRoot) Then
    Set folder = fileSystem.GetFolder(queueRoot)
    For Each file In folder.Files
        If LCase(Right(file.Name, 13)) = ".request.json" Then
            donePath = queueRoot & "\" & Left(file.Name, Len(file.Name) - 13) & ".done.json"
            If Not fileSystem.FileExists(donePath) Then
                hasPending = True
                Exit For
            End If
        End If
    Next
End If

If Not hasPending Then
    Dim stdoutFile
    Dim stderrFile
    Dim exitFile
    Set stdoutFile = fileSystem.CreateTextFile(logDir & "\baemin_acl_repair_wsl.stdout.log", True)
    stdoutFile.WriteLine "baemin ACL queue skipped: requests=0 completed=0"
    stdoutFile.Close
    Set stderrFile = fileSystem.CreateTextFile(logDir & "\baemin_acl_repair_wsl.stderr.log", True)
    stderrFile.Close
    Set exitFile = fileSystem.CreateTextFile(logDir & "\baemin_acl_repair_wsl.exit", True)
    exitFile.WriteLine "0"
    exitFile.Close
    WScript.Quit 0
End If

command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\airflow\scripts\run_baemin_acl_repair_wsl_hidden.ps1"""
exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode
