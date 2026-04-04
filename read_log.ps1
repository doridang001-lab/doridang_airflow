$d08 = Get-ChildItem "c:\airflow\logs" -Directory | Where-Object { $_.Name -like "*Orders_08*" }
$lastRun = Get-ChildItem $d08.FullName -Directory | Sort-Object LastWriteTime | Select-Object -Last 1
$uploadTask = Get-ChildItem $lastRun.FullName -Directory | Where-Object { $_.Name -like "*upload*" }
$logFile = Get-ChildItem $uploadTask.FullName -File | Select-Object -First 1
Copy-Item $logFile.FullName "c:\airflow\temp_upload_log.txt"
