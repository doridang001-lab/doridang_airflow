$d07 = Get-ChildItem "c:\airflow\logs" -Directory | Where-Object { $_.Name -like "*Orders_07*" }
$lastRun = Get-ChildItem $d07.FullName -Directory | Sort-Object LastWriteTime | Select-Object -Last 1
$tasks = Get-ChildItem $lastRun.FullName -Directory
foreach($t in $tasks) {
  $f = Get-ChildItem $t.FullName -File | Select-Object -Last 1
  Copy-Item $f.FullName "c:\airflow\temp_07_$($t.Name -replace 'task_id=','').txt"
}
