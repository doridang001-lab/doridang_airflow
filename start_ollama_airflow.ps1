$env:OLLAMA_MODELS = "C:\airflow\ollama_models"

$existing = Get-Process | Where-Object { $_.ProcessName -like "ollama*" }
if ($existing) {
    $existing | Stop-Process -Force
    Start-Sleep -Seconds 2
}

Start-Process `
    -FilePath "$env:LOCALAPPDATA\Programs\Ollama\ollama.exe" `
    -ArgumentList "serve" `
    -WindowStyle Hidden
