# Airflow 메타DB + doridangdb 일일 덤프 (Windows 작업스케줄러용).
# 2026-09-12 Docker 데이터 이전 중 빈 vhdx가 생성되어 메타DB(이력·Variable·Pool)가 초기화된 사고 이후 추가.
# 그날은 전날 수동 덤프(E:\airflow_backup\20260911)가 우연히 있어 복구할 수 있었다.
param(
    [string]$BackupRoot = "E:\airflow_backup\meta",
    [int]$KeepDays = 7
)

$ErrorActionPreference = "Stop"
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null

$metaPath = Join-Path $BackupRoot "airflow_meta_$stamp.dump"
$doridangPath = Join-Path $BackupRoot "doridangdb_$stamp.dump"

# pg_dump custom format(-Fc) → pg_restore 로 그대로 복원 가능. stdout 바이너리를 파일로 직결한다.
cmd /c "docker exec airflow-postgres-1 pg_dump -U airflow -Fc airflow > `"$metaPath`""
if ($LASTEXITCODE -ne 0) { throw "airflow meta pg_dump failed ($LASTEXITCODE)" }
cmd /c "docker exec airflow-doridang-postgres-1 pg_dump -U doridang -Fc doridangdb > `"$doridangPath`""
if ($LASTEXITCODE -ne 0) { throw "doridangdb pg_dump failed ($LASTEXITCODE)" }

$metaSize = (Get-Item -LiteralPath $metaPath).Length
if ($metaSize -lt 1MB) { throw "airflow meta dump too small: $metaSize bytes" }

Get-ChildItem -LiteralPath $BackupRoot -Filter "*.dump" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepDays) } |
    Remove-Item -Force

Write-Host "backup ok: $metaPath ($([math]::Round($metaSize/1MB,1)) MB), $doridangPath"
