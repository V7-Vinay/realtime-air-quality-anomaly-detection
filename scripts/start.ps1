# PowerShell Start Script for Windows
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host " Starting Big Data Analytics Cluster (Kafka, HDFS, Spark)" -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

docker compose up -d

Write-Host "[*] Waiting for services to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 12

Write-Host "[*] Initializing HDFS directories..." -ForegroundColor Yellow
docker compose exec -T namenode hdfs dfs -mkdir -p /air_quality/parquet/raw /air_quality/parquet/clean /air_quality/parquet/anomalies /air_quality/checkpoints 2>$null
docker compose exec -T namenode hdfs dfs -chmod -R 777 /air_quality 2>$null

Write-Host "==============================================================" -ForegroundColor Green
Write-Host "[OK] Cluster started successfully!" -ForegroundColor Green
Write-Host "     - Spark Web UI: http://localhost:8085" -ForegroundColor White
Write-Host "     - HDFS Web UI:  http://localhost:9870" -ForegroundColor White
Write-Host "     - Kafka Broker: localhost:9092" -ForegroundColor White
Write-Host "==============================================================" -ForegroundColor Green
