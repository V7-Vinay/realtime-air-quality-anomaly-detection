# PowerShell Stop Script for Windows
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host " Stopping Big Data Analytics Cluster (Kafka, HDFS, Spark)" -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

docker compose down

Write-Host "[OK] Cluster stopped cleanly." -ForegroundColor Green
