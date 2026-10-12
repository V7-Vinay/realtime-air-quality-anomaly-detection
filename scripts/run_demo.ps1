# ====================================================================
# AirSentinel 5-Minute Lab Demo Script (PowerShell for Windows)
# ====================================================================
$ErrorActionPreference = "Continue"

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

Write-Host "====================================================================" -ForegroundColor Cyan
Write-Host "  AIRSENTINEL: REAL-TIME AIR QUALITY ANOMALY DETECTION (DEMO)" -ForegroundColor Cyan
Write-Host "====================================================================" -ForegroundColor Cyan

# 1. Start Cluster if needed
Write-Host "[Step 1/6] Checking Docker Cluster..." -ForegroundColor Yellow
& "$rootDir\scripts\start.ps1"

# 2. Check Preprocessing & Baselines
Write-Host "[Step 2/6] Verifying Data Preprocessing & Baselines..." -ForegroundColor Yellow
if (-not (Test-Path "$rootDir\data\AirQualityCleaned.csv")) {
    python "$rootDir\preprocessing\clean.py"
}
if (-not (Test-Path "$rootDir\config\baselines.json")) {
    python "$rootDir\batch\baseline_stats.py"
}

# 3. Launch Spark Structured Streaming Job inside Spark Master container
Write-Host "[Step 3/6] Starting Spark Structured Streaming Anomaly Detector..." -ForegroundColor Yellow
docker exec -d bda-spark-master spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 `
    --master spark://spark-master:7077 `
    /workspace/streaming/anomaly_detector.py

Write-Host "[*] Waiting 10 seconds for Spark Streaming queries to initialize..." -ForegroundColor White
Start-Sleep -Seconds 10

# 4. Start Alert Handler in Background Process
Write-Host "[Step 4/6] Launching Alert Handler (logging to logs/alerts.log)..." -ForegroundColor Yellow
$alertJob = Start-Job -ScriptBlock {
    param($root)
    Set-Location $root
    python "$root\alerts\alert_handler.py" --timeout 30
} -ArgumentList $rootDir

# 5. Start Kafka Producer with Spike Injection
Write-Host "[Step 5/6] Replaying Sensor Stream & Injecting Synthetic Spikes..." -ForegroundColor Yellow
python "$rootDir\producer\producer.py" --limit 40 --interval 0.1 --inject-spikes --spike-interval 10

# 6. Verify HDFS Data Storage
Write-Host "[Step 6/6] Verifying Parquet Persistence in HDFS..." -ForegroundColor Yellow
Start-Sleep -Seconds 6
Write-Host "`n--- HDFS Raw Storage (/air_quality/parquet/raw) ---" -ForegroundColor Cyan
docker exec bda-namenode hdfs dfs -ls -R /air_quality/parquet/raw

Write-Host "`n--- HDFS Anomalies Storage (/air_quality/parquet/anomalies) ---" -ForegroundColor Cyan
docker exec bda-namenode hdfs dfs -ls -R /air_quality/parquet/anomalies

Wait-Job $alertJob -Timeout 10 | Out-Null
Receive-Job $alertJob | Out-Null
Remove-Job $alertJob -Force 2>$null

Write-Host "`n====================================================================" -ForegroundColor Green
Write-Host "  [OK] 5-MINUTE DEMO COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "  - Web Dashboard : http://localhost:8000" -ForegroundColor White
Write-Host "  - Spark Web UI  : http://localhost:8085" -ForegroundColor White
Write-Host "  - HDFS Web UI   : http://localhost:9870" -ForegroundColor White
Write-Host "  - Alert Logs    : logs/alerts.log" -ForegroundColor White
Write-Host "====================================================================" -ForegroundColor Green
