#!/usr/bin/env bash
# ====================================================================
# AirSentinel 5-Minute Lab Demo Script (POSIX Bash)
# ====================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo "===================================================================="
echo "  AIRSENTINEL: REAL-TIME AIR QUALITY ANOMALY DETECTION (DEMO)"
echo "===================================================================="

# 1. Start Cluster if needed
echo "[Step 1/6] Checking Docker Cluster..."
bash "$SCRIPT_DIR/start.sh"

# 2. Check Dataset and Preprocessing
echo "[Step 2/6] Verifying Data Preprocessing & Baselines..."
if [ ! -f "data/AirQualityCleaned.csv" ]; then
    python3 preprocessing/clean.py
fi

if [ ! -f "config/baselines.json" ]; then
    python3 batch/baseline_stats.py
fi

# 3. Launch Streaming Anomaly Detector in Spark Master container
echo "[Step 3/6] Starting Spark Structured Streaming Anomaly Detector..."
docker exec -d bda-spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --master spark://spark-master:7077 \
    /workspace/streaming/anomaly_detector.py

echo "[*] Waiting 10 seconds for Spark Streaming queries to initialize..."
sleep 10

# 4. Launch Live Alert Handler in background
echo "[Step 4/6] Launching Alert Handler (logging to logs/alerts.log)..."
python3 alerts/alert_handler.py --timeout 30 &
ALERT_PID=$!

# 5. Start Kafka Producer with Spike Injection
echo "[Step 5/6] Replaying Sensor Stream & Injecting Synthetic Pollution Spikes..."
python3 producer/producer.py --limit 40 --interval 0.1 --inject-spikes --spike-interval 10

# 6. Verify HDFS Data Storage
echo "[Step 6/6] Verifying Parquet Persistence in HDFS..."
sleep 5
echo "--- HDFS Raw Storage (/air_quality/parquet/raw) ---"
docker exec bda-namenode hdfs dfs -ls -R /air_quality/parquet/raw || true

echo "--- HDFS Anomalies Storage (/air_quality/parquet/anomalies) ---"
docker exec bda-namenode hdfs dfs -ls -R /air_quality/parquet/anomalies || true

wait $ALERT_PID || true

echo "===================================================================="
echo "  [✓] 5-MINUTE DEMO COMPLETED SUCCESSFULLY!"
echo "  - Web Dashboard : http://localhost:8000"
echo "  - Spark UI      : http://localhost:8085"
echo "  - HDFS UI       : http://localhost:9870"
echo "  - Alert Logs    : logs/alerts.log"
echo "===================================================================="
