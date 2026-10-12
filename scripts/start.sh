#!/usr/bin/env bash
# ==============================================================
# Start Script - Real-Time Air Quality Big Data Pipeline
# ==============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=============================================================="
echo " Starting Big Data Analytics Cluster (Kafka, HDFS, Spark)"
echo "=============================================================="

cd "$ROOT_DIR"

# Check docker compose
if ! command -v docker &> /dev/null; then
    echo "[!] Error: Docker is not installed or not in PATH."
    exit 1
fi

echo "[*] Launching containers via Docker Compose..."
docker compose up -d

echo "[*] Waiting for services to become healthy..."
echo "    - Kafka broker on :9092"
echo "    - HDFS NameNode on :9000 (Web UI :9870)"
echo "    - Spark Master on :7077 (Web UI :8085)"

sleep 10

# Create HDFS directories if namenode is up
echo "[*] Initializing HDFS directories (/air_quality/parquet and checkpoints)..."
docker compose exec -T namenode hdfs dfs -mkdir -p /air_quality/parquet/raw /air_quality/parquet/clean /air_quality/parquet/anomalies /air_quality/checkpoints 2>/dev/null || true
docker compose exec -T namenode hdfs dfs -chmod -R 777 /air_quality 2>/dev/null || true

echo "=============================================================="
echo "[✓] Cluster started successfully!"
echo "    - Spark UI:    http://localhost:8085"
echo "    - HDFS UI:     http://localhost:9870"
echo "    - Kafka:       localhost:9092"
echo "=============================================================="
