#!/usr/bin/env bash
# ==============================================================
# Stop Script - Real-Time Air Quality Big Data Pipeline
# ==============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=============================================================="
echo " Stopping Big Data Analytics Cluster (Kafka, HDFS, Spark)"
echo "=============================================================="

cd "$ROOT_DIR"
docker compose down

echo "[✓] Cluster stopped cleanly."
