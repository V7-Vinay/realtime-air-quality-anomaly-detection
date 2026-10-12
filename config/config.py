"""
Centralized Configuration Module
Loads variables from .env or system environment with sane laboratory defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "air-quality-raw")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "air-quality-alerts")
KAFKA_RAW_PARTITIONS = int(os.getenv("KAFKA_RAW_PARTITIONS", "3"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

# HDFS
HDFS_NAMENODE_HOST = os.getenv("HDFS_NAMENODE_HOST", "localhost")
HDFS_NAMENODE_PORT = int(os.getenv("HDFS_NAMENODE_PORT", "9000"))
HDFS_WEBHDFS_PORT = int(os.getenv("HDFS_WEBHDFS_PORT", "9870"))
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/air_quality")
HDFS_RAW_PARQUET_PATH = os.getenv("HDFS_RAW_PARQUET_PATH", "hdfs://namenode:9000/air_quality/parquet/raw")
HDFS_CLEAN_PARQUET_PATH = os.getenv("HDFS_CLEAN_PARQUET_PATH", "hdfs://namenode:9000/air_quality/parquet/clean")
HDFS_ANOMALIES_PARQUET_PATH = os.getenv("HDFS_ANOMALIES_PARQUET_PATH", "hdfs://namenode:9000/air_quality/parquet/anomalies")
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "hdfs://namenode:9000/air_quality/checkpoints")

# Spark
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_UI_PORT = int(os.getenv("SPARK_UI_PORT", "8085"))
SPARK_STREAMING_TRIGGER_SECONDS = int(os.getenv("SPARK_STREAMING_TRIGGER_SECONDS", "5"))

# Windowing & Watermark
STREAMING_WATERMARK_DELAY = os.getenv("STREAMING_WATERMARK_DELAY", "10 minutes")
STREAMING_WINDOW_DURATION = os.getenv("STREAMING_WINDOW_DURATION", "1 hour")
STREAMING_SLIDE_DURATION = os.getenv("STREAMING_SLIDE_DURATION", "1 minute")

# Anomaly Detection & Statistical Thresholds
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "3.0"))

HARD_THRESHOLDS = {
    "CO_GT": float(os.getenv("HARD_THRESHOLD_CO", "10.0")),         # mg/m3
    "C6H6_GT": float(os.getenv("HARD_THRESHOLD_C6H6", "15.0")),     # ug/m3
    "NOx_GT": float(os.getenv("HARD_THRESHOLD_NOX", "400.0")),      # ppb
    "NO2_GT": float(os.getenv("HARD_THRESHOLD_NO2", "200.0")),      # ug/m3
}

# Baseline Statistics (Historical Means and Standard Deviations from UCI Dataset)
# These act as reference parameters for live Z-score calculation: Z = (X - mean) / std
DEFAULT_BASELINES = {
    "CO_GT": {"mean": 2.15, "std": 1.45, "unit": "mg/m^3"},
    "PT08_S1_CO": {"mean": 1098.9, "std": 217.1, "unit": "sensor_res"},
    "C6H6_GT": {"mean": 10.08, "std": 7.45, "unit": "ug/m^3"},
    "PT08_S2_NMHC": {"mean": 939.1, "std": 266.8, "unit": "sensor_res"},
    "NOx_GT": {"mean": 246.9, "std": 212.1, "unit": "ppb"},
    "PT08_S3_NOx": {"mean": 835.5, "std": 256.9, "unit": "sensor_res"},
    "NO2_GT": {"mean": 113.1, "std": 48.3, "unit": "ug/m^3"},
    "PT08_S4_NO2": {"mean": 1456.2, "std": 346.9, "unit": "sensor_res"},
    "PT08_S5_O3": {"mean": 1022.9, "std": 398.5, "unit": "sensor_res"},
    "T": {"mean": 18.3, "std": 8.8, "unit": "degC"},
    "RH": {"mean": 49.2, "std": 17.3, "unit": "%"},
    "AH": {"mean": 1.025, "std": 0.403, "unit": "g/m^3"},
}

# Producer Replay Settings
PRODUCER_REPLAY_INTERVAL_SECONDS = float(os.getenv("PRODUCER_REPLAY_INTERVAL_SECONDS", "0.1"))
PRODUCER_INJECT_SPIKES = os.getenv("PRODUCER_INJECT_SPIKES", "false").lower() == "true"
PRODUCER_SPIKE_INTERVAL_RECORDS = int(os.getenv("PRODUCER_SPIKE_INTERVAL_RECORDS", "20"))
PRODUCER_SPIKE_MULTIPLIER = float(os.getenv("PRODUCER_SPIKE_MULTIPLIER", "4.5"))

# Latency Verification Target (Seconds)
LATENCY_SLA_SECONDS = int(os.getenv("LATENCY_SLA_SECONDS", "300"))
LATENCY_STRICT_TARGET_SECONDS = int(os.getenv("LATENCY_STRICT_TARGET_SECONDS", "60"))

# Local Data Paths
DATA_DIR = BASE_DIR / "data"
RAW_CSV_PATH = DATA_DIR / "AirQualityUCI.csv"
CLEANED_CSV_PATH = DATA_DIR / "AirQualityCleaned.csv"
BASELINES_JSON_PATH = BASE_DIR / "config" / "baselines.json"
LOGS_DIR = BASE_DIR / "logs"
ALERT_LOG_PATH = LOGS_DIR / "alerts.log"

# Ensure runtime directories exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)
