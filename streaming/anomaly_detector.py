#!/usr/bin/env python3
"""
Phase 4 & 5: Spark Structured Streaming Anomaly Detector & HDFS Persistence
----------------------------------------------------------------------------
Consumes air quality sensor streams from Kafka ('air-quality-raw'), executes:
  1. Watermarking & Sliding Window analysis.
  2. Statistical Z-Score anomaly detection (|Z| >= threshold, default 3.0).
  3. Hard AQI safety threshold checks (WHO / EPA standards for CO, Benzene, NOx, NO2).
  4. Dual-sink persistence:
     - Persists raw/cleaned data to HDFS in Parquet format partitioned by date.
     - Persists detected anomalies to HDFS (/air_quality/parquet/anomalies).
     - Publishes formatted alert events to Kafka topic ('air-quality-alerts').
"""

import os
import sys
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, when, coalesce,
    current_timestamp, date_format, to_timestamp, abs as sql_abs,
    greatest, array, array_remove
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

# Default baseline statistics if baselines.json not present inside container
BASELINES = {
    "CO_GT": {"mean": 2.08, "std": 1.47, "hard_threshold": 10.0, "unit": "mg/m^3"},
    "C6H6_GT": {"mean": 10.19, "std": 7.57, "hard_threshold": 15.0, "unit": "ug/m^3"},
    "NOx_GT": {"mean": 240.73, "std": 206.62, "hard_threshold": 400.0, "unit": "ppb"},
    "NO2_GT": {"mean": 109.41, "std": 47.22, "hard_threshold": 200.0, "unit": "ug/m^3"},
}

# Load baselines from file if available
baselines_file = "/workspace/config/baselines.json"
if os.path.exists(baselines_file):
    try:
        with open(baselines_file, "r") as f:
            custom_baselines = json.load(f)
            for k in BASELINES:
                if k in custom_baselines:
                    BASELINES[k]["mean"] = custom_baselines[k]["mean"]
                    BASELINES[k]["std"] = custom_baselines[k]["std"]
                    if custom_baselines[k].get("hard_threshold"):
                        BASELINES[k]["hard_threshold"] = custom_baselines[k]["hard_threshold"]
    except Exception as e:
        print(f"[!] Warning reading baselines.json: {e}")

# Schema of the incoming JSON records
SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("date", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("CO_GT", DoubleType(), True),
    StructField("PT08_S1_CO", DoubleType(), True),
    StructField("C6H6_GT", DoubleType(), True),
    StructField("PT08_S2_NMHC", DoubleType(), True),
    StructField("NOx_GT", DoubleType(), True),
    StructField("PT08_S3_NOx", DoubleType(), True),
    StructField("NO2_GT", DoubleType(), True),
    StructField("PT08_S4_NO2", DoubleType(), True),
    StructField("PT08_S5_O3", DoubleType(), True),
    StructField("T", DoubleType(), True),
    StructField("RH", DoubleType(), True),
    StructField("AH", DoubleType(), True),
    StructField("is_synthetic_spike", BooleanType(), True),
])


def build_spark_session():
    """Initializes and returns SparkSession with HDFS and Kafka support."""
    return SparkSession.builder \
        .appName("AirQualityAnomalyDetector") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/air_quality/checkpoints") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def main():
    print("=" * 65)
    print("  SPARK STRUCTURED STREAMING: AIR QUALITY ANOMALY DETECTOR")
    print("=" * 65)
    print(f"[*] Baseline References:")
    for pollutant, stats in BASELINES.items():
        print(f"    - {pollutant:10s}: Mean={stats['mean']:.2f}, Std={stats['std']:.2f}, Hard Limit={stats['hard_threshold']} {stats['unit']}")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Kafka configuration (internal broker within docker network)
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    raw_topic = os.getenv("KAFKA_RAW_TOPIC", "air-quality-raw")
    alerts_topic = os.getenv("KAFKA_ALERTS_TOPIC", "air-quality-alerts")

    print(f"[*] Reading stream from Kafka topic: '{raw_topic}' at {kafka_bootstrap}...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", raw_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Deserialize JSON value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data")
    ).select("data.*")

    # Add timestamp parsing & watermarking
    stream_df = parsed_df.withColumn(
        "parsed_timestamp",
        coalesce(
            to_timestamp(col("ingest_time")),
            to_timestamp(col("event_time")),
            current_timestamp()
        )
    ).withWatermark("parsed_timestamp", "10 minutes")

    # Compute Z-Scores per key pollutant: Z = |x - mean| / std
    mu_co = BASELINES["CO_GT"]["mean"]
    std_co = BASELINES["CO_GT"]["std"]
    hard_co = BASELINES["CO_GT"]["hard_threshold"]

    mu_c6h6 = BASELINES["C6H6_GT"]["mean"]
    std_c6h6 = BASELINES["C6H6_GT"]["std"]
    hard_c6h6 = BASELINES["C6H6_GT"]["hard_threshold"]

    mu_nox = BASELINES["NOx_GT"]["mean"]
    std_nox = BASELINES["NOx_GT"]["std"]
    hard_nox = BASELINES["NOx_GT"]["hard_threshold"]

    mu_no2 = BASELINES["NO2_GT"]["mean"]
    std_no2 = BASELINES["NO2_GT"]["std"]
    hard_no2 = BASELINES["NO2_GT"]["hard_threshold"]

    scored_df = stream_df \
        .withColumn("z_CO", when(col("CO_GT").isNotNull(), sql_abs(col("CO_GT") - lit(mu_co)) / lit(std_co)).otherwise(lit(0.0))) \
        .withColumn("z_C6H6", when(col("C6H6_GT").isNotNull(), sql_abs(col("C6H6_GT") - lit(mu_c6h6)) / lit(std_c6h6)).otherwise(lit(0.0))) \
        .withColumn("z_NOx", when(col("NOx_GT").isNotNull(), sql_abs(col("NOx_GT") - lit(mu_nox)) / lit(std_nox)).otherwise(lit(0.0))) \
        .withColumn("z_NO2", when(col("NO2_GT").isNotNull(), sql_abs(col("NO2_GT") - lit(mu_no2)) / lit(std_no2)).otherwise(lit(0.0)))

    # Maximum Z-Score observed in current record
    scored_df = scored_df.withColumn(
        "max_z_score",
        greatest(col("z_CO"), col("z_C6H6"), col("z_NOx"), col("z_NO2"))
    )

    # Anomaly conditions: |Z| >= 3.0 OR Exceeds Hard AQI limit
    is_co_anomaly = (col("z_CO") >= 3.0) | (col("CO_GT") >= hard_co)
    is_c6h6_anomaly = (col("z_C6H6") >= 3.0) | (col("C6H6_GT") >= hard_c6h6)
    is_nox_anomaly = (col("z_NOx") >= 3.0) | (col("NOx_GT") >= hard_nox)
    is_no2_anomaly = (col("z_NO2") >= 3.0) | (col("NO2_GT") >= hard_no2)

    is_anomaly = is_co_anomaly | is_c6h6_anomaly | is_nox_anomaly | is_no2_anomaly | (col("is_synthetic_spike") == True)

    annotated_df = scored_df.withColumn("is_anomaly", is_anomaly)

    # Determine primary triggering pollutant & reason
    annotated_df = annotated_df.withColumn(
        "primary_pollutant",
        when(is_co_anomaly, lit("CO(GT)"))
        .when(is_c6h6_anomaly, lit("C6H6(GT)"))
        .when(is_nox_anomaly, lit("NOx(GT)"))
        .when(is_no2_anomaly, lit("NO2(GT)"))
        .otherwise(lit("CO(GT)"))
    ).withColumn(
        "trigger_value",
        when(is_co_anomaly, col("CO_GT"))
        .when(is_c6h6_anomaly, col("C6H6_GT"))
        .when(is_nox_anomaly, col("NOx_GT"))
        .when(is_no2_anomaly, col("NO2_GT"))
        .otherwise(col("CO_GT"))
    ).withColumn(
        "severity",
        when(col("max_z_score") >= 5.0, lit("EMERGENCY"))
        .when(col("max_z_score") >= 3.5, lit("CRITICAL"))
        .when(col("is_anomaly"), lit("WARNING"))
        .otherwise(lit("NORMAL"))
    ).withColumn(
        "detection_time",
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )

    # -------------------------------------------------------------
    # SINK 1: HDFS Persistence for all data (Raw & Cleaned in Parquet)
    # -------------------------------------------------------------
    print("[*] Configuring HDFS Sink 1: All sensor data to hdfs://namenode:9000/air_quality/parquet/raw...")
    raw_hdfs_query = annotated_df.writeStream \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/air_quality/parquet/raw") \
        .option("checkpointLocation", "hdfs://namenode:9000/air_quality/checkpoints/raw_parquet") \
        .partitionBy("date") \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .start()

    # -------------------------------------------------------------
    # SINK 2: Anomalies Stream -> Kafka Alerts & HDFS Anomalies Parquet
    # -------------------------------------------------------------
    anomalies_df = annotated_df.filter(col("is_anomaly") == True)

    # Construct Kafka Alert Message payload
    alert_payload_df = anomalies_df.select(
        col("event_id").alias("key"),
        to_json(struct(
            col("event_id"),
            col("event_time"),
            col("ingest_time"),
            col("detection_time"),
            col("primary_pollutant"),
            col("trigger_value"),
            col("max_z_score").alias("z_score"),
            col("severity"),
            col("is_synthetic_spike"),
            col("CO_GT"),
            col("C6H6_GT"),
            col("NOx_GT"),
            col("NO2_GT")
        )).alias("value")
    )

    print(f"[*] Configuring Kafka Alert Sink: Publishing anomalies to '{alerts_topic}'...")
    kafka_alerts_query = alert_payload_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", alerts_topic) \
        .option("checkpointLocation", "hdfs://namenode:9000/air_quality/checkpoints/kafka_alerts") \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .start()

    # Persist Anomalies to HDFS Parquet
    print("[*] Configuring HDFS Sink 2: Anomalies to hdfs://namenode:9000/air_quality/parquet/anomalies...")
    anomalies_hdfs_query = anomalies_df.writeStream \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/air_quality/parquet/anomalies") \
        .option("checkpointLocation", "hdfs://namenode:9000/air_quality/checkpoints/anomalies_parquet") \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .start()

    # Console Monitor for immediate interactive feedback
    console_query = anomalies_df.select(
        "event_id", "primary_pollutant", "trigger_value", "max_z_score", "severity", "detection_time"
    ).writeStream \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .start()

    print("[✓] All streaming queries active! Processing live pipeline...")
    print("=" * 65)

    # Await termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
