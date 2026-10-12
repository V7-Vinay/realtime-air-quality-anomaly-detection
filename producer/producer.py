#!/usr/bin/env python3
"""
Phase 3: Air Quality Kafka Streaming Producer
----------------------------------------------
Streams sensor readings from the cleaned UCI dataset into Apache Kafka.
Features:
  1. Configurable replay speed:
     - Real-time or accelerated (e.g. 1 hour of data per second, or custom interval).
  2. Synthetic pollution spike injection:
     - Deterministically multiplies pollutant concentrations (CO, NOx, C6H6) by 3-5x
       to trigger and test anomaly detection rules in real-time.
  3. Latency tracking timestamps:
     - 'event_time': Original historical timestamp (or synchronized relative time).
     - 'ingest_time': Exact UTC timestamp when the producer emitted the message to Kafka.
  4. Robust multi-driver support:
     - Automatically uses confluent-kafka or kafka-python-ng.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_TOPIC,
    CLEANED_CSV_PATH,
    PRODUCER_REPLAY_INTERVAL_SECONDS,
    PRODUCER_INJECT_SPIKES,
    PRODUCER_SPIKE_INTERVAL_RECORDS,
    PRODUCER_SPIKE_MULTIPLIER,
)


def get_kafka_producer(bootstrap_servers: str):
    """Initializes and returns a Kafka producer instance."""
    # Attempt confluent-kafka first
    try:
        from confluent_kafka import Producer
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "air-quality-producer",
            "linger.ms": 10,
            "acks": "all",
        }
        producer = Producer(conf)

        class ConfluentWrapper:
            def __init__(self, p):
                self.p = p

            def send(self, topic, value):
                payload = json.dumps(value).encode("utf-8")
                self.p.produce(topic, value=payload)
                self.p.poll(0)

            def flush(self):
                self.p.flush()

        print(f"[OK] Connected via confluent-kafka to {bootstrap_servers}")
        return ConfluentWrapper(producer)
    except Exception as e1:
        # Fallback to kafka-python-ng
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=10,
            )
            print(f"[OK] Connected via kafka-python to {bootstrap_servers}")
            return producer
        except Exception as e2:
            print(f"[!] Error: Could not connect to Kafka at {bootstrap_servers}.")
            print(f"    confluent-kafka error: {e1}")
            print(f"    kafka-python error: {e2}")
            raise ConnectionError(f"Kafka unavailable at {bootstrap_servers}")


def run_producer(
    csv_path: Path,
    topic: str,
    bootstrap_servers: str,
    interval: float,
    inject_spikes: bool,
    spike_interval: int,
    spike_mult: float,
    limit: int = None,
):
    """Replays sensor data into Kafka with optional spike injection."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Cleaned dataset not found at {csv_path}. Run preprocessing/clean.py first.")

    df = pd.read_csv(csv_path)
    total_rows = len(df)
    if limit:
        df = df.head(limit)
        total_rows = len(df)

    print("=" * 65)
    print("  PHASE 3: AIR QUALITY KAFKA STREAMING PRODUCER")
    print("=" * 65)
    print(f"[*] Target Kafka Topic  : {topic}")
    print(f"[*] Bootstrap Servers   : {bootstrap_servers}")
    print(f"[*] Emission Interval   : {interval:.3f} s/msg")
    print(f"[*] Spike Injection     : {'ACTIVE' if inject_spikes else 'DISABLED'}")
    if inject_spikes:
        print(f"    - Interval Every    : {spike_interval} records")
        print(f"    - Multiplier        : {spike_mult}x baseline")
    print(f"[*] Total Records to Stream: {total_rows:,}")
    print("=" * 65)

    producer = get_kafka_producer(bootstrap_servers)
    emitted_count = 0
    spikes_injected = 0
    start_wall_time = time.time()

    try:
        for idx, row in df.iterrows():
            current_utc = datetime.now(timezone.utc).isoformat()
            
            # Construct message dictionary
            record = {
                "event_id": f"evt-{idx:06d}",
                "event_time": str(row.get("timestamp", current_utc)),
                "ingest_time": current_utc,
                "date": str(row.get("date", "")),
                "hour": int(row.get("hour", 0)),
                "CO_GT": None if pd.isna(row.get("CO_GT")) else float(row.get("CO_GT")),
                "PT08_S1_CO": None if pd.isna(row.get("PT08_S1_CO")) else float(row.get("PT08_S1_CO")),
                "C6H6_GT": None if pd.isna(row.get("C6H6_GT")) else float(row.get("C6H6_GT")),
                "PT08_S2_NMHC": None if pd.isna(row.get("PT08_S2_NMHC")) else float(row.get("PT08_S2_NMHC")),
                "NOx_GT": None if pd.isna(row.get("NOx_GT")) else float(row.get("NOx_GT")),
                "PT08_S3_NOx": None if pd.isna(row.get("PT08_S3_NOx")) else float(row.get("PT08_S3_NOx")),
                "NO2_GT": None if pd.isna(row.get("NO2_GT")) else float(row.get("NO2_GT")),
                "PT08_S4_NO2": None if pd.isna(row.get("PT08_S4_NO2")) else float(row.get("PT08_S4_NO2")),
                "PT08_S5_O3": None if pd.isna(row.get("PT08_S5_O3")) else float(row.get("PT08_S5_O3")),
                "T": None if pd.isna(row.get("T")) else float(row.get("T")),
                "RH": None if pd.isna(row.get("RH")) else float(row.get("RH")),
                "AH": None if pd.isna(row.get("AH")) else float(row.get("AH")),
                "is_synthetic_spike": False,
            }

            # Inject deterministic pollution spike if configured
            if inject_spikes and (emitted_count > 0) and (emitted_count % spike_interval == 0):
                record["is_synthetic_spike"] = True
                spikes_injected += 1
                if record["CO_GT"] is not None:
                    record["CO_GT"] = round(record["CO_GT"] * spike_mult, 2)
                else:
                    record["CO_GT"] = round(12.5 * spike_mult, 2)

                if record["C6H6_GT"] is not None:
                    record["C6H6_GT"] = round(record["C6H6_GT"] * spike_mult, 2)
                else:
                    record["C6H6_GT"] = round(22.0 * spike_mult, 2)

                if record["NOx_GT"] is not None:
                    record["NOx_GT"] = round(record["NOx_GT"] * spike_mult, 2)
                else:
                    record["NOx_GT"] = round(550.0 * spike_mult, 2)

                print(f"[!] >>> SYNTHETIC SPIKE INJECTED at record #{emitted_count}: "
                      f"CO={record['CO_GT']}, Benzene={record['C6H6_GT']}, NOx={record['NOx_GT']}")

            # Emit to Kafka
            producer.send(topic, record)
            emitted_count += 1

            if emitted_count % 50 == 0:
                elapsed = time.time() - start_wall_time
                rate = emitted_count / elapsed if elapsed > 0 else 0
                print(f"[STREAM] Emitted {emitted_count:5d}/{total_rows} msgs | "
                      f"Spikes: {spikes_injected:2d} | Rate: {rate:5.1f} msg/s | "
                      f"Last: CO={record.get('CO_GT')}, C6H6={record.get('C6H6_GT')}")

            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[!] Streaming interrupted by user.")
    finally:
        print("[*] Flushing pending messages to Kafka...")
        producer.flush()
        elapsed = time.time() - start_wall_time
        print(f"[OK] Completed: {emitted_count:,} messages sent in {elapsed:.2f}s "
              f"({spikes_injected} synthetic spikes injected).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Air Quality Kafka Streaming Producer")
    parser.add_argument("--csv", type=Path, default=CLEANED_CSV_PATH, help="Path to cleaned dataset CSV")
    parser.add_argument("--topic", type=str, default=KAFKA_RAW_TOPIC, help="Kafka destination topic")
    parser.add_argument("--bootstrap", type=str, default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap server")
    parser.add_argument("--interval", type=float, default=PRODUCER_REPLAY_INTERVAL_SECONDS, help="Seconds per record")
    parser.add_argument("--speed", type=float, default=None, help="Speedup factor (e.g. 3600 for 1h/sec)")
    parser.add_argument("--inject-spikes", action="store_true", default=PRODUCER_INJECT_SPIKES, help="Inject spikes")
    parser.add_argument("--spike-interval", type=int, default=PRODUCER_SPIKE_INTERVAL_RECORDS, help="Spike frequency")
    parser.add_argument("--spike-mult", type=float, default=PRODUCER_SPIKE_MULTIPLIER, help="Spike multiplier")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of messages")

    args = parser.parse_args()

    # Calculate interval if speedup factor provided (dataset is hourly: 3600s between records)
    replay_interval = args.interval
    if args.speed and args.speed > 0:
        replay_interval = 3600.0 / args.speed

    run_producer(
        csv_path=args.csv,
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        interval=replay_interval,
        inject_spikes=args.inject_spikes,
        spike_interval=args.spike_interval,
        spike_mult=args.spike_mult,
        limit=args.limit,
    )
