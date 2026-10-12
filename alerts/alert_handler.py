#!/usr/bin/env python3
"""
Phase 6: Automated Real-Time Alert Handler & Latency Monitor
------------------------------------------------------------
Subscribes to the Kafka topic 'air-quality-alerts', processes anomaly alerts,
formats visual alerts with severity classification, and computes end-to-end latency:
  - Ingest-to-Alert Latency: Time from Kafka ingestion to alert handler reception
  - Event-to-Alert Latency: Time from sensor observation to alert handler reception
Saves alert history to logs/alerts.log and metrics to logs/latency_metrics.json.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_ALERTS_TOPIC,
    ALERT_LOG_PATH,
    LOGS_DIR,
    LATENCY_SLA_SECONDS
)

# Severity color formatting (ANSI)
COLOR_RESET = "\033[0m"
COLOR_RED_BOLD = "\033[1;31m"
COLOR_YELLOW_BOLD = "\033[1;33m"
COLOR_MAGENTA_BOLD = "\033[1;35m"
COLOR_CYAN = "\033[0;36m"
COLOR_GREEN = "\033[0;32m"


def parse_iso_datetime(dt_str: str):
    """Safely parses ISO timestamp strings to UTC datetime."""
    if not dt_str:
        return None
    try:
        # Standard ISO format with Z or timezone offset
        dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str)
    except Exception:
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None


class AlertHandler:
    def __init__(self, bootstrap_servers: str, topic: str, log_file: Path = ALERT_LOG_PATH):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.latency_records = []
        self.alert_count = 0

    def get_consumer(self):
        """Initializes Kafka consumer with confluent-kafka or kafka-python."""
        try:
            from confluent_kafka import Consumer
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"alert-handler-{int(time.time())}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
            c = Consumer(conf)
            c.subscribe([self.topic])

            class ConfluentConsumerWrapper:
                def __init__(self, consumer):
                    self.c = consumer

                def poll_message(self, timeout=1.0):
                    msg = self.c.poll(timeout)
                    if msg is None or msg.error():
                        return None
                    return json.loads(msg.value().decode("utf-8"))

                def close(self):
                    self.c.close()

            print(f"[OK] AlertHandler connected via confluent-kafka to {self.bootstrap_servers}")
            return ConfluentConsumerWrapper(c)
        except Exception as e1:
            try:
                from kafka import KafkaConsumer
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=f"alert-handler-{int(time.time())}",
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=1000
                )
                print(f"[OK] AlertHandler connected via kafka-python to {self.bootstrap_servers}")

                class PythonConsumerWrapper:
                    def __init__(self, consumer):
                        self.c = consumer
                        self.iter = None

                    def poll_message(self, timeout=1.0):
                        try:
                            msg_batch = self.c.poll(timeout_ms=int(timeout * 1000), max_records=1)
                            for tp, messages in msg_batch.items():
                                for m in messages:
                                    return m.value
                            return None
                        except Exception:
                            return None

                    def close(self):
                        self.c.close()

                return PythonConsumerWrapper(consumer)
            except Exception as e2:
                print(f"[!] Error: Could not connect Kafka consumer: {e1} / {e2}")
                raise ConnectionError("Kafka consumer initialization failed")

    def format_and_dispatch(self, alert_data: dict) -> dict:
        """Processes alert payload, computes latency, prints formatted alert and logs."""
        alert_receipt_time = datetime.now(timezone.utc)
        alert_receipt_iso = alert_receipt_time.isoformat()

        event_time_str = alert_data.get("event_time")
        ingest_time_str = alert_data.get("ingest_time")
        detection_time_str = alert_data.get("detection_time")

        # Compute Latency
        ingest_dt = parse_iso_datetime(ingest_time_str)
        if ingest_dt:
            if ingest_dt.tzinfo is None:
                ingest_dt = ingest_dt.replace(tzinfo=timezone.utc)
            latency_ingest_sec = max(0.0, (alert_receipt_time - ingest_dt).total_seconds())
        else:
            latency_ingest_sec = 0.0

        event_dt = parse_iso_datetime(event_time_str)
        if event_dt:
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
            latency_event_sec = max(0.0, (alert_receipt_time - event_dt).total_seconds())
        else:
            latency_event_sec = latency_ingest_sec

        severity = alert_data.get("severity", "WARNING").upper()
        pollutant = alert_data.get("primary_pollutant", "Air Pollutant")
        value = alert_data.get("trigger_value", 0.0)
        z_score = alert_data.get("z_score", 0.0)
        event_id = alert_data.get("event_id", "N/A")
        is_spike = alert_data.get("is_synthetic_spike", False)

        # Color and icon selection
        if severity == "EMERGENCY":
            sev_badge = f"{COLOR_RED_BOLD}[🚨 EMERGENCY HAZARD]{COLOR_RESET}"
        elif severity == "CRITICAL":
            sev_badge = f"{COLOR_MAGENTA_BOLD}[⚠️ CRITICAL SPIKE]{COLOR_RESET}"
        else:
            sev_badge = f"{COLOR_YELLOW_BOLD}[🔔 WARNING ALERT]{COLOR_RESET}"

        self.alert_count += 1
        self.latency_records.append({
            "alert_id": self.alert_count,
            "event_id": event_id,
            "pollutant": pollutant,
            "severity": severity,
            "latency_seconds": round(latency_ingest_sec, 3),
            "timestamp": alert_receipt_iso
        })

        # Terminal formatted card
        print("\n" + "=" * 70)
        print(f" {sev_badge} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("=" * 70)
        print(f"  • Event ID           : {event_id} {'(Synthetic Spike Test)' if is_spike else ''}")
        print(f"  • Trigger Pollutant  : {COLOR_CYAN}{pollutant}{COLOR_RESET}")
        print(f"  • Measured Reading   : {COLOR_RED_BOLD}{value:.2f}{COLOR_RESET}")
        print(f"  • Statistical Z-Score: {COLOR_MAGENTA_BOLD}{z_score:.2f} \u03c3{COLOR_RESET} (Threshold: \u2265 3.0)")
        print(f"  • Event Time         : {event_time_str}")
        print(f"  • Kafka Ingest Time  : {ingest_time_str}")
        print(f"  • Detection Time     : {detection_time_str}")
        print(f"  • Alert Receipt Time : {alert_receipt_iso}")
        print(f"  • End-to-End Latency : {COLOR_GREEN}{latency_ingest_sec:.3f} seconds{COLOR_RESET} "
              f"(SLA Target: < {LATENCY_SLA_SECONDS}s)")
        print("=" * 70)

        # Persist structured entry to log file
        log_entry = {
            "timestamp": alert_receipt_iso,
            "event_id": event_id,
            "severity": severity,
            "primary_pollutant": pollutant,
            "trigger_value": value,
            "z_score": round(float(z_score), 2),
            "latency_ingest_seconds": round(latency_ingest_sec, 3),
            "is_synthetic_spike": is_spike,
            "raw_payload": alert_data
        }

        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

        # Save cumulative metrics
        self.save_metrics()
        return log_entry

    def save_metrics(self):
        """Calculates and writes summary latency statistics to JSON."""
        if not self.latency_records:
            return

        latencies = [r["latency_seconds"] for r in self.latency_records]
        metrics = {
            "total_alerts": len(latencies),
            "latency_min_seconds": round(min(latencies), 3),
            "latency_avg_seconds": round(sum(latencies) / len(latencies), 3),
            "latency_max_seconds": round(max(latencies), 3),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "sla_target_met": max(latencies) <= LATENCY_SLA_SECONDS
        }

        metrics_file = LOGS_DIR / "latency_metrics.json"
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

    def listen(self, max_alerts: int = None, timeout_seconds: float = None):
        """Continuously polls Kafka for alerts until interrupted or limits met."""
        print("=" * 65)
        print("  PHASE 6: AIR QUALITY AUTOMATED ALERT HANDLER")
        print("=" * 65)
        print(f"[*] Subscribed Topic  : {self.topic}")
        print(f"[*] Bootstrap Servers : {self.bootstrap_servers}")
        print(f"[*] Alert Log Destination : {self.log_file}")
        print(f"[*] Awaiting real-time anomaly alerts (Press Ctrl+C to exit)...")
        print("=" * 65)

        consumer = self.get_consumer()
        start_time = time.time()

        try:
            while True:
                msg = consumer.poll_message(timeout=1.0)
                if msg is not None:
                    self.format_and_dispatch(msg)
                    if max_alerts and self.alert_count >= max_alerts:
                        print(f"\n[OK] Target of {max_alerts} alerts received.")
                        break

                if timeout_seconds and (time.time() - start_time) >= timeout_seconds:
                    print(f"\n[!] Timeout of {timeout_seconds}s reached.")
                    break
        except KeyboardInterrupt:
            print("\n[!] AlertHandler stopped by user.")
        finally:
            consumer.close()
            self.print_summary()

    def print_summary(self):
        """Prints a latency summary report."""
        if not self.latency_records:
            print("\n[*] No alerts received during this session.")
            return

        latencies = [r["latency_seconds"] for r in self.latency_records]
        print("\n" + "=" * 65)
        print("  ALERT HANDLER LATENCY SUMMARY REPORT")
        print("=" * 65)
        print(f"  • Total Alerts Handled : {len(latencies)}")
        print(f"  • Minimum Latency      : {min(latencies):.3f} seconds")
        print(f"  • Average Latency      : {sum(latencies)/len(latencies):.3f} seconds")
        print(f"  • Maximum Latency      : {max(latencies):.3f} seconds")
        print(f"  • SLA Target (< 300s)  : {'PASSED [✓]' if max(latencies) < 300 else 'FAILED [X]'}")
        print(f"  • Metrics Saved To     : {LOGS_DIR / 'latency_metrics.json'}")
        print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Air Quality Automated Alert Handler")
    parser.add_argument("--topic", type=str, default=KAFKA_ALERTS_TOPIC, help="Kafka alerts topic")
    parser.add_argument("--bootstrap", type=str, default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka broker")
    parser.add_argument("--max-alerts", type=int, default=None, help="Stop after N alerts")
    parser.add_argument("--timeout", type=float, default=None, help="Stop after N seconds")

    args = parser.parse_args()
    handler = AlertHandler(bootstrap_servers=args.bootstrap, topic=args.topic)
    handler.listen(max_alerts=args.max_alerts, timeout_seconds=args.timeout)
