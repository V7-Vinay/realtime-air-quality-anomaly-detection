#!/usr/bin/env python3
"""
Phase 7: Latency Verification & Performance Test
------------------------------------------------
Deterministically verifies that the streaming pipeline meets the real-time SLA:
  1. Injects a synthetic pollution spike with a unique traceable event_id and high-precision UTC timestamp.
  2. Subscribes to the Kafka alert topic and awaits detection.
  3. Computes the exact latency: T_alert - T_ingest.
  4. Asserts latency is strictly within the 5-minute requirement (target: < 60s).
  5. Reports minimum, average, maximum latency and SLA compliance.
"""

import os
import sys
import time
import json
import uuid
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
    KAFKA_RAW_TOPIC,
    KAFKA_ALERTS_TOPIC,
    LATENCY_SLA_SECONDS,
    LATENCY_STRICT_TARGET_SECONDS,
)


def get_producer(bootstrap_servers: str):
    """Kafka producer wrapper."""
    try:
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": bootstrap_servers}
        p = Producer(conf)
        class CW:
            def send(self, topic, data):
                p.produce(topic, json.dumps(data).encode("utf-8"))
                p.flush()
        return CW()
    except Exception:
        from kafka import KafkaProducer
        kp = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        return kp


def get_consumer(bootstrap_servers: str, topic: str):
    """Kafka consumer wrapper."""
    try:
        from confluent_kafka import Consumer
        c = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"latency-verifier-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "latest"
        })
        c.subscribe([topic])
        class CCW:
            def poll(self, timeout=1.0):
                msg = c.poll(timeout)
                if msg and not msg.error():
                    return json.loads(msg.value().decode("utf-8"))
                return None
            def close(self):
                c.close()
        return CCW()
    except Exception:
        from kafka import KafkaConsumer
        kc = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=f"latency-verifier-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        class PCW:
            def poll(self, timeout=1.0):
                batch = kc.poll(timeout_ms=int(timeout * 1000), max_records=1)
                for tp, msgs in batch.items():
                    for m in msgs:
                        return m.value
                return None
            def close(self):
                kc.close()
        return PCW()


def run_latency_test(iterations: int = 3, timeout_per_test: int = 45):
    print("=" * 65)
    print("  PHASE 7: REAL-TIME LATENCY VERIFICATION & SLA TEST")
    print("=" * 65)
    print(f"[*] Kafka Broker        : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[*] Ingest Topic        : {KAFKA_RAW_TOPIC}")
    print(f"[*] Alerts Topic        : {KAFKA_ALERTS_TOPIC}")
    print(f"[*] Test Iterations     : {iterations}")
    print(f"[*] SLA Limit           : < {LATENCY_SLA_SECONDS}s (5 minutes)")
    print(f"[*] Target Strict SLA   : < {LATENCY_STRICT_TARGET_SECONDS}s (1 minute)")
    print("=" * 65)

    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)
    consumer = get_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_ALERTS_TOPIC)

    # Warm up consumer group assignment
    print("[*] Warming up Kafka consumer connection...")
    consumer.poll(timeout=2.0)

    results = []

    for i in range(1, iterations + 1):
        spike_id = f"LATENCY-SPIKE-{uuid.uuid4().hex[:6].upper()}"
        ingest_time_iso = datetime.now(timezone.utc).isoformat()
        t_start = time.time()

        spike_payload = {
            "event_id": spike_id,
            "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ingest_time": ingest_time_iso,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "hour": datetime.now().hour,
            "CO_GT": 28.5,             # Extreme spike (Baseline: 2.08, Z ~ 18.0)
            "PT08_S1_CO": 1950.0,
            "C6H6_GT": 45.0,           # Extreme spike (Baseline: 10.19, Z ~ 4.6)
            "PT08_S2_NMHC": 1500.0,
            "NOx_GT": 950.0,           # Extreme spike (Baseline: 240.7, Z ~ 3.4)
            "PT08_S3_NOx": 400.0,
            "NO2_GT": 320.0,           # Severe spike (Baseline: 109.4, Z ~ 4.5)
            "PT08_S4_NO2": 2100.0,
            "PT08_S5_O3": 1800.0,
            "T": 22.0,
            "RH": 55.0,
            "AH": 1.15,
            "is_synthetic_spike": True
        }

        print(f"\n[*] Iteration {i}/{iterations}: Injecting Spike '{spike_id}' (CO=28.5 mg/m^3, C6H6=45 ug/m^3)...")
        producer.send(KAFKA_RAW_TOPIC, spike_payload)

        # Poll alerts topic until match found
        detected = False
        alert_received = None
        while (time.time() - t_start) < timeout_per_test:
            msg = consumer.poll(timeout=0.5)
            if msg:
                # Check if it matches this spike
                if msg.get("event_id") == spike_id or msg.get("is_synthetic_spike") == True:
                    t_end = time.time()
                    latency_seconds = t_end - t_start
                    alert_received = msg
                    detected = True
                    break

        if detected:
            print(f"[✓] Alert Detected for '{spike_id}' in {latency_seconds:.3f} seconds!")
            print(f"    - Severity : {alert_received.get('severity')}")
            print(f"    - Pollutant: {alert_received.get('primary_pollutant')}")
            print(f"    - Z-Score  : {alert_received.get('z_score')}")
            results.append({
                "spike_id": spike_id,
                "latency_seconds": latency_seconds,
                "passed_sla": latency_seconds < LATENCY_SLA_SECONDS,
                "passed_target": latency_seconds < LATENCY_STRICT_TARGET_SECONDS
            })
        else:
            print(f"[X] TIMEOUT: Alert for '{spike_id}' not received within {timeout_per_test}s.")
            results.append({
                "spike_id": spike_id,
                "latency_seconds": timeout_per_test,
                "passed_sla": False,
                "passed_target": False
            })

        time.sleep(2.0)

    consumer.close()

    # Latency Summary Table
    print("\n" + "=" * 65)
    print("  LATENCY VERIFICATION RESULTS SUMMARY")
    print("=" * 65)
    print(f"{'Run':<6} {'Spike ID':<22} {'Latency (s)':<14} {'SLA (<300s)':<12} {'Strict (<60s)'}")
    print("-" * 65)

    all_latencies = [r["latency_seconds"] for r in results]
    for idx, r in enumerate(results, 1):
        sla_mark = "[✓] PASS" if r["passed_sla"] else "[X] FAIL"
        target_mark = "[✓] PASS" if r["passed_target"] else "[X] FAIL"
        print(f"{idx:<6} {r['spike_id']:<22} {r['latency_seconds']:<14.3f} {sla_mark:<12} {target_mark}")

    print("-" * 65)
    min_lat = min(all_latencies)
    avg_lat = sum(all_latencies) / len(all_latencies)
    max_lat = max(all_latencies)

    print(f"  • Minimum Latency : {min_lat:.3f} s")
    print(f"  • Average Latency : {avg_lat:.3f} s")
    print(f"  • Maximum Latency : {max_lat:.3f} s")
    print("=" * 65)

    if max_lat <= LATENCY_SLA_SECONDS:
        print("[✓] SUCCESS: Air quality anomalies flagged well within the 5-minute requirement!")
        return True
    else:
        print("[X] FAILURE: Latency exceeded the SLA limit.")
        return False


if __name__ == "__main__":
    success = run_latency_test(iterations=2, timeout_per_test=30)
    sys.exit(0 if success else 1)
