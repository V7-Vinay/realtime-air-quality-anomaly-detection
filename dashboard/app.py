#!/usr/bin/env python3
"""
Real-Time Air Quality Monitoring Dashboard Backend
FastAPI server serving live telemetry, alerts, and interactive spike injection via Server-Sent Events (SSE).
"""

import os
import sys
import json
import time
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_TOPIC,
    KAFKA_ALERTS_TOPIC,
    BASELINES_JSON_PATH,
    ALERT_LOG_PATH,
    LOGS_DIR
)
from producer.producer import get_kafka_producer

app = FastAPI(title="Air Quality Real-Time Anomaly Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HTML_PATH = BASE_DIR / "dashboard" / "index.html"


@app.get("/", response_class=HTMLResponse)
async def get_index():
    if not HTML_PATH.exists():
        return "<h1>Dashboard UI Not Found</h1>"
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/baselines")
async def get_baselines():
    if BASELINES_JSON_PATH.exists():
        with open(BASELINES_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


@app.get("/api/metrics")
async def get_metrics():
    metrics_file = LOGS_DIR / "latency_metrics.json"
    if metrics_file.exists():
        with open(metrics_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "total_alerts": 0,
        "latency_min_seconds": 0.0,
        "latency_avg_seconds": 0.0,
        "latency_max_seconds": 0.0,
        "sla_target_met": True
    }


@app.post("/api/inject-spike")
async def inject_spike():
    """Injects a deterministic pollution spike into Kafka."""
    try:
        producer = get_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
        spike_id = f"UI-SPIKE-{int(time.time() * 1000) % 100000}"
        now_utc = datetime.now(timezone.utc).isoformat()
        
        payload = {
            "event_id": spike_id,
            "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ingest_time": now_utc,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "hour": datetime.now().hour,
            "CO_GT": 24.5,
            "PT08_S1_CO": 1850.0,
            "C6H6_GT": 38.0,
            "PT08_S2_NMHC": 1420.0,
            "NOx_GT": 880.0,
            "PT08_S3_NOx": 380.0,
            "NO2_GT": 290.0,
            "PT08_S4_NO2": 2100.0,
            "PT08_S5_O3": 1750.0,
            "T": 25.4,
            "RH": 62.0,
            "AH": 1.28,
            "is_synthetic_spike": True
        }
        producer.send(KAFKA_RAW_TOPIC, payload)
        producer.flush()
        return {"status": "success", "event_id": spike_id, "ingest_time": now_utc}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/stream-alerts")
async def stream_alerts():
    """Server-Sent Events streaming live alert notifications."""
    async def event_generator():
        last_pos = 0
        while True:
            if ALERT_LOG_PATH.exists():
                with open(ALERT_LOG_PATH, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    if len(lines) > last_pos:
                        new_lines = lines[last_pos:]
                        last_pos = len(lines)
                        for line in new_lines:
                            line = line.strip()
                            if line:
                                yield f"data: {line}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
