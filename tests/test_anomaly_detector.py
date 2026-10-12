#!/usr/bin/env python3
"""
Unit Tests for Statistical Z-Score Anomaly Detector & Safety Thresholds (Phase 5)
"""

import unittest
import sys
from pathlib import Path

# Add project root
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import HARD_THRESHOLDS, DEFAULT_BASELINES


def calculate_z_score(val: float, mean: float, std: float) -> float:
    """Calculates absolute Z-Score."""
    if std <= 0 or val is None:
        return 0.0
    return abs(val - mean) / std


def classify_anomaly(pollutant: str, val: float, baselines: dict, hard_limits: dict) -> dict:
    """
    Evaluates whether a sensor reading is anomalous based on:
      1. |Z-Score| >= 3.0
      2. Hard AQI safety threshold exceeded
    """
    if val is None or pollutant not in baselines:
        return {"is_anomaly": False, "z_score": 0.0, "severity": "NORMAL", "reason": "No data"}

    stats = baselines[pollutant]
    mean = stats["mean"]
    std = stats["std"]
    z = calculate_z_score(val, mean, std)

    hard_limit = hard_limits.get(pollutant, float("inf"))
    is_hard_breach = val >= hard_limit
    is_z_breach = z >= 3.0

    is_anomaly = is_hard_breach or is_z_breach

    if z >= 5.0:
        severity = "EMERGENCY"
    elif z >= 3.5:
        severity = "CRITICAL"
    elif is_anomaly:
        severity = "WARNING"
    else:
        severity = "NORMAL"

    reason = []
    if is_z_breach:
        reason.append(f"Z-Score ({z:.2f} >= 3.0)")
    if is_hard_breach:
        reason.append(f"Hard Limit ({val:.2f} >= {hard_limit})")

    return {
        "is_anomaly": is_anomaly,
        "z_score": round(z, 2),
        "severity": severity,
        "reason": " & ".join(reason) if reason else "Normal"
    }


class TestAnomalyDetector(unittest.TestCase):
    def setUp(self):
        self.baselines = {
            "CO_GT": {"mean": 2.08, "std": 1.47},
            "C6H6_GT": {"mean": 10.19, "std": 7.57},
            "NOx_GT": {"mean": 240.73, "std": 206.62},
            "NO2_GT": {"mean": 109.41, "std": 47.22},
        }
        self.hard_limits = {
            "CO_GT": 10.0,
            "C6H6_GT": 15.0,
            "NOx_GT": 400.0,
            "NO2_GT": 200.0,
        }

    def test_normal_reading(self):
        # CO of 2.5 is very close to mean 2.08 (Z ~ 0.28)
        res = classify_anomaly("CO_GT", 2.5, self.baselines, self.hard_limits)
        self.assertFalse(res["is_anomaly"])
        self.assertEqual(res["severity"], "NORMAL")
        self.assertLess(res["z_score"], 1.0)

    def test_z_score_breach(self):
        # CO of 7.0: Z = (7.0 - 2.08) / 1.47 = 3.34 >= 3.0 (WARNING/CRITICAL)
        res = classify_anomaly("CO_GT", 7.0, self.baselines, self.hard_limits)
        self.assertTrue(res["is_anomaly"])
        self.assertGreaterEqual(res["z_score"], 3.0)
        self.assertIn(res["severity"], ["WARNING", "CRITICAL"])

    def test_hard_threshold_breach(self):
        # Benzene of 16.0 exceeds hard limit of 15.0
        res = classify_anomaly("C6H6_GT", 16.0, self.baselines, self.hard_limits)
        self.assertTrue(res["is_anomaly"])
        self.assertIn("Hard Limit", res["reason"])

    def test_emergency_spike(self):
        # Extreme CO spike of 20.0 (Z = (20 - 2.08)/1.47 = 12.19 >> 5.0)
        res = classify_anomaly("CO_GT", 20.0, self.baselines, self.hard_limits)
        self.assertTrue(res["is_anomaly"])
        self.assertEqual(res["severity"], "EMERGENCY")
        self.assertGreaterEqual(res["z_score"], 5.0)


if __name__ == "__main__":
    unittest.main()
