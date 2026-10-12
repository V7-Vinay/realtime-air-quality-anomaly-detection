#!/usr/bin/env python3
"""
Phase 5 Baseline: Historical Statistics Batch Job
-------------------------------------------------
Calculates the historical reference statistics (Mean, Std Dev, Min, Max, Quantiles)
per pollutant using historical observations.
Saves the baseline metadata to `config/baselines.json` so the Spark Streaming
Anomaly Detector can dynamically query reference parameters:
    Z-Score = (x - mean) / std
"""

import os
import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add base directory to path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import DATA_DIR, BASELINES_JSON_PATH, HARD_THRESHOLDS

KEY_POLLUTANTS = [
    "CO_GT", "PT08_S1_CO", "C6H6_GT", "PT08_S2_NMHC",
    "NOx_GT", "PT08_S3_NOx", "NO2_GT", "PT08_S4_NO2",
    "PT08_S5_O3", "T", "RH", "AH"
]

UNITS = {
    "CO_GT": "mg/m^3",
    "PT08_S1_CO": "sensor_res",
    "C6H6_GT": "ug/m^3",
    "PT08_S2_NMHC": "sensor_res",
    "NOx_GT": "ppb",
    "PT08_S3_NOx": "sensor_res",
    "NO2_GT": "ug/m^3",
    "PT08_S4_NO2": "sensor_res",
    "PT08_S5_O3": "sensor_res",
    "T": "degC",
    "RH": "%",
    "AH": "g/m^3",
}


def compute_baseline_statistics(input_csv: Path) -> dict:
    """Computes robust statistical metrics per pollutant."""
    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found at {input_csv}. Run preprocessing/clean.py first.")

    df = pd.read_csv(input_csv)
    baselines = {}

    print("=" * 65)
    print("  PHASE 5: HISTORICAL BASELINE STATISTICS COMPUTATION")
    print("=" * 65)
    print(f"{'Pollutant':<14} {'Mean':<10} {'Std Dev':<10} {'Median':<10} {'Hard Threshold':<15}")
    print("-" * 65)

    for col in KEY_POLLUTANTS:
        if col in df.columns:
            series = df[col].dropna()
            mean_val = float(series.mean())
            std_val = float(series.std())
            median_val = float(series.median())
            q25 = float(series.quantile(0.25))
            q75 = float(series.quantile(0.75))
            q99 = float(series.quantile(0.99))
            min_val = float(series.min())
            max_val = float(series.max())

            hard_limit = HARD_THRESHOLDS.get(col, None)
            hard_limit_str = f"{hard_limit}" if hard_limit is not None else "N/A"

            print(f"{col:<14} {mean_val:<10.2f} {std_val:<10.2f} {median_val:<10.2f} {hard_limit_str:<15}")

            baselines[col] = {
                "mean": round(mean_val, 4),
                "std": round(std_val, 4),
                "median": round(median_val, 4),
                "q25": round(q25, 4),
                "q75": round(q75, 4),
                "q99": round(q99, 4),
                "min": round(min_val, 4),
                "max": round(max_val, 4),
                "unit": UNITS.get(col, "unknown"),
                "hard_threshold": hard_limit,
                "count": int(len(series))
            }

    print("-" * 65)
    
    # Save to JSON
    BASELINES_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(baselines, f, indent=2)

    print(f"[OK] Baselines exported to: {BASELINES_JSON_PATH}")
    print("=" * 65)
    return baselines


if __name__ == "__main__":
    imputed_file = DATA_DIR / "AirQualityImputed.csv"
    if not imputed_file.exists():
        imputed_file = DATA_DIR / "AirQualityCleaned.csv"
    compute_baseline_statistics(imputed_file)
