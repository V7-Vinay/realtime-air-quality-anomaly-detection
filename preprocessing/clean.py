#!/usr/bin/env python3
"""
Phase 2: Data Cleaning & Preprocessing Pipeline
------------------------------------------------
Processes the raw UCI Air Quality dataset:
  1. Drops trailing empty columns (e.g. Unnamed: 15, 16) and blank rows.
  2. Resolves comma decimal representations (e.g. '2,6' -> 2.6).
  3. Replaces sentinel -200 values with standard IEEE NaN / None (null).
  4. Parses 'Date' (DD/MM/YYYY) and 'Time' (HH.MM.SS) into an ISO-8601 timestamp.
  5. Renames column headers to Big Data compliant identifiers (no parens/dots).
  6. Imputes missing values using forward-fill (LOCF - Last Observation Carried Forward)
     and backward-fill to maintain atmospheric physical continuity for baseline statistics.

Design Justification for Missing Value Imputation:
  - Atmospheric sensor streams exhibit high temporal autocorrelation (inertia).
  - Forward-fill (LOCF) preserves diurnal patterns (e.g. rush-hour NOx and benzene peaks)
    without introducing artificial global mean step changes that distort Z-scores.
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.config import RAW_CSV_PATH, CLEANED_CSV_PATH, DATA_DIR

# Standard column mappings for Big Data compatibility
COLUMN_RENAME_MAP = {
    "Date": "Date",
    "Time": "Time",
    "CO(GT)": "CO_GT",
    "PT08.S1(CO)": "PT08_S1_CO",
    "NMHC(GT)": "NMHC_GT",
    "C6H6(GT)": "C6H6_GT",
    "PT08.S2(NMHC)": "PT08_S2_NMHC",
    "NOx(GT)": "NOx_GT",
    "PT08.S3(NOx)": "PT08_S3_NOx",
    "NO2(GT)": "NO2_GT",
    "PT08.S4(NO2)": "PT08_S4_NO2",
    "PT08.S5(O3)": "PT08_S5_O3",
    "T": "T",
    "RH": "RH",
    "AH": "AH",
}

NUMERIC_COLUMNS = [
    "CO_GT", "PT08_S1_CO", "NMHC_GT", "C6H6_GT", "PT08_S2_NMHC",
    "NOx_GT", "PT08_S3_NOx", "NO2_GT", "PT08_S4_NO2", "PT08_S5_O3",
    "T", "RH", "AH"
]


def load_raw_dataset(csv_path: Path) -> pd.DataFrame:
    """Loads raw semicolon-separated CSV with comma decimal conversion."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Raw dataset not found at {csv_path}. Run data/download_dataset.py first.")

    # UCI dataset is semicolon-delimited
    df = pd.read_csv(
        csv_path,
        sep=";",
        decimal=",",
        encoding="utf-8"
    )
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans raw DataFrame by removing trailing columns, formatting timestamps,
    and converting -200 sentinels to NaN.
    """
    # 1. Drop completely empty trailing columns and rows
    df = df.dropna(how="all", axis=1)
    df = df.dropna(subset=["Date", "Time"], how="any")

    # 2. Rename columns to Big Data safe names
    rename_dict = {k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_dict)

    # 3. Clean Date and Time strings and merge into ISO timestamp
    # Date format: dd/mm/yyyy, Time format: hh.mm.ss
    df["Date"] = df["Date"].astype(str).str.strip()
    df["Time"] = df["Time"].astype(str).str.strip().str.replace(".", ":", regex=False)
    
    # Combined datetime series
    datetime_str = df["Date"] + " " + df["Time"]
    df["timestamp"] = pd.to_datetime(datetime_str, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    
    # Drop records where timestamp parsing failed
    df = df.dropna(subset=["timestamp"]).copy()
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["timestamp"].dt.hour

    # 4. Replace -200 sentinel values with NaN across numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            # Convert comma decimals in string data (e.g. '2,6' -> '2.6')
            if not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].astype(str).str.strip().str.replace(",", ".", regex=False)
            # Force numeric coercion
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Replace -200 sentinel values
            df.loc[df[col] <= -200, col] = np.nan

    # Sort chronologically
    df = df.sort_values(by="timestamp").reset_index(drop=True)

    # Reorder columns with timestamp first
    ordered_cols = ["timestamp", "date", "hour"] + [c for c in NUMERIC_COLUMNS if c in df.columns]
    return df[ordered_cols]


def impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes missing values using forward-fill (LOCF) followed by backward-fill (NOCB).
    Documented for university viva:
      Atmospheric sensor readings depend heavily on prior air quality states.
      Forward-fill maintains continuous baseline estimates without introducing artificial
      mean-reversion artifacts.
    """
    df_imputed = df.copy()
    # Forward fill then back fill along the chronological sequence
    cols_to_fill = [c for c in NUMERIC_COLUMNS if c in df_imputed.columns]
    df_imputed[cols_to_fill] = df_imputed[cols_to_fill].ffill().bfill()
    return df_imputed


def run_pipeline():
    """Executes the data cleaning and exports cleaned and imputed CSV files."""
    print("=" * 60)
    print("  PHASE 2: AIR QUALITY DATA CLEANING & PREPROCESSING")
    print("=" * 60)
    
    print(f"[*] Reading raw dataset from: {RAW_CSV_PATH}")
    raw_df = load_raw_dataset(RAW_CSV_PATH)
    print(f"    Raw records loaded: {len(raw_df):,} rows x {raw_df.shape[1]} columns")

    print("[*] Cleaning fields, replacing -200 sentinels, unifying timestamps...")
    cleaned_df = clean_dataframe(raw_df)
    print(f"    Clean records: {len(cleaned_df):,} rows")
    
    # Missing statistics summary
    print("[*] Null percentage per key pollutant after sentinel replacement:")
    key_pollutants = ["CO_GT", "C6H6_GT", "NOx_GT", "NO2_GT", "T", "RH"]
    for col in key_pollutants:
        null_count = cleaned_df[col].isna().sum()
        pct = (null_count / len(cleaned_df)) * 100
        print(f"    - {col:12s}: {null_count:4d} nulls ({pct:5.2f}%)")

    # Save cleaned dataset (with nulls preserved for stream producer)
    cleaned_df.to_csv(CLEANED_CSV_PATH, index=False, date_format="%Y-%m-%d %H:%M:%S")
    print(f"[OK] Cleaned dataset saved to: {CLEANED_CSV_PATH}")

    # Generate and save imputed dataset for baseline modeling
    imputed_path = DATA_DIR / "AirQualityImputed.csv"
    imputed_df = impute_missing_values(cleaned_df)
    imputed_df.to_csv(imputed_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
    print(f"[OK] Imputed baseline dataset saved to: {imputed_path}")
    print(f"    Remaining nulls in imputed dataset: {imputed_df[NUMERIC_COLUMNS].isna().sum().sum()}")
    print("=" * 60)
    return cleaned_df, imputed_df


if __name__ == "__main__":
    run_pipeline()
