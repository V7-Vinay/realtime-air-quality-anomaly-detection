#!/usr/bin/env python3
"""
Unit Tests for Data Cleaning & Preprocessing (Phase 2)
"""

import unittest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add project root
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from preprocessing.clean import clean_dataframe, impute_missing_values


class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        # Create a sample raw dataframe mimicking UCI dataset quirks
        self.raw_data = {
            "Date": ["10/03/2004", "10/03/2004", "11/03/2004", None],
            "Time": ["18.00.00", "19.00.00", "04.00.00", None],
            "CO(GT)": ["2,6", "2,0", "-200", None],
            "PT08.S1(CO)": ["1360", "1292", "1011", None],
            "NMHC(GT)": ["150", "112", "-200", None],
            "C6H6(GT)": ["11,9", "9,4", "1,3", None],
            "PT08.S2(NMHC)": ["1046", "955", "527", None],
            "NOx(GT)": ["166", "103", "-200", None],
            "PT08.S3(NOx)": ["1056", "1174", "1818", None],
            "NO2(GT)": ["113", "92", "34", None],
            "PT08.S4(NO2)": ["1692", "1559", "1197", None],
            "PT08.S5(O3)": ["1268", "972", "445", None],
            "T": ["13,6", "13,3", "10,1", None],
            "RH": ["48,9", "47,7", "60,5", None],
            "AH": ["0,7578", "0,7255", "0,7465", None],
            "Unnamed: 15": [None, None, None, None],
            "Unnamed: 16": [None, None, None, None],
        }
        self.raw_df = pd.DataFrame(self.raw_data)

    def test_clean_dataframe_drops_trailing_columns(self):
        cleaned = clean_dataframe(self.raw_df)
        self.assertNotIn("Unnamed: 15", cleaned.columns)
        self.assertNotIn("Unnamed: 16", cleaned.columns)

    def test_clean_dataframe_timestamp_merge(self):
        cleaned = clean_dataframe(self.raw_df)
        self.assertIn("timestamp", cleaned.columns)
        self.assertIn("date", cleaned.columns)
        self.assertIn("hour", cleaned.columns)
        # Verify first timestamp
        first_ts = cleaned.iloc[0]["timestamp"]
        self.assertEqual(first_ts, pd.Timestamp("2004-03-10 18:00:00"))
        self.assertEqual(cleaned.iloc[0]["hour"], 18)

    def test_clean_dataframe_sentinel_replacement(self):
        cleaned = clean_dataframe(self.raw_df)
        # Check that row 3 CO(GT) which had "-200" is now NaN
        row3_co = cleaned.iloc[2]["CO_GT"]
        self.assertTrue(pd.isna(row3_co), f"Expected NaN but got {row3_co}")
        
        # Check that row 1 CO(GT) '2,6' is parsed as 2.6
        row1_co = cleaned.iloc[0]["CO_GT"]
        self.assertAlmostEqual(row1_co, 2.6, places=2)

    def test_impute_missing_values(self):
        cleaned = clean_dataframe(self.raw_df)
        imputed = impute_missing_values(cleaned)
        # Row 3 CO_GT was NaN, forward-fill should take Row 2's value (2.0)
        self.assertFalse(pd.isna(imputed.iloc[2]["CO_GT"]))
        self.assertAlmostEqual(imputed.iloc[2]["CO_GT"], 2.0, places=2)


if __name__ == "__main__":
    unittest.main()
