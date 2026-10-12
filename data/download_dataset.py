#!/usr/bin/env python3
"""
Air Quality Dataset Downloader
Retrieves the official UCI Air Quality dataset (ID: 360) and extracts AirQualityUCI.csv.
Includes offline fallback generation if network is unavailable.
"""

import os
import sys
import zipfile
import io
import urllib.request

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(DATA_DIR, "AirQualityUCI.csv")
ZIP_URL = "https://archive.ics.uci.edu/static/public/360/air+quality.zip"
BACKUP_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00360/AirQualityUCI.zip"

def download_and_extract():
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 1000:
        print(f"[OK] AirQualityUCI.csv already exists at {CSV_PATH} ({os.path.getsize(CSV_PATH)} bytes)")
        return CSV_PATH

    print(f"[*] Downloading UCI Air Quality dataset from {ZIP_URL}...")
    urls = [ZIP_URL, BACKUP_URL]
    downloaded = False

    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            with urllib.request.urlopen(req, timeout=30) as response:
                content = response.read()
                print(f"[OK] Downloaded {len(content):,} bytes from {url}")
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for filename in zf.namelist():
                        if filename.endswith(".csv"):
                            zf.extract(filename, DATA_DIR)
                            print(f"[OK] Extracted {filename} to {DATA_DIR}")
                            downloaded = True
                            break
            if downloaded:
                break
        except Exception as e:
            print(f"[!] Warning: Failed from {url}: {e}")

    if not downloaded:
        print("[!] Generating synthetic UCI-compatible sample dataset as offline fallback...")
        generate_fallback_dataset(CSV_PATH)

    print(f"[OK] Ready: {CSV_PATH}")
    return CSV_PATH

def generate_fallback_dataset(target_path):
    import random
    from datetime import datetime, timedelta
    
    headers = "Date;Time;CO(GT);PT08.S1(CO);NMHC(GT);C6H6(GT);PT08.S2(NMHC);NOx(GT);PT08.S3(NOx);NO2(GT);PT08.S4(NO2);PT08.S5(O3);T;RH;AH;;\n"
    start_time = datetime(2004, 3, 10, 18, 0, 0)
    lines = [headers]
    
    for i in range(1000):
        current_time = start_time + timedelta(hours=i)
        d_str = current_time.strftime("%d/%m/%Y")
        t_str = current_time.strftime("%H.%M.%S")
        
        # Semicolon separated, comma as decimal, -200 as missing
        co = f"{random.uniform(1.0, 3.5):.1f}".replace(".", ",") if random.random() > 0.05 else "-200"
        pt_s1 = f"{random.randint(900, 1500)}"
        nmhc = "-200"
        c6h6 = f"{random.uniform(5.0, 15.0):.1f}".replace(".", ",")
        pt_s2 = f"{random.randint(700, 1200)}"
        nox = f"{random.randint(80, 250)}" if random.random() > 0.05 else "-200"
        pt_s3 = f"{random.randint(600, 1100)}"
        no2 = f"{random.randint(60, 180)}"
        pt_s4 = f"{random.randint(1200, 1800)}"
        pt_s5 = f"{random.randint(800, 1500)}"
        temp = f"{random.uniform(10.0, 25.0):.1f}".replace(".", ",")
        rh = f"{random.uniform(30.0, 70.0):.1f}".replace(".", ",")
        ah = f"{random.uniform(0.5, 1.5):.4f}".replace(".", ",")
        
        line = f"{d_str};{t_str};{co};{pt_s1};{nmhc};{c6h6};{pt_s2};{nox};{pt_s3};{no2};{pt_s4};{pt_s5};{temp};{rh};{ah};;\n"
        lines.append(line)
        
    with open(target_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"[OK] Generated fallback dataset with {len(lines)} records at {target_path}")

if __name__ == "__main__":
    download_and_extract()
