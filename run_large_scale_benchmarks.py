#!/usr/bin/env python3
"""
Run large-scale benchmarks (1M rows)

Usage:
    python run_large_scale_benchmarks.py
"""

import os
import pandas as pd
from format_converter import FormatConverter
from benchmark_runner import BenchmarkRunner

ROW_COUNT = 1000000
DATA_DIR = "data"
WORKLOADS = ["core", "bi", "classic", "geo", "log", "ml"]

print("=" * 60)
print(f"LARGE-SCALE BENCHMARKS ({ROW_COUNT:,} rows)")
print("=" * 60)

print("\n[1/3] Converting CSV to Parquet...")
parquet_files = []
for workload in WORKLOADS:
    csv_file = os.path.join(DATA_DIR, f"{workload}_r{ROW_COUNT}_c20_generated.csv")
    parquet_file = os.path.join(DATA_DIR, f"{workload}_r{ROW_COUNT}_c20_generated.parquet")
    
    if os.path.exists(csv_file):
        if not os.path.exists(parquet_file):
            print(f"  Converting {workload} CSV to Parquet...")
            df = pd.read_csv(csv_file)
            df.to_parquet(parquet_file, index=False)
            print(f"    ✓ Created {parquet_file} ({os.path.getsize(parquet_file) / (1024*1024):.2f} MB)")
        else:
            print(f"  ✓ {workload} Parquet already exists")
        parquet_files.append(parquet_file)
    else:
        print(f"  ⚠ {workload} CSV not found: {csv_file}")

if not parquet_files:
    print("\n  ERROR: No CSV files found to convert!")
    print(f"  Expected format: data/{{workload}}_r{ROW_COUNT}_c20_generated.csv")
    exit(1)

print("\n[2/3] Converting Parquet to ORC...")
converter = FormatConverter(data_dir=DATA_DIR, row_count=ROW_COUNT)
converter.convert_all_workloads()

print("\n[3/3] Running benchmarks...")
runner = BenchmarkRunner(data_dir=DATA_DIR, row_count=ROW_COUNT)
results = runner.run_all_benchmarks(formats=["parquet", "orc"])

print("\n" + "=" * 60)
print("LARGE-SCALE BENCHMARKS COMPLETE")
print("=" * 60)
print(f"\nResults saved to: results/benchmark_results_{runner.environment}.json")

