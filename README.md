# Storage Format Benchmark

## Setup
```bash
pip install -r requirements.txt
```

## Usage
```bash
python workload_generator.py
```

## Current Status
- Workload generator with NDV, skew, and sortedness controls
- All 6 workloads (core, bi, classic, geo, log, ml) generated
- Distribution validation against paper specifications
- 1000 rows × 20 columns per workload (~0.1MB Parquet files)

## Requirements
- Python 3.7+
- pandas, pyarrow, numpy, scipy, pyyaml