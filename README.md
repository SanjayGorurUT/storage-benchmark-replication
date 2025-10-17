# Storage Format Benchmark - Data Sourcing

## Setup
```bash
pip install -r requirements.txt
```

## Usage
```bash
python data_sourcer.py
```

## Current Status
**Working**: 
- All 6 workloads generated locally (1M rows, 20 columns each)
- Data preprocessing to Parquet format for all workloads
- Basic validation framework
- YAML config files for all 6 workloads (`configs/` directory)
- Local data generation matching paper's workload characteristics

**Week 1 Milestones Complete**:
- ✅ Scope finalization (Figure 6 replication focus)
- ✅ Environment setup (Python, libraries, project structure)  
- ✅ Dataset selection (6 workloads: bi, classic, geo, log, ml, core)
- ✅ Define metrics and workloads (YAML configs created)

**To Clean and Regenerate**:
```bash
./run.sh
```

**To View Generated Parquet Files**:
```bash
python -c "import pandas as pd; df = pd.read_parquet('data/core_r1000000_c20_processed.parquet'); print('Shape:', df.shape); print(df.head())"
```

## Requirements
- Python 3.7+
- pandas, pyarrow, numpy, requests (see requirements.txt)