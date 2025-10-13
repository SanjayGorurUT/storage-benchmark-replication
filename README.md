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
- `core` workload generation using paper's generator (1M rows, 20 columns)
- Data preprocessing to Parquet format
- Basic validation framework
**Not Working**:
- `bi`, `classic`, `geo`, `log`, `ml` workloads fail due to directory creation issues in paper's generator
- CloudFront URL (`d3m9osc9baovkk.cloudfront.net`) is down (DNS resolution fails)
- Alternative dataset URLs return 404 errors

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