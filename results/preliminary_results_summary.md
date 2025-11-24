# Preliminary Benchmark Results

Generated: 2025-11-15 21:33:01

## Overview

This report contains preliminary performance results from our storage format benchmarking pipeline. Results are generated across 6 workloads (core, bi, classic, geo, log, ml) comparing Parquet and ORC formats.

## Key Metrics

For each workload, we measure:
- **File Size**: **Compression efficiency**
- **Full Scan Performance**: **Throughput** (rows/sec) and **latency** (ms)
- **Selection Query Performance**: **Latency** at different selectivities (1%, 10%, 50%)

## Results Summary

### File Size Comparison (MB)

| Workload | Parquet | ORC | Ratio (ORC/Parquet) |
|----------|---------|-----|---------------------|
| bi | 0.08 | 0.13 | 1.65x |
| classic | 0.10 | 0.14 | 1.49x |
| core | 0.09 | 0.13 | 1.51x |
| geo | 0.09 | 0.14 | 1.58x |
| log | 0.08 | 0.14 | 1.62x |
| ml | 0.10 | 0.14 | 1.44x |

### Full Scan Performance

| Workload | Format | Throughput (rows/sec) | Latency (ms) |
|----------|--------|----------------------|--------------|
| bi | parquet | 590,098 | 1.69 |
| bi | orc | 1,119,341 | 0.89 |
| classic | parquet | 574,292 | 1.74 |
| classic | orc | 1,055,864 | 0.95 |
| core | parquet | 452,218 | 2.21 |
| core | orc | 924,976 | 1.08 |
| geo | parquet | 603,941 | 1.66 |
| geo | orc | 1,102,647 | 0.91 |
| log | parquet | 616,475 | 1.62 |
| log | orc | 1,064,084 | 0.94 |
| ml | parquet | 566,203 | 1.77 |
| ml | orc | 990,262 | 1.01 |

### Selection Query Performance (10% Selectivity)

| Workload | Format | Latency (ms) | Rows Selected |
|----------|--------|--------------|---------------|
| bi | parquet | 0.23 | 45 |
| bi | orc | 0.09 | 45 |
| classic | parquet | 0.11 | 98 |
| classic | orc | 0.09 | 98 |
| core | parquet | 0.13 | 97 |
| core | orc | 0.12 | 97 |
| geo | parquet | 0.08 | 0 |
| geo | orc | 0.08 | 0 |
| log | parquet | 0.07 | 0 |
| log | orc | 0.08 | 0 |
| ml | parquet | 0.10 | 96 |
| ml | orc | 0.10 | 96 |

## Detailed Results

For complete detailed results including all selectivities and statistics, see `results/benchmark_results.json`.

## Notes

- Results are based on 5 iterations per measurement
- All workloads use 1,000 rows and 20 columns
- Selection queries test column `col_0` with varying selectivity thresholds
- These are preliminary results; full-scale benchmarks will use larger datasets

