# 1M Row Benchmark Status

## Current State

### ✅ What We Have
1. **1M row CSV files exist:**
   - `data/core_r1000000_c20_generated.csv` (97MB) - already generated
   - `dataset/` directory has 1M row CSVs for all 6 workloads

2. **Preliminary results (1K rows) are available:**
   - `results/benchmark_results.json` - Detailed JSON results
   - `results/preliminary_results_summary.md` - Summary report
   - Generated on: 2025-11-15 21:33:01

### ❌ What's Missing for 1M Row Benchmarks

1. **Format Conversion**
   - The 1M row CSV files need to be converted to Parquet and ORC
   - `format_converter.py` is hardcoded to look for `_r1000_c20_generated.parquet` files
   - Need to convert: `core_r1000000_c20_generated.csv` → Parquet/ORC

2. **Benchmark Runner Paths**
   - `benchmark_runner.py` is hardcoded to look for `_r1000_c20_generated.parquet` files
   - Line 82: `f"{workload}_r1000_c20_generated.parquet"`
   - Line 85: `f"{workload}_r1000_c20_generated.orc"`
   - Need to make row count configurable

3. **Workload Generator**
   - Currently hardcoded to generate 1000 rows (line 138)
   - Would need to generate 1M rows for other workloads if CSV doesn't exist

## Quick Fix to Run 1M Row Benchmarks

### Option 1: Manual Conversion + Path Update
1. Convert existing 1M CSV to Parquet/ORC manually
2. Temporarily update benchmark_runner.py paths to `_r1000000_c20_generated`

### Option 2: Make It Configurable (Better)
1. Add row count parameter to benchmark_runner
2. Add row count parameter to format_converter
3. Convert 1M CSVs to Parquet/ORC
4. Run benchmarks with row_count=1000000

## Where Are Preliminary Results?

**Location:** `results/` directory
- **JSON:** `results/benchmark_results.json` (9.3KB)
- **Summary:** `results/preliminary_results_summary.md` (2.3KB)

**These are for 1K rows**, showing:
- File sizes (0.08-0.10 MB)
- Full scan performance (450K-620K rows/sec)
- Selection query latencies (0.07-0.23 ms)

## Next Steps to Enable 1M Row Benchmarks

1. **Make row count configurable** in:
   - `benchmark_runner.py` 
   - `format_converter.py`
   - `generate_preliminary_results.py`

2. **Convert existing 1M CSVs** to Parquet/ORC

3. **Run benchmarks** with row_count=1000000

Estimated time: 1-2 hours to make configurable + conversion time

