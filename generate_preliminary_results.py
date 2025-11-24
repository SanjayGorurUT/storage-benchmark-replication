#!/usr/bin/env python3
"""
Generate Preliminary Results Script

This script runs the full benchmarking pipeline to generate preliminary results
for the storage format benchmark project. It:
1. Ensures data and workloads are generated
2. Converts formats (Parquet and ORC)
3. Runs benchmarks on all workloads
4. Generates a summary report
"""

import json
import os
from datetime import datetime
from typing import Dict

from benchmark_runner import BenchmarkRunner
from data_sourcer import DataSourcer
from format_converter import FormatConverter
from workload_generator import WorkloadGenerator


def generate_summary_report(results: Dict, output_file: str = "results/preliminary_results_summary.md"):
    """Generate a markdown summary report of preliminary results."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        f.write("# Preliminary Benchmark Results\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## Overview\n\n")
        f.write("This report contains preliminary performance results from our storage format ")
        f.write("benchmarking pipeline. Results are generated across 6 workloads (core, bi, ")
        f.write("classic, geo, log, ml) comparing Parquet and ORC formats.\n\n")
        
        f.write("## Key Metrics\n\n")
        f.write("For each workload, we measure:\n")
        f.write("- **File Size**: **Compression efficiency**\n")
        f.write("- **Full Scan Performance**: **Throughput** (rows/sec) and **latency** (ms)\n")
        f.write("- **Selection Query Performance**: **Latency** at different selectivities (1%, 10%, 50%)\n\n")
        
        f.write("## Results Summary\n\n")
        
        # File size comparison
        f.write("### File Size Comparison (MB)\n\n")
        f.write("| Workload | Parquet | ORC | Ratio (ORC/Parquet) |\n")
        f.write("|----------|---------|-----|---------------------|\n")
        
        for workload in sorted(results.keys()):
            if 'parquet' in results[workload] and 'orc' in results[workload]:
                p_size = results[workload]['parquet']['file_size_mb']
                o_size = results[workload]['orc']['file_size_mb']
                ratio = o_size / p_size if p_size > 0 else 0
                f.write(f"| {workload} | {p_size:.2f} | {o_size:.2f} | {ratio:.2f}x |\n")
            elif 'parquet' in results[workload]:
                p_size = results[workload]['parquet']['file_size_mb']
                f.write(f"| {workload} | {p_size:.2f} | N/A | N/A |\n")
        
        f.write("\n")
        
        # Full scan performance
        f.write("### Full Scan Performance\n\n")
        f.write("| Workload | Format | Throughput (rows/sec) | Latency (ms) |\n")
        f.write("|----------|--------|----------------------|--------------|\n")
        
        for workload in sorted(results.keys()):
            for fmt in ['parquet', 'orc']:
                if fmt in results[workload]:
                    scan = results[workload][fmt]['full_scan']
                    throughput = scan['rows_per_sec']
                    latency = scan['mean_time_ms']
                    f.write(f"| {workload} | {fmt} | {throughput:,.0f} | {latency:.2f} |\n")
        
        f.write("\n")
        
        # Selection query performance
        f.write("### Selection Query Performance (10% Selectivity)\n\n")
        f.write("| Workload | Format | Latency (ms) | Rows Selected |\n")
        f.write("|----------|--------|--------------|---------------|\n")
        
        for workload in sorted(results.keys()):
            for fmt in ['parquet', 'orc']:
                if fmt in results[workload]:
                    queries = results[workload][fmt]['selection_queries']
                    # Find query with 0.1 selectivity
                    sel_query = next((q for q in queries if abs(q['selectivity'] - 0.1) < 0.01), None)
                    if sel_query:
                        latency = sel_query['mean_time_ms']
                        rows = sel_query['rows_selected']
                        f.write(f"| {workload} | {fmt} | {latency:.2f} | {rows:,} |\n")
        
        f.write("\n")
        
        # Detailed results
        f.write("## Detailed Results\n\n")
        f.write("For complete detailed results including all selectivities and statistics, ")
        f.write("see `results/benchmark_results.json`.\n\n")
        
        f.write("## Notes\n\n")
        f.write("- Results are based on 5 iterations per measurement\n")
        f.write("- All workloads use 1,000 rows and 20 columns\n")
        f.write("- Selection queries test column `col_0` with varying selectivity thresholds\n")
        f.write("- These are preliminary results; full-scale benchmarks will use larger datasets\n\n")
    
    print(f"\nSummary report saved to: {output_file}")


def main():
    """Main function to generate preliminary results."""
    print("=" * 60)
    print("PRELIMINARY RESULTS GENERATION")
    print("=" * 60)
    
    # Step 1: Ensure data is generated
    print("\n[1/5] Checking data generation...")
    sourcer = DataSourcer()
    data_files_exist = all(
        os.path.exists(os.path.join("data", f"{w}_r1000_c20_generated.parquet"))
        for w in ["core", "bi", "classic", "geo", "log", "ml"]
    )
    
    if not data_files_exist:
        print("  Generating base datasets...")
        sourcer.clean_data_dir()
        for workload in sourcer.workloads:
            sourcer.generate_synthetic_dataset(workload)
    else:
        print("  ✓ Data files already exist")
    
    # Step 2: Ensure workloads are generated
    print("\n[2/5] Checking workload generation...")
    workload_files_exist = all(
        os.path.exists(os.path.join("data", f"{w}_r1000_c20_generated.parquet"))
        for w in ["core", "bi", "classic", "geo", "log", "ml"]
    )
    
    if not workload_files_exist:
        print("  Generating workloads...")
        generator = WorkloadGenerator()
        generator.generate_all_workloads()
    else:
        print("  ✓ Workload files already exist")
    
    # Step 3: Convert to ORC
    print("\n[3/5] Converting formats...")
    converter = FormatConverter()
    orc_files_exist = all(
        os.path.exists(os.path.join("data", f"{w}_r1000_c20_generated.orc"))
        for w in ["core", "bi", "classic", "geo", "log", "ml"]
    )
    
    if not orc_files_exist:
        print("  Converting Parquet to ORC...")
        converter.convert_all_workloads()
    else:
        print("  ✓ ORC files already exist")
    
    # Step 4: Run benchmarks
    print("\n[4/5] Running benchmarks...")
    runner = BenchmarkRunner()
    results = runner.run_all_benchmarks(formats=["parquet", "orc"])
    
    # Step 5: Generate summary report
    print("\n[5/5] Generating summary report...")
    generate_summary_report(results)
    
    print("\n" + "=" * 60)
    print("PRELIMINARY RESULTS GENERATION COMPLETE")
    print("=" * 60)
    print("\nResults saved to:")
    print("  - results/benchmark_results.json (detailed JSON)")
    print("  - results/preliminary_results_summary.md (summary report)")
    print("\nNext steps:")
    print("  - Review the summary report")
    print("  - Analyze detailed results in JSON format")
    print("  - Generate visualizations using visualizer.py")


if __name__ == "__main__":
    main()

