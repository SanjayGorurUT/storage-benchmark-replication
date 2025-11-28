#!/usr/bin/env python3
"""
Extract and compare with paper's Figure 6 values.

This script helps extract numerical values from Figure 6 in the paper
and creates side-by-side comparisons with our results.
"""

import json
import matplotlib.pyplot as plt
import numpy as np

# Our results (from benchmark_results_bare-metal.json)
OUR_RESULTS = {
    'workloads': ['bi', 'classic', 'geo', 'log', 'ml', 'core'],
    'file_size_mb': {
        'parquet': [20.51, 188.71, 106.40, 17.05, 74.66, 47.54],
        'orc': [75.52, 223.57, 162.13, 137.77, 127.12, 66.58]
    },
    'full_scan_time_sec': {
        'parquet': [0.353, 1.418, 0.938, 0.372, 0.656, 0.445],
        'orc': [0.412, 1.746, 1.495, 0.514, 0.798, 0.678]
    },
    'select_time_ms': {
        'parquet': [0.35, 13.06, 0.36, 0.38, 23.63, 0.20],
        'orc': [0.34, 12.68, 0.32, 0.32, 24.01, 0.23]
    }
}

# Paper's Figure 6 values (TO BE FILLED IN from PDF)
# Extract these values by reading the bar chart heights in Figure 6
PAPER_RESULTS = {
    'workloads': ['bi', 'classic', 'geo', 'log', 'ml', 'core'],
    'file_size_mb': {
        'parquet': None,  # Fill in from Figure 6a
        'orc': None
    },
    'full_scan_time_sec': {
        'parquet': None,  # Fill in from Figure 6b
        'orc': None
    },
    'select_time_ms': {
        'parquet': None,  # Fill in from Figure 6c
        'orc': None
    }
}

def plot_side_by_side_comparison(our_data, paper_data, workloads, metric_name, ylabel, filename):
    """Create side-by-side comparison plots."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    x = np.arange(len(workloads))
    width = 0.35
    
    # Our results
    ax = axes[0]
    ax.bar(x - width/2, our_data['parquet'], width, label='Parquet', color='#1f77b4', alpha=0.8)
    ax.bar(x + width/2, our_data['orc'], width, label='ORC', color='#ff7f0e', alpha=0.8)
    ax.set_xlabel('Workload', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(f'Our Results: {metric_name}', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Paper's results (if available)
    if paper_data and paper_data['parquet'] and paper_data['orc']:
        ax = axes[1]
        ax.bar(x - width/2, paper_data['parquet'], width, label='Parquet', color='#1f77b4', alpha=0.8)
        ax.bar(x + width/2, paper_data['orc'], width, label='ORC', color='#ff7f0e', alpha=0.8)
        ax.set_xlabel('Workload', fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_title(f'Paper (Zeng et al.): {metric_name}', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(workloads)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Match y-axis scales
        y_max = max(
            max(our_data['parquet'] + our_data['orc']),
            max(paper_data['parquet'] + paper_data['orc'])
        ) * 1.1
        axes[0].set_ylim(0, y_max)
        axes[1].set_ylim(0, y_max)
    else:
        axes[1].text(0.5, 0.5, 'Paper values not yet extracted\n\nPlease fill in PAPER_RESULTS\ndictionary with values from\nFigure 6 in the PDF', 
                    ha='center', va='center', fontsize=12, transform=axes[1].transAxes)
        axes[1].set_title(f'Paper (Zeng et al.): {metric_name}', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(f'figures/{filename}', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: figures/{filename}")

def print_comparison_table(our_data, paper_data, workloads, metric_name, unit=""):
    """Print detailed comparison table."""
    print(f"\n{'='*80}")
    print(f"{metric_name} Comparison")
    print(f"{'='*80}")
    print(f"{'Workload':<10} | {'Our Parquet':<15} | {'Our ORC':<15} | ", end="")
    if paper_data and paper_data['parquet'] and paper_data['orc']:
        print(f"{'Paper Parquet':<15} | {'Paper ORC':<15} | {'Parquet Diff %':<15} | {'ORC Diff %':<15}")
        print("-" * 110)
        for i, wl in enumerate(workloads):
            our_p = our_data['parquet'][i]
            our_o = our_data['orc'][i]
            paper_p = paper_data['parquet'][i]
            paper_o = paper_data['orc'][i]
            p_diff = ((our_p - paper_p) / paper_p * 100) if paper_p > 0 else 0
            o_diff = ((our_o - paper_o) / paper_o * 100) if paper_o > 0 else 0
            print(f"{wl:<10} | {our_p:>14.2f}{unit:<1} | {our_o:>14.2f}{unit:<1} | "
                  f"{paper_p:>14.2f}{unit:<1} | {paper_o:>14.2f}{unit:<1} | "
                  f"{p_diff:>14.1f}% | {o_diff:>14.1f}%")
    else:
        print(f"{'Parquet':<15} | {'ORC':<15}")
        print("-" * 50)
        for i, wl in enumerate(workloads):
            print(f"{wl:<10} | {our_data['parquet'][i]:>14.2f}{unit:<1} | {our_data['orc'][i]:>14.2f}{unit:<1}")

def main():
    """Main comparison function."""
    print("=" * 80)
    print("FIGURE 6 COMPARISON: OUR RESULTS vs. PAPER'S RESULTS")
    print("=" * 80)
    
    workloads = OUR_RESULTS['workloads']
    
    # Print comparison tables
    print_comparison_table(OUR_RESULTS['file_size_mb'], PAPER_RESULTS['file_size_mb'], 
                          workloads, "File Size", " MB")
    print_comparison_table(OUR_RESULTS['full_scan_time_sec'], PAPER_RESULTS['full_scan_time_sec'], 
                          workloads, "Full Scan Time", " s")
    print_comparison_table(OUR_RESULTS['select_time_ms'], PAPER_RESULTS['select_time_ms'], 
                          workloads, "Select Query Time", " ms")
    
    # Generate comparison plots
    print("\n" + "=" * 80)
    print("Generating Comparison Visualizations")
    print("=" * 80)
    
    import os
    os.makedirs('figures', exist_ok=True)
    
    plot_side_by_side_comparison(
        OUR_RESULTS['file_size_mb'],
        PAPER_RESULTS['file_size_mb'],
        workloads,
        "File Size",
        "File Size (MB)",
        "figure6a_file_size_comparison.png"
    )
    
    plot_side_by_side_comparison(
        OUR_RESULTS['full_scan_time_sec'],
        PAPER_RESULTS['full_scan_time_sec'],
        workloads,
        "Full Scan Time",
        "Time (seconds)",
        "figure6b_scan_time_comparison.png"
    )
    
    plot_side_by_side_comparison(
        OUR_RESULTS['select_time_ms'],
        PAPER_RESULTS['select_time_ms'],
        workloads,
        "Select Query Time",
        "Time (ms)",
        "figure6c_select_time_comparison.png"
    )
    
    # Analysis
    print("\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    
    if PAPER_RESULTS['file_size_mb']['parquet']:
        print("\n✅ Direct comparison possible - Paper's values have been extracted")
        print("   Review the comparison tables and figures above.")
    else:
        print("\n⚠️  Paper's values need to be extracted from Figure 6")
        print("\nTo extract values from Figure 6:")
        print("1. Open the PDF and locate Figure 6 (page ~7)")
        print("2. Read the bar chart heights for each workload")
        print("3. Update PAPER_RESULTS dictionary in this script")
        print("4. Re-run this script")
        print("\nFigure 6 shows:")
        print("  (a) File Size in MB - bar heights represent file sizes")
        print("  (b) Scan Time in seconds - bar heights represent scan times")
        print("  (c) Select Time in ms - bar heights represent select query latencies")
    
    # Export for manual comparison
    export = {
        'our_results': OUR_RESULTS,
        'paper_results': PAPER_RESULTS,
        'note': 'Fill in paper_results with values extracted from Figure 6 in the PDF'
    }
    with open('results/figure6_comparison_data.json', 'w') as f:
        json.dump(export, f, indent=2)
    print(f"\n✓ Exported data to: results/figure6_comparison_data.json")

if __name__ == "__main__":
    main()




