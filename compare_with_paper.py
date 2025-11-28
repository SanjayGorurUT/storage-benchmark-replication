#!/usr/bin/env python3
"""
Compare our benchmark results with the paper's Figure 6 results.

This script creates side-by-side comparisons and extracts our results
in a format that can be directly compared with the paper's values.
"""

import json
import os
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np


def load_our_results(results_file: str = "results/benchmark_results_bare-metal.json") -> Dict:
    """Load our benchmark results."""
    with open(results_file, 'r') as f:
        data = json.load(f)
    return data['results']


def extract_our_metrics(results: Dict) -> Dict:
    """Extract metrics in the same format as paper's Figure 6."""
    workloads = ["bi", "classic", "geo", "log", "ml", "core"]
    
    metrics = {
        'file_size': {'parquet': [], 'orc': []},
        'full_scan': {'parquet': [], 'orc': []},
        'selection_latency_10pct': {'parquet': [], 'orc': []}
    }
    
    for wl in workloads:
        if wl in results:
            # File size (MB)
            metrics['file_size']['parquet'].append(results[wl]['parquet']['file_size_mb'])
            metrics['file_size']['orc'].append(results[wl]['orc']['file_size_mb'])
            
            # Full scan throughput (rows/sec)
            metrics['full_scan']['parquet'].append(results[wl]['parquet']['full_scan']['rows_per_sec'])
            metrics['full_scan']['orc'].append(results[wl]['orc']['full_scan']['rows_per_sec'])
            
            # Selection latency at 10% (ms)
            sel_10 = next(
                q['mean_time_ms'] 
                for q in results[wl]['parquet']['selection_queries'] 
                if abs(q['selectivity'] - 0.1) < 0.01
            )
            metrics['selection_latency_10pct']['parquet'].append(sel_10)
            
            sel_10 = next(
                q['mean_time_ms'] 
                for q in results[wl]['orc']['selection_queries'] 
                if abs(q['selectivity'] - 0.1) < 0.01
            )
            metrics['selection_latency_10pct']['orc'].append(sel_10)
    
    return metrics, workloads


def plot_comparison(
    our_metrics: Dict,
    paper_metrics: Optional[Dict],
    workloads: List[str],
    metric_name: str,
    ylabel: str,
    filename: str,
    paper_label: str = "Paper (Zeng et al.)"
):
    """Create side-by-side comparison plot."""
    fig, axes = plt.subplots(1, 2 if paper_metrics else 1, figsize=(14, 6))
    if paper_metrics:
        axes = [axes[0], axes[1]]
    else:
        axes = [axes]
    
    x = np.arange(len(workloads))
    width = 0.35
    
    # Plot our results
    ax = axes[0]
    ax.bar(x - width/2, our_metrics['parquet'], width, label='Parquet', color='#1f77b4')
    ax.bar(x + width/2, our_metrics['orc'], width, label='ORC', color='#ff7f0e')
    ax.set_xlabel('Workload')
    ax.set_ylabel(ylabel)
    ax.set_title(f'Our Results: {metric_name}')
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Plot paper's results if available
    if paper_metrics:
        ax = axes[1]
        ax.bar(x - width/2, paper_metrics['parquet'], width, label='Parquet', color='#1f77b4')
        ax.bar(x + width/2, paper_metrics['orc'], width, label='ORC', color='#ff7f0e')
        ax.set_xlabel('Workload')
        ax.set_ylabel(ylabel)
        ax.set_title(f'{paper_label}: {metric_name}')
        ax.set_xticks(x)
        ax.set_xticklabels(workloads)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Match y-axis scales for fair comparison
        y_max = max(
            max(our_metrics['parquet'] + our_metrics['orc']),
            max(paper_metrics['parquet'] + paper_metrics['orc'])
        )
        axes[0].set_ylim(0, y_max * 1.1)
        axes[1].set_ylim(0, y_max * 1.1)
    
    plt.tight_layout()
    os.makedirs('figures', exist_ok=True)
    plt.savefig(f'figures/{filename}', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: figures/{filename}")


def print_comparison_table(our_metrics: Dict, paper_metrics: Optional[Dict], workloads: List[str], metric_name: str):
    """Print comparison table."""
    print(f"\n=== {metric_name} Comparison ===")
    print(f"{'Workload':<10} | {'Our Parquet':<15} | {'Our ORC':<15} | ", end="")
    if paper_metrics:
        print(f"{'Paper Parquet':<15} | {'Paper ORC':<15} | {'Parquet Diff %':<12} | {'ORC Diff %':<12}")
        print("-" * 100)
        for i, wl in enumerate(workloads):
            our_p = our_metrics['parquet'][i]
            our_o = our_metrics['orc'][i]
            paper_p = paper_metrics['parquet'][i]
            paper_o = paper_metrics['orc'][i]
            p_diff = ((our_p - paper_p) / paper_p * 100) if paper_p > 0 else 0
            o_diff = ((our_o - paper_o) / paper_o * 100) if paper_o > 0 else 0
            print(f"{wl:<10} | {our_p:>14.2f} | {our_o:>14.2f} | {paper_p:>14.2f} | {paper_o:>14.2f} | {p_diff:>11.1f}% | {o_diff:>11.1f}%")
    else:
        print(f"{'Parquet':<15} | {'ORC':<15}")
        print("-" * 60)
        for i, wl in enumerate(workloads):
            print(f"{wl:<10} | {our_metrics['parquet'][i]:>14.2f} | {our_metrics['orc'][i]:>14.2f}")


def main():
    """Main comparison function."""
    print("=" * 60)
    print("COMPARING OUR RESULTS WITH PAPER'S FIGURE 6")
    print("=" * 60)
    
    # Load our results
    results = load_our_results()
    our_metrics, workloads = extract_our_metrics(results)
    
    # Print our results summary
    print("\n=== OUR RESULTS (1M rows) ===")
    print_comparison_table(our_metrics['file_size'], None, workloads, "File Size (MB)")
    print_comparison_table(our_metrics['full_scan'], None, workloads, "Full Scan Throughput (rows/sec)")
    print_comparison_table(our_metrics['selection_latency_10pct'], None, workloads, "Selection Latency 10% (ms)")
    
    # Generate our visualizations
    print("\n=== Generating Our Visualizations ===")
    plot_comparison(
        our_metrics['file_size'],
        None,
        workloads,
        "File Size",
        "File Size (MB)",
        "our_file_size_comparison.png"
    )
    plot_comparison(
        our_metrics['full_scan'],
        None,
        workloads,
        "Full Scan Throughput",
        "Throughput (rows/sec)",
        "our_full_scan_comparison.png"
    )
    plot_comparison(
        our_metrics['selection_latency_10pct'],
        None,
        workloads,
        "Selection Query Latency (10% selectivity)",
        "Latency (ms)",
        "our_selection_latency_comparison.png"
    )
    
    # Instructions for adding paper's data
    print("\n" + "=" * 60)
    print("TO ADD PAPER'S DATA FOR COMPARISON:")
    print("=" * 60)
    print("""
1. Extract values from Figure 6 in the paper
2. Create a dictionary in this format:

paper_metrics = {
    'file_size': {
        'parquet': [<values for bi, classic, geo, log, ml, core>],
        'orc': [<values for bi, classic, geo, log, ml, core>]
    },
    'full_scan': {
        'parquet': [<values>],
        'orc': [<values>]
    },
    'selection_latency_10pct': {
        'parquet': [<values>],
        'orc': [<values>]
    }
}

3. Then call:
   plot_comparison(our_metrics['file_size'], paper_metrics['file_size'], ...)
   print_comparison_table(our_metrics['file_size'], paper_metrics['file_size'], ...)
    """)
    
    # Export our data in easy-to-compare format
    print("\n=== Exporting Our Results for Manual Comparison ===")
    export_data = {
        'workloads': workloads,
        'file_size_mb': {
            'parquet': our_metrics['file_size']['parquet'],
            'orc': our_metrics['file_size']['orc']
        },
        'full_scan_rows_per_sec': {
            'parquet': our_metrics['full_scan']['parquet'],
            'orc': our_metrics['full_scan']['orc']
        },
        'selection_latency_10pct_ms': {
            'parquet': our_metrics['selection_latency_10pct']['parquet'],
            'orc': our_metrics['selection_latency_10pct']['orc']
        }
    }
    
    with open('results/our_results_for_comparison.json', 'w') as f:
        json.dump(export_data, f, indent=2)
    print("Exported to: results/our_results_for_comparison.json")
    print("\nYou can now manually add the paper's values to compare!")


if __name__ == "__main__":
    main()




