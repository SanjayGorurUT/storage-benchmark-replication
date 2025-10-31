import os
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np


class BenchmarkVisualizer:
    def __init__(self, results_dir: str = "results", figures_dir: str = "figures"):
        self.results_dir = results_dir
        self.figures_dir = figures_dir
        os.makedirs(figures_dir, exist_ok=True)

    def _plot_bar_chart(
            self,
            workloads: List[str],
            parquet_values: List[float],
            orc_values: List[float],
            ylabel: str,
            title: str,
            filename: str,
    ):
        x = np.arange(len(workloads))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(x - width / 2, parquet_values, width, label="Parquet")
        ax.bar(x + width / 2, orc_values, width, label="ORC")

        ax.set_xlabel("Workload")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(workloads)
        ax.legend()

        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, filename))
        plt.close()

    def plot_file_sizes(self, parquet_results: Dict, orc_results: Dict):
        workloads = list(parquet_results.keys())
        parquet_sizes = [parquet_results[w]["file_size_mb"] for w in workloads]
        orc_sizes = [orc_results[w]["file_size_mb"] for w in workloads]

        self._plot_bar_chart(
            workloads,
            parquet_sizes,
            orc_sizes,
            ylabel="File Size (MB)",
            title="File Size Comparison Across Workloads",
            filename="file_size_comparison.png",
        )

    def plot_full_scan_performance(self, parquet_results: Dict, orc_results: Dict):
        workloads = list(parquet_results.keys())
        parquet_throughput = [
            parquet_results[w]["full_scan"]["rows_per_sec"] for w in workloads
        ]
        orc_throughput = [orc_results[w]["full_scan"]["rows_per_sec"] for w in workloads]

        self._plot_bar_chart(
            workloads,
            parquet_throughput,
            orc_throughput,
            ylabel="Throughput (rows/sec)",
            title="Full Scan Performance Comparison",
            filename="full_scan_performance.png",
        )

    def plot_selection_latency(
            self, parquet_results: Dict, orc_results: Dict, selectivity: float = 0.1
    ):
        workloads = list(parquet_results.keys())

        parquet_latencies = []
        orc_latencies = []

        for w in workloads:
            p_query = next(
                q for q in parquet_results[w]["selection_queries"] if q["selectivity"] == selectivity
            )
            o_query = next(
                q for q in orc_results[w]["selection_queries"] if q["selectivity"] == selectivity
            )
            parquet_latencies.append(p_query["mean_time_ms"])
            orc_latencies.append(o_query["mean_time_ms"])

        self._plot_bar_chart(
            workloads,
            parquet_latencies,
            orc_latencies,
            ylabel="Latency (ms)",
            title=f"Selection Query Latency (Selectivity={selectivity})",
            filename=f"selection_latency_{selectivity}.png",
        )
