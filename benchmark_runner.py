import json
import os
import time
from typing import Dict

import numpy as np
import pandas as pd


class BenchmarkRunner:
    def __init__(self, data_dir: str = "data", results_dir: str = "results"):
        self.data_dir = data_dir
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)

    def measure_file_size(self, filepath: str) -> float:
        return os.path.getsize(filepath) / (1024 * 1024)

    def measure_full_scan(self, filepath: str, iterations: int = 5) -> Dict:
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            df = pd.read_parquet(filepath)
            _ = df.values
            end = time.perf_counter()
            times.append(end - start)

        return {
            'mean_time_ms': np.mean(times) * 1000,
            'std_time_ms': np.std(times) * 1000,
            'rows_per_sec': len(df) / np.mean(times)
        }

    def measure_selection_query(self, filepath: str, column: str,
                                selectivity: float, iterations: int = 5) -> Dict:
        df = pd.read_parquet(filepath)

        sorted_vals = df[column].dropna().sort_values()
        threshold_idx = int(len(sorted_vals) * selectivity)
        threshold = sorted_vals.iloc[threshold_idx]

        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            result = df[df[column] < threshold]
            _ = result.values
            end = time.perf_counter()
            times.append(end - start)

        return {
            'mean_time_ms': np.mean(times) * 1000,
            'selectivity': selectivity,
            'rows_selected': len(result),
            'column': column
        }

    def benchmark_workload(self, workload: str) -> Dict:
        parquet_file = os.path.join(self.data_dir,
                                    f"{workload}_r1000_c20_generated.parquet")

        results = {
            'workload': workload,
            'file_size_mb': self.measure_file_size(parquet_file),
            'full_scan': self.measure_full_scan(parquet_file),
            'selection_queries': []
        }

        df = pd.read_parquet(parquet_file)
        test_column = 'col_0'

        for selectivity in [0.01, 0.1, 0.5]:
            sel_results = self.measure_selection_query(
                parquet_file, test_column, selectivity
            )
            results['selection_queries'].append(sel_results)

        return results

    def run_all_benchmarks(self) -> Dict:
        workloads = ["core", "bi", "classic", "geo", "log", "ml"]
        all_results = {}

        for workload in workloads:
            print(f"Benchmarking {workload}...")
            all_results[workload] = self.benchmark_workload(workload)

        output_file = os.path.join(self.results_dir, "benchmark_results.json")
        with open(output_file, 'w') as f:
            json.dump(all_results, f, indent=2)

        return all_results
