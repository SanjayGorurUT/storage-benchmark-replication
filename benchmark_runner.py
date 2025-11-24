import json
import os
import time
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow.orc as orc


class BenchmarkRunner:
    def __init__(self, data_dir: str = "data", results_dir: str = "results", environment: str = None):
        self.data_dir = data_dir
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)
        
        if environment is None:
            self.environment = self._detect_environment()
        else:
            self.environment = environment
    
    def _detect_environment(self) -> str:
        if os.path.exists("/.dockerenv") or os.environ.get("container") == "docker":
            return "docker"
        else:
            return "bare-metal"

    def measure_file_size(self, filepath: str) -> float:
        return os.path.getsize(filepath) / (1024 * 1024)

    def _read_file(self, filepath: str):
        """Read file based on extension."""
        if filepath.endswith('.parquet'):
            return pd.read_parquet(filepath)
        elif filepath.endswith('.orc'):
            return orc.read_table(filepath).to_pandas()
        else:
            raise ValueError(f"Unsupported file format: {filepath}")

    def measure_full_scan(self, filepath: str, iterations: int = 5) -> Dict:
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            df = self._read_file(filepath)
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
        df = self._read_file(filepath)

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

    def benchmark_workload(self, workload: str, format_type: str = "parquet") -> Dict:
        """Benchmark a workload for a specific format (parquet or orc)."""
        if format_type == "parquet":
            filepath = os.path.join(self.data_dir,
                                    f"{workload}_r1000_c20_generated.parquet")
        elif format_type == "orc":
            filepath = os.path.join(self.data_dir,
                                   f"{workload}_r1000_c20_generated.orc")
        else:
            raise ValueError(f"Unsupported format: {format_type}")

        if not os.path.exists(filepath):
            return None

        results = {
            'workload': workload,
            'format': format_type,
            'environment': self.environment,
            'file_size_mb': self.measure_file_size(filepath),
            'full_scan': self.measure_full_scan(filepath),
            'selection_queries': []
        }

        df = self._read_file(filepath)
        test_column = 'col_0'

        for selectivity in [0.01, 0.1, 0.5]:
            sel_results = self.measure_selection_query(
                filepath, test_column, selectivity
            )
            results['selection_queries'].append(sel_results)

        return results

    def run_all_benchmarks(self, formats: list = None) -> Dict:
        """Run benchmarks for all workloads and formats."""
        if formats is None:
            formats = ["parquet"]
        
        workloads = ["core", "bi", "classic", "geo", "log", "ml"]
        all_results = {}

        for workload in workloads:
            print(f"Benchmarking {workload}...")
            workload_results = {}
            
            for fmt in formats:
                print(f"  Format: {fmt}")
                result = self.benchmark_workload(workload, fmt)
                if result:
                    workload_results[fmt] = result
            
            if workload_results:
                all_results[workload] = workload_results

        metadata = {
            'environment': self.environment,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'results': all_results
        }
        
        output_file = os.path.join(self.results_dir, f"benchmark_results_{self.environment}.json")
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        return all_results
