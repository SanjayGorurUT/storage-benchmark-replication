import pandas as pd
import numpy as np
import os
import shutil
from typing import Dict, List, Optional
import json

class DataSourcer:
    def __init__(self):
        self.workloads = ["core", "bi", "classic", "geo", "log", "ml"]
        self.workload_configs = {
            "core": {"ndv_range": (0.01, 0.9), "null_range": (0.0, 0.3), "skew_types": ["uniform", "zipf"]},
            "bi": {"ndv_range": (0.001, 0.5), "null_range": (0.0, 0.4), "skew_types": ["zipf", "hotspot"]},
            "classic": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "hotspot"]},
            "geo": {"ndv_range": (0.01, 0.5), "null_range": (0.0, 0.2), "skew_types": ["uniform", "zipf"]},
            "log": {"ndv_range": (0.001, 0.3), "null_range": (0.0, 0.15), "skew_types": ["zipf", "hotspot"]},
            "ml": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "hotspot"]}
        }
    
    def generate_synthetic_dataset(self, workload: str, output_dir: str = "data") -> str:
        np.random.seed(42)
        n_rows, n_cols = 1000000, 20
        
        data = {}
        config = self.workload_configs[workload]
        
        for i in range(n_cols):
            ndv_ratio = np.random.uniform(*config["ndv_range"])
            null_ratio = np.random.uniform(*config["null_range"])
            ndv = max(1, int(n_rows * ndv_ratio))
            
            if i % 3 == 0:
                values = np.random.randint(0, ndv, n_rows).astype(float)
            elif i % 3 == 1:
                values = np.random.uniform(0, ndv, n_rows)
            else:
                unique_vals = [f'str_{j}' for j in range(ndv)]
                values = np.random.choice(unique_vals, n_rows)
            
            if null_ratio > 0:
                null_indices = np.random.choice(n_rows, size=int(n_rows * null_ratio), replace=False)
                if isinstance(values[0], str):
                    values[null_indices] = None
                else:
                    values[null_indices] = np.nan
            
            data[f'col_{i}'] = values
        
        df = pd.DataFrame(data)
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"{workload}_r{n_rows}_c{n_cols}.csv")
        df.to_csv(filepath, index=False)
        print(f"Generated {workload} dataset: {filepath} ({df.shape})")
        return filepath
    
    def clean_data_dir(self, output_dir: str = "data"):
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            print(f"Cleaned {output_dir} directory")
        os.makedirs(output_dir, exist_ok=True)

class DataPreprocessor:
    def __init__(self):
        self.workload_configs = {
            "core": {"ndv_range": (0.01, 0.9), "null_range": (0.0, 0.3), "skew_types": ["uniform", "zipf"]},
            "bi": {"ndv_range": (0.001, 0.5), "null_range": (0.0, 0.4), "skew_types": ["zipf", "hotspot"]},
            "classic": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "hotspot"]},
            "geo": {"ndv_range": (0.01, 0.5), "null_range": (0.0, 0.2), "skew_types": ["uniform", "zipf"]},
            "log": {"ndv_range": (0.001, 0.3), "null_range": (0.0, 0.15), "skew_types": ["zipf", "hotspot"]},
            "ml": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "hotspot"]}
        }
    
    def _calculate_ndv_ratio(self, series: pd.Series) -> float:
        return series.nunique() / len(series)
    
    def _calculate_null_ratio(self, series: pd.Series) -> float:
        return series.isnull().sum() / len(series)
    
    def process_workload(self, filepath: str) -> Dict:
        df = pd.read_csv(filepath)
        output_file = filepath.replace(".csv", "_processed.parquet")
        df.to_parquet(output_file, index=False)
        
        workload_type = os.path.basename(filepath).split('_')[0]
        config = self.workload_configs.get(workload_type, {})
        
        validation_passed = True
        issues = []
        for col_name in df.columns:
            series = df[col_name]
            ndv_ratio = self._calculate_ndv_ratio(series)
            null_ratio = self._calculate_null_ratio(series)
            
            if "ndv_range" in config and not (config["ndv_range"][0] <= ndv_ratio <= config["ndv_range"][1]):
                issues.append(f"Column {col_name}: NDV ratio {ndv_ratio:.3f} outside expected range {config['ndv_range']}")
                validation_passed = False
            if "null_range" in config and not (config["null_range"][0] <= null_ratio <= config["null_range"][1]):
                issues.append(f"Column {col_name}: Null ratio {null_ratio:.3f} outside expected range {config['null_range']}")
                validation_passed = False
        
        result = {
            "input_file": filepath,
            "output_file": output_file,
            "validation": {
                "workload_type": workload_type,
                "shape": list(df.shape),
                "columns_analyzed": len(df.columns),
                "validation_passed": validation_passed,
                "issues": issues
            },
            "file_size_mb": os.path.getsize(output_file) / (1024 * 1024)
        }
        
        return result

def main():
    sourcer = DataSourcer()
    preprocessor = DataPreprocessor()
    
    print("Cleaning data directory...")
    sourcer.clean_data_dir()
    
    print("Generating datasets for all workloads...")
    generated_files = []
    
    for workload in sourcer.workloads:
        filepath = sourcer.generate_synthetic_dataset(workload)
        generated_files.append(filepath)
    
    print(f"\nProcessing {len(generated_files)} datasets...")
    results = []
    
    for filepath in generated_files:
        print(f"Processing {filepath}...")
        result = preprocessor.process_workload(filepath)
        results.append(result)
        
        validation = result["validation"]
        print(f"  Shape: {validation['shape']}")
        print(f"  Validation: {'PASSED' if validation['validation_passed'] else 'FAILED'}")
        if validation["issues"]:
            for issue in validation["issues"][:3]:
                print(f"    - {issue}")
        print(f"  Output: {result['output_file']} ({result['file_size_mb']:.2f} MB)")
        print()
    
    with open("data/processing_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"Processing complete. Results saved to data/processing_results.json")

if __name__ == "__main__":
    main()