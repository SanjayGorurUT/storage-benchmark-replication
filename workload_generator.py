import pandas as pd
import numpy as np
import yaml
import os
from typing import Dict, List, Tuple, Any
from scipy import stats
import json

class WorkloadGenerator:
    def __init__(self, config_dir: str = "configs"):
        self.config_dir = config_dir
        self.workloads = ["core", "bi", "classic", "geo", "log", "ml"]
        self.results = {}
        
    def load_config(self, workload: str) -> Dict:
        config_path = os.path.join(self.config_dir, f"{workload}.yaml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def generate_zipf_distribution(self, n_rows: int, ndv: int, alpha: float = 1.5) -> np.ndarray:
        x = np.arange(1, ndv + 1)
        weights = 1 / (x ** alpha)
        weights = weights / weights.sum()
        return np.random.choice(ndv, size=n_rows, p=weights)
    
    def generate_hotspot_distribution(self, n_rows: int, ndv: int, hotspot_ratio: float = 0.1) -> np.ndarray:
        hotspot_size = max(1, int(ndv * hotspot_ratio))
        cold_size = max(1, ndv - hotspot_size)
        hotspot_values = np.random.randint(0, hotspot_size, int(n_rows * 0.8))
        cold_values = np.random.randint(hotspot_size, ndv, int(n_rows * 0.2))
        return np.concatenate([hotspot_values, cold_values])[:n_rows]
    
    def generate_uniform_distribution(self, n_rows: int, ndv: int) -> np.ndarray:
        return np.random.randint(0, ndv, n_rows)
    
    def apply_sortedness(self, values: np.ndarray, sortedness: float) -> np.ndarray:
        if sortedness >= 1.0:
            return np.sort(values)
        elif sortedness <= 0.0:
            return values
        
        n_shuffles = int(len(values) * (1 - sortedness))
        indices = np.random.choice(len(values), size=n_shuffles, replace=False)
        shuffled_values = values.copy()
        np.random.shuffle(shuffled_values[indices])
        return shuffled_values
    
    def generate_column(self, workload: str, col_idx: int, n_rows: int, config: Dict) -> Tuple[np.ndarray, Dict]:
        ndv_min, ndv_max = config['characteristics']['ndv_range']
        null_min, null_max = config['characteristics']['null_range']
        skew_types = config['characteristics']['skew_types']
        
        ndv_ratio = np.random.uniform(ndv_min, ndv_max)
        ndv = max(1, int(n_rows * ndv_ratio))
        null_ratio = np.random.uniform(null_min, null_max)
        sortedness = np.random.uniform(0.0, 0.8)
        
        skew_type = np.random.choice(skew_types)
        
        if col_idx % 3 == 0:
            dtype = 'float64'
            if skew_type == 'zipf':
                values = self.generate_zipf_distribution(n_rows, ndv).astype(float)
            elif skew_type == 'hotspot':
                values = self.generate_hotspot_distribution(n_rows, ndv).astype(float)
            else:
                values = self.generate_uniform_distribution(n_rows, ndv).astype(float)
        elif col_idx % 3 == 1:
            dtype = 'float64'
            if skew_type == 'zipf':
                base_values = self.generate_zipf_distribution(n_rows, ndv)
                values = base_values.astype(float) + np.random.normal(0, 0.1, n_rows)
            elif skew_type == 'hotspot':
                base_values = self.generate_hotspot_distribution(n_rows, ndv)
                values = base_values.astype(float) + np.random.normal(0, 0.1, n_rows)
            else:
                values = np.random.uniform(0, ndv, n_rows)
        else:
            dtype = 'object'
            unique_vals = [f'str_{j}' for j in range(ndv)]
            if skew_type == 'zipf':
                indices = self.generate_zipf_distribution(n_rows, ndv)
            elif skew_type == 'hotspot':
                indices = self.generate_hotspot_distribution(n_rows, ndv)
            else:
                indices = self.generate_uniform_distribution(n_rows, ndv)
            values = np.array([unique_vals[i] for i in indices])
        
        values = self.apply_sortedness(values, sortedness)
        
        if null_ratio > 0:
            null_indices = np.random.choice(n_rows, size=int(n_rows * null_ratio), replace=False)
            if dtype == 'object':
                values[null_indices] = None
            else:
                values[null_indices] = np.nan
        
        metadata = {
            'workload': workload,
            'column': col_idx,
            'dtype': dtype,
            'ndv_ratio': ndv_ratio,
            'actual_ndv': len(np.unique(values[~pd.isna(values)])) if dtype != 'object' else len(set(v for v in values if v is not None)),
            'null_ratio': null_ratio,
            'actual_null_ratio': np.isnan(values).sum() / len(values) if dtype != 'object' else sum(1 for v in values if v is None) / len(values),
            'skew_type': skew_type,
            'sortedness': sortedness,
            'actual_sortedness': self.calculate_sortedness(values)
        }
        
        return values, metadata
    
    def calculate_sortedness(self, values: np.ndarray) -> float:
        if len(values) <= 1:
            return 1.0
        
        sample_size = min(10000, len(values))
        sample_indices = np.random.choice(len(values), size=sample_size, replace=False)
        sample_values = values[sample_indices]
        
        sorted_sample = np.sort(sample_values)
        inversions = 0
        n = len(sample_values)
        
        for i in range(n):
            for j in range(i + 1, n):
                if sample_values[i] > sample_values[j]:
                    inversions += 1
        
        max_inversions = n * (n - 1) // 2
        if max_inversions == 0:
            return 1.0
        
        return 1.0 - (inversions / max_inversions)
    
    def generate_workload(self, workload: str, output_dir: str = "data") -> Dict:
        config = self.load_config(workload)
        n_rows = 1000
        n_cols = config['data']['columns']
        
        np.random.seed(42 + hash(workload) % 1000)
        
        data = {}
        metadata_list = []
        
        for col_idx in range(n_cols):
            col_name = f'col_{col_idx}'
            values, metadata = self.generate_column(workload, col_idx, n_rows, config)
            data[col_name] = values
            metadata_list.append(metadata)
        
        df = pd.DataFrame(data)
        
        csv_path = os.path.join(output_dir, f"{workload}_r{n_rows}_c{n_cols}_generated.csv")
        df.to_csv(csv_path, index=False)
        
        parquet_path = csv_path.replace('.csv', '.parquet')
        df.to_parquet(parquet_path, index=False)
        
        workload_metadata = {
            'workload': workload,
            'shape': df.shape,
            'csv_file': csv_path,
            'parquet_file': parquet_path,
            'columns': metadata_list,
            'file_size_mb': os.path.getsize(parquet_path) / (1024 * 1024),
            'config_used': config
        }
        
        return workload_metadata
    
    def validate_distributions(self, metadata: Dict) -> Dict:
        workload = metadata['workload']
        config = metadata['config_used']
        expected_ndv_range = config['characteristics']['ndv_range']
        expected_null_range = config['characteristics']['null_range']
        
        validation_results = {
            'workload': workload,
            'total_columns': len(metadata['columns']),
            'ndv_validations': [],
            'null_validations': [],
            'overall_valid': True
        }
        
        for col_meta in metadata['columns']:
            ndv_valid = expected_ndv_range[0] <= col_meta['ndv_ratio'] <= expected_ndv_range[1]
            null_valid = expected_null_range[0] <= col_meta['actual_null_ratio'] <= expected_null_range[1]
            
            validation_results['ndv_validations'].append({
                'column': col_meta['column'],
                'expected_range': expected_ndv_range,
                'actual': col_meta['ndv_ratio'],
                'valid': ndv_valid
            })
            
            validation_results['null_validations'].append({
                'column': col_meta['column'],
                'expected_range': expected_null_range,
                'actual': col_meta['actual_null_ratio'],
                'valid': null_valid
            })
            
            if not (ndv_valid and null_valid):
                validation_results['overall_valid'] = False
        
        return validation_results
    
    def generate_all_workloads(self, output_dir: str = "data") -> Dict:
        os.makedirs(output_dir, exist_ok=True)
        
        all_results = {}
        
        for workload in self.workloads:
            print(f"Generating {workload} workload...")
            metadata = self.generate_workload(workload, output_dir)
            validation = self.validate_distributions(metadata)
            
            all_results[workload] = {
                'metadata': metadata,
                'validation': validation
            }
            
            print(f"  Shape: {metadata['shape']}")
            print(f"  File size: {metadata['file_size_mb']:.2f} MB")
            print(f"  Validation: {'PASSED' if validation['overall_valid'] else 'FAILED'}")
            
            if not validation['overall_valid']:
                ndv_failures = sum(1 for v in validation['ndv_validations'] if not v['valid'])
                null_failures = sum(1 for v in validation['null_validations'] if not v['valid'])
                print(f"    NDV failures: {ndv_failures}/{len(validation['ndv_validations'])}")
                print(f"    Null failures: {null_failures}/{len(validation['null_validations'])}")
            print()
        
        results_file = os.path.join(output_dir, "workload_generation_results.json")
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        print(f"Results saved to {results_file}")
        return all_results

def main():
    generator = WorkloadGenerator()
    results = generator.generate_all_workloads()
    
    print("\n=== SUMMARY ===")
    total_valid = sum(1 for r in results.values() if r['validation']['overall_valid'])
    print(f"Workloads generated: {len(results)}")
    print(f"Valid distributions: {total_valid}/{len(results)}")
    
    for workload, result in results.items():
        validation = result['validation']
        print(f"{workload}: {'✓' if validation['overall_valid'] else '✗'} "
              f"({validation['total_columns']} cols, {result['metadata']['file_size_mb']:.1f}MB)")

if __name__ == "__main__":
    main()
