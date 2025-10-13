import pandas as pd
import numpy as np
import requests
import os
import shutil
import sys
from typing import Dict, List, Optional
import json

class DataSourcer:
    def __init__(self):
        self.workloads = ["core", "bi", "classic", "geo", "log", "ml"]
        self.base_urls = [
            "https://d3m9osc9baovkk.cloudfront.net",
            "https://storage.googleapis.com/embryo-labs-datasets",
            "https://github.com/embryo-labs/EvaluationOfColumnarFormats/raw/main/data",
            "https://raw.githubusercontent.com/embryo-labs/EvaluationOfColumnarFormats/main/data"
        ]
    
    def download_dataset(self, workload: str, output_dir: str = "data") -> Optional[str]:
        for base_url in self.base_urls:
            url = f"{base_url}/{workload}_r1000000_c20/gen_data/{workload}_r1000000_c20.csv"
            try:
                print(f"Trying {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                os.makedirs(output_dir, exist_ok=True)
                filepath = os.path.join(output_dir, f"{workload}_r1000000_c20.csv")
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                print(f"Downloaded {workload} dataset to {filepath}")
                return filepath
            except Exception as e:
                print(f"Failed {base_url}: {e}")
                continue
        return None
    
    def generate_dataset_from_paper(self, workload: str, output_dir: str = "data") -> Optional[str]:
        try:
            import subprocess
            import tempfile
            import zipfile
            
            print(f"Generating {workload} dataset using paper's generator...")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_url = "https://github.com/XinyuZeng/OpenFormat-Public/archive/49a9ec791a9c303425083f44be2569d4a06589b5.zip"
                zip_path = os.path.join(temp_dir, "generator.zip")
                
                print(f"Downloading generator from {zip_url}")
                response = requests.get(zip_url)
                response.raise_for_status()
                
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                extracted_dir = os.path.join(temp_dir, "OpenFormat-Public-49a9ec791a9c303425083f44be2569d4a06589b5")
                generator_dir = os.path.join(extracted_dir, "benchmark", "generator_v2")
                
                if not os.path.exists(generator_dir):
                    print(f"Generator directory not found: {generator_dir}")
                    return None
                
                python_cmd = sys.executable
                gen_script_path = os.path.join(generator_dir, "gen_workloads.py")
                
                result = subprocess.run([
                    python_cmd, gen_script_path, workload, "1000000", "20", "temp_output"
                ], cwd=generator_dir, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    source_file = os.path.join(generator_dir, "temp_output", "gen_data", "temp_output.csv")
                    if os.path.exists(source_file):
                        os.makedirs(output_dir, exist_ok=True)
                        target_file = os.path.join(output_dir, f"{workload}_r1000000_c20.csv")
                        shutil.copy2(source_file, target_file)
                        print(f"Generated {workload} dataset using paper's generator: {target_file}")
                        return target_file
                else:
                    print(f"Paper's generator failed: {result.stderr}")
                    print(f"Generator stdout: {result.stdout}")
                    
        except Exception as e:
            print(f"Failed to generate {workload} dataset: {e}")
        
        return None
    
    def download_all(self, output_dir: str = "data") -> List[str]:
        downloaded = []
        for workload in self.workloads:
            filepath = self.download_dataset(workload, output_dir)
            if not filepath:
                filepath = self.generate_dataset_from_paper(workload, output_dir)
            if filepath:
                downloaded.append(filepath)
        return downloaded
    
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
            "classic": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "zipf"]},
            "geo": {"ndv_range": (0.01, 0.5), "null_range": (0.0, 0.2), "skew_types": ["hotspot", "zipf"]},
            "log": {"ndv_range": (0.001, 0.3), "null_range": (0.0, 0.15), "skew_types": ["zipf", "hotspot"]},
            "ml": {"ndv_range": (0.1, 0.9), "null_range": (0.0, 0.1), "skew_types": ["uniform", "zipf"]}
        }
    
    def analyze_distribution(self, df: pd.DataFrame) -> Dict:
        stats = {}
        for col in df.columns:
            col_data = df[col]
            stats[col] = {
                "dtype": str(col_data.dtype),
                "null_ratio": col_data.isnull().sum() / len(col_data),
                "ndv_ratio": col_data.nunique() / len(col_data),
                "unique_count": col_data.nunique(),
                "total_count": len(col_data)
            }
        return stats
    
    def validate_workload(self, df: pd.DataFrame, workload_type: str) -> Dict:
        config = self.workload_configs.get(workload_type, {})
        stats = self.analyze_distribution(df)
        
        validation = {
            "workload_type": workload_type,
            "shape": df.shape,
            "columns_analyzed": len(stats),
            "validation_passed": True,
            "issues": []
        }
        
        for col, col_stats in stats.items():
            ndv_ratio = col_stats["ndv_ratio"]
            null_ratio = col_stats["null_ratio"]
            
            if "ndv_range" in config:
                min_ndv, max_ndv = config["ndv_range"]
                if not (min_ndv <= ndv_ratio <= max_ndv):
                    validation["issues"].append(f"Column {col}: NDV ratio {ndv_ratio:.3f} outside expected range [{min_ndv}, {max_ndv}]")
            
            if "null_range" in config:
                min_null, max_null = config["null_range"]
                if not (min_null <= null_ratio <= max_null):
                    validation["issues"].append(f"Column {col}: Null ratio {null_ratio:.3f} outside expected range [{min_null}, {max_null}]")
        
        if validation["issues"]:
            validation["validation_passed"] = False
        
        return validation
    
    def preprocess_to_columnar(self, filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath)
        
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    pass
        
        return df
    
    def process_workload(self, filepath: str) -> Dict:
        workload_type = os.path.basename(filepath).split('_')[0]
        
        df = self.preprocess_to_columnar(filepath)
        validation = self.validate_workload(df, workload_type)
        
        output_file = filepath.replace('.csv', '_processed.parquet')
        df.to_parquet(output_file, index=False)
        
        result = {
            "input_file": filepath,
            "output_file": output_file,
            "validation": validation,
            "file_size_mb": os.path.getsize(output_file) / (1024 * 1024)
        }
        
        return result

def main():
    sourcer = DataSourcer()
    preprocessor = DataPreprocessor()
    
    print("Cleaning data directory...")
    sourcer.clean_data_dir()
    
    print("Downloading datasets from paper's repository...")
    downloaded_files = sourcer.download_all()
    
    if not downloaded_files:
        print("WARNING: Could not download datasets from paper's repository.")
        print("This may be due to:")
        print("1. Network connectivity issues")
        print("2. Dataset URLs being temporarily unavailable")
        print("3. Changes in the repository structure")
        print("\nCreating synthetic datasets with realistic workload characteristics...")
        
        for workload in sourcer.workloads:
            np.random.seed(42)
            n_rows, n_cols = 100000, 20
            
            data = {}
            config = preprocessor.workload_configs[workload]
            
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
            filepath = f"data/{workload}_r{n_rows}_c{n_cols}.csv"
            df.to_csv(filepath, index=False)
            downloaded_files.append(filepath)
            print(f"Created {workload} dataset: {filepath} ({df.shape})")
    
    print(f"\nProcessing {len(downloaded_files)} datasets...")
    results = []
    
    for filepath in downloaded_files:
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
