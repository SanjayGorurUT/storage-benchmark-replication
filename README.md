# Storage Format Benchmark

## Report-to-Code Mapping

This document provides a clear map connecting each major section of the report to the corresponding code implementation. This project replicates the columnar storage format evaluation from Zeng et al. (VLDB 2023), comparing Parquet and ORC formats across six workloads.

### Section 1: Introduction

**Section 1 (Introduction):**
- `configs/workloads.yaml`: NEW FILE - Defines benchmark configuration, workloads, formats, and metrics
- `README.md`: MODIFIED - Project documentation and setup instructions
- `main.py`: NEW FILE - Complete pipeline orchestration (Lines 8-38: `main()` function)
- `run_large_scale_benchmarks.py`: NEW FILE - Large-scale benchmarks (1M rows) implementation

### Section 2: Methodology

**Section 2.1 (Original Paper's Approach):**
- `configs/core.yaml`: NEW FILE - Core workload characteristics matching paper specifications
- `configs/bi.yaml`: NEW FILE - Business Intelligence workload configuration
- `configs/classic.yaml`: NEW FILE - Classic workload configuration
- `configs/geo.yaml`: NEW FILE - Geographic workload configuration
- `configs/log.yaml`: NEW FILE - Log workload configuration
- `configs/ml.yaml`: NEW FILE - Machine Learning workload configuration
- `configs/workloads.yaml`: NEW FILE - Overall benchmark configuration

**Section 2.2 (Our Implementation):**

*Phase 1: Synthetic Data Generation*
- `data_sourcer.py`: NEW FILE - Core synthetic data generation with workload-specific characteristics
  - Lines 8-18: `DataSourcer` class initialization with workload configurations (NDV ranges, null ratios, skew types)
  - Lines 20-54: `generate_synthetic_dataset()` - Generates base datasets with 1M rows and 20 columns per workload
  - Lines 56-60: `clean_data_dir()` - Data directory management

- `workload_generator.py`: NEW FILE - Distribution-aware workload generation
  - Lines 15-18: `load_config()` - Loads YAML configuration files for each workload
  - Lines 20-24: `generate_zipf_distribution()` - Zipfian distribution implementation (alpha=1.5)
  - Lines 26-31: `generate_hotspot_distribution()` - Hotspot distribution (80/20 pattern)
  - Lines 33-34: `generate_uniform_distribution()` - Uniform distribution
  - Lines 36-46: `apply_sortedness()` - Applies sortedness parameter to columns
  - Lines 48-111: `generate_column()` - Main column generation with distribution selection, null injection, and sortedness; alternates integer, float, and string types
  - Lines 113-134: `calculate_sortedness()` - Measures actual sortedness using inversion counting
  - Lines 136-170: `generate_workload()` - Generates complete workload with metadata tracking
  - Lines 172-207: `validate_distributions()` - Validates generated distributions against config requirements (5% tolerance)
  - Lines 209-240: `generate_all_workloads()` - Orchestrates generation for all 6 workloads

*Phase 2: Workload Configuration*
- `data_sourcer.py`: NEW FILE - Data validation and preprocessing
  - Lines 62-71: `DataPreprocessor` class with workload configurations
  - Lines 73-77: `_calculate_ndv_ratio()` and `_calculate_null_ratio()` - Statistical validation methods
  - Lines 79-114: `process_workload()` - Validates NDV and null ratios against expected ranges

*Phase 3: Format Conversion and Benchmarking*
- `format_converter.py`: NEW FILE - Format conversion implementation
  - Lines 8-15: `convert_to_orc()` - Converts Parquet files to ORC format using PyArrow 9.0.0+
  - Lines 18-33: `FormatConverter` class - Orchestrates conversion for all workloads

- `benchmark_runner.py`: NEW FILE - Core benchmarking implementation
  - Lines 11-27: `BenchmarkRunner` class initialization with environment detection
  - Lines 29-30: `measure_file_size()` - File size measurement in MB
  - Lines 32-39: `_read_file()` - Format-aware file reading (Parquet/ORC)
  - Lines 41-54: `measure_full_scan()` - Full table scan performance with 5 iterations, measures throughput (rows/sec) and latency (ms)
  - Lines 56-77: `measure_selection_query()` - Selection query performance at varying selectivities (1%, 10%, 50%) using percentile-based thresholds
  - Lines 79-115: `benchmark_workload()` - Complete workload benchmarking for a single format
  - Lines 117-148: `run_all_benchmarks()` - Orchestrates benchmarks across all workloads and formats

*Phase 4: Running in Two Setups*
- `Dockerfile`: NEW FILE - Reproducible containerized environment using Python 3.11-slim
- `docker-compose.yml`: NEW FILE - Container orchestration and volume mounting
- `run_docker.sh`: NEW FILE - Script to run benchmarks in Docker environment
- `run_bare_metal.sh`: NEW FILE - Script to run benchmarks on bare-metal
- `run_all_benchmarks.sh`: NEW FILE - Script to run both environments for comparison
- `benchmark_runner.py`: MODIFIED lines 12-27 - Environment detection logic (`_detect_environment()` method)
  - Automatically detects Docker vs bare-metal execution
  - Tags results with environment type for comparison

**Section 2.3 (Divergences from Original Paper):**
- `generate_preliminary_results.py`: NEW FILE - Scaled development approach (1K rows for validation)
  - Lines 108-180: `main()` - Supports both 1K and 1M row benchmarks
- `run_large_scale_benchmarks.py`: NEW FILE - Large-scale benchmarks (1M rows)
  - Lines 14-57: Main execution for 1M row benchmarks with CSV → Parquet → ORC conversion
- `Dockerfile`: NEW FILE - Docker-based execution environment (not in original paper)
- `docker-compose.yml`: NEW FILE - Container orchestration for reproducibility

### Section 3: Experimental Setup

**Section 3.1 (Hardware and Software Environment):**
- `Dockerfile`: NEW FILE - Defines Python 3.11-slim container environment
- `requirements.txt`: NEW FILE - Specifies software stack (pandas 1.5.0+, numpy 1.21.0+, pyarrow 9.0.0+, scipy 1.9.0+)
- `benchmark_runner.py`: MODIFIED lines 12-27 - Environment detection for bare-metal vs Docker
- `run_docker.sh`: NEW FILE - Docker execution script
- `run_bare_metal.sh`: NEW FILE - Bare-metal execution script

**Section 3.2 (Dataset Characteristics):**
- `workload_generator.py`: NEW FILE - Generates 1M rows × 20 columns per workload
  - Lines 48-111: `generate_column()` - Creates columns with alternating data types (integer, float, string)
  - Lines 136-170: `generate_workload()` - Produces workload datasets with metadata
- `data_sourcer.py`: NEW FILE - Base dataset generation
  - Lines 20-54: `generate_synthetic_dataset()` - Creates 1M row datasets
- `benchmark_runner.py`: NEW FILE - File size measurement
  - Lines 29-30: `measure_file_size()` - Measures resulting file sizes in MB

**Section 3.3 (Benchmark Metrics and Methodology):**
- `benchmark_runner.py`: NEW FILE - Three key performance evaluations
  - Lines 29-30: `measure_file_size()` - File size (compression efficiency) measurement
  - Lines 41-54: `measure_full_scan()` - Full scan throughput (rows/sec) with 5 iterations using `time.perf_counter()`
  - Lines 56-77: `measure_selection_query()` - Selection query latency at 1%, 10%, 50% selectivity with 5 iterations
  - Lines 32-39: `_read_file()` - Format-specific readers (pd.read_parquet, orc.read_table) with full materialization

**Section 3.4 (Comparison to Original Paper Setup):**
- `benchmark_runner.py`: MODIFIED lines 12-27 - Environment detection and result tagging
- `results/benchmark_results_bare-metal.json`: NEW FILE - Bare-metal results (main results)
- `results/benchmark_results_docker.json`: NEW FILE - Docker results (reproducibility validation)

### Section 4: Evaluation

**Section 4.1 (Results):**
- `benchmark_runner.py`: NEW FILE - Generates structured benchmark results
  - Lines 117-148: `run_all_benchmarks()` - Produces JSON results with file sizes, scan throughput, and selection latencies
- `results/benchmark_results_bare-metal.json`: NEW FILE - Complete benchmark results in structured format
- `compare_with_paper.py`: NEW FILE - Result extraction and comparison
  - Lines 17-21: `load_our_results()` - Loads benchmark results
  - Lines 24-59: `extract_our_metrics()` - Extracts metrics in paper's format (file size, full scan, selection latency)
  - Lines 121-140: `print_comparison_table()` - Generates comparison tables with ORC/Parquet ratios

**Section 4.2 (Analysis):**
- `visualizer.py`: NEW FILE - Visualization generation for analysis
  - Lines 8-12: `BenchmarkVisualizer` class initialization
  - Lines 14-39: `_plot_bar_chart()` - Generic bar chart plotting utility
  - Lines 41-53: `plot_file_sizes()` - File size comparison across workloads
  - Lines 55-69: `plot_full_scan_performance()` - Full scan throughput visualization
  - Lines 71-96: `plot_selection_latency()` - Selection query latency at specific selectivity
- `generate_preliminary_results.py`: NEW FILE - Result summary generation
  - Lines 24-105: `generate_summary_report()` - Generates markdown summary with tables for file sizes, scan performance, and selection queries
- `compare_with_paper.py`: NEW FILE - Comparison with paper results
  - Lines 62-118: `plot_comparison()` - Side-by-side comparison plots (our results vs paper)
  - Lines 143-239: `main()` - Orchestrates comparison and exports results

**Section 4.3 (Differences):**
- `extract_paper_figure6.py`: NEW FILE - Paper result extraction and comparison
  - Lines 13-28: `OUR_RESULTS` dictionary - Our benchmark results
  - Lines 30-46: `PAPER_RESULTS` dictionary - Placeholder for paper's Figure 6 values
  - Lines 48-95: `plot_side_by_side_comparison()` - Creates side-by-side visualizations
  - Lines 97-120: `print_comparison_table()` - Prints detailed comparison tables with percentage differences
  - Lines 122-204: `main()` - Main comparison workflow
- `compare_with_paper.py`: NEW FILE - Quantitative comparison analysis
  - Lines 143-239: `main()` - Exports results for manual comparison with paper

### External Dependencies and Attribution

**Original Paper:**
- Zeng, K., et al. "Columnar Storage Formats: A Comprehensive Evaluation" (VLDB 2023)
- This project replicates Figure 6 from the paper comparing Parquet and ORC formats

**Third-Party Libraries:**
- `pandas` (>=1.5.0): Data manipulation and CSV/Parquet I/O
- `numpy` (>=1.21.0): Numerical operations and random number generation
- `pyarrow` (>=9.0.0): Parquet and ORC format support
- `scipy` (>=1.9.0): Statistical distributions (used in workload generation)
- `pyyaml` (>=6.0): YAML configuration file parsing
- `matplotlib`: Visualization (used in visualizer.py and comparison scripts)

**All code in this repository is original implementation** for the replication study. No code was directly copied from external sources; the implementation follows the methodology described in the paper.

---

## Project Status
- Workload generation with distribution validation
- Synthetic data for all 6 workloads (core, bi, classic, geo, log, ml)
- Performance benchmarking infrastructure
- ORC format support
- Preliminary results generation (1K rows)
- Dockerized setup for reproducibility
- Bare-metal benchmarking support
- Figure 6 visualization structure

## Quick Start

### Option 1: Docker (Recommended for Reproducibility)
```bash
# Run benchmarks in Docker container
./run_docker.sh

# Or using docker-compose
docker-compose up
```

### Option 2: Bare Metal (Recommended for Final Benchmarks)
```bash
# Run benchmarks on bare metal
./run_bare_metal.sh

# Or manually
pip install -r requirements.txt
python generate_preliminary_results.py
```

### Option 3: Compare Both Environments
```bash
# Run benchmarks in both Docker and bare-metal for comparison
./run_all_benchmarks.sh
```

### Manual Execution
```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python main.py

# Generate preliminary results
python generate_preliminary_results.py

# Or run individual phases
python data_sourcer.py          # Generate base data
python workload_generator.py    # Generate workloads with distributions
python format_converter.py      # Convert to ORC
python benchmark_runner.py      # Run performance tests
```

## Project Structure
```
├── data/                    # Generated datasets
├── configs/                 # Workload YAML configs
├── results/                 # Benchmark results (JSON)
├── figures/                 # Generated plots
├── Dockerfile              # Docker container definition
├── docker-compose.yml      # Docker Compose configuration
├── .dockerignore           # Docker ignore patterns
├── run_docker.sh           # Script to run benchmarks in Docker
├── run_bare_metal.sh       # Script to run benchmarks on bare metal
├── run_all_benchmarks.sh   # Script to run both environments
├── data_sourcer.py         # Synthetic data generation
├── workload_generator.py   # Distribution-aware workload generation
├── format_converter.py     # Parquet ↔ ORC conversion
├── benchmark_runner.py     # Performance measurement
├── visualizer.py           # Figure 6 reproduction
├── generate_preliminary_results.py  # Generate preliminary results & summary
└── main.py                 # Full pipeline orchestration
```

## Docker Setup and Benchmarking Strategy

### What Was Added

This project now supports both **Dockerized** and **bare-metal** benchmarking environments. The following components were added:

1. **Dockerfile**: Defines a reproducible containerized environment using Python 3.11-slim
2. **docker-compose.yml**: Simplifies container management and volume mounting
3. **Environment Detection**: The benchmark runner automatically detects whether it's running in Docker or bare-metal
4. **Separate Result Files**: Results are tagged with environment type (`benchmark_results_docker.json` vs `benchmark_results_bare-metal.json`)
5. **Convenience Scripts**: Shell scripts to easily run benchmarks in either environment

### How It Helps

**Dockerized Setup Benefits:**
- **Reproducibility**: Ensures consistent execution environment across different machines and operating systems
- **Isolation**: Prevents conflicts with system-level dependencies or Python versions
- **Portability**: Easy to share and run on any system with Docker installed
- **Consistency**: Same Python version, library versions, and system configuration for all team members
- **Development**: Faster onboarding for new contributors who don't need to configure local environments

**Bare-Metal Setup Benefits:**
- **Performance Accuracy**: Eliminates potential overhead from containerization (I/O, CPU throttling, NUMA sensitivity)
- **Real-World Conditions**: Reflects actual production deployment scenarios
- **Hardware Optimization**: Can leverage system-specific optimizations and direct hardware access
- **Validation**: Allows comparison with Dockerized results to identify any containerization effects

### Why This Approach

We implemented a **dual-environment strategy** for the following reasons:

1. **Reproducibility vs. Accuracy Trade-off**: Docker provides excellent reproducibility for development and relative comparisons, but containerization can introduce small performance differences due to:
   - I/O virtualization overhead
   - CPU throttling and cgroup limitations
   - NUMA (Non-Uniform Memory Access) sensitivity
   - Filesystem layer abstraction

2. **Best of Both Worlds**: 
   - Use Docker for **development, testing, and preliminary results** where reproducibility is key
   - Use bare-metal for **final benchmarking** where absolute performance accuracy is critical

3. **Validation**: Running both environments allows us to:
   - Quantify any performance differences between environments
   - Justify using Docker if differences are negligible
   - Identify if containerization affects specific workloads differently

4. **Academic Rigor**: For research publication, having both environments demonstrates thoroughness and allows us to report results from the most appropriate environment for each metric.

### Results Organization

Benchmark results are automatically tagged with their environment:
- `results/benchmark_results_docker.json` - Results from Docker container
- `results/benchmark_results_bare-metal.json` - Results from bare-metal execution

Each result file includes metadata:
```json
{
  "environment": "docker" or "bare-metal",
  "timestamp": "2025-11-15 21:33:01",
  "results": { ... }
}
```

### Usage Recommendations

- **Development/Testing**: Use Docker (`./run_docker.sh`) for quick iterations and consistent results
- **Preliminary Results**: Docker is sufficient for relative format comparisons (Parquet vs ORC)
- **Final Benchmarks**: Use bare-metal (`./run_bare_metal.sh`) for publication-ready results
- **Validation**: Run both (`./run_all_benchmarks.sh`) to compare and validate consistency