# Storage Format Benchmark

## Project Status
- ✅ Workload generation with distribution validation
- ✅ Synthetic data for all 6 workloads (core, bi, classic, geo, log, ml)
- ✅ Performance benchmarking infrastructure
- ✅ ORC format support
- ✅ Preliminary results generation (1K rows)
- ✅ Dockerized setup for reproducibility
- ✅ Bare-metal benchmarking support
- ✅ Figure 6 visualization structure (needs polish)
- ⏳ Scale up to 100K rows for final benchmarks
- ⏳ Final analysis and report

**See [REMAINING_WORK.md](REMAINING_WORK.md) for focused 2-week plan.**

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