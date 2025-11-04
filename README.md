# Storage Format Benchmark

## Project Status
- ✅ Workload generation with distribution validation
- ✅ Synthetic data for all 6 workloads (core, bi, classic, geo, log, ml)
- 🚧 Performance benchmarking infrastructure
- 🚧 ORC format support
- ⏳ Figure 6 reproduction

## Quick Start
```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python main.py

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
├── data_sourcer.py         # Synthetic data generation
├── workload_generator.py   # Distribution-aware workload generation
├── format_converter.py     # Parquet ↔ ORC conversion
├── benchmark_runner.py     # Performance measurement
├── visualizer.py           # Figure 6 reproduction
└── main.py                 # Full pipeline orchestration
```