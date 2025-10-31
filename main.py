from data_sourcer import DataSourcer, DataPreprocessor
from workload_generator import WorkloadGenerator
from format_converter import FormatConverter
from benchmark_runner import BenchmarkRunner
from visualizer import BenchmarkVisualizer


def main():
    print("=== Phase 1: Data Generation ===")
    sourcer = DataSourcer()
    sourcer.clean_data_dir()

    print("\n=== Phase 2: Workload Generation ===")
    generator = WorkloadGenerator()
    generator.generate_all_workloads()

    print("\n=== Phase 3: Format Conversion ===")
    converter = FormatConverter()
    converter.convert_all_workloads()

    print("\n=== Phase 4: Benchmarking ===")
    runner = BenchmarkRunner()
    parquet_results = runner.run_all_benchmarks()

    # TODO: Run ORC benchmarks similarly

    print("\n=== Phase 5: Visualization ===")
    visualizer = BenchmarkVisualizer()
    # visualizer.plot_file_sizes(parquet_results, orc_results)
    # visualizer.plot_full_scan_performance(parquet_results, orc_results)
    # visualizer.plot_selection_latency(parquet_results, orc_results)

    print("\nBenchmark complete! Check results/ and figures/ directories.")


if __name__ == "__main__":
    main()