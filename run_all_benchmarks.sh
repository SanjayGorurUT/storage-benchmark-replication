#!/bin/bash

set -e

echo "=========================================="
echo "Running All Benchmarks (Docker + Bare Metal)"
echo "=========================================="
echo ""
echo "This script will run benchmarks in both Docker and bare-metal environments"
echo "for comparison purposes."
echo ""

read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Step 1/2: Running Docker benchmarks..."
bash run_docker.sh

echo ""
echo "Step 2/2: Running bare-metal benchmarks..."
bash run_bare_metal.sh

echo ""
echo "=========================================="
echo "All benchmarks complete!"
echo "=========================================="
echo ""
echo "Results saved to:"
echo "  - results/benchmark_results_docker.json"
echo "  - results/benchmark_results_bare-metal.json"
echo ""
echo "You can now compare the results between environments."

