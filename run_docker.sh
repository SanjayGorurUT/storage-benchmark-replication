#!/bin/bash

set -e

echo "=========================================="
echo "Running Benchmarks in Docker"
echo "=========================================="

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

echo "Building Docker image..."
docker build -t storage-benchmark:latest .

echo ""
echo "Running benchmarks in Docker container..."
docker run --rm \
    -v "$(pwd)/data:/app/data" \
    -v "$(pwd)/results:/app/results" \
    -v "$(pwd)/figures:/app/figures" \
    -v "$(pwd)/configs:/app/configs" \
    storage-benchmark:latest

echo ""
echo "=========================================="
echo "Docker benchmarks complete!"
echo "Results saved to: results/benchmark_results_docker.json"
echo "=========================================="

