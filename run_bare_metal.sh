#!/bin/bash

set -e

echo "=========================================="
echo "Running Benchmarks on Bare Metal"
echo "=========================================="

if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "Error: requirements.txt not found. Are you in the project root?"
    exit 1
fi

if [ ! -d "venv" ] && [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Installing/updating dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo ""
echo "Running benchmarks on bare metal..."
python3 generate_preliminary_results.py

echo ""
echo "=========================================="
echo "Bare metal benchmarks complete!"
echo "Results saved to: results/benchmark_results_bare-metal.json"
echo "=========================================="

