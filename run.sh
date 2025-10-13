#!/bin/bash
echo "Cleaning data directory and regenerating datasets..."
rm -rf data/
python data_sourcer.py
