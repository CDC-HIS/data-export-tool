#!/bin/bash

# Use the virtual environment
PYTHON=".venv/bin/python"
PIP=".venv/bin/pip"

# Ensure virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
$PIP install --upgrade pip
$PIP install -r requirements.txt

# Clean previous build
if [ -d "export_distributable" ]; then
    echo "Cleaning previous build..."
    rm -rf export_distributable
fi

# Build with cx_Freeze
echo "Building with cx_Freeze..."
python setup.py build

echo "Build complete. Output is in 'export_distributable/' directory."
echo "Run the executable with: ./export_distributable/bin/export"
