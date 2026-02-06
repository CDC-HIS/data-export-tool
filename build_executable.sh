#!/bin/bash

# Use the virtual environment
PYTHON=".venv/bin/python"
PIP=".venv/bin/pip"
PYINSTALLER=".venv/bin/pyinstaller"

# Ensure dependencies are installed
$PIP install -r requirements.txt

# Run PyInstaller
# --onefile: Create a single executable
# --windowed: No console window (GUI only)
# --name: Name of the executable
# --icon: Icon file
# --add-data: Include sql_queries folder and config files
# --clean: Clean PyInstaller cache
# --noconfirm: Replace output directory without asking

echo "Building single-file executable..."

$PYINSTALLER --noconfirm --clean \
    --onefile \
    --windowed \
    --name "ExportTool" \
    --icon "moh.ico" \
    --add-data "sql_queries:sql_queries" \
    --add-data "export_config.json:." \
    --add-data "moh.ico:." \
    --add-data "moh.png:." \
    export.py

echo "Build complete. Executable is in the 'dist' folder."
