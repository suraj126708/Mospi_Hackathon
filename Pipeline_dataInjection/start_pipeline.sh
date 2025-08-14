#!/bin/bash

echo "========================================"
echo "   Data Injection Pipeline Startup"
echo "========================================"
echo

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed or not in PATH"
    echo "Please install Python 3.8 or higher"
    exit 1
fi

echo "Python found. Checking version..."
python3 --version

echo
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

echo
echo "Activating virtual environment..."
source venv/bin/activate

echo
echo "Installing/updating dependencies..."
pip install -r requirements.txt

echo
echo "Starting the pipeline application..."
echo
python run.py

echo
echo "Application stopped."
