#!/bin/bash
echo "============================================"
echo "  RecirQ Global — Shipment Check Server"
echo "============================================"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed."
    echo "Install with: brew install python3"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip3 install -r requirements.txt --quiet 2>/dev/null || pip install -r requirements.txt --quiet

echo ""
echo "Starting server..."
echo "Open your browser to: http://localhost:5000"
echo ""
python3 app.py
