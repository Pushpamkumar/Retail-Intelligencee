#!/bin/bash
echo "=========================================================="
echo "Starting Purplle Store Intelligence System Compliance Run"
echo "=========================================================="

# 1. Start the FastAPI web server in the background
echo "[1/2] Launching FastAPI REST API on http://localhost:8000/..."
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 &
API_PID=$!

# 2. Wait for FastAPI to initialize
echo "Waiting for API server startup..."
sleep 4

# 3. Check if server is running
python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: FastAPI server failed to initialize."
    kill $API_PID
    exit 1
fi

# 4. Start the Computer Vision pipeline
echo "[2/2] Booting all 5 CCTV camera processors..."
python detect.py --camera all

# Cleanup background processes on exit
trap "kill $API_PID" EXIT
