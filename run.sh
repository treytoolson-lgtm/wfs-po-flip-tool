#!/bin/bash
# WFS Escalation Tool — startup script (Mac / Linux)
# Windows users: run these commands manually in your terminal:
#   .venv\Scripts\activate
#   python -m uvicorn main:app --port 8765
set -e

cd "$(dirname "$0")"

echo "🐶 WFS Escalation Tool - Starting up..."

# Activate venv
if [ -d ".venv" ]; then
    source .venv/bin/activate
else
    echo "⚠️  No venv found. Run: uv venv && uv pip install -r requirements.txt"
    exit 1
fi

# Check gcloud auth — uses PATH (set by config.py or system installer)
if ! gcloud auth application-default print-access-token &>/dev/null; then
    echo "⚠️  GCloud not authenticated. Run:"
    echo "    gcloud auth application-default login"
    exit 1
fi

echo "✅ GCloud authenticated"
echo "🚀 Starting on http://localhost:${APP_PORT:-8766}"

".venv/bin/python" -m uvicorn main:app --port "${APP_PORT:-8766}"