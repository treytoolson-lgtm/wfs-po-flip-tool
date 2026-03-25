#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "🐶 WFS Escalation Tool - Starting up..."

# Activate venv if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
else
    echo "⚠️  No venv found. Run: uv venv && uv pip install -r requirements.txt"
    exit 1
fi

# Check gcloud auth
if ! "/Users/t0t0ech/Documents/gCloud CLI/google-cloud-sdk/bin/gcloud" auth application-default print-access-token &>/dev/null; then
    echo "⚠️  GCloud not authenticated. Run:"
    echo '    "/Users/t0t0ech/Documents/gCloud CLI/google-cloud-sdk/bin/gcloud" auth application-default login'
    exit 1
fi

echo "✅ GCloud authenticated"
echo "🚀 Starting uvicorn on http://localhost:${APP_PORT:-8765}"

uvicorn main:app --reload --port "${APP_PORT:-8765}"
