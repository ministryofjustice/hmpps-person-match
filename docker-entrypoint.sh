#!/bin/bash
set -e

source /opt/.venv/bin/activate

exec uvicorn \
    --host 0.0.0.0 \
    --port 5000 \
    --forwarded-allow-ips='*' \
    asgi:app \
    --workers 4 \
