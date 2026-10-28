#!/bin/sh
set -eu

exec /app/.venv/bin/python -m uvicorn \
    --host 0.0.0.0 \
    --port 5000 \
    --forwarded-allow-ips='*' \
    asgi:app \
    --workers 1
