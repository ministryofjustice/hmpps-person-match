#!/bin/bash
set -e

source /opt/pysetup/.venv/bin/activate

exec gunicorn \
    --bind 0.0.0.0:5000 \
    --forwarded-allow-ips='*' \
    wsgi:app \
    --timeout 120 \
    --workers 4 \
    --worker-class gevent
