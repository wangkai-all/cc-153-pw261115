#!/usr/bin/env bash
# Start the local demo on http://127.0.0.1:8000 (store on :9001).
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 -m app.run "$@"
