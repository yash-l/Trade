#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
curl -fsS "http://127.0.0.1:${HYDRA_PORT:-8181}/api/health" | python -m json.tool
