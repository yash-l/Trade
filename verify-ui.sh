#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
PORT="${HYDRA_PORT:-8181}"
echo "[HYDRA] Health:"
curl -fsS "http://127.0.0.1:$PORT/api/health" | python -m json.tool
echo
echo "[HYDRA] UI marker:"
curl -fsS "http://127.0.0.1:$PORT/?ui=221" | grep -o 'data-ui-build="[^"]*"' | head -1
