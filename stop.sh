#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
if [ -f .runtime/hydra.pid ]; then kill "$(cat .runtime/hydra.pid)" 2>/dev/null || true; rm -f .runtime/hydra.pid; fi
echo "HYDRA stopped"
