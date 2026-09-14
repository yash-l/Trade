#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p .runtime data
PORT="${HYDRA_PORT:-8181}"
PIDFILE=.runtime/hydra.pid
LOGFILE=.runtime/hydra.log
EXPECTED_VERSION="2.2.2"
EXPECTED_UI="2.2.2-adaptive-paper"

# If this build's own PID is alive, verify it is actually serving this build.
if [ -f "$PIDFILE" ]; then
  OLD_PID="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    HEALTH="$(curl -fsS "http://127.0.0.1:$PORT/api/health" 2>/dev/null || true)"
    if printf '%s' "$HEALTH" | grep -q '"version":"2.2.2"' && printf '%s' "$HEALTH" | grep -q '"ui_build":"2.2.2-adaptive-paper"'; then
      echo "HYDRA v2.2.2 already READY — PID $OLD_PID"
      echo "Dashboard: http://127.0.0.1:$PORT/?ui=221"
      exit 0
    fi
  fi
  rm -f "$PIDFILE"
fi

# Never accept a healthy OLD HYDRA on our port as proof that this build started.
EXISTING="$(curl -fsS "http://127.0.0.1:$PORT/api/health" 2>/dev/null || true)"
if [ -n "$EXISTING" ]; then
  echo "[HYDRA] PORT $PORT is already occupied by another HYDRA/server."
  echo "Existing health: $EXISTING"
  echo "Run the supplied v2.2.2 update command, which stops stale HYDRA processes before start."
  exit 1
fi

: > "$LOGFILE"
nohup python -m hydra.main > "$LOGFILE" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"

for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "[HYDRA] START FAILED: process exited during boot."
    rm -f "$PIDFILE"
    echo "----- HYDRA LOG -----"
    tail -n 100 "$LOGFILE" || true
    exit 1
  fi
  HEALTH="$(curl -fsS "http://127.0.0.1:$PORT/api/health" 2>/dev/null || true)"
  HOME_HTML="$(curl -fsS "http://127.0.0.1:$PORT/?ui=221" 2>/dev/null || true)"
  if printf '%s' "$HEALTH" | grep -q '"version":"2.2.2"' \
     && printf '%s' "$HEALTH" | grep -q '"ui_build":"2.2.2-adaptive-paper"' \
     && printf '%s' "$HOME_HTML" | grep -q 'data-ui-build="2.2.2-adaptive-paper"'; then
    echo "HYDRA v2.2.2 5PAISA LIVE-DATA PAPER READY — PID $PID"
    echo "UI build: $EXPECTED_UI"
    echo "Dashboard: http://127.0.0.1:$PORT/?ui=221"
    exit 0
  fi
  sleep 1
done

echo "[HYDRA] START FAILED: this v2.2.2 UI did not become the active server."
kill "$PID" 2>/dev/null || true
rm -f "$PIDFILE"
echo "----- HYDRA LOG -----"
tail -n 100 "$LOGFILE" || true
exit 1
