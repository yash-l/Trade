#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[HYDRA] Updating Termux package metadata..."
pkg update -y

echo "[HYDRA] Installing Termux-managed runtime packages..."
# Do NOT upgrade/reinstall pip through pip itself. Termux owns python-pip.
pkg install -y python python-pip git curl

python -m pip --version

echo "[HYDRA] Installing lightweight Python dependencies (includes IANA tzdata)..."
python -m pip install --disable-pip-version-check --no-cache-dir -r requirements.txt

[ -f .env ] || cp .env.example .env
mkdir -p "$PWD/data" "$PWD/.runtime"
chmod 700 "$PWD/.runtime" "$PWD/data" || true

# Force the default database into this writable project directory unless user explicitly overrides it.
if ! grep -q "^HYDRA_DB_PATH=" .env 2>/dev/null; then
  printf "\nHYDRA_DB_PATH=%s/data/hydra.sqlite3\n" "$PWD" >> .env
fi
chmod +x start.sh stop.sh health.sh verify-ui.sh install-termux.sh || true

echo "[HYDRA] Verifying IST timezone support..."
python - <<'PY'
from datetime import datetime, timedelta
from hydra.timeutil import IST
assert datetime.now(IST).utcoffset() == timedelta(hours=5, minutes=30)
print("[PASS] IST timezone:", IST)
PY

echo "[HYDRA] Verifying SQLite write access..."
python - <<'PY'
from pathlib import Path
import sqlite3
p=Path.cwd()/".runtime"/"sqlite-probe.sqlite3"
with sqlite3.connect(str(p)) as c:
    c.execute("CREATE TABLE IF NOT EXISTS probe(x INTEGER)")
    c.execute("INSERT INTO probe VALUES(1)")
p.unlink(missing_ok=True)
Path(str(p)+"-wal").unlink(missing_ok=True)
Path(str(p)+"-shm").unlink(missing_ok=True)
print("[PASS] SQLite writable:", p.parent)
PY

echo "[HYDRA] Running self-test (installer stops here if it fails)..."
python scripts/selftest.py

echo
echo "HYDRA v2.2.2 installed successfully."
echo "Start: ./start.sh"
echo "Dashboard: http://127.0.0.1:${HYDRA_PORT:-8181}/?ui=221"
