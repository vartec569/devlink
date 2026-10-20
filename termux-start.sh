#!/data/data/com.termux/files/usr/bin/bash
# devlink - start the server inside Termux.
cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  echo "[devlink] Node.js not found - run termux-setup.sh first."
  exit 1
fi

if [ ! -d node_modules ]; then
  echo "[devlink] installing dependencies (first run)..."
  npm install
fi

# Keep the CPU awake while the server runs (built into Termux; harmless if absent).
if command -v termux-wake-lock >/dev/null 2>&1; then
  termux-wake-lock
  echo "[devlink] wake lock on"
fi

echo "[devlink] starting server..."
echo "[devlink] on first run the API secret is generated in .dl-secret in this folder."
exec node server.js
