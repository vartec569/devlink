#!/data/data/com.termux/files/usr/bin/bash
# devlink - one-time setup for Termux (Android). Safe to re-run:
# it only installs what is missing and pulls updates.
set -e

echo "[devlink] updating package lists..."
pkg update -y

echo "[devlink] installing git + Node.js LTS (includes npm)..."
pkg install -y git nodejs-lts

if [ -f "./server.js" ]; then
  DIR="$(pwd)"
else
  DIR="$HOME/devlink"
  if [ -d "$DIR/.git" ]; then
    echo "[devlink] repo already present - pulling updates..."
    git -C "$DIR" pull --ff-only
  elif [ -e "$DIR" ]; then
    echo "[devlink] $DIR exists but is not a repo clone - stopping, check it manually."
    exit 1
  else
    git clone https://github.com/vartec569/devlink.git "$DIR"
  fi
fi

cd "$DIR"
echo "[devlink] installing dependencies..."
npm install

echo
echo "[devlink] setup done. Folder: $DIR"
echo "[devlink] start the server with:  cd $DIR && bash termux-start.sh"
