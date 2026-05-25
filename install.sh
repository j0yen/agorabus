#!/usr/bin/env bash
# install.sh — install agorabus (the `agorabus` binary).
#
# Modes:
#   1. Repo-local: invoked as `./install.sh` from a checkout.
#   2. Curl-piped: invoked as `curl ... | bash`. Self-clones into
#      ~/.local/share/agorabus/ then continues.

set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR=""
if [ -f "$SCRIPT_PATH" ]; then
  SCRIPT_DIR=$(cd "$(dirname "$SCRIPT_PATH")" && pwd)
fi

if [ -z "$SCRIPT_DIR" ] || [ ! -f "$SCRIPT_DIR/Cargo.toml" ] \
   || ! grep -q '^name = "agorabus"' "$SCRIPT_DIR/Cargo.toml" 2>/dev/null; then
  echo "→ self-cloning j0yen/agorabus..."
  command -v git >/dev/null 2>&1 || { echo "fatal: git not found"; exit 1; }

  CLONE_ROOT="${AGORABUS_CLONE_ROOT:-$HOME/.local/share/agorabus}"
  mkdir -p "$(dirname "$CLONE_ROOT")"

  if [ -d "$CLONE_ROOT/.git" ]; then
    echo "→ existing clone at $CLONE_ROOT — refreshing"
    git -C "$CLONE_ROOT" fetch --depth 1 origin main
    git -C "$CLONE_ROOT" reset --hard origin/main
  else
    git clone --depth 1 https://github.com/j0yen/agorabus.git "$CLONE_ROOT"
  fi

  SCRIPT_DIR="$CLONE_ROOT"
fi

cd "$SCRIPT_DIR"

command -v cargo >/dev/null 2>&1 || {
  echo "fatal: cargo not found. Install Rust: https://rustup.rs/"
  exit 1
}

echo "→ building + installing agorabus via cargo install (this can take a few minutes)..."
cargo install --path . --locked

if ! command -v agorabus >/dev/null 2>&1; then
  echo
  echo "! agorabus installed but not on PATH. Add ~/.cargo/bin to PATH:"
  echo "    export PATH=\"\$HOME/.cargo/bin:\$PATH\""
fi

echo "✓ agorabus installed."
