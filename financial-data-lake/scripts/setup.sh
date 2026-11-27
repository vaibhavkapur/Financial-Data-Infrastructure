#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if command -v uv >/dev/null 2>&1; then
  uv sync --python 3.12 --extra dev
elif python3 -m uv --version >/dev/null 2>&1; then
  python3 -m uv sync --python 3.12 --extra dev
else
  echo "uv not found; falling back to python3 -m venv"
  python3 -m venv .venv
  .venv/bin/pip install -e ".[dev]"
fi

echo "Setup complete. Verify with: .venv/bin/python scripts/validate.py"
