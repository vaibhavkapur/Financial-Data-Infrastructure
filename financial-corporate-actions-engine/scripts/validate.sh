#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

export PYTHONPATH="${PROJECT_ROOT}/src"

echo "[1/3] Running full unit, integration, and boundary test suite..."
python3 -m unittest discover -s tests -p "test_*.py" -v

echo "[2/3] Running reproducible end-to-end demo..."
python3 scripts/run_demo.py

echo "[3/3] Running deterministic synthetic benchmark..."
python3 scripts/run_benchmark.py

echo "All validation steps completed successfully."
