#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p bin reports
go build -o bin/tickdb ./src/cmd/tickdb
./bin/tickdb benchmark \
  --config config/benchmark.json \
  --events 100000 \
  --batch-size 2000 \
  --queries 300 \
  --output reports/benchmark_results.json
