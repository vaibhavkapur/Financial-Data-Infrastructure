#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p bin reports
go build -o bin/tickdb ./src/cmd/tickdb
./bin/tickdb demo \
  --config config/default.json \
  --fixture data/fixtures/valid_reference_ticks.jsonl \
  --report reports/demo_replay_comparison.json
