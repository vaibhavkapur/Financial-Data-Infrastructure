#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
echo "==> Running go vet"
go vet ./...

echo "==> Running full test suite with race detector"
go test -v -race ./...

echo "==> Running reproducible end-to-end demo"
./scripts/run_demo.sh > /dev/null

echo "==> Running benchmarks"
./scripts/run_benchmarks.sh > /dev/null

echo "==> All validations passed."
