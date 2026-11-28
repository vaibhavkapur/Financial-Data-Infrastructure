PYTHON ?= python3
PYTHONPATH := src

.PHONY: help setup test demo benchmark serve validate clean

help:
	@echo "Security Master Platform targets:"
	@echo "  make setup      - Verify local Python runtime and create runtime directories"
	@echo "  make test       - Run full unit, integration, boundary, and API test suite"
	@echo "  make demo       - Run reproducible end-to-end demo and write reports/demo_baseline.json"
	@echo "  make benchmark  - Run deterministic benchmark and write reports/benchmark_results.json"
	@echo "  make serve      - Launch local HTTP API + Quality Dashboard on 127.0.0.1:8080"
	@echo "  make validate   - Run tests, demo, and benchmark (full CI validation)"
	@echo "  make clean      - Remove temporary database and cache artifacts"

setup:
	@$(PYTHON) --version
	@mkdir -p reports .runtime

test:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/run_validation_report.py

demo:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/run_demo.py

benchmark:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/run_benchmark.py

serve:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/serve.py --host 127.0.0.1 --port 8080

validate: test demo benchmark

clean:
	@rm -rf .runtime __pycache__ src/sec_master/__pycache__ tests/__pycache__
