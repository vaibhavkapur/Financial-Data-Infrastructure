"""Validation, micro-benchmarking, and acceptance evidence generator."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from financial_data_lake import AccessContext, FinancialDataLake

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from demo import run_demo  # noqa: E402


def run_microbenchmark(sample_size: int = 2000) -> dict[str, Any]:
    """Measure baseline synthetic tick ingestion and DuckDB as-of query latency."""
    with tempfile.TemporaryDirectory(prefix="fdl_bench_") as tmp:
        lake = FinancialDataLake(
            lake_root=Path(tmp) / "bench_lake",
            config_path=REPO_ROOT / "config" / "lake_config.json",
            entitlements_path=REPO_ROOT / "config" / "entitlements.json",
            schemas_path=REPO_ROOT / "config" / "schemas" / "registry.json",
        )
        eng_ctx = AccessContext(principal="bench", role="data_engineer", purpose="ingest")
        res_ctx = AccessContext(principal="bench", role="quant_research", purpose="research")

        base_ts = datetime(2024, 6, 3, 13, 30, 0, tzinfo=UTC)
        records = [
            {
                "tick_id": f"BENCH-{i:06d}",
                "instrument_id": "INST-AAPL" if i % 2 == 0 else "INST-MSFT",
                "symbol": "AAPL" if i % 2 == 0 else "MSFT",
                "event_ts": (base_ts + timedelta(milliseconds=i * 10)).isoformat(),
                "available_at": (base_ts + timedelta(milliseconds=i * 10 + 50)).isoformat(),
                "event_date": "2024-06-03",
                "price": 200.0 + (i % 50) * 0.01,
                "size": 100.0,
                "currency": "USD",
                "venue": "XNAS",
                "side": "BUY" if i % 2 == 0 else "SELL",
                "revision_number": 1,
                "correction_of": None,
                "source_system": "synthetic-bench",
            }
            for i in range(sample_size)
        ]

        t0 = time.perf_counter()
        ingest_res = lake.ingest.ingest_records(
            dataset_id="market_ticks",
            records=records,
            source_name="bench_ticks.jsonl",
            provenance="synthetic-benchmark",
            access=eng_ctx,
        )
        ingest_ms = (time.perf_counter() - t0) * 1000.0

        as_of_mid = base_ts + timedelta(milliseconds=(sample_size // 2) * 10 + 50)
        query_latencies_ms: list[float] = []
        rows_returned = 0
        for _ in range(5):
            qt0 = time.perf_counter()
            rows = lake.query_as_of("market_ticks", as_of_mid, res_ctx)
            query_latencies_ms.append((time.perf_counter() - qt0) * 1000.0)
            rows_returned = len(rows)

        return {
            "environment": {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "machine": platform.machine(),
            },
            "sample_size_ticks": sample_size,
            "ingested_rows": ingest_res.curated_rows,
            "ingest_wall_ms": round(ingest_ms, 2),
            "ingest_rows_per_sec": round(sample_size / (ingest_ms / 1000.0), 1),
            "as_of_query_runs": len(query_latencies_ms),
            "as_of_rows_returned": rows_returned,
            "as_of_query_min_ms": round(min(query_latencies_ms), 2),
            "as_of_query_median_ms": round(sorted(query_latencies_ms)[len(query_latencies_ms) // 2], 2),
            "as_of_query_max_ms": round(max(query_latencies_ms), 2),
            "note": "Local prototype baseline on synthetic data; not a claim of production SLA.",
        }


def execute_notebook_cells(nb_path: Path) -> dict[str, Any]:
    """Execute all code cells in the sample research notebook to verify it runs cleanly."""
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    ns: dict[str, Any] = {}
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    for idx, cell in enumerate(code_cells, start=1):
        code = "".join(cell["source"])
        exec(compile(code, f"{nb_path.name}#cell-{idx}", "exec"), ns)  # noqa: S102
    return {"notebook": nb_path.name, "code_cells_executed": len(code_cells), "passed": True}


def main() -> int:
    python_bin = sys.executable
    ruff_bin = Path(python_bin).parent / "ruff"

    ruff_proc = subprocess.run(
        [str(ruff_bin), "check", "src", "tests", "scripts"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    pytest_proc = subprocess.run(
        [python_bin, "-m", "pytest", "-v"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    demo_out = run_demo(REPO_ROOT)
    bench_out = run_microbenchmark(sample_size=2000)
    nb_out = execute_notebook_cells(REPO_ROOT / "notebooks" / "research_as_of_demo.ipynb")

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "ruff": {
            "returncode": ruff_proc.returncode,
            "stdout": ruff_proc.stdout.strip(),
            "stderr": ruff_proc.stderr.strip(),
        },
        "pytest": {
            "returncode": pytest_proc.returncode,
            "summary_lines": pytest_proc.stdout.strip().splitlines()[-15:],
        },
        "demo": demo_out,
        "notebook_verification": nb_out,
        "baseline_microbenchmark": bench_out,
    }

    reports_dir = REPO_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / "validation_report.json"
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote validation evidence to {out_path}")
    print(f"ruff rc={ruff_proc.returncode} | pytest rc={pytest_proc.returncode} | notebook={nb_out['passed']}")
    if ruff_proc.returncode != 0:
        print(ruff_proc.stdout)
        print(ruff_proc.stderr)
    if pytest_proc.returncode != 0:
        print(pytest_proc.stdout)
        print(pytest_proc.stderr)
    return 0 if (ruff_proc.returncode == 0 and pytest_proc.returncode == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
