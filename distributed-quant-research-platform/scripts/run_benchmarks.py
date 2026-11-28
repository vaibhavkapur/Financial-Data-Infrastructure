#!/usr/bin/env python3
"""Run scaling benchmarks (local 1w vs distributed 1w/2w/4w), verify parity, and emit cost estimates."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.orchestrator import ExperimentOrchestrator, RunOptions, load_experiment_spec
from dqrp.storage.registry import SqliteExperimentRegistry


def main() -> int:
    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    registry_path = reports_dir / "experiment_registry.sqlite"
    spec_path = PROJECT_ROOT / "config" / "benchmark_experiment.json"
    spec = load_experiment_spec(spec_path)

    orchestrator = ExperimentOrchestrator()
    runs_cfg = [
        ("local", 1, "bench-local-1w"),
        ("distributed", 1, "bench-dist-1w"),
        ("distributed", 2, "bench-dist-2w"),
        ("distributed", 4, "bench-dist-4w"),
    ]

    entries: List[Dict[str, Any]] = []
    baseline_wall: float = 0.0
    baseline_hash: str = ""
    env_meta: Dict[str, Any] = {}

    for backend, workers, run_id in runs_cfg:
        summary = orchestrator.run(
            spec,
            options=RunOptions(
                backend=backend,
                max_workers=workers,
                output_dir=reports_dir / "runs",
                registry_path=registry_path,
                run_id=run_id,
            ),
            spec_file_path=spec_path,
        )
        env_meta = summary.environment
        if backend == "local" and workers == 1:
            baseline_wall = summary.total_wall_seconds
            baseline_hash = summary.aggregate_sha256

        speedup = round(baseline_wall / max(1e-6, summary.total_wall_seconds), 4)
        efficiency = round(speedup / float(workers), 4)
        entries.append(
            {
                "run_id": summary.run_id,
                "backend": backend,
                "workers": workers,
                "status": summary.status.value,
                "total_trials": summary.total_trials,
                "succeeded_trials": summary.succeeded_trials,
                "duplicate_completed_trials": summary.duplicate_completed_trials,
                "aggregate_sha256": summary.aggregate_sha256,
                "parity_with_local_baseline": summary.aggregate_sha256 == baseline_hash,
                "wall_seconds": summary.total_wall_seconds,
                "cpu_seconds": summary.total_cpu_seconds,
                "peak_memory_mb": summary.peak_memory_mb,
                "speedup_vs_local_1w": speedup,
                "parallel_efficiency": efficiency,
                "estimated_cost_usd": summary.total_estimated_cost_usd,
            }
        )

    # Project synthetic cost estimates at larger research sweep scales using measured per-trial CPU & memory
    ref_4w = entries[-1]
    avg_cpu_per_trial = ref_4w["cpu_seconds"] / float(max(1, ref_4w["succeeded_trials"]))
    avg_cost_per_trial = ref_4w["estimated_cost_usd"] / float(max(1, ref_4w["succeeded_trials"]))
    cost_projections = []
    for scale in (24, 100, 1000, 10000):
        cost_projections.append(
            {
                "trial_count": scale,
                "projected_cpu_seconds": round(avg_cpu_per_trial * scale, 4),
                "projected_estimated_cost_usd": round(avg_cost_per_trial * scale, 8),
                "note": "Synthetic linear projection from measured benchmark per-trial averages; actual cloud billing varies.",
            }
        )

    report = {
        "measurement_type": "observed_local_hardware_benchmark_on_synthetic_fixture",
        "experiment_id": spec.experiment_id,
        "experiment_sha256": spec.spec_sha256,
        "dataset_sha256": spec.expected_dataset_sha256,
        "cost_model": spec.cost_model.to_dict(),
        "environment": env_meta,
        "runs": entries,
        "all_parity_matched": all(e["parity_with_local_baseline"] for e in entries),
        "cost_projections": cost_projections,
    }
    (reports_dir / "scaling_benchmark.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    # Refresh registry export so benchmark runs appear in exported experiment registry
    SqliteExperimentRegistry(registry_path).export_json(reports_dir / "experiment_registry_export.json")

    print(json.dumps({"runs": entries, "all_parity_matched": report["all_parity_matched"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
