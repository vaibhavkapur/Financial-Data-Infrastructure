#!/usr/bin/env python3
"""Reproducible end-to-end demo script: oracle verification + local/distributed parity + registry export."""

from __future__ import annotations

import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.orchestrator import ExperimentOrchestrator, RunOptions, load_experiment_spec
from dqrp.storage.registry import SqliteExperimentRegistry


def main() -> int:
    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    registry_path = reports_dir / "experiment_registry.sqlite"
    if registry_path.exists():
        registry_path.unlink()

    orchestrator = ExperimentOrchestrator()

    # 1. Verify against independent hand-calculated oracle
    oracle_spec_path = PROJECT_ROOT / "config" / "oracle_experiment.json"
    oracle_spec = load_experiment_spec(oracle_spec_path)
    oracle_summary = orchestrator.run(
        oracle_spec,
        options=RunOptions(
            backend="local",
            max_workers=1,
            output_dir=reports_dir / "runs",
            registry_path=registry_path,
            run_id="demo-oracle-local",
        ),
        spec_file_path=oracle_spec_path,
    )

    expected_oracle = json.loads(
        (PROJECT_ROOT / "data" / "fixtures" / "oracle" / "expected_oracle.json").read_text(encoding="utf-8")
    )
    tol = float(expected_oracle["tolerance"])
    actual_metrics = oracle_summary.trials[0].metrics.to_dict()
    oracle_diffs = {}
    for key, exp_val in expected_oracle["expected_metrics"].items():
        act_val = actual_metrics[key]
        diff = abs(float(act_val) - float(exp_val))
        oracle_diffs[key] = {"expected": exp_val, "actual": act_val, "abs_diff": round(diff, 12), "within_tol": diff <= tol}
        assert diff <= tol, f"Oracle mismatch for {key}: expected {exp_val}, got {act_val} (diff={diff})"

    # 2. Run demo parameter sweep locally and on distributed multi-process workers
    demo_spec_path = PROJECT_ROOT / "config" / "demo_experiment.json"
    demo_spec = load_experiment_spec(demo_spec_path)

    local_summary = orchestrator.run(
        demo_spec,
        options=RunOptions(
            backend="local",
            max_workers=1,
            output_dir=reports_dir / "runs",
            registry_path=registry_path,
            run_id="demo-sweep-local",
        ),
        spec_file_path=demo_spec_path,
    )
    dist_summary = orchestrator.run(
        demo_spec,
        options=RunOptions(
            backend="distributed",
            max_workers=4,
            output_dir=reports_dir / "runs",
            registry_path=registry_path,
            run_id="demo-sweep-distributed-4w",
        ),
        spec_file_path=demo_spec_path,
    )

    assert local_summary.aggregate_sha256 == dist_summary.aggregate_sha256, (
        f"Local/distributed parity mismatch: {local_summary.aggregate_sha256} != {dist_summary.aggregate_sha256}"
    )
    assert local_summary.duplicate_completed_trials == 0
    assert dist_summary.duplicate_completed_trials == 0

    baseline_report = {
        "oracle_comparison": {
            "tolerance": tol,
            "all_within_tolerance": all(v["within_tol"] for v in oracle_diffs.values()),
            "metrics": oracle_diffs,
        },
        "local_distributed_parity": {
            "local_run_id": local_summary.run_id,
            "distributed_run_id": dist_summary.run_id,
            "local_aggregate_sha256": local_summary.aggregate_sha256,
            "distributed_aggregate_sha256": dist_summary.aggregate_sha256,
            "bit_exact_match": local_summary.aggregate_sha256 == dist_summary.aggregate_sha256,
            "local_best_trial_id": local_summary.best_trial_id,
            "distributed_best_trial_id": dist_summary.best_trial_id,
            "local_wall_seconds": local_summary.total_wall_seconds,
            "distributed_wall_seconds": dist_summary.total_wall_seconds,
            "local_duplicate_completed_trials": local_summary.duplicate_completed_trials,
            "distributed_duplicate_completed_trials": dist_summary.duplicate_completed_trials,
        },
        "environment": dist_summary.environment,
    }
    (reports_dir / "baseline_comparison.json").write_text(
        json.dumps(baseline_report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    registry = SqliteExperimentRegistry(registry_path)
    registry.export_json(reports_dir / "experiment_registry_export.json")

    print(json.dumps(baseline_report["local_distributed_parity"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
