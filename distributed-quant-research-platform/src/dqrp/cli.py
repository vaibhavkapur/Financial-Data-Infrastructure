"""Command-line interface for the Distributed Quant Research Platform."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import List, Optional

from dqrp.contracts import SUPPORTED_BACKENDS
from dqrp.errors import DqrpError
from dqrp.orchestrator import ExperimentOrchestrator, RunOptions, load_experiment_spec
from dqrp.storage.dataset import validate_and_load_dataset
from dqrp.storage.registry import SqliteExperimentRegistry


def _cmd_validate_manifest(args: argparse.Namespace) -> int:
    loaded = validate_and_load_dataset(args.manifest)
    out = {
        "status": "VALID",
        "dataset_id": loaded.manifest.dataset_id,
        "version": loaded.manifest.version,
        "manifest_sha256": loaded.manifest.manifest_sha256,
        "row_count": len(loaded.bars),
        "completeness_ratio": loaded.completeness_ratio,
        "freshness_lag_seconds": loaded.freshness_lag_seconds,
        "verified_partitions": len(loaded.verified_partition_hashes),
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _cmd_validate_spec(args: argparse.Namespace) -> int:
    spec = load_experiment_spec(args.spec)
    expanded = spec.expand_parameter_sets()
    out = {
        "status": "VALID",
        "experiment_id": spec.experiment_id,
        "spec_sha256": spec.spec_sha256,
        "strategy_name": spec.strategy_name,
        "expanded_trials": len(expanded),
        "budget": spec.budget.to_dict(),
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    spec = load_experiment_spec(args.spec)
    options = RunOptions(
        backend=args.backend,
        max_workers=args.workers,
        output_dir=Path(args.output_dir),
        registry_path=Path(args.registry) if args.registry else None,
        resume_from_artifacts=bool(args.resume),
        run_id=args.run_id,
    )
    orchestrator = ExperimentOrchestrator()
    summary = orchestrator.run(spec, options=options, spec_file_path=args.spec)
    compact = {
        "run_id": summary.run_id,
        "status": summary.status.value,
        "backend": summary.backend,
        "experiment_sha256": summary.experiment_sha256,
        "dataset_sha256": summary.dataset_sha256,
        "aggregate_sha256": summary.aggregate_sha256,
        "succeeded_trials": summary.succeeded_trials,
        "failed_trials": summary.failed_trials,
        "cancelled_trials": summary.cancelled_trials,
        "skipped_budget_trials": summary.skipped_budget_trials,
        "duplicate_completed_trials": summary.duplicate_completed_trials,
        "best_trial_id": summary.best_trial_id,
        "best_sharpe_ratio": summary.best_sharpe_ratio,
        "total_wall_seconds": summary.total_wall_seconds,
        "total_cpu_seconds": summary.total_cpu_seconds,
        "total_estimated_cost_usd": summary.total_estimated_cost_usd,
    }
    print(json.dumps(compact, indent=2, sort_keys=True))
    return 0


def _cmd_registry(args: argparse.Namespace) -> int:
    reg = SqliteExperimentRegistry(args.registry)
    if args.registry_action == "list":
        print(json.dumps(reg.list_runs(limit=args.limit), indent=2, sort_keys=True))
        return 0
    if args.registry_action == "show":
        item = reg.get_run(args.run_id)
        if item is None:
            print(json.dumps({"error": f"Run not found: {args.run_id}"}), file=sys.stderr)
            return 2
        print(json.dumps(item, indent=2, sort_keys=True))
        return 0
    if args.registry_action == "export":
        payload = reg.export_json(args.output)
        print(json.dumps({"exported_runs": payload["run_count"], "output": args.output}, indent=2))
        return 0
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dqrp",
        description="Distributed Quant Research Platform CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_vm = sub.add_parser("validate-manifest", help="Validate dataset manifest and partition SHA-256 hashes")
    p_vm.add_argument("manifest", help="Path to dataset manifest JSON")
    p_vm.set_defaults(func=_cmd_validate_manifest)

    p_vs = sub.add_parser("validate-spec", help="Validate experiment specification and expand trial count")
    p_vs.add_argument("spec", help="Path to experiment spec JSON")
    p_vs.set_defaults(func=_cmd_validate_spec)

    p_run = sub.add_parser("run", help="Execute a budgeted research experiment")
    p_run.add_argument("spec", help="Path to experiment spec JSON")
    p_run.add_argument("--backend", choices=list(SUPPORTED_BACKENDS), default="local")
    p_run.add_argument("--workers", type=int, default=None, help="Bounded worker concurrency")
    p_run.add_argument("--output-dir", default="reports/runs", help="Directory for artifacts and run summaries")
    p_run.add_argument("--registry", default="reports/experiment_registry.sqlite", help="SQLite registry path")
    p_run.add_argument("--resume", action="store_true", help="Reuse verified immutable artifacts from cache")
    p_run.add_argument("--run-id", default=None, help="Optional explicit run identifier")
    p_run.set_defaults(func=_cmd_run)

    p_reg = sub.add_parser("registry", help="Inspect or export the SQLite Experiment Registry")
    p_reg.add_argument("--registry", default="reports/experiment_registry.sqlite")
    reg_sub = p_reg.add_subparsers(dest="registry_action", required=True)
    p_reg_list = reg_sub.add_parser("list", help="List recorded runs")
    p_reg_list.add_argument("--limit", type=int, default=20)
    p_reg_show = reg_sub.add_parser("show", help="Show full run details")
    p_reg_show.add_argument("run_id")
    p_reg_exp = reg_sub.add_parser("export", help="Export full registry as JSON")
    p_reg_exp.add_argument("--output", required=True)
    p_reg.set_defaults(func=_cmd_registry)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except DqrpError as exc:
        print(json.dumps(exc.to_dict(), indent=2, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
