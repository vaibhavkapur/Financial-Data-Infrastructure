"""Command-line interface for operating the Financial Data Lake prototype."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from financial_data_lake.lake import FinancialDataLake
from financial_data_lake.models import AccessContext


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_lake(args: argparse.Namespace) -> FinancialDataLake:
    repo = _default_repo_root()
    return FinancialDataLake(
        lake_root=args.lake_root,
        config_path=args.config or (repo / "config" / "lake_config.json"),
        entitlements_path=args.entitlements or (repo / "config" / "entitlements.json"),
        schemas_path=args.schemas or (repo / "config" / "schemas" / "registry.json"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fdl", description="Financial Data Lake CLI")
    parser.add_argument("--lake-root", type=Path, default=Path(".lake_data"))
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--entitlements", type=Path, default=None)
    parser.add_argument("--schemas", type=Path, default=None)

    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest-fixtures", help="Verify and ingest all synthetic fixtures.")
    p_ing.add_argument("--fixtures-dir", type=Path, default=_default_repo_root() / "data" / "fixtures")
    p_ing.add_argument("--principal", default="cli-operator")
    p_ing.add_argument("--role", default="data_engineer")

    p_q = sub.add_parser("query-as-of", help="Run a point-in-time query.")
    p_q.add_argument("--dataset", required=True)
    p_q.add_argument("--as-of", required=True, help="ISO-8601 UTC timestamp, e.g. 2024-06-05T23:59:59Z")
    p_q.add_argument("--principal", default="cli-researcher")
    p_q.add_argument("--role", default="quant_research")
    p_q.add_argument("--purpose", default="research")

    sub.add_parser("verify", help="Verify SHA-256 integrity of all active partitions.")
    sub.add_parser("export-catalog", help="Export full catalog manifest JSON.")

    args = parser.parse_args(argv)
    lake = _build_lake(args)

    if args.command == "ingest-fixtures":
        ctx = AccessContext(principal=args.principal, role=args.role, purpose="ingest")
        results = lake.ingest_all_fixtures(args.fixtures_dir, ctx)
        summary = [
            {
                "dataset_id": r.dataset_id,
                "source_name": r.source_name,
                "skipped_idempotent": r.skipped_idempotent,
                "raw_rows": r.raw_rows,
                "curated_rows": r.curated_rows,
                "deduplicated_rows": r.deduplicated_rows,
            }
            for r in results
        ]
        print(json.dumps(summary, indent=2))
        return 0

    if args.command == "query-as-of":
        as_of_ts = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")).astimezone(UTC)
        ctx = AccessContext(principal=args.principal, role=args.role, purpose=args.purpose)
        rows = lake.query_as_of(args.dataset, as_of_ts, ctx)
        print(json.dumps(rows, indent=2, default=str))
        return 0

    if args.command == "verify":
        rep = lake.verify_integrity()
        print(
            json.dumps(
                {
                    "checked_at": rep.checked_at.isoformat(),
                    "total_active_partitions": rep.total_active_partitions,
                    "healthy_partitions": rep.healthy_partitions,
                    "corrupt_partition_ids": rep.corrupt_partition_ids,
                },
                indent=2,
            )
        )
        return 0 if not rep.corrupt_partition_ids else 1

    if args.command == "export-catalog":
        print(json.dumps(lake.catalog.export_catalog_manifest(), indent=2, default=str))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
