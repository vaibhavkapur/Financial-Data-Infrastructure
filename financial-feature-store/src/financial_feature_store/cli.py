"""Command-line interface for inspecting and running the Financial Feature Store."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from financial_feature_store.registry import FeatureRegistry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ffs",
        description="Financial Feature Store CLI — inspect versioned registries and manifests.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    reg_cmd = sub.add_parser("inspect-registry", help="Print summary of a saved feature registry JSON.")
    reg_cmd.add_argument("registry_path", type=Path, help="Path to feature_registry.json")

    args = parser.parse_args(argv)
    if args.command == "inspect-registry":
        reg = FeatureRegistry.load(args.registry_path)
        summary = {
            "snapshot_hash": reg.snapshot_hash(),
            "feature_count": len(reg.list_all()),
            "features": [
                {
                    "name": f.name,
                    "version": f.version,
                    "version_id": f.version_id,
                    "stage": f.stage.value,
                    "owner": f.owner,
                    "unit": f.unit,
                    "ttl_seconds": f.ttl_seconds,
                    "transform": f.transform.transform_name,
                    "source": f"{f.source.source_id}@{f.source.source_version}",
                }
                for f in reg.list_all()
            ],
        }
        print(json.dumps(summary, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
