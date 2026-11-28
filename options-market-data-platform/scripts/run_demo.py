#!/usr/bin/env python3
"""Reproducible end-to-end demo script for Options Market Data Platform.

Ingests reference, edge-case, and integration fixtures into a fresh SQLite DB,
derives implied-volatility features with full provenance, verifies deterministic
replay, and writes machine-readable output to reports/demo_output.json.
"""

from __future__ import annotations

import json
import platform as py_platform
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import StorageConfig, load_config
from omdp.service.platform import OptionsMarketDataPlatform


def main() -> int:
    cfg = load_config(ROOT / "config" / "default_config.json")
    with tempfile.TemporaryDirectory() as tmpdir:
        demo_db = Path(tmpdir) / "demo_omdp.sqlite3"
        demo_cfg = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=str(demo_db)))
        platform = OptionsMarketDataPlatform(config=demo_cfg)
        try:
            bundles = [
                ROOT / "data" / "fixtures" / "reference_chain.json",
                ROOT / "data" / "fixtures" / "invalid_and_edge_cases.json",
                ROOT / "data" / "fixtures" / "integration_scenarios.json",
            ]
            ingested = []
            replays = []
            for bundle_path in bundles:
                bundle = load_fixture_bundle(bundle_path)
                results = platform.ingest_bundle(bundle)
                for res in results:
                    ingested.append(
                        {
                            "bundle": bundle.bundle_name,
                            "snapshot_id": res.snapshot.snapshot_id,
                            "underlying_symbol": res.snapshot.underlying_symbol,
                            "snapshot_hash": res.snapshot.snapshot_hash,
                            "quality_report": res.quality_report.to_dict(),
                            "features": [f.to_dict() for f in res.features],
                        }
                    )
                    replay = platform.replay_snapshot(res.snapshot.snapshot_id)
                    replays.append(
                        {
                            "snapshot_id": replay.snapshot_id,
                            "snapshot_hash_match": replay.snapshot_hash_match,
                            "feature_ids_match": replay.feature_ids_match,
                            "is_identical": replay.is_identical,
                        }
                    )

            output = {
                "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "runtime": {
                    "python_version": py_platform.python_version(),
                    "system": py_platform.system(),
                    "machine": py_platform.machine(),
                },
                "config_version": cfg.config_version,
                "config_hash": cfg.config_hash(),
                "counters": platform.counters.to_dict(),
                "replays": replays,
                "snapshots": ingested,
            }

            out_path = ROOT / "reports" / "demo_output.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            all_replays_match = all(r["is_identical"] for r in replays)
            print(f"Demo complete: {len(ingested)} snapshots ingested, all_replays_identical={all_replays_match}")
            print(f"Output written to: {out_path}")
            return 0 if all_replays_match else 1
        finally:
            platform.close()


if __name__ == "__main__":
    raise SystemExit(main())
