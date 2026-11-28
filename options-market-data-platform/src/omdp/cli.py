"""Command-line interface for Options Market Data Platform (OMDP)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import ConfigurationError, load_config
from omdp.domain.contracts import DomainError, parse_utc_timestamp
from omdp.service.http_api import create_http_server
from omdp.service.platform import OptionsMarketDataPlatform


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="omdp",
        description="Options Market Data Platform — ingest, derive IV features, query, and replay option chains.",
    )
    parser.add_argument(
        "--config",
        default="config/default_config.json",
        help="Path to platform JSON configuration file (default: config/default_config.json)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Optional override for SQLite database path",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Ingest a JSON option chain bundle and derive IV features")
    p_ingest.add_argument("--input", required=True, help="Path to JSON bundle file")

    p_query = sub.add_parser("query", help="Query stored historical chain snapshots")
    p_query.add_argument("--underlying", default=None, help="Filter by underlying symbol")
    p_query.add_argument("--start", default=None, help="Start UTC timestamp (ISO-8601)")
    p_query.add_argument("--end", default=None, help="End UTC timestamp (ISO-8601)")
    p_query.add_argument("--snapshot-id", default=None, help="Fetch a single snapshot with features")

    p_quality = sub.add_parser("quality", help="Show data-quality report for a snapshot")
    p_quality.add_argument("--snapshot-id", required=True, help="Target snapshot ID")

    p_replay = sub.add_parser("replay", help="Replay a snapshot and verify deterministic hash equality")
    p_replay.add_argument("--snapshot-id", required=True, help="Target snapshot ID")

    p_serve = sub.add_parser("serve", help="Start HTTP API server for historical chains, quality, and replay")
    p_serve.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p_serve.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except ConfigurationError as exc:
        print(json.dumps({"error": "CONFIGURATION_ERROR", "detail": str(exc)}), file=sys.stderr)
        return 2

    if args.db is not None:
        from dataclasses import replace
        from omdp.config import StorageConfig

        config = replace(config, storage=StorageConfig(backend="sqlite", sqlite_path=args.db))

    platform = OptionsMarketDataPlatform(config=config)
    try:
        if args.command == "ingest":
            bundle = load_fixture_bundle(Path(args.input))
            results = platform.ingest_bundle(bundle)
            summary = {
                "bundle_name": bundle.bundle_name,
                "snapshots_ingested": len(results),
                "snapshots": [
                    {
                        "snapshot_id": r.snapshot.snapshot_id,
                        "snapshot_hash": r.snapshot.snapshot_hash,
                        "quote_count": r.snapshot.quote_count,
                        "valid_quotes": r.snapshot.valid_quote_count,
                        "flagged_quotes": r.snapshot.flagged_quote_count,
                        "rejected_quotes": r.snapshot.rejected_quote_count,
                        "computed_ivs": r.quality_report.computed_iv_count,
                    }
                    for r in results
                ],
            }
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0

        if args.command == "query":
            if args.snapshot_id:
                snap = platform.get_chain_snapshot(args.snapshot_id)
                if snap is None:
                    print(json.dumps({"error": "NOT_FOUND", "snapshot_id": args.snapshot_id}), file=sys.stderr)
                    return 1
                feats = platform.get_derived_features(args.snapshot_id)
                print(
                    json.dumps(
                        {"snapshot": snap.to_dict(), "features": [f.to_dict() for f in feats]},
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            start_dt = parse_utc_timestamp(args.start) if args.start else None
            end_dt = parse_utc_timestamp(args.end) if args.end else None
            snaps = platform.query_historical_chains(
                underlying_symbol=args.underlying,
                start_time=start_dt,
                end_time=end_dt,
            )
            print(json.dumps({"count": len(snaps), "snapshots": [s.to_dict() for s in snaps]}, indent=2, sort_keys=True))
            return 0

        if args.command == "quality":
            report = platform.get_quality_report(args.snapshot_id)
            if report is None:
                print(json.dumps({"error": "NOT_FOUND", "snapshot_id": args.snapshot_id}), file=sys.stderr)
                return 1
            print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
            return 0

        if args.command == "replay":
            replay = platform.replay_snapshot(args.snapshot_id)
            print(json.dumps(replay.to_dict(), indent=2, sort_keys=True))
            return 0 if replay.is_identical else 3

        if args.command == "serve":
            server = create_http_server(platform, host=args.host, port=args.port)
            print(f"OMDP HTTP server listening on http://{args.host}:{args.port}")
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
    except (DomainError, FileNotFoundError, KeyError) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}), file=sys.stderr)
        return 1
    finally:
        platform.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
