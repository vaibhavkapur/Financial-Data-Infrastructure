"""Command-line interface for Security Master Platform."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from .config import load_config
from .errors import SecurityMasterError
from .http_api import run_server
from .models import ConflictStatus, IdentifierNamespace, ReviewAction
from .service import SecurityMasterService
from .validation import parse_required_ts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sec-master",
        description="Bitemporal Security Master Platform CLI",
    )
    parser.add_argument(
        "--config",
        default="config/default.json",
        help="Path to JSON configuration file (default: config/default.json)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Ingest a JSON fixture file of events")
    p_ingest.add_argument("fixture_path", help="Path to JSON fixture file")

    p_lookup = sub.add_parser("lookup", help="Run an as-of bitemporal identifier lookup")
    p_lookup.add_argument("--namespace", required=True)
    p_lookup.add_argument("--value", required=True)
    p_lookup.add_argument("--as-of-valid", required=True)
    p_lookup.add_argument("--as-of-observed", required=True)
    p_lookup.add_argument("--mic", default=None)

    p_conflicts = sub.add_parser("conflicts", help="List quarantined or resolved conflicts")
    p_conflicts.add_argument("--status", default=None, choices=["OPEN", "RESOLVED"])

    p_resolve = sub.add_parser("resolve", help="Resolve an open conflict ticket")
    p_resolve.add_argument("--conflict-id", required=True)
    p_resolve.add_argument("--reviewer-id", required=True)
    p_resolve.add_argument(
        "--action",
        required=True,
        choices=["ACCEPT_INCOMING", "KEEP_EXISTING", "CUSTOM_OVERRIDE"],
    )
    p_resolve.add_argument("--rationale", required=True)
    p_resolve.add_argument("--observed-at", default=None)

    p_quality = sub.add_parser("quality", help="Print quality, completeness, and freshness metrics")
    p_quality.add_argument("--as-of-observed", required=True)

    sub.add_parser("replay", help="Deterministically replay state from durable event journal")

    p_serve = sub.add_parser("serve", help="Run HTTP REST API + HTML Quality Dashboard")
    p_serve.add_argument("--host", default=None)
    p_serve.add_argument("--port", type=int, default=None)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(Path(args.config))
        service = SecurityMasterService(config)
        try:
            if args.command == "ingest":
                raw = json.loads(Path(args.fixture_path).read_text(encoding="utf-8"))
                records = raw["events"] if isinstance(raw, dict) and "events" in raw else raw
                outcomes = service.ingest_raw_records(records)
                print(json.dumps({"outcomes": [o.to_dict() for o in outcomes]}, indent=2))
            elif args.command == "lookup":
                res = service.lookup_as_of(
                    namespace=IdentifierNamespace(args.namespace),
                    value=args.value,
                    as_of_valid=parse_required_ts(args.as_of_valid, "as_of_valid"),
                    as_of_observed=parse_required_ts(args.as_of_observed, "as_of_observed"),
                    mic=args.mic,
                )
                print(json.dumps(res.to_dict(), indent=2))
            elif args.command == "conflicts":
                st = ConflictStatus(args.status) if args.status else None
                tickets = service.list_conflicts(status=st)
                print(json.dumps({"conflicts": [t.to_dict() for t in tickets]}, indent=2))
            elif args.command == "resolve":
                obs = (
                    parse_required_ts(args.observed_at, "observed_at")
                    if args.observed_at
                    else None
                )
                outcome = service.resolve_conflict(
                    conflict_id=args.conflict_id,
                    reviewer_id=args.reviewer_id,
                    action=ReviewAction(args.action),
                    rationale=args.rationale,
                    observed_at=obs,
                )
                print(json.dumps(outcome.to_dict(), indent=2))
            elif args.command == "quality":
                obs = parse_required_ts(args.as_of_observed, "as_of_observed")
                print(
                    json.dumps(
                        {
                            "quality": service.quality_metrics(obs).to_dict(),
                            "diagnostics": service.diagnostics(),
                        },
                        indent=2,
                    )
                )
            elif args.command == "replay":
                print(json.dumps(service.replay_from_journal(), indent=2))
            elif args.command == "serve":
                host = args.host or config.http.host
                port = args.port or config.http.port
                run_server(service, host=host, port=port)
        finally:
            service.close()
        return 0
    except SecurityMasterError as exc:
        print(json.dumps(exc.to_dict(), indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
