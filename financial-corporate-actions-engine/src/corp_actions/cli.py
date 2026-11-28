"""Command-line interface for running PIT corporate action queries and replays."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from corp_actions.adapters.json_adapter import load_fixture_bundle
from corp_actions.config import EngineConfig
from corp_actions.engine import CorporateActionsEngine
from corp_actions.errors import CorporateActionsError


def _parse_iso_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corp-actions",
        description="Financial Corporate Actions Engine — PIT adjustment & replay CLI",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/engine_config.json"),
        help="Path to engine_config.json",
    )
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=Path("data/fixtures"),
        help="Directory containing raw_prices.json, positions.json, fx_rates.json, corporate_actions.json",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    q = sub.add_parser("query", help="Run a PIT as-of query for a security")
    q.add_argument("--security-id", required=True)
    q.add_argument("--as-of", required=True, help="ISO-8601 UTC timestamp (e.g. 2025-01-15T20:00:00Z)")
    q.add_argument("--start-date", default=None)
    q.add_argument("--end-date", default=None)
    q.add_argument("--account-id", default="ACCT-SIM-001")

    r = sub.add_parser("replay", help="Replay and diff two PIT as-of timestamps")
    r.add_argument("--security-id", required=True)
    r.add_argument("--from-as-of", required=True)
    r.add_argument("--to-as-of", required=True)
    r.add_argument("--start-date", default=None)
    r.add_argument("--end-date", default=None)
    r.add_argument("--account-id", default="ACCT-SIM-001")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = EngineConfig.from_file(args.config)
        engine = CorporateActionsEngine(config=config)
        prices, positions, fx_rates, actions = load_fixture_bundle(args.fixtures)
        engine.ingest_prices(prices)
        engine.ingest_positions(positions)
        engine.ingest_fx_rates(fx_rates)
        engine.ingest_actions(actions)

        start_d = date.fromisoformat(args.start_date) if args.start_date else None
        end_d = date.fromisoformat(args.end_date) if args.end_date else None

        if args.command == "query":
            result = engine.query_as_of(
                args.security_id,
                as_of_time=_parse_iso_dt(args.as_of),
                start_date=start_d,
                end_date=end_d,
                account_id=args.account_id,
            )
            print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        elif args.command == "replay":
            diff = engine.replay_correction(
                args.security_id,
                from_as_of=_parse_iso_dt(args.from_as_of),
                to_as_of=_parse_iso_dt(args.to_as_of),
                start_date=start_d,
                end_date=end_d,
                account_id=args.account_id,
            )
            print(json.dumps(diff.to_dict(), indent=2, sort_keys=True))
        engine.close()
        return 0
    except CorporateActionsError as exc:
        print(json.dumps(exc.to_dict(), indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
