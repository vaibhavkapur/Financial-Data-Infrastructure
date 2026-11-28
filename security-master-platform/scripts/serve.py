#!/usr/bin/env python3
"""Launch the Security Master Platform HTTP API and Quality Dashboard."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sec_master.config import load_config
from sec_master.http_api import run_server
from sec_master.service import SecurityMasterService


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve Security Master Platform UI & API")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config/default.json"))
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument(
        "--seed-demo",
        action="store_true",
        help="Seed synthetic fixtures before starting the server (leaves conflicts open for UI demo).",
    )
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    service = SecurityMasterService(cfg)
    if args.seed_demo:
        for fname in (
            "ticker_reuse_dual_listing.json",
            "late_correction_conflicts.json",
            "corporate_actions.json",
        ):
            data = json.loads(
                (PROJECT_ROOT / "data/fixtures" / fname).read_text(encoding="utf-8")
            )
            service.ingest_raw_records(data["events"])

    try:
        run_server(service, host=args.host or cfg.http.host, port=args.port or cfg.http.port)
    except KeyboardInterrupt:
        pass
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
