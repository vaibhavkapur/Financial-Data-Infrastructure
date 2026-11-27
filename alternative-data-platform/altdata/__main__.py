import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from .domain import Problem, unpack
from .store import Store


def main():
    parser = argparse.ArgumentParser(description="Local alternative data platform")
    parser.add_argument("--db", required=True, help="operator's SQLite database")
    commands = parser.add_subparsers(dest="command", required=True)
    apply = commands.add_parser("apply-registry")
    apply.add_argument("file", type=Path)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("source")
    ingest.add_argument("--root", type=Path, default=Path("data/fixtures"))
    health = commands.add_parser("health")
    health.add_argument("source", nargs="?")
    export = commands.add_parser("export")
    export.add_argument("--source", dest="sources", action="append", required=True)
    export.add_argument("--purpose", required=True)
    export.add_argument("--as-of")
    export.add_argument("--history", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    db = None
    try:
        db = Store(args.db)
        if args.command == "apply-registry":
            result = {"registry_version": db.apply(unpack(args.file.read_bytes()))}
        elif args.command == "ingest":
            result = db.ingest(args.source, args.root)
        elif args.command == "health":
            sources = [args.source] if args.source else [
                row[0] for row in db.db.execute("SELECT source FROM progress ORDER BY source")]
            result = [db.status(source) for source in sources]
        else:
            result = db.export(args.sources, args.purpose, args.as_of, args.history)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except (OSError, sqlite3.Error, Problem) as exc:
        error = exc.code if isinstance(exc, Problem) else "storage_error"
        print(json.dumps({"error": error, "detail": str(exc)}), file=sys.stderr)
        return 1
    finally:
        if db:
            db.db.close()


if __name__ == "__main__":
    sys.exit(main())
