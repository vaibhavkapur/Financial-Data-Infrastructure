# Alternative Data Platform

This local prototype archives two synthetic feeds, a macro release series and a financial news bulletin. A consumer can reproduce what was available at a UTC cutoff, trace every exported revision to its original bytes, and see which license permits its use.

## Getting started

From this directory, use CPython 3.9 or newer with the standard `sqlite3` module. Setup requires no external packages, credentials or network connection.

```sh
python3 scripts/validate.py
python3 scripts/demo.py
```

The demo creates a temporary database and replays two dated arrivals. At the first cutoff, two macro observations are visible, including a reported missing value. At the second cutoff, the export history also contains a revised macro observation and one released article. Health reports a duplicate delivery and an ambiguous entity.

## Use a persistent archive

```sh
umask 077
mkdir -p .local
python3 -m altdata --db .local/platform.sqlite apply-registry config/registry.json
python3 -m altdata --db .local/platform.sqlite ingest macro
python3 -m altdata --db .local/platform.sqlite ingest news
python3 -m altdata --db .local/platform.sqlite health
python3 -m altdata --db .local/platform.sqlite export --source macro --source news --purpose demo
```

Use `--history` to include all eligible revisions and `--as-of 2025-02-03T10:00:00Z` to select a cutoff. The persistent CLI records the actual time of ingestion, so a cutoff before the first local receipt returns zero rows. The demo uses an injected clock to reproduce the historical example.

## Documentation

- [Architecture, adapter and data contracts](docs/architecture.md)
- [Operator runbook, access boundaries and rollout decisions](docs/runbook.md)
- [Milestones, acceptance evidence and measured baseline](reports/acceptance.md)
- [Original implementation plan](plan.md)
