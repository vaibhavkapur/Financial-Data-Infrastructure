# Trading Data Infrastructure

Eight independent Python and Go projects use synthetic financial data to store, normalize, version, and replay market, reference, and research data without look-ahead bias. Together they cover a bitemporal data lake, causal feature store, alternative data archive, sharded tick database, security master, corporate actions engine, options market data platform, and distributed quantitative research runner.

## Getting started

Start with the Alternative Data Platform. Its offline demo uses Python 3.9 or later with SQLite and requires no additional packages.

```bash
git clone https://github.com/vaibhavkapur/Trading-Data-Infrastructure.git
cd Trading-Data-Infrastructure/alternative-data-platform
python3 scripts/demo.py
python3 scripts/validate.py
```

Compare the two cutoffs in the demo output to see when a revised macro observation and an embargoed news article become visible. The validator checks the replay against the [reference exports and health counters](alternative-data-platform/reports/demo.json). Each project has its own dependencies, demo, and validation commands. Run them from that project's directory.

## Projects

- [Financial Data Lake](financial-data-lake/) — Ingests ticks, prices, fundamentals, and corporate actions into versioned Parquet partitions. Researchers can query a historical cutoff or reproduce a snapshot from its checksums.
- [Financial Feature Store](financial-feature-store/) — Computes features from order books, bars, and transactions. It enforces freshness when serving values online and replays the same inputs to check agreement with offline training data.
- [Alternative Data Platform](alternative-data-platform/) — Archives macro releases and financial news with their revisions, embargoes, and licenses. Each exported record can be traced to the original source bytes.
- [Distributed Tick Data Database](distributed-tick-data-database/) — Stores high-volume synthetic market ticks in a Go multi-shard columnar engine with CRC32 WAL crash recovery, sequence deduplication, gap tracking, bounded range queries, and deterministic Parquet replay.
- [Security Master Platform](security-master-platform/) — Maintains bitemporal instrument identities, exchange listings, and corporate-action identifier lineage with conflict quarantine, reviewer resolution workflows, and deterministic SQLite journal replay.
- [Financial Corporate Actions Engine](financial-corporate-actions-engine/) — Applies splits, reverse splits, regular and special dividends, FX-denominated dividends with withholding tax, and symbol changes point-in-time using exact `Decimal` arithmetic with late-correction replay diffs and total-return reconciliation.
- [Options Market Data Platform](options-market-data-platform/) — Normalizes versioned option chains, validates quotes, derives Black-Scholes implied volatility and Greeks with full SHA-256 provenance, and verifies deterministic historical snapshot replay.
- [Distributed Quant Research Platform](distributed-quant-research-platform/) — Runs budgeted parallel backtest sweeps over SHA-256-sealed dataset manifests with multi-process worker isolation, atomic artifact commits, resume recovery, and bit-exact local-versus-distributed parity.

Each project README links to its architecture, data contracts, operator runbook, and validation evidence.
