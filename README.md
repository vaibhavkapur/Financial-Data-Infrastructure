# Financial Data Infrastructure

Three independent Python projects use synthetic financial data to reconstruct what was known at a given time. The data lake tracks market data revisions, the feature store checks that training and serving produce the same values, and the alternative data platform records when a release became available for use.

## Getting started

Start with the Alternative Data Platform. Its offline demo uses Python 3.9 or later with SQLite and requires no additional packages.

```bash
git clone https://github.com/vaibhavkapur/Financial-Data-Infrastructure.git
cd Financial-Data-Infrastructure/alternative-data-platform
python3 scripts/demo.py
python3 scripts/validate.py
```

Compare the two cutoffs in the demo output to see when a revised macro observation and an embargoed news article become visible. The validator checks the replay against the [reference exports and health counters](alternative-data-platform/reports/demo.json). Run each project's setup, demo, and validation commands from its own directory.

## Projects

- [Financial Data Lake](financial-data-lake/) — Ingests ticks, prices, fundamentals, and corporate actions into versioned Parquet partitions. Researchers can query a historical cutoff or reproduce a snapshot from its checksums.
- [Financial Feature Store](financial-feature-store/) — Computes features from order books, bars, and transactions. It enforces freshness when serving values online and replays the same inputs to check agreement with offline training data.
- [Alternative Data Platform](alternative-data-platform/) — Archives macro releases and financial news with their revisions, embargoes, and licenses. Each exported record can be traced to the original source bytes.

Each project README links to its architecture, operator runbook, and validation evidence.
