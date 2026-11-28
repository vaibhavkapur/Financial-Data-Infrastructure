# ADR-001: Python 3.9+ Zero-Dependency Core, SQLite Default Storage, and Reference PostgreSQL DDL

- **Status:** Accepted
- **Date:** 2026-11-28
- **Context:** `plan.md` suggests *Go or Python, PostgreSQL, FastAPI* while explicitly noting that technology choices are proposed engineering targets and must be adapted with documented rationale so another developer can reproduce the demo cleanly from checkout. In the target local/CI environment, Python 3.9.6 and `sqlite3` are available out-of-the-box with no external PostgreSQL daemon or network package mirror required.

## Decision

1. **Language & Core Runtime:** Implement the Security Master Platform in **Python 3.9+** using only the standard library (`dataclasses`, `decimal`, `sqlite3`, `wsgiref`, `unittest`, `json`, `hashlib`, `logging`) for zero-dependency offline reproducibility.
2. **Storage:**
   - Default durable persistence uses **SQLite** (`SQLiteBitemporalStore` in WAL mode) storing an append-only `event_journal` plus materialized bitemporal tables (`instrument_versions`, `listing_versions`, `identifier_versions`, `corporate_action_links`, `conflict_tickets`).
   - Provide a reference **PostgreSQL 16+ DDL** (`config/postgres_schema.sql`) utilizing `tstzrange` and `btree_gist` exclusion constraints for future production scaling. Requesting `backend: "postgres"` at runtime raises an explicit `ConfigError` rather than silently falling back, and PostgreSQL live tests are explicitly marked **not run** in `reports/acceptance.md`.
3. **HTTP Service:**
   - Default REST API and HTML Quality Dashboard run on a zero-dependency **WSGI** application (`src/sec_master/http_api.py`).
   - Provide an optional **FastAPI** wrapper (`src/sec_master/fastapi_app.py`) and `[fastapi]` extra in `pyproject.toml` mounting the exact same `SecurityMasterService` contract.

## Consequences

- **Positive:** Clean checkout runs `make test`, `make demo`, `make benchmark`, and `make serve` immediately without Docker, network downloads, or external database provisioning.
- **Negative / Limitations:** Single-node SQLite is not a multi-writer distributed database; high-concurrency multi-service deployments should implement a `psycopg` adapter against `config/postgres_schema.sql`.
