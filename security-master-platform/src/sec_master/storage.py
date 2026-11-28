"""Durable append-only event journal and bitemporal persistence layer.

Provides:
- ``InMemoryEventStore`` for fast pure-memory runs and unit tests
- ``SQLiteBitemporalStore`` for ACID durable persistence (event journal + materialized
  bitemporal snapshot tables) supporting clean restarts and deterministic full replay.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List, Protocol

from .domain import BitemporalStateEngine
from .models import EventEnvelope, format_ts


class EventStore(Protocol):
    def append_event(self, envelope: EventEnvelope) -> None: ...

    def load_events(self) -> List[EventEnvelope]: ...

    def sync_materialized_state(self, engine: BitemporalStateEngine) -> None: ...

    def close(self) -> None: ...


class InMemoryEventStore:
    """Volatile event journal for isolated testing."""

    def __init__(self) -> None:
        self._events: List[EventEnvelope] = []

    def append_event(self, envelope: EventEnvelope) -> None:
        self._events.append(envelope)

    def load_events(self) -> List[EventEnvelope]:
        return list(self._events)

    def sync_materialized_state(self, engine: BitemporalStateEngine) -> None:
        # Engine already holds state in memory.
        return None

    def close(self) -> None:
        return None


_SQLITE_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS event_journal (
    seq_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL UNIQUE,
    event_type          TEXT NOT NULL,
    schema_version      TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_event_id     TEXT NOT NULL,
    valid_from          TEXT NOT NULL,
    valid_to            TEXT,
    observed_at         TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    payload_sha256      TEXT NOT NULL,
    supersedes_event_id TEXT,
    UNIQUE (source_system, source_event_id)
);

CREATE TABLE IF NOT EXISTS instrument_versions (
    version_id            TEXT PRIMARY KEY,
    instrument_id         TEXT NOT NULL,
    name                  TEXT NOT NULL,
    asset_class           TEXT NOT NULL,
    issuer_name           TEXT NOT NULL,
    country_of_risk       TEXT NOT NULL,
    status                TEXT NOT NULL,
    valid_from            TEXT NOT NULL,
    valid_to              TEXT,
    observed_at           TEXT NOT NULL,
    superseded_at         TEXT,
    lineage_json          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS listing_versions (
    version_id            TEXT PRIMARY KEY,
    listing_id            TEXT NOT NULL,
    instrument_id         TEXT NOT NULL,
    mic                   TEXT NOT NULL,
    ticker                TEXT NOT NULL,
    currency              TEXT NOT NULL,
    is_primary            INTEGER NOT NULL,
    status                TEXT NOT NULL,
    valid_from            TEXT NOT NULL,
    valid_to              TEXT,
    observed_at           TEXT NOT NULL,
    superseded_at         TEXT,
    lineage_json          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identifier_versions (
    version_id            TEXT PRIMARY KEY,
    instrument_id         TEXT NOT NULL,
    listing_id            TEXT,
    namespace             TEXT NOT NULL,
    identifier_value      TEXT NOT NULL,
    mic                   TEXT,
    valid_from            TEXT NOT NULL,
    valid_to              TEXT,
    observed_at           TEXT NOT NULL,
    superseded_at         TEXT,
    lineage_json          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS corporate_action_links (
    link_id               TEXT PRIMARY KEY,
    action_type           TEXT NOT NULL,
    from_instrument_id    TEXT NOT NULL,
    to_instrument_id      TEXT NOT NULL,
    from_listing_id       TEXT,
    to_listing_id         TEXT,
    ratio                 TEXT NOT NULL,
    effective_at          TEXT NOT NULL,
    observed_at           TEXT NOT NULL,
    superseded_at         TEXT,
    lineage_json          TEXT NOT NULL,
    metadata_json         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conflict_tickets (
    conflict_id           TEXT PRIMARY KEY,
    conflict_type         TEXT NOT NULL,
    entity_key            TEXT NOT NULL,
    status                TEXT NOT NULL,
    created_at            TEXT NOT NULL,
    resolved_at           TEXT,
    existing_claim_json   TEXT NOT NULL,
    incoming_claim_json   TEXT NOT NULL,
    resolution_json       TEXT
);

CREATE INDEX IF NOT EXISTS idx_ident_lookup
    ON identifier_versions (namespace, identifier_value, valid_from, observed_at);
CREATE INDEX IF NOT EXISTS idx_listing_inst
    ON listing_versions (instrument_id, mic, ticker);
"""


class SQLiteBitemporalStore:
    """ACID SQLite persistence backing the append-only event log and materialized bitemporal tables."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SQLITE_DDL)
        self._conn.commit()

    def append_event(self, envelope: EventEnvelope) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO event_journal (
                    event_id, event_type, schema_version, source_system, source_event_id,
                    valid_from, valid_to, observed_at, payload_json, payload_sha256,
                    supersedes_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.event_id,
                    envelope.event_type.value,
                    envelope.schema_version,
                    envelope.source_system,
                    envelope.source_event_id,
                    format_ts(envelope.valid_from),
                    format_ts(envelope.valid_to),
                    format_ts(envelope.observed_at),
                    json.dumps(envelope.payload, sort_keys=True),
                    envelope.sha256,
                    envelope.supersedes_event_id,
                ),
            )

    def load_events(self) -> List[EventEnvelope]:
        rows = self._conn.execute(
            """
            SELECT event_id, event_type, schema_version, source_system, source_event_id,
                   valid_from, valid_to, observed_at, payload_json, supersedes_event_id
            FROM event_journal
            ORDER BY seq_id ASC
            """
        ).fetchall()
        events: List[EventEnvelope] = []
        for row in rows:
            events.append(
                EventEnvelope.from_dict(
                    {
                        "event_id": row["event_id"],
                        "event_type": row["event_type"],
                        "schema_version": row["schema_version"],
                        "source_system": row["source_system"],
                        "source_event_id": row["source_event_id"],
                        "valid_from": row["valid_from"],
                        "valid_to": row["valid_to"],
                        "observed_at": row["observed_at"],
                        "payload": json.loads(row["payload_json"]),
                        "supersedes_event_id": row["supersedes_event_id"],
                    }
                )
            )
        return events

    def sync_materialized_state(self, engine: BitemporalStateEngine) -> None:
        """Atomically persist materialized bitemporal projections for direct SQL inspection."""
        with self._conn:
            self._conn.execute("DELETE FROM instrument_versions")
            self._conn.executemany(
                """
                INSERT INTO instrument_versions (
                    version_id, instrument_id, name, asset_class, issuer_name,
                    country_of_risk, status, valid_from, valid_to, observed_at,
                    superseded_at, lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.version_id,
                        r.instrument_id,
                        r.name,
                        r.asset_class.value,
                        r.issuer_name,
                        r.country_of_risk,
                        r.status,
                        format_ts(r.valid_from),
                        format_ts(r.valid_to),
                        format_ts(r.observed_at),
                        format_ts(r.superseded_at),
                        json.dumps(r.lineage.to_dict(), sort_keys=True),
                    )
                    for r in engine.instruments
                ],
            )

            self._conn.execute("DELETE FROM listing_versions")
            self._conn.executemany(
                """
                INSERT INTO listing_versions (
                    version_id, listing_id, instrument_id, mic, ticker, currency,
                    is_primary, status, valid_from, valid_to, observed_at,
                    superseded_at, lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.version_id,
                        r.listing_id,
                        r.instrument_id,
                        r.mic,
                        r.ticker,
                        r.currency,
                        1 if r.is_primary else 0,
                        r.status,
                        format_ts(r.valid_from),
                        format_ts(r.valid_to),
                        format_ts(r.observed_at),
                        format_ts(r.superseded_at),
                        json.dumps(r.lineage.to_dict(), sort_keys=True),
                    )
                    for r in engine.listings
                ],
            )

            self._conn.execute("DELETE FROM identifier_versions")
            self._conn.executemany(
                """
                INSERT INTO identifier_versions (
                    version_id, instrument_id, listing_id, namespace, identifier_value,
                    mic, valid_from, valid_to, observed_at, superseded_at, lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.version_id,
                        r.instrument_id,
                        r.listing_id,
                        r.namespace.value,
                        r.identifier_value,
                        r.mic,
                        format_ts(r.valid_from),
                        format_ts(r.valid_to),
                        format_ts(r.observed_at),
                        format_ts(r.superseded_at),
                        json.dumps(r.lineage.to_dict(), sort_keys=True),
                    )
                    for r in engine.identifiers
                ],
            )

            self._conn.execute("DELETE FROM corporate_action_links")
            self._conn.executemany(
                """
                INSERT INTO corporate_action_links (
                    link_id, action_type, from_instrument_id, to_instrument_id,
                    from_listing_id, to_listing_id, ratio, effective_at,
                    observed_at, superseded_at, lineage_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        a.link_id,
                        a.action_type.value,
                        a.from_instrument_id,
                        a.to_instrument_id,
                        a.from_listing_id,
                        a.to_listing_id,
                        str(a.ratio),
                        format_ts(a.effective_at),
                        format_ts(a.observed_at),
                        format_ts(a.superseded_at),
                        json.dumps(a.lineage.to_dict(), sort_keys=True),
                        json.dumps(a.metadata, sort_keys=True),
                    )
                    for a in engine.corporate_actions
                ],
            )

            self._conn.execute("DELETE FROM conflict_tickets")
            self._conn.executemany(
                """
                INSERT INTO conflict_tickets (
                    conflict_id, conflict_type, entity_key, status, created_at,
                    resolved_at, existing_claim_json, incoming_claim_json, resolution_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        t.conflict_id,
                        t.conflict_type,
                        t.entity_key,
                        t.status.value,
                        format_ts(t.created_at),
                        format_ts(t.resolved_at),
                        json.dumps(t.existing_claim, sort_keys=True),
                        json.dumps(t.incoming_claim, sort_keys=True),
                        json.dumps(t.resolution, sort_keys=True) if t.resolution else None,
                    )
                    for t in engine.conflicts.values()
                ],
            )

    def table_counts(self) -> dict:
        tables = (
            "event_journal",
            "instrument_versions",
            "listing_versions",
            "identifier_versions",
            "corporate_action_links",
            "conflict_tickets",
        )
        counts = {}
        for tbl in tables:
            row = self._conn.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
            counts[tbl] = int(row["cnt"])
        return counts

    def close(self) -> None:
        self._conn.close()
