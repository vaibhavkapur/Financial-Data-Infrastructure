"""Pluggable online serving backends: InMemory, SQLite (durable), and Redis adapter."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from financial_feature_store.contracts import FeatureRecord
from financial_feature_store.errors import AdapterError


@runtime_checkable
class OnlineBackend(Protocol):
    """Persistence contract for low-latency online feature serving."""

    def put_batch(self, records: list[FeatureRecord]) -> int:
        ...

    def get(self, entity_id: str, feature_name: str, feature_version: str) -> FeatureRecord | None:
        ...

    def delete_feature(self, feature_name: str, feature_version: str) -> int:
        ...

    def health(self) -> dict[str, Any]:
        ...


class InMemoryOnlineBackend:
    """Fast local in-memory KV backend enforcing monotonic event/availability ordering."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str, str], FeatureRecord] = {}

    def put_batch(self, records: list[FeatureRecord]) -> int:
        written = 0
        for rec in records:
            key = (rec.entity_id, rec.feature_name, rec.feature_version)
            existing = self._store.get(key)
            if existing is not None:
                # Monotonic freshness guard: never overwrite a newer event/availability with an older one
                if (rec.event_time, rec.availability_time, rec.materialized_at) < (
                    existing.event_time,
                    existing.availability_time,
                    existing.materialized_at,
                ):
                    continue
            self._store[key] = rec
            written += 1
        return written

    def get(self, entity_id: str, feature_name: str, feature_version: str) -> FeatureRecord | None:
        return self._store.get((entity_id, feature_name, feature_version))

    def delete_feature(self, feature_name: str, feature_version: str) -> int:
        to_delete = [k for k in self._store if k[1] == feature_name and k[2] == feature_version]
        for k in to_delete:
            del self._store[k]
        return len(to_delete)

    def health(self) -> dict[str, Any]:
        return {"backend": "in_memory", "status": "ok", "entries": len(self._store)}


class SQLiteOnlineBackend:
    """Durable ACID local SQLite backend simulating relational/KV online feature storage."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS online_features (
                    entity_id TEXT NOT NULL,
                    feature_name TEXT NOT NULL,
                    feature_version TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    availability_time TEXT NOT NULL,
                    materialized_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (entity_id, feature_name, feature_version)
                )
                """
            )
            conn.commit()

    def put_batch(self, records: list[FeatureRecord]) -> int:
        if not records:
            return 0
        written = 0
        with self._connect() as conn:
            for rec in records:
                row = conn.execute(
                    """
                    SELECT event_time, availability_time, materialized_at
                    FROM online_features
                    WHERE entity_id = ? AND feature_name = ? AND feature_version = ?
                    """,
                    (rec.entity_id, rec.feature_name, rec.feature_version),
                ).fetchone()
                if row is not None:
                    existing_tuple = (
                        row["event_time"],
                        row["availability_time"],
                        row["materialized_at"],
                    )
                    incoming_tuple = (
                        rec.event_time.isoformat(),
                        rec.availability_time.isoformat(),
                        rec.materialized_at.isoformat(),
                    )
                    if incoming_tuple < existing_tuple:
                        continue
                conn.execute(
                    """
                    INSERT INTO online_features (
                        entity_id, feature_name, feature_version, version_id,
                        event_time, availability_time, materialized_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(entity_id, feature_name, feature_version) DO UPDATE SET
                        version_id = excluded.version_id,
                        event_time = excluded.event_time,
                        availability_time = excluded.availability_time,
                        materialized_at = excluded.materialized_at,
                        payload_json = excluded.payload_json
                    """,
                    (
                        rec.entity_id,
                        rec.feature_name,
                        rec.feature_version,
                        rec.version_id,
                        rec.event_time.isoformat(),
                        rec.availability_time.isoformat(),
                        rec.materialized_at.isoformat(),
                        rec.model_dump_json(),
                    ),
                )
                written += 1
            conn.commit()
        return written

    def get(self, entity_id: str, feature_name: str, feature_version: str) -> FeatureRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json
                FROM online_features
                WHERE entity_id = ? AND feature_name = ? AND feature_version = ?
                """,
                (entity_id, feature_name, feature_version),
            ).fetchone()
        if row is None:
            return None
        return FeatureRecord.model_validate_json(row["payload_json"])

    def delete_feature(self, feature_name: str, feature_version: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM online_features WHERE feature_name = ? AND feature_version = ?",
                (feature_name, feature_version),
            )
            conn.commit()
            return int(cur.rowcount)

    def health(self) -> dict[str, Any]:
        with self._connect() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM online_features").fetchone()[0]
        return {
            "backend": "sqlite",
            "status": "ok",
            "db_path": str(self.db_path),
            "entries": int(cnt),
        }


@runtime_checkable
class RedisClientProtocol(Protocol):
    """Minimal subset of redis-py client methods required by RedisOnlineBackend."""

    def get(self, name: str) -> bytes | str | None:
        ...

    def set(self, name: str, value: str) -> Any:
        ...

    def delete(self, *names: str) -> int:
        ...

    def scan_iter(self, match: str | None = None) -> Any:
        ...

    def ping(self) -> bool:
        ...


class RedisOnlineBackend:
    """Optional Redis adapter isolated behind RedisClientProtocol (no hard Redis dependency)."""

    def __init__(self, client: RedisClientProtocol, *, key_prefix: str = "ffs:v1") -> None:
        self._client = client
        self.key_prefix = key_prefix

    def _key(self, entity_id: str, feature_name: str, feature_version: str) -> str:
        return f"{self.key_prefix}:{feature_name}:{feature_version}:{entity_id}"

    def put_batch(self, records: list[FeatureRecord]) -> int:
        written = 0
        try:
            for rec in records:
                k = self._key(rec.entity_id, rec.feature_name, rec.feature_version)
                raw = self._client.get(k)
                if raw is not None:
                    decoded = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                    existing = FeatureRecord.model_validate_json(decoded)
                    if (rec.event_time, rec.availability_time, rec.materialized_at) < (
                        existing.event_time,
                        existing.availability_time,
                        existing.materialized_at,
                    ):
                        continue
                self._client.set(k, rec.model_dump_json())
                written += 1
        except Exception as exc:
            raise AdapterError(f"Redis backend write failed: {exc}") from exc
        return written

    def get(self, entity_id: str, feature_name: str, feature_version: str) -> FeatureRecord | None:
        try:
            raw = self._client.get(self._key(entity_id, feature_name, feature_version))
        except Exception as exc:
            raise AdapterError(f"Redis backend read failed: {exc}") from exc
        if raw is None:
            return None
        decoded = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return FeatureRecord.model_validate(json.loads(decoded))

    def delete_feature(self, feature_name: str, feature_version: str) -> int:
        pattern = f"{self.key_prefix}:{feature_name}:{feature_version}:*"
        keys = list(self._client.scan_iter(match=pattern))
        if not keys:
            return 0
        return int(self._client.delete(*keys))

    def health(self) -> dict[str, Any]:
        ok = bool(self._client.ping())
        return {"backend": "redis", "status": "ok" if ok else "degraded", "key_prefix": self.key_prefix}
