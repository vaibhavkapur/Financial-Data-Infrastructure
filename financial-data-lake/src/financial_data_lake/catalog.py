"""SQLite-backed Data Catalog, Entitlement Registry, Lineage Store, and Audit Log."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from financial_data_lake.errors import EntitlementError, SnapshotError
from financial_data_lake.models import (
    AccessContext,
    EntitlementPolicy,
    LineageEdge,
    PartitionManifest,
    QualityReport,
    SnapshotManifest,
)

CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS entitlements (
    dataset_id TEXT PRIMARY KEY,
    source_vendor TEXT NOT NULL,
    license_id TEXT NOT NULL,
    license_terms TEXT NOT NULL,
    permitted_roles_json TEXT NOT NULL,
    permitted_purposes_json TEXT NOT NULL,
    redistribution_allowed INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    classification TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_versions (
    dataset_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_json TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (dataset_id, version)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_checksum TEXT NOT NULL,
    license_id TEXT NOT NULL,
    provenance TEXT NOT NULL,
    principal TEXT NOT NULL,
    status TEXT NOT NULL,
    raw_rows INTEGER NOT NULL,
    normalized_rows INTEGER NOT NULL,
    deduplicated_rows INTEGER NOT NULL,
    curated_rows INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    partition_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    partition_value TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    object_path TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    min_event_ts TEXT,
    max_event_ts TEXT,
    min_available_at TEXT,
    max_available_at TEXT,
    ingestion_run_id TEXT NOT NULL,
    source_checksum TEXT NOT NULL,
    license_id TEXT NOT NULL,
    provenance TEXT NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
);

CREATE INDEX IF NOT EXISTS idx_partitions_lookup
    ON partitions (dataset_id, layer, partition_key, partition_value, status);

CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id TEXT PRIMARY KEY,
    ingestion_run_id TEXT NOT NULL,
    source_dataset TEXT NOT NULL,
    source_layer TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_dataset TEXT NOT NULL,
    target_layer TEXT NOT NULL,
    target_path TEXT NOT NULL,
    target_sha256 TEXT NOT NULL,
    transformation TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_reports (
    report_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    ingestion_run_id TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    total_rows INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    checks_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    as_of_ts TEXT NOT NULL,
    datasets_json TEXT NOT NULL,
    partition_ids_json TEXT NOT NULL,
    partition_checksums_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    license_ids_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ts TEXT NOT NULL,
    principal TEXT NOT NULL,
    role TEXT NOT NULL,
    purpose TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    action TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    details_json TEXT NOT NULL
);
"""


def _dt_to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).isoformat()


def _iso_to_dt(val: str | None) -> datetime | None:
    if val is None:
        return None
    return datetime.fromisoformat(val).astimezone(UTC)


class CatalogRegistry:
    """SQLite-backed catalog database and entitlement registry."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(CATALOG_DDL)

    # ---- Entitlements & Authorization ----

    def load_entitlements_file(self, path: Path | str) -> int:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        count = 0
        for item in payload["entitlements"]:
            policy = EntitlementPolicy.model_validate(item)
            self.register_entitlement(policy)
            count += 1
        return count

    def register_entitlement(self, policy: EntitlementPolicy) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO entitlements (
                    dataset_id, source_vendor, license_id, license_terms,
                    permitted_roles_json, permitted_purposes_json,
                    redistribution_allowed, effective_from, expires_at, classification
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET
                    source_vendor=excluded.source_vendor,
                    license_id=excluded.license_id,
                    license_terms=excluded.license_terms,
                    permitted_roles_json=excluded.permitted_roles_json,
                    permitted_purposes_json=excluded.permitted_purposes_json,
                    redistribution_allowed=excluded.redistribution_allowed,
                    effective_from=excluded.effective_from,
                    expires_at=excluded.expires_at,
                    classification=excluded.classification
                """,
                (
                    policy.dataset_id,
                    policy.source_vendor,
                    policy.license_id,
                    policy.license_terms,
                    json.dumps(policy.permitted_roles),
                    json.dumps(policy.permitted_purposes),
                    1 if policy.redistribution_allowed else 0,
                    _dt_to_iso(policy.effective_from),
                    _dt_to_iso(policy.expires_at),
                    policy.classification,
                ),
            )

    def get_entitlement(self, dataset_id: str) -> EntitlementPolicy | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM entitlements WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchone()
        if row is None:
            return None
        return EntitlementPolicy(
            dataset_id=row["dataset_id"],
            source_vendor=row["source_vendor"],
            license_id=row["license_id"],
            license_terms=row["license_terms"],
            permitted_roles=json.loads(row["permitted_roles_json"]),
            permitted_purposes=json.loads(row["permitted_purposes_json"]),
            redistribution_allowed=bool(row["redistribution_allowed"]),
            effective_from=datetime.fromisoformat(row["effective_from"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            classification=row["classification"],
        )

    def list_entitlements(self) -> list[EntitlementPolicy]:
        with self._connect() as conn:
            rows = conn.execute("SELECT dataset_id FROM entitlements ORDER BY dataset_id").fetchall()
        return [
            pol for r in rows if (pol := self.get_entitlement(r["dataset_id"])) is not None
        ]

    def authorize(
        self,
        dataset_id: str,
        access: AccessContext,
        *,
        action: str = "read",
    ) -> EntitlementPolicy:
        """Fail-closed entitlement check; logs every allow/deny decision to audit_log."""
        policy = self.get_entitlement(dataset_id)
        if policy is None:
            reason = f"Dataset '{dataset_id}' has no registered entitlement."
            self.record_audit(access, dataset_id, action, "DENY", reason)
            raise EntitlementError(reason, context={"dataset_id": dataset_id, "role": access.role})

        if access.purpose.lower() in {"redistribute", "redistribution", "external_export"}:
            if not policy.redistribution_allowed:
                reason = f"Redistribution of dataset '{dataset_id}' is forbidden by license '{policy.license_id}'."
                self.record_audit(access, dataset_id, action, "DENY", reason)
                raise EntitlementError(
                    reason,
                    context={"dataset_id": dataset_id, "license_id": policy.license_id},
                )

        if access.requested_at < policy.effective_from or access.requested_at > policy.expires_at:
            reason = (
                f"Entitlement for '{dataset_id}' ({policy.license_id}) is not active at "
                f"{access.requested_at.isoformat()} (valid {policy.effective_from.isoformat()} "
                f"to {policy.expires_at.isoformat()})."
            )
            self.record_audit(access, dataset_id, action, "DENY", reason)
            raise EntitlementError(
                reason,
                context={"dataset_id": dataset_id, "license_id": policy.license_id},
            )

        if access.role not in policy.permitted_roles:
            reason = (
                f"Role '{access.role}' is not entitled to access dataset '{dataset_id}' "
                f"(permitted: {policy.permitted_roles})."
            )
            self.record_audit(access, dataset_id, action, "DENY", reason)
            raise EntitlementError(
                reason,
                context={"dataset_id": dataset_id, "role": access.role},
            )

        if access.purpose not in policy.permitted_purposes:
            reason = (
                f"Purpose '{access.purpose}' is not permitted for dataset '{dataset_id}' "
                f"(permitted: {policy.permitted_purposes})."
            )
            self.record_audit(access, dataset_id, action, "DENY", reason)
            raise EntitlementError(
                reason,
                context={"dataset_id": dataset_id, "purpose": access.purpose},
            )

        self.record_audit(
            access,
            dataset_id,
            action,
            "ALLOW",
            f"Authorized under license {policy.license_id}",
            details={"license_id": policy.license_id},
        )
        return policy

    def record_audit(
        self,
        access: AccessContext,
        dataset_id: str,
        action: str,
        decision: str,
        reason: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (
                    event_ts, principal, role, purpose, dataset_id, action, decision, reason, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _dt_to_iso(datetime.now(UTC)),
                    access.principal,
                    access.role,
                    access.purpose,
                    dataset_id,
                    action,
                    decision,
                    reason,
                    json.dumps(details or {}),
                ),
            )

    def get_audit_log(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if dataset_id:
                rows = conn.execute(
                    "SELECT * FROM audit_log WHERE dataset_id = ? ORDER BY event_id",
                    (dataset_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM audit_log ORDER BY event_id").fetchall()
        return [dict(r) for r in rows]

    # ---- Schema version history ----

    def record_schema_version(self, dataset_id: str, version: int, schema_payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO schema_versions (dataset_id, version, schema_json, registered_at)
                VALUES (?, ?, ?, ?)
                """,
                (dataset_id, version, json.dumps(schema_payload), _dt_to_iso(datetime.now(UTC))),
            )

    # ---- Ingestion runs & Idempotency ----

    def has_completed_ingest(self, dataset_id: str, source_checksum: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM ingestion_runs
                WHERE dataset_id = ? AND source_checksum = ? AND status = 'COMPLETED'
                ORDER BY started_at DESC LIMIT 1
                """,
                (dataset_id, source_checksum),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_ingestion_run(
        self,
        *,
        run_id: str,
        dataset_id: str,
        source_name: str,
        source_checksum: str,
        license_id: str,
        provenance: str,
        principal: str,
        status: str,
        raw_rows: int,
        normalized_rows: int,
        deduplicated_rows: int,
        curated_rows: int,
        started_at: datetime,
        completed_at: datetime,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, dataset_id, source_name, source_checksum, license_id,
                    provenance, principal, status, raw_rows, normalized_rows,
                    deduplicated_rows, curated_rows, started_at, completed_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    dataset_id,
                    source_name,
                    source_checksum,
                    license_id,
                    provenance,
                    principal,
                    status,
                    raw_rows,
                    normalized_rows,
                    deduplicated_rows,
                    curated_rows,
                    _dt_to_iso(started_at),
                    _dt_to_iso(completed_at),
                    json.dumps(details or {}),
                ),
            )

    def list_ingestion_runs(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if dataset_id:
                rows = conn.execute(
                    "SELECT * FROM ingestion_runs WHERE dataset_id = ? ORDER BY started_at",
                    (dataset_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at").fetchall()
        return [dict(r) for r in rows]

    # ---- Partitions ----

    def next_partition_version(
        self,
        dataset_id: str,
        layer: str,
        partition_key: str,
        partition_value: str,
    ) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) AS max_ver
                FROM partitions
                WHERE dataset_id = ? AND layer = ? AND partition_key = ? AND partition_value = ?
                """,
                (dataset_id, layer, partition_key, partition_value),
            ).fetchone()
        return int(row["max_ver"]) + 1

    def register_partition(self, manifest: PartitionManifest) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO partitions (
                    partition_id, dataset_id, layer, partition_key, partition_value,
                    version, schema_version, object_path, sha256, byte_size, row_count,
                    min_event_ts, max_event_ts, min_available_at, max_available_at,
                    ingestion_run_id, source_checksum, license_id, provenance, created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest.partition_id,
                    manifest.dataset_id,
                    manifest.layer,
                    manifest.partition_key,
                    manifest.partition_value,
                    manifest.version,
                    manifest.schema_version,
                    manifest.object_path,
                    manifest.sha256,
                    manifest.byte_size,
                    manifest.row_count,
                    _dt_to_iso(manifest.min_event_ts),
                    _dt_to_iso(manifest.max_event_ts),
                    _dt_to_iso(manifest.min_available_at),
                    _dt_to_iso(manifest.max_available_at),
                    manifest.ingestion_run_id,
                    manifest.source_checksum,
                    manifest.license_id,
                    manifest.provenance,
                    _dt_to_iso(manifest.created_at),
                    manifest.status,
                ),
            )

    def _row_to_partition(self, row: sqlite3.Row) -> PartitionManifest:
        return PartitionManifest(
            partition_id=row["partition_id"],
            dataset_id=row["dataset_id"],
            layer=row["layer"],
            partition_key=row["partition_key"],
            partition_value=row["partition_value"],
            version=row["version"],
            schema_version=row["schema_version"],
            object_path=row["object_path"],
            sha256=row["sha256"],
            byte_size=row["byte_size"],
            row_count=row["row_count"],
            min_event_ts=_iso_to_dt(row["min_event_ts"]),
            max_event_ts=_iso_to_dt(row["max_event_ts"]),
            min_available_at=_iso_to_dt(row["min_available_at"]),
            max_available_at=_iso_to_dt(row["max_available_at"]),
            ingestion_run_id=row["ingestion_run_id"],
            source_checksum=row["source_checksum"],
            license_id=row["license_id"],
            provenance=row["provenance"],
            created_at=_iso_to_dt(row["created_at"]) or datetime.now(UTC),
            status=row["status"],
        )

    def list_partitions(
        self,
        dataset_id: str | None = None,
        layer: str | None = None,
        status: str | None = "ACTIVE",
        partition_value: str | None = None,
    ) -> list[PartitionManifest]:
        clauses: list[str] = []
        params: list[Any] = []
        if dataset_id is not None:
            clauses.append("dataset_id = ?")
            params.append(dataset_id)
        if layer is not None:
            clauses.append("layer = ?")
            params.append(layer)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if partition_value is not None:
            clauses.append("partition_value = ?")
            params.append(partition_value)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM partitions {where} ORDER BY dataset_id, layer, partition_value, version"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_partition(r) for r in rows]

    def get_partition(self, partition_id: str) -> PartitionManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM partitions WHERE partition_id = ?",
                (partition_id,),
            ).fetchone()
        return self._row_to_partition(row) if row is not None else None

    def update_partition_status(self, partition_id: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE partitions SET status = ? WHERE partition_id = ?",
                (status, partition_id),
            )

    # ---- Lineage & Quality ----

    def record_lineage(self, edge: LineageEdge) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO lineage_edges (
                    edge_id, ingestion_run_id, source_dataset, source_layer,
                    source_path, source_sha256, target_dataset, target_layer,
                    target_path, target_sha256, transformation, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    edge.edge_id,
                    edge.ingestion_run_id,
                    edge.source_dataset,
                    edge.source_layer,
                    edge.source_path,
                    edge.source_sha256,
                    edge.target_dataset,
                    edge.target_layer,
                    edge.target_path,
                    edge.target_sha256,
                    edge.transformation,
                    _dt_to_iso(edge.created_at),
                ),
            )

    def list_lineage(self, dataset_id: str | None = None) -> list[LineageEdge]:
        with self._connect() as conn:
            if dataset_id:
                rows = conn.execute(
                    "SELECT * FROM lineage_edges WHERE target_dataset = ? OR source_dataset = ? ORDER BY created_at",
                    (dataset_id, dataset_id),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM lineage_edges ORDER BY created_at").fetchall()
        return [
            LineageEdge(
                edge_id=r["edge_id"],
                ingestion_run_id=r["ingestion_run_id"],
                source_dataset=r["source_dataset"],
                source_layer=r["source_layer"],
                source_path=r["source_path"],
                source_sha256=r["source_sha256"],
                target_dataset=r["target_dataset"],
                target_layer=r["target_layer"],
                target_path=r["target_path"],
                target_sha256=r["target_sha256"],
                transformation=r["transformation"],
                created_at=datetime.fromisoformat(r["created_at"]).astimezone(UTC),
            )
            for r in rows
        ]

    def record_quality_report(self, report_id: str, report: QualityReport) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO quality_reports (
                    report_id, dataset_id, layer, ingestion_run_id, checked_at,
                    total_rows, passed, checks_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    report.dataset_id,
                    report.layer,
                    report.ingestion_run_id,
                    _dt_to_iso(report.checked_at),
                    report.total_rows,
                    1 if report.passed else 0,
                    json.dumps([c.model_dump(mode="json") for c in report.checks]),
                ),
            )

    def list_quality_reports(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if dataset_id:
                rows = conn.execute(
                    "SELECT * FROM quality_reports WHERE dataset_id = ? ORDER BY checked_at",
                    (dataset_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM quality_reports ORDER BY checked_at").fetchall()
        return [dict(r) for r in rows]

    # ---- Snapshots ----

    def register_snapshot(self, snapshot: SnapshotManifest) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (
                    snapshot_id, name, as_of_ts, datasets_json, partition_ids_json,
                    partition_checksums_json, manifest_sha256, created_by, created_at, license_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.name,
                    _dt_to_iso(snapshot.as_of_ts),
                    json.dumps(snapshot.datasets),
                    json.dumps(snapshot.partition_ids),
                    json.dumps(snapshot.partition_checksums),
                    snapshot.manifest_sha256,
                    snapshot.created_by,
                    _dt_to_iso(snapshot.created_at),
                    json.dumps(snapshot.license_ids),
                ),
            )

    def get_snapshot(self, name_or_id: str) -> SnapshotManifest:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM snapshots WHERE name = ? OR snapshot_id = ?",
                (name_or_id, name_or_id),
            ).fetchone()
        if row is None:
            raise SnapshotError(
                f"Snapshot '{name_or_id}' not found in catalog.",
                context={"snapshot": name_or_id},
            )
        return SnapshotManifest(
            snapshot_id=row["snapshot_id"],
            name=row["name"],
            as_of_ts=datetime.fromisoformat(row["as_of_ts"]).astimezone(UTC),
            datasets=json.loads(row["datasets_json"]),
            partition_ids=json.loads(row["partition_ids_json"]),
            partition_checksums=json.loads(row["partition_checksums_json"]),
            manifest_sha256=row["manifest_sha256"],
            created_by=row["created_by"],
            created_at=datetime.fromisoformat(row["created_at"]).astimezone(UTC),
            license_ids=json.loads(row["license_ids_json"]),
        )

    def list_snapshots(self) -> list[SnapshotManifest]:
        with self._connect() as conn:
            rows = conn.execute("SELECT snapshot_id FROM snapshots ORDER BY created_at").fetchall()
        return [self.get_snapshot(r["snapshot_id"]) for r in rows]

    def export_catalog_manifest(self) -> dict[str, Any]:
        """Export a complete, reviewable snapshot of the data catalog."""
        return {
            "exported_at": _dt_to_iso(datetime.now(UTC)),
            "entitlements": [e.model_dump(mode="json") for e in self.list_entitlements()],
            "datasets": sorted({p.dataset_id for p in self.list_partitions(status=None)}),
            "active_partitions": [p.model_dump(mode="json") for p in self.list_partitions(status="ACTIVE")],
            "quarantined_partitions": [
                p.model_dump(mode="json") for p in self.list_partitions(status="QUARANTINED")
            ],
            "snapshots": [s.model_dump(mode="json") for s in self.list_snapshots()],
            "lineage_edge_count": len(self.list_lineage()),
            "ingestion_run_count": len(self.list_ingestion_runs()),
        }
