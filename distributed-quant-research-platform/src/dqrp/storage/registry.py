"""Durable Experiment Registry (SQLite default + optional PostgreSQL adapter)."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Protocol, Union

from dqrp.contracts import ExperimentSummary, TrialResult, TrialStatus
from dqrp.errors import BackendUnavailableError, DuplicateTaskError


class ExperimentRegistry(Protocol):
    """Typed storage boundary for recording and querying experiment runs."""

    def record_run(self, summary: ExperimentSummary) -> None: ...
    def record_trial_attempt(self, run_id: str, result: TrialResult) -> None: ...
    def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]: ...
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    def export_json(self, output_path: Union[str, Path]) -> Dict[str, Any]: ...


SQLITE_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    experiment_id TEXT NOT NULL,
    experiment_sha256 TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    dataset_sha256 TEXT NOT NULL,
    backend TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    total_trials INTEGER NOT NULL,
    succeeded_trials INTEGER NOT NULL,
    failed_trials INTEGER NOT NULL,
    cancelled_trials INTEGER NOT NULL,
    skipped_budget_trials INTEGER NOT NULL,
    duplicate_completed_trials INTEGER NOT NULL,
    aggregate_sha256 TEXT NOT NULL,
    best_trial_id TEXT,
    best_sharpe_ratio REAL,
    mean_sharpe_ratio REAL,
    mean_total_return REAL,
    total_cpu_seconds REAL NOT NULL,
    total_wall_seconds REAL NOT NULL,
    peak_memory_mb REAL NOT NULL,
    total_estimated_cost_usd REAL NOT NULL,
    dataset_completeness_ratio REAL NOT NULL,
    dataset_freshness_lag_seconds REAL NOT NULL,
    termination_reason TEXT,
    counters_json TEXT NOT NULL,
    environment_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS completed_trials (
    run_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    trial_index INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    artifact_sha256 TEXT,
    params_json TEXT NOT NULL,
    seed INTEGER NOT NULL,
    metrics_json TEXT,
    cpu_seconds REAL NOT NULL,
    wall_seconds REAL NOT NULL,
    peak_memory_mb REAL NOT NULL,
    estimated_cost_usd REAL NOT NULL,
    error_code TEXT,
    error_message TEXT,
    PRIMARY KEY (run_id, trial_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trial_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    cpu_seconds REAL NOT NULL,
    wall_seconds REAL NOT NULL,
    peak_memory_mb REAL NOT NULL,
    estimated_cost_usd REAL NOT NULL,
    error_code TEXT,
    error_message TEXT,
    UNIQUE(run_id, trial_id, attempt)
);
"""


class SqliteExperimentRegistry:
    """Zero-dependency SQLite Experiment Registry with database-enforced uniqueness."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SQLITE_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def record_trial_attempt(self, run_id: str, result: TrialResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO trial_attempts (
                    run_id, trial_id, attempt, status, worker_id,
                    started_at, completed_at, cpu_seconds, wall_seconds,
                    peak_memory_mb, estimated_cost_usd, error_code, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    result.trial_id,
                    result.attempt,
                    result.status.value,
                    result.worker_id,
                    result.started_at,
                    result.completed_at,
                    result.cpu_seconds,
                    result.wall_seconds,
                    result.peak_memory_mb,
                    result.estimated_cost_usd,
                    result.error_code,
                    result.error_message,
                ),
            )

    def record_run(self, summary: ExperimentSummary) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs (
                    run_id, schema_version, experiment_id, experiment_sha256,
                    dataset_id, dataset_version, dataset_sha256, backend, status,
                    started_at, completed_at, total_trials, succeeded_trials,
                    failed_trials, cancelled_trials, skipped_budget_trials,
                    duplicate_completed_trials, aggregate_sha256, best_trial_id,
                    best_sharpe_ratio, mean_sharpe_ratio, mean_total_return,
                    total_cpu_seconds, total_wall_seconds, peak_memory_mb,
                    total_estimated_cost_usd, dataset_completeness_ratio,
                    dataset_freshness_lag_seconds, termination_reason,
                    counters_json, environment_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.run_id,
                    summary.schema_version,
                    summary.experiment_id,
                    summary.experiment_sha256,
                    summary.dataset_id,
                    summary.dataset_version,
                    summary.dataset_sha256,
                    summary.backend,
                    summary.status.value,
                    summary.started_at,
                    summary.completed_at,
                    summary.total_trials,
                    summary.succeeded_trials,
                    summary.failed_trials,
                    summary.cancelled_trials,
                    summary.skipped_budget_trials,
                    summary.duplicate_completed_trials,
                    summary.aggregate_sha256,
                    summary.best_trial_id,
                    summary.best_sharpe_ratio,
                    summary.mean_sharpe_ratio,
                    summary.mean_total_return,
                    summary.total_cpu_seconds,
                    summary.total_wall_seconds,
                    summary.peak_memory_mb,
                    summary.total_estimated_cost_usd,
                    summary.dataset_completeness_ratio,
                    summary.dataset_freshness_lag_seconds,
                    summary.termination_reason,
                    json.dumps(summary.counters, sort_keys=True),
                    json.dumps(summary.environment, sort_keys=True),
                ),
            )
            seen_trial_ids = set()
            for trial in summary.trials:
                if trial.trial_id in seen_trial_ids:
                    raise DuplicateTaskError(
                        f"Refusing to persist duplicate trial_id {trial.trial_id} for run {summary.run_id}"
                    )
                seen_trial_ids.add(trial.trial_id)
                conn.execute(
                    """
                    INSERT INTO completed_trials (
                        run_id, trial_id, trial_index, attempt, status, worker_id,
                        input_sha256, artifact_sha256, params_json, seed, metrics_json,
                        cpu_seconds, wall_seconds, peak_memory_mb, estimated_cost_usd,
                        error_code, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        summary.run_id,
                        trial.trial_id,
                        trial.trial_index,
                        trial.attempt,
                        trial.status.value,
                        trial.worker_id,
                        trial.input_sha256,
                        trial.artifact.artifact_sha256 if trial.artifact else None,
                        json.dumps(trial.params, sort_keys=True),
                        trial.seed,
                        json.dumps(trial.metrics.to_dict(), sort_keys=True) if trial.metrics else None,
                        trial.cpu_seconds,
                        trial.wall_seconds,
                        trial.peak_memory_mb,
                        trial.estimated_cost_usd,
                        trial.error_code,
                        trial.error_message,
                    ),
                )

    def count_duplicate_succeeded_trials(self, run_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) - COUNT(DISTINCT trial_id) AS dups
                FROM completed_trials
                WHERE run_id = ? AND status = ?
                """,
                (run_id, TrialStatus.SUCCEEDED.value),
            ).fetchone()
            return int(row["dups"]) if row else 0

    def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC, run_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            out: List[Dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["counters"] = json.loads(item.pop("counters_json"))
                item["environment"] = json.loads(item.pop("environment_json"))
                out.append(item)
            return out

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["counters"] = json.loads(item.pop("counters_json"))
            item["environment"] = json.loads(item.pop("environment_json"))
            trial_rows = conn.execute(
                "SELECT * FROM completed_trials WHERE run_id = ? ORDER BY trial_index ASC, trial_id ASC",
                (run_id,),
            ).fetchall()
            trials = []
            for tr in trial_rows:
                td = dict(tr)
                td["params"] = json.loads(td.pop("params_json"))
                mj = td.pop("metrics_json")
                td["metrics"] = json.loads(mj) if mj else None
                trials.append(td)
            item["trials"] = trials
            attempt_rows = conn.execute(
                "SELECT * FROM trial_attempts WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            ).fetchall()
            item["attempts"] = [dict(ar) for ar in attempt_rows]
            return item

    def export_json(self, output_path: Union[str, Path]) -> Dict[str, Any]:
        runs = self.list_runs(limit=1000)
        detailed = []
        for r in runs:
            full = self.get_run(r["run_id"])
            if full is not None:
                detailed.append(full)
        payload = {
            "registry_backend": "sqlite",
            "db_path": str(self.db_path),
            "run_count": len(detailed),
            "runs": detailed,
        }
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return payload


class PostgresExperimentRegistry:
    """Optional PostgreSQL registry adapter (disabled unless psycopg + local DSN are provided).

    Production credentials are out of scope for this research prototype.
    """

    def __init__(self, dsn: str) -> None:
        try:
            import psycopg  # type: ignore  # noqa: F401
        except ImportError as exc:
            raise BackendUnavailableError(
                "PostgreSQL registry requires optional dependency 'psycopg[binary]'. "
                "Install with: pip install -e '.[postgres]' or use the default SQLite registry.",
                code="POSTGRES_BACKEND_UNAVAILABLE",
            ) from exc
        if not dsn:
            raise BackendUnavailableError("Non-empty PostgreSQL DSN is required", code="POSTGRES_DSN_MISSING")
        self.dsn = dsn

    def record_run(self, summary: ExperimentSummary) -> None:
        raise BackendUnavailableError("PostgreSQL persistence not enabled in offline prototype mode")

    def record_trial_attempt(self, run_id: str, result: TrialResult) -> None:
        raise BackendUnavailableError("PostgreSQL persistence not enabled in offline prototype mode")

    def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        raise BackendUnavailableError("PostgreSQL persistence not enabled in offline prototype mode")

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        raise BackendUnavailableError("PostgreSQL persistence not enabled in offline prototype mode")

    def export_json(self, output_path: Union[str, Path]) -> Dict[str, Any]:
        raise BackendUnavailableError("PostgreSQL persistence not enabled in offline prototype mode")
