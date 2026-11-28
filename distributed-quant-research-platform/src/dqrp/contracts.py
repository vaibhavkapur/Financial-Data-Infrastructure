"""Canonical data contracts, schema validation, and immutable job descriptors.

All timestamps use UTC ISO-8601 strings (e.g. '2025-01-02T16:00:00Z').
Point-in-time invariant: availability_time >= event_time for every bar.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import itertools
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from dqrp.errors import ContractValidationError, UnsupportedOperationError
from dqrp.hashing import compute_trial_id, derive_trial_seed, sha256_canonical

SCHEMA_VERSION = "dqrp.v1"
SUPPORTED_FORMATS = ("jsonl", "csv", "parquet")
SUPPORTED_STRATEGIES = ("sma_crossover", "momentum_zscore", "mean_reversion")
SUPPORTED_BACKENDS = ("local", "distributed", "ray", "dask")


def parse_utc_timestamp(raw: str, *, field_name: str = "timestamp") -> datetime:
    """Parse an ISO-8601 UTC timestamp string and enforce timezone awareness."""
    if not isinstance(raw, str) or not raw.strip():
        raise ContractValidationError(
            f"Field '{field_name}' must be a non-empty UTC ISO-8601 string",
            code="INVALID_TIMESTAMP",
            details={"field": field_name, "value": raw},
        )
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ContractValidationError(
            f"Field '{field_name}' is not a valid ISO-8601 timestamp: {raw!r}",
            code="INVALID_TIMESTAMP",
            details={"field": field_name, "value": raw},
        ) from exc
    if dt.tzinfo is None:
        raise ContractValidationError(
            f"Field '{field_name}' must include UTC timezone offset or 'Z': {raw!r}",
            code="TIMEZONE_NAIVE_TIMESTAMP",
            details={"field": field_name, "value": raw},
        )
    return dt.astimezone(timezone.utc)


def utc_now_iso() -> str:
    """Return current UTC time formatted as ISO-8601 with trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _require_finite_float(value: Any, field_name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractValidationError(
            f"Field '{field_name}' must be numeric",
            code="INVALID_NUMBER",
            details={"field": field_name, "value": value},
        )
    fval = float(value)
    if math.isnan(fval) or math.isinf(fval):
        raise ContractValidationError(
            f"Field '{field_name}' must be finite",
            code="NON_FINITE_NUMBER",
            details={"field": field_name, "value": value},
        )
    if min_value is not None and fval < min_value:
        raise ContractValidationError(
            f"Field '{field_name}' must be >= {min_value}, got {fval}",
            code="OUT_OF_RANGE",
            details={"field": field_name, "value": fval, "min_value": min_value},
        )
    return fval


def _require_positive_int(value: Any, field_name: str, *, min_value: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < min_value:
        raise ContractValidationError(
            f"Field '{field_name}' must be an integer >= {min_value}",
            code="INVALID_INTEGER",
            details={"field": field_name, "value": value, "min_value": min_value},
        )
    return int(value)


def _validate_schema_version(version: Any) -> str:
    if not isinstance(version, str) or not version:
        raise ContractValidationError(
            "Missing required 'schema_version'",
            code="MISSING_SCHEMA_VERSION",
        )
    if version != SCHEMA_VERSION:
        raise UnsupportedOperationError(
            f"Unsupported schema_version {version!r}; expected {SCHEMA_VERSION!r}",
            code="UNSUPPORTED_SCHEMA_VERSION",
            details={"received": version, "supported": SCHEMA_VERSION},
        )
    return version


class TrialStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED_BUDGET = "SKIPPED_BUDGET"


class RunStatus(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    CANCELLED = "CANCELLED"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILED = "FAILED"


@dataclass(frozen=True)
class MarketBar:
    """Canonical OHLCV market bar with explicit event and availability times."""

    symbol: str
    event_time: str
    availability_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str = "synthetic"
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ContractValidationError("Bar 'symbol' must be a non-empty string", code="INVALID_SYMBOL")
        ev_dt = parse_utc_timestamp(self.event_time, field_name="event_time")
        av_dt = parse_utc_timestamp(self.availability_time, field_name="availability_time")
        if av_dt < ev_dt:
            raise ContractValidationError(
                f"Point-in-time violation: availability_time ({self.availability_time}) "
                f"< event_time ({self.event_time}) for symbol {self.symbol}",
                code="POINT_IN_TIME_VIOLATION",
                details={
                    "symbol": self.symbol,
                    "event_time": self.event_time,
                    "availability_time": self.availability_time,
                },
            )
        o = _require_finite_float(self.open, "open", min_value=1e-9)
        h = _require_finite_float(self.high, "high", min_value=1e-9)
        l = _require_finite_float(self.low, "low", min_value=1e-9)
        c = _require_finite_float(self.close, "close", min_value=1e-9)
        _require_finite_float(self.volume, "volume", min_value=0.0)
        if h < max(o, c, l) or l > min(o, c, h):
            raise ContractValidationError(
                f"Invalid OHLC envelope for {self.symbol} at {self.event_time}: O={o} H={h} L={l} C={c}",
                code="INVALID_OHLC_ENVELOPE",
                details={"symbol": self.symbol, "event_time": self.event_time},
            )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketBar":
        required = ("symbol", "event_time", "availability_time", "open", "high", "low", "close", "volume")
        missing = [k for k in required if k not in data]
        if missing:
            raise ContractValidationError(
                f"MarketBar missing required fields: {missing}",
                code="MISSING_FIELDS",
                details={"missing": missing},
            )
        return cls(
            symbol=str(data["symbol"]),
            event_time=str(data["event_time"]),
            availability_time=str(data["availability_time"]),
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
            source=str(data.get("source", "synthetic")),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetPartition:
    """Single immutable data file partition inside a versioned DatasetManifest."""

    partition_id: str
    relative_path: str
    row_count: int
    content_sha256: str
    min_event_time: str
    max_event_time: str
    max_availability_time: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.partition_id or not isinstance(self.partition_id, str):
            raise ContractValidationError("partition_id must be non-empty", code="INVALID_PARTITION")
        if not self.relative_path or ".." in self.relative_path.split("/"):
            raise ContractValidationError(
                f"Invalid partition relative_path: {self.relative_path!r}",
                code="INVALID_PARTITION_PATH",
            )
        _require_positive_int(self.row_count, "row_count", min_value=1)
        _require_positive_int(self.size_bytes, "size_bytes", min_value=1)
        if not isinstance(self.content_sha256, str) or len(self.content_sha256) != 64:
            raise ContractValidationError(
                "content_sha256 must be a 64-char hex SHA-256 digest",
                code="INVALID_SHA256",
            )
        min_ev = parse_utc_timestamp(self.min_event_time, field_name="min_event_time")
        max_ev = parse_utc_timestamp(self.max_event_time, field_name="max_event_time")
        max_av = parse_utc_timestamp(self.max_availability_time, field_name="max_availability_time")
        if max_ev < min_ev:
            raise ContractValidationError(
                "max_event_time must be >= min_event_time",
                code="INVALID_TIME_RANGE",
            )
        if max_av < max_ev:
            raise ContractValidationError(
                "max_availability_time must be >= max_event_time",
                code="POINT_IN_TIME_VIOLATION",
            )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetPartition":
        required = (
            "partition_id",
            "relative_path",
            "row_count",
            "content_sha256",
            "min_event_time",
            "max_event_time",
            "max_availability_time",
            "size_bytes",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise ContractValidationError(
                f"DatasetPartition missing fields: {missing}",
                code="MISSING_FIELDS",
                details={"missing": missing},
            )
        return cls(
            partition_id=str(data["partition_id"]),
            relative_path=str(data["relative_path"]),
            row_count=int(data["row_count"]),
            content_sha256=str(data["content_sha256"]),
            min_event_time=str(data["min_event_time"]),
            max_event_time=str(data["max_event_time"]),
            max_availability_time=str(data["max_availability_time"]),
            size_bytes=int(data["size_bytes"]),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetManifest:
    """Versioned dataset manifest with lineage, schema version, and content hash."""

    schema_version: str
    dataset_id: str
    version: str
    format: str
    source_name: str
    source_lineage: str
    created_at: str
    expected_row_count: int
    symbols: Tuple[str, ...]
    partitions: Tuple[DatasetPartition, ...]
    manifest_sha256: str = ""

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if not self.dataset_id or not isinstance(self.dataset_id, str):
            raise ContractValidationError("dataset_id must be non-empty", code="INVALID_DATASET_ID")
        if not self.version or not isinstance(self.version, str):
            raise ContractValidationError("version must be non-empty", code="INVALID_VERSION")
        if self.format not in SUPPORTED_FORMATS:
            raise UnsupportedOperationError(
                f"Unsupported dataset format {self.format!r}; supported: {SUPPORTED_FORMATS}",
                code="UNSUPPORTED_FORMAT",
                details={"format": self.format, "supported": list(SUPPORTED_FORMATS)},
            )
        if not self.source_name or not self.source_lineage:
            raise ContractValidationError("source_name and source_lineage are required", code="MISSING_LINEAGE")
        parse_utc_timestamp(self.created_at, field_name="created_at")
        _require_positive_int(self.expected_row_count, "expected_row_count", min_value=1)
        if not self.symbols:
            raise ContractValidationError("symbols must be non-empty", code="EMPTY_SYMBOLS")
        if not self.partitions:
            raise ContractValidationError("partitions must be non-empty", code="EMPTY_PARTITIONS")
        seen_ids = set()
        for part in self.partitions:
            if part.partition_id in seen_ids:
                raise ContractValidationError(
                    f"Duplicate partition_id {part.partition_id!r}",
                    code="DUPLICATE_PARTITION",
                )
            seen_ids.add(part.partition_id)

        computed = self.compute_hash()
        if self.manifest_sha256 and self.manifest_sha256 != computed:
            raise ContractValidationError(
                f"Manifest hash mismatch: declared {self.manifest_sha256}, computed {computed}",
                code="MANIFEST_HASH_MISMATCH",
                details={"declared": self.manifest_sha256, "computed": computed},
            )
        if not self.manifest_sha256:
            object.__setattr__(self, "manifest_sha256", computed)

    def compute_hash(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "format": self.format,
            "source_name": self.source_name,
            "source_lineage": self.source_lineage,
            "expected_row_count": self.expected_row_count,
            "symbols": sorted(self.symbols),
            "partitions": [p.to_dict() for p in sorted(self.partitions, key=lambda p: p.partition_id)],
        }
        return sha256_canonical(payload)

    @property
    def total_partition_rows(self) -> int:
        return sum(p.row_count for p in self.partitions)

    @property
    def completeness_ratio(self) -> float:
        return min(1.0, self.total_partition_rows / float(self.expected_row_count))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetManifest":
        required = (
            "schema_version",
            "dataset_id",
            "version",
            "format",
            "source_name",
            "source_lineage",
            "created_at",
            "expected_row_count",
            "symbols",
            "partitions",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise ContractValidationError(
                f"DatasetManifest missing required fields: {missing}",
                code="MISSING_FIELDS",
                details={"missing": missing},
            )
        partitions = tuple(DatasetPartition.from_dict(p) for p in data["partitions"])
        symbols = tuple(str(s) for s in data["symbols"])
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            version=str(data["version"]),
            format=str(data["format"]),
            source_name=str(data["source_name"]),
            source_lineage=str(data["source_lineage"]),
            created_at=str(data["created_at"]),
            expected_row_count=int(data["expected_row_count"]),
            symbols=symbols,
            partitions=partitions,
            manifest_sha256=str(data.get("manifest_sha256", "")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "format": self.format,
            "source_name": self.source_name,
            "source_lineage": self.source_lineage,
            "created_at": self.created_at,
            "expected_row_count": self.expected_row_count,
            "symbols": list(self.symbols),
            "partitions": [p.to_dict() for p in self.partitions],
            "manifest_sha256": self.manifest_sha256,
        }


@dataclass(frozen=True)
class ResourceBudget:
    """Hard resource bounds enforced by the scheduler."""

    max_trials: int = 100
    max_concurrency: int = 4
    max_cpu_seconds: float = 300.0
    max_wall_seconds: float = 300.0
    max_memory_mb: float = 2048.0
    max_estimated_cost_usd: float = 5.0

    def __post_init__(self) -> None:
        _require_positive_int(self.max_trials, "max_trials", min_value=1)
        _require_positive_int(self.max_concurrency, "max_concurrency", min_value=1)
        _require_finite_float(self.max_cpu_seconds, "max_cpu_seconds", min_value=1e-6)
        _require_finite_float(self.max_wall_seconds, "max_wall_seconds", min_value=1e-6)
        _require_finite_float(self.max_memory_mb, "max_memory_mb", min_value=1.0)
        _require_finite_float(self.max_estimated_cost_usd, "max_estimated_cost_usd", min_value=0.0)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ResourceBudget":
        return cls(
            max_trials=int(data.get("max_trials", 100)),
            max_concurrency=int(data.get("max_concurrency", 4)),
            max_cpu_seconds=float(data.get("max_cpu_seconds", 300.0)),
            max_wall_seconds=float(data.get("max_wall_seconds", 300.0)),
            max_memory_mb=float(data.get("max_memory_mb", 2048.0)),
            max_estimated_cost_usd=float(data.get("max_estimated_cost_usd", 5.0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CostModel:
    """Transparent synthetic cloud-equivalent unit cost estimator (USD).

    These are explicit estimation rates, never billing guarantees.
    """

    cpu_core_hour_usd: float = 0.048
    memory_gb_hour_usd: float = 0.006
    artifact_gb_write_usd: float = 0.020
    model_label: str = "synthetic-cpu-worker-v1"

    def __post_init__(self) -> None:
        _require_finite_float(self.cpu_core_hour_usd, "cpu_core_hour_usd", min_value=0.0)
        _require_finite_float(self.memory_gb_hour_usd, "memory_gb_hour_usd", min_value=0.0)
        _require_finite_float(self.artifact_gb_write_usd, "artifact_gb_write_usd", min_value=0.0)

    def estimate_trial_cost_usd(
        self,
        *,
        cpu_seconds: float,
        wall_seconds: float,
        peak_memory_mb: float,
        artifact_bytes: int,
    ) -> float:
        cpu_cost = (max(0.0, cpu_seconds) / 3600.0) * self.cpu_core_hour_usd
        mem_gb_hours = (max(0.0, peak_memory_mb) / 1024.0) * (max(0.0, wall_seconds) / 3600.0)
        mem_cost = mem_gb_hours * self.memory_gb_hour_usd
        io_cost = (max(0, artifact_bytes) / (1024.0 ** 3)) * self.artifact_gb_write_usd
        return round(cpu_cost + mem_cost + io_cost, 10)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CostModel":
        return cls(
            cpu_core_hour_usd=float(data.get("cpu_core_hour_usd", 0.048)),
            memory_gb_hour_usd=float(data.get("memory_gb_hour_usd", 0.006)),
            artifact_gb_write_usd=float(data.get("artifact_gb_write_usd", 0.020)),
            model_label=str(data.get("model_label", "synthetic-cpu-worker-v1")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExperimentSpec:
    """Canonical experiment specification with deterministic parameter expansion."""

    schema_version: str
    experiment_id: str
    name: str
    dataset_manifest_path: str
    strategy_name: str
    base_seed: int
    budget: ResourceBudget
    parameter_grid: Dict[str, List[Any]] = field(default_factory=dict)
    explicit_trials: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)
    expected_dataset_sha256: Optional[str] = None
    evaluation_as_of: Optional[str] = None
    max_retries: int = 2
    retry_backoff_seconds: float = 0.01
    cost_model: CostModel = field(default_factory=CostModel)
    description: str = ""
    spec_sha256: str = ""

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if not self.experiment_id or not isinstance(self.experiment_id, str):
            raise ContractValidationError("experiment_id must be non-empty", code="INVALID_EXPERIMENT_ID")
        if not self.name or not isinstance(self.name, str):
            raise ContractValidationError("name must be non-empty", code="INVALID_NAME")
        if not self.dataset_manifest_path:
            raise ContractValidationError("dataset_manifest_path is required", code="MISSING_MANIFEST_PATH")
        if self.strategy_name not in SUPPORTED_STRATEGIES:
            raise UnsupportedOperationError(
                f"Unsupported strategy {self.strategy_name!r}; supported: {SUPPORTED_STRATEGIES}",
                code="UNSUPPORTED_STRATEGY",
                details={"strategy_name": self.strategy_name, "supported": list(SUPPORTED_STRATEGIES)},
            )
        if isinstance(self.base_seed, bool) or not isinstance(self.base_seed, int) or self.base_seed < 0:
            raise ContractValidationError("base_seed must be a non-negative integer", code="INVALID_SEED")
        if self.max_retries < 0 or isinstance(self.max_retries, bool):
            raise ContractValidationError("max_retries must be >= 0", code="INVALID_MAX_RETRIES")
        _require_finite_float(self.retry_backoff_seconds, "retry_backoff_seconds", min_value=0.0)
        if self.evaluation_as_of is not None:
            parse_utc_timestamp(self.evaluation_as_of, field_name="evaluation_as_of")
        if self.expected_dataset_sha256 is not None and len(self.expected_dataset_sha256) != 64:
            raise ContractValidationError(
                "expected_dataset_sha256 must be a 64-char hex SHA-256 digest",
                code="INVALID_SHA256",
            )
        if not self.parameter_grid and not self.explicit_trials:
            raise ContractValidationError(
                "ExperimentSpec must specify either 'parameter_grid' or 'explicit_trials'",
                code="EMPTY_TRIAL_SPACE",
            )
        if self.parameter_grid and self.explicit_trials:
            raise ContractValidationError(
                "Specify either 'parameter_grid' or 'explicit_trials', not both",
                code="AMBIGUOUS_TRIAL_SPACE",
            )
        for key, vals in self.parameter_grid.items():
            if not isinstance(vals, list) or len(vals) == 0:
                raise ContractValidationError(
                    f"parameter_grid[{key!r}] must be a non-empty list",
                    code="INVALID_PARAMETER_GRID",
                )

        computed = self.compute_hash()
        if self.spec_sha256 and self.spec_sha256 != computed:
            raise ContractValidationError(
                f"Spec hash mismatch: declared {self.spec_sha256}, computed {computed}",
                code="SPEC_HASH_MISMATCH",
            )
        if not self.spec_sha256:
            object.__setattr__(self, "spec_sha256", computed)

    def compute_hash(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "experiment_id": self.experiment_id,
            "name": self.name,
            "strategy_name": self.strategy_name,
            "base_seed": self.base_seed,
            "budget": self.budget.to_dict(),
            "parameter_grid": self.parameter_grid,
            "explicit_trials": list(self.explicit_trials),
            "expected_dataset_sha256": self.expected_dataset_sha256,
            "evaluation_as_of": self.evaluation_as_of,
            "max_retries": self.max_retries,
            "cost_model": self.cost_model.to_dict(),
        }
        return sha256_canonical(payload)

    def expand_parameter_sets(self) -> List[Dict[str, Any]]:
        """Deterministically expand trial parameter sets in canonical key order."""
        if self.explicit_trials:
            return [dict(sorted(trial.items(), key=lambda kv: kv[0])) for trial in self.explicit_trials]
        keys = sorted(self.parameter_grid.keys())
        value_lists = [self.parameter_grid[k] for k in keys]
        expanded: List[Dict[str, Any]] = []
        for combo in itertools.product(*value_lists):
            expanded.append(dict(zip(keys, combo)))
        return expanded

    def build_job_contracts(self, dataset_sha256: str) -> List["JobContract"]:
        """Materialize immutable JobContracts for every expanded trial."""
        if self.expected_dataset_sha256 and self.expected_dataset_sha256 != dataset_sha256:
            raise ContractValidationError(
                f"Dataset hash mismatch: spec expects {self.expected_dataset_sha256}, "
                f"manifest has {dataset_sha256}",
                code="DATASET_HASH_MISMATCH",
                details={
                    "expected_dataset_sha256": self.expected_dataset_sha256,
                    "actual_dataset_sha256": dataset_sha256,
                },
            )
        param_sets = self.expand_parameter_sets()
        contracts: List[JobContract] = []
        for idx, params in enumerate(param_sets):
            seed = derive_trial_seed(self.base_seed, self.spec_sha256, idx)
            trial_id = compute_trial_id(
                experiment_sha256=self.spec_sha256,
                dataset_sha256=dataset_sha256,
                trial_index=idx,
                strategy_name=self.strategy_name,
                params=params,
                seed=seed,
            )
            contracts.append(
                JobContract(
                    schema_version=SCHEMA_VERSION,
                    trial_id=trial_id,
                    experiment_id=self.experiment_id,
                    experiment_sha256=self.spec_sha256,
                    dataset_sha256=dataset_sha256,
                    trial_index=idx,
                    strategy_name=self.strategy_name,
                    params=params,
                    seed=seed,
                    evaluation_as_of=self.evaluation_as_of,
                    attempt=1,
                )
            )
        return contracts

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExperimentSpec":
        required = ("schema_version", "experiment_id", "name", "dataset_manifest_path", "strategy_name", "base_seed")
        missing = [k for k in required if k not in data]
        if missing:
            raise ContractValidationError(
                f"ExperimentSpec missing required fields: {missing}",
                code="MISSING_FIELDS",
                details={"missing": missing},
            )
        budget = ResourceBudget.from_dict(data.get("budget", {}))
        cost_model = CostModel.from_dict(data.get("cost_model", {}))
        explicit = tuple(dict(t) for t in data.get("explicit_trials", ()))
        grid = {str(k): list(v) for k, v in data.get("parameter_grid", {}).items()}
        return cls(
            schema_version=str(data["schema_version"]),
            experiment_id=str(data["experiment_id"]),
            name=str(data["name"]),
            dataset_manifest_path=str(data["dataset_manifest_path"]),
            strategy_name=str(data["strategy_name"]),
            base_seed=int(data["base_seed"]),
            budget=budget,
            parameter_grid=grid,
            explicit_trials=explicit,
            expected_dataset_sha256=data.get("expected_dataset_sha256"),
            evaluation_as_of=data.get("evaluation_as_of"),
            max_retries=int(data.get("max_retries", 2)),
            retry_backoff_seconds=float(data.get("retry_backoff_seconds", 0.01)),
            cost_model=cost_model,
            description=str(data.get("description", "")),
            spec_sha256=str(data.get("spec_sha256", "")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "experiment_id": self.experiment_id,
            "name": self.name,
            "description": self.description,
            "dataset_manifest_path": self.dataset_manifest_path,
            "expected_dataset_sha256": self.expected_dataset_sha256,
            "strategy_name": self.strategy_name,
            "base_seed": self.base_seed,
            "evaluation_as_of": self.evaluation_as_of,
            "max_retries": self.max_retries,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "budget": self.budget.to_dict(),
            "cost_model": self.cost_model.to_dict(),
            "parameter_grid": self.parameter_grid,
            "explicit_trials": list(self.explicit_trials),
            "spec_sha256": self.spec_sha256,
        }


@dataclass(frozen=True)
class JobContract:
    """Immutable single-trial execution contract with content-addressed input hash."""

    schema_version: str
    trial_id: str
    experiment_id: str
    experiment_sha256: str
    dataset_sha256: str
    trial_index: int
    strategy_name: str
    params: Dict[str, Any]
    seed: int
    evaluation_as_of: Optional[str] = None
    attempt: int = 1
    input_sha256: str = ""

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _require_positive_int(self.attempt, "attempt", min_value=1)
        if not self.trial_id:
            raise ContractValidationError("trial_id must be non-empty", code="INVALID_TRIAL_ID")
        computed = self.compute_input_hash()
        if self.input_sha256 and self.input_sha256 != computed:
            raise ContractValidationError(
                f"JobContract input_sha256 mismatch: {self.input_sha256} != {computed}",
                code="INPUT_HASH_MISMATCH",
            )
        if not self.input_sha256:
            object.__setattr__(self, "input_sha256", computed)

    def compute_input_hash(self) -> str:
        return sha256_canonical(
            {
                "schema_version": self.schema_version,
                "experiment_sha256": self.experiment_sha256,
                "dataset_sha256": self.dataset_sha256,
                "trial_index": self.trial_index,
                "strategy_name": self.strategy_name,
                "params": self.params,
                "seed": self.seed,
                "evaluation_as_of": self.evaluation_as_of,
            }
        )

    @property
    def idempotency_key(self) -> str:
        return f"{self.experiment_sha256[:12]}:{self.dataset_sha256[:12]}:{self.trial_id}"

    def with_attempt(self, next_attempt: int) -> "JobContract":
        return JobContract(
            schema_version=self.schema_version,
            trial_id=self.trial_id,
            experiment_id=self.experiment_id,
            experiment_sha256=self.experiment_sha256,
            dataset_sha256=self.dataset_sha256,
            trial_index=self.trial_index,
            strategy_name=self.strategy_name,
            params=dict(self.params),
            seed=self.seed,
            evaluation_as_of=self.evaluation_as_of,
            attempt=next_attempt,
            input_sha256=self.input_sha256,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobContract":
        return cls(
            schema_version=str(data["schema_version"]),
            trial_id=str(data["trial_id"]),
            experiment_id=str(data["experiment_id"]),
            experiment_sha256=str(data["experiment_sha256"]),
            dataset_sha256=str(data["dataset_sha256"]),
            trial_index=int(data["trial_index"]),
            strategy_name=str(data["strategy_name"]),
            params=dict(data["params"]),
            seed=int(data["seed"]),
            evaluation_as_of=data.get("evaluation_as_of"),
            attempt=int(data.get("attempt", 1)),
            input_sha256=str(data.get("input_sha256", "")),
        )


@dataclass(frozen=True)
class TrialMetrics:
    """Deterministic quantitative performance metrics for one completed trial."""

    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    final_equity: float
    turnover: float
    bars_evaluated: int
    mc_sharpe_p05: Optional[float] = None
    mc_sharpe_p95: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TrialMetrics":
        return cls(
            total_return=float(data["total_return"]),
            annualized_return=float(data["annualized_return"]),
            annualized_volatility=float(data["annualized_volatility"]),
            sharpe_ratio=float(data["sharpe_ratio"]),
            max_drawdown=float(data["max_drawdown"]),
            win_rate=float(data["win_rate"]),
            trade_count=int(data["trade_count"]),
            final_equity=float(data["final_equity"]),
            turnover=float(data["turnover"]),
            bars_evaluated=int(data["bars_evaluated"]),
            mc_sharpe_p05=None if data.get("mc_sharpe_p05") is None else float(data["mc_sharpe_p05"]),
            mc_sharpe_p95=None if data.get("mc_sharpe_p95") is None else float(data["mc_sharpe_p95"]),
        )


@dataclass(frozen=True)
class ArtifactRecord:
    """Immutable verified artifact pointer produced by a completed trial."""

    trial_id: str
    input_sha256: str
    artifact_sha256: str
    relative_path: str
    size_bytes: int
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ArtifactRecord":
        return cls(
            trial_id=str(data["trial_id"]),
            input_sha256=str(data["input_sha256"]),
            artifact_sha256=str(data["artifact_sha256"]),
            relative_path=str(data["relative_path"]),
            size_bytes=int(data["size_bytes"]),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
        )


@dataclass(frozen=True)
class TrialResult:
    """Result envelope returned by a worker adapter for a single attempt."""

    trial_id: str
    trial_index: int
    attempt: int
    status: TrialStatus
    worker_id: str
    input_sha256: str
    params: Dict[str, Any]
    seed: int
    started_at: str
    completed_at: str
    cpu_seconds: float
    wall_seconds: float
    peak_memory_mb: float
    estimated_cost_usd: float = 0.0
    metrics: Optional[TrialMetrics] = None
    artifact: Optional[ArtifactRecord] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retryable: bool = False
    from_cache: bool = False

    def logical_payload(self) -> Dict[str, Any]:
        """Return strictly deterministic, backend-independent logical fields for parity hashing."""
        return {
            "trial_id": self.trial_id,
            "trial_index": self.trial_index,
            "status": self.status.value,
            "input_sha256": self.input_sha256,
            "params": self.params,
            "seed": self.seed,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "artifact_sha256": self.artifact.artifact_sha256 if self.artifact else None,
            "error_code": self.error_code,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trial_id": self.trial_id,
            "trial_index": self.trial_index,
            "attempt": self.attempt,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "input_sha256": self.input_sha256,
            "params": self.params,
            "seed": self.seed,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "cpu_seconds": self.cpu_seconds,
            "wall_seconds": self.wall_seconds,
            "peak_memory_mb": self.peak_memory_mb,
            "estimated_cost_usd": self.estimated_cost_usd,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "artifact": self.artifact.to_dict() if self.artifact else None,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "retryable": self.retryable,
            "from_cache": self.from_cache,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TrialResult":
        return cls(
            trial_id=str(data["trial_id"]),
            trial_index=int(data["trial_index"]),
            attempt=int(data["attempt"]),
            status=TrialStatus(str(data["status"])),
            worker_id=str(data["worker_id"]),
            input_sha256=str(data["input_sha256"]),
            params=dict(data["params"]),
            seed=int(data["seed"]),
            started_at=str(data["started_at"]),
            completed_at=str(data["completed_at"]),
            cpu_seconds=float(data["cpu_seconds"]),
            wall_seconds=float(data["wall_seconds"]),
            peak_memory_mb=float(data["peak_memory_mb"]),
            estimated_cost_usd=float(data.get("estimated_cost_usd", 0.0)),
            metrics=TrialMetrics.from_dict(data["metrics"]) if data.get("metrics") else None,
            artifact=ArtifactRecord.from_dict(data["artifact"]) if data.get("artifact") else None,
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            retryable=bool(data.get("retryable", False)),
            from_cache=bool(data.get("from_cache", False)),
        )


@dataclass(frozen=True)
class ExperimentSummary:
    """Deterministic top-level summary emitted for every experiment run."""

    schema_version: str
    run_id: str
    experiment_id: str
    experiment_sha256: str
    dataset_id: str
    dataset_version: str
    dataset_sha256: str
    backend: str
    status: RunStatus
    started_at: str
    completed_at: str
    total_trials: int
    succeeded_trials: int
    failed_trials: int
    cancelled_trials: int
    skipped_budget_trials: int
    duplicate_completed_trials: int
    aggregate_sha256: str
    best_trial_id: Optional[str]
    best_sharpe_ratio: Optional[float]
    mean_sharpe_ratio: Optional[float]
    mean_total_return: Optional[float]
    total_cpu_seconds: float
    total_wall_seconds: float
    peak_memory_mb: float
    total_estimated_cost_usd: float
    dataset_completeness_ratio: float
    dataset_freshness_lag_seconds: float
    termination_reason: Optional[str]
    counters: Dict[str, int]
    environment: Dict[str, Any]
    trials: Tuple[TrialResult, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "experiment_id": self.experiment_id,
            "experiment_sha256": self.experiment_sha256,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "dataset_sha256": self.dataset_sha256,
            "backend": self.backend,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_trials": self.total_trials,
            "succeeded_trials": self.succeeded_trials,
            "failed_trials": self.failed_trials,
            "cancelled_trials": self.cancelled_trials,
            "skipped_budget_trials": self.skipped_budget_trials,
            "duplicate_completed_trials": self.duplicate_completed_trials,
            "aggregate_sha256": self.aggregate_sha256,
            "best_trial_id": self.best_trial_id,
            "best_sharpe_ratio": self.best_sharpe_ratio,
            "mean_sharpe_ratio": self.mean_sharpe_ratio,
            "mean_total_return": self.mean_total_return,
            "total_cpu_seconds": self.total_cpu_seconds,
            "total_wall_seconds": self.total_wall_seconds,
            "peak_memory_mb": self.peak_memory_mb,
            "total_estimated_cost_usd": self.total_estimated_cost_usd,
            "dataset_completeness_ratio": self.dataset_completeness_ratio,
            "dataset_freshness_lag_seconds": self.dataset_freshness_lag_seconds,
            "termination_reason": self.termination_reason,
            "counters": dict(self.counters),
            "environment": dict(self.environment),
            "trials": [t.to_dict() for t in self.trials],
        }
