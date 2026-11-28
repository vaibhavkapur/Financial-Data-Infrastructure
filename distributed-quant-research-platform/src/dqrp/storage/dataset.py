"""Versioned dataset manifest validation and multi-format bar loading adapters.

Supports JSONL and CSV natively via Python stdlib, and Parquet when optional
dependency `pyarrow` is installed. Enforces immutable partition SHA-256 verification,
point-in-time invariants, and publishes completeness and freshness lag metrics.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Tuple, Union

from dqrp.contracts import (
    DatasetManifest,
    DatasetPartition,
    MarketBar,
    parse_utc_timestamp,
)
from dqrp.errors import (
    BackendUnavailableError,
    ContractValidationError,
    UnsupportedOperationError,
)
from dqrp.hashing import sha256_file


@dataclass(frozen=True)
class LoadedDataset:
    """Validated dataset bars paired with lineage and quality metrics."""

    manifest: DatasetManifest
    manifest_path: Path
    bars: Tuple[MarketBar, ...]
    completeness_ratio: float
    freshness_lag_seconds: float
    verified_partition_hashes: Dict[str, str]


def load_manifest(manifest_path: Union[str, Path]) -> DatasetManifest:
    """Read and contract-validate a JSON DatasetManifest from disk."""
    path = Path(manifest_path)
    if not path.exists():
        raise ContractValidationError(
            f"Dataset manifest not found: {path}",
            code="MANIFEST_NOT_FOUND",
            details={"path": str(path)},
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ContractValidationError(
            f"Invalid JSON in dataset manifest {path}: {exc}",
            code="INVALID_JSON",
            details={"path": str(path)},
        ) from exc
    if not isinstance(raw, dict):
        raise ContractValidationError(
            f"Dataset manifest must be a JSON object: {path}",
            code="INVALID_MANIFEST_TYPE",
        )
    return DatasetManifest.from_dict(raw)


def _read_jsonl_bars(file_path: Path) -> List[MarketBar]:
    bars: List[MarketBar] = []
    with file_path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ContractValidationError(
                    f"Invalid JSONL at {file_path}:{line_no}: {exc}",
                    code="INVALID_BAR_RECORD",
                ) from exc
            if not isinstance(obj, dict):
                raise ContractValidationError(
                    f"JSONL row must be an object at {file_path}:{line_no}",
                    code="INVALID_BAR_RECORD",
                )
            bars.append(MarketBar.from_dict(obj))
    return bars


def _read_csv_bars(file_path: Path) -> List[MarketBar]:
    bars: List[MarketBar] = []
    with file_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            bars.append(MarketBar.from_dict(row))
    return bars


def _read_parquet_bars(file_path: Path) -> List[MarketBar]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise BackendUnavailableError(
            "Parquet dataset loading requires optional dependency 'pyarrow'. "
            "Install with: pip install -e '.[parquet]'",
            code="PARQUET_BACKEND_UNAVAILABLE",
            details={"file": str(file_path)},
        ) from exc
    table = pq.read_table(file_path)
    return [MarketBar.from_dict(row) for row in table.to_pylist()]


def _read_partition_bars(file_path: Path, fmt: str) -> List[MarketBar]:
    if fmt == "jsonl":
        return _read_jsonl_bars(file_path)
    if fmt == "csv":
        return _read_csv_bars(file_path)
    if fmt == "parquet":
        return _read_parquet_bars(file_path)
    raise UnsupportedOperationError(
        f"Unsupported dataset format: {fmt!r}",
        code="UNSUPPORTED_FORMAT",
    )


def _verify_partition(base_dir: Path, part: DatasetPartition, fmt: str) -> Tuple[List[MarketBar], str]:
    file_path = (base_dir / part.relative_path).resolve()
    try:
        file_path.relative_to(base_dir.resolve())
    except ValueError as exc:
        raise ContractValidationError(
            f"Partition path escapes manifest directory: {part.relative_path}",
            code="INVALID_PARTITION_PATH",
        ) from exc

    if not file_path.exists():
        raise ContractValidationError(
            f"Dataset partition file missing: {file_path}",
            code="PARTITION_MISSING",
            details={"partition_id": part.partition_id, "path": str(file_path)},
        )

    actual_size = file_path.stat().st_size
    if actual_size != part.size_bytes:
        raise ContractValidationError(
            f"Partition '{part.partition_id}' size mismatch: expected {part.size_bytes}, got {actual_size}",
            code="PARTITION_SIZE_MISMATCH",
            details={"partition_id": part.partition_id, "expected": part.size_bytes, "actual": actual_size},
        )

    actual_hash = sha256_file(file_path)
    if actual_hash != part.content_sha256:
        raise ContractValidationError(
            f"Partition '{part.partition_id}' SHA-256 mismatch: expected {part.content_sha256}, got {actual_hash}",
            code="PARTITION_HASH_MISMATCH",
            details={"partition_id": part.partition_id, "expected": part.content_sha256, "actual": actual_hash},
        )

    bars = _read_partition_bars(file_path, fmt)
    if len(bars) != part.row_count:
        raise ContractValidationError(
            f"Partition '{part.partition_id}' row count mismatch: expected {part.row_count}, got {len(bars)}",
            code="PARTITION_ROW_COUNT_MISMATCH",
            details={"partition_id": part.partition_id, "expected": part.row_count, "actual": len(bars)},
        )
    return bars, actual_hash


def validate_and_load_dataset(manifest_path: Union[str, Path]) -> LoadedDataset:
    """Validate manifest, verify every partition SHA-256, load bars, and compute quality metrics."""
    path = Path(manifest_path).resolve()
    manifest = load_manifest(path)
    base_dir = path.parent

    all_bars: List[MarketBar] = []
    verified_hashes: Dict[str, str] = {}
    max_freshness_lag = 0.0

    for part in sorted(manifest.partitions, key=lambda p: p.partition_id):
        part_bars, actual_hash = _verify_partition(base_dir, part, manifest.format)
        verified_hashes[part.partition_id] = actual_hash
        for bar in part_bars:
            ev_dt = parse_utc_timestamp(bar.event_time, field_name="event_time")
            av_dt = parse_utc_timestamp(bar.availability_time, field_name="availability_time")
            lag = (av_dt - ev_dt).total_seconds()
            if lag > max_freshness_lag:
                max_freshness_lag = lag
        all_bars.extend(part_bars)

    completeness = min(1.0, len(all_bars) / float(manifest.expected_row_count))
    return LoadedDataset(
        manifest=manifest,
        manifest_path=path,
        bars=tuple(all_bars),
        completeness_ratio=round(completeness, 8),
        freshness_lag_seconds=round(max_freshness_lag, 6),
        verified_partition_hashes=verified_hashes,
    )
