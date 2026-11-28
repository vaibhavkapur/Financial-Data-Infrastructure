"""Canonical serialization and immutable SHA-256 hashing utilities."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Union

FLOAT_PRECISION_DIGITS = 10


def _normalize_value(value: Any) -> Any:
    """Normalize nested structures for canonical JSON hashing.

    Floats are validated as finite (rejecting NaN/Inf) and normalized to avoid
    signed-zero (-0.0) drift across platforms.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"Non-finite float cannot be canonically hashed: {value!r}")
        if value == 0.0:
            return 0.0
        return round(value, FLOAT_PRECISION_DIGITS)
    if isinstance(value, Mapping):
        return {str(k): _normalize_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(v) for v in value]
    raise TypeError(f"Unsupported type for canonical serialization: {type(value).__name__}")


def canonical_json(payload: Any) -> str:
    """Serialize payload into deterministic, compact UTF-8 JSON."""
    normalized = _normalize_value(payload)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(data: bytes) -> str:
    """Return lowercase hex SHA-256 digest for raw bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_canonical(payload: Any) -> str:
    """Return SHA-256 digest over canonical JSON representation of payload."""
    return sha256_bytes(canonical_json(payload).encode("utf-8"))


def sha256_file(path: Union[str, Path], chunk_size: int = 65536) -> str:
    """Stream-hash a file on disk using SHA-256."""
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def derive_trial_seed(base_seed: int, experiment_sha256: str, trial_index: int) -> int:
    """Derive a deterministic 32-bit unsigned trial seed from base seed and index."""
    digest = sha256_canonical(
        {
            "base_seed": int(base_seed),
            "experiment_sha256": experiment_sha256,
            "trial_index": int(trial_index),
        }
    )
    return int(digest[:8], 16)


def compute_trial_id(
    experiment_sha256: str,
    dataset_sha256: str,
    trial_index: int,
    strategy_name: str,
    params: Mapping[str, Any],
    seed: int,
) -> str:
    """Compute a stable, content-addressed trial identifier."""
    digest = sha256_canonical(
        {
            "experiment_sha256": experiment_sha256,
            "dataset_sha256": dataset_sha256,
            "trial_index": int(trial_index),
            "strategy_name": strategy_name,
            "params": dict(params),
            "seed": int(seed),
        }
    )
    return f"trial-{trial_index:04d}-{digest[:12]}"


def compute_aggregate_hash(ordered_trial_payloads: Iterable[Dict[str, Any]]) -> str:
    """Compute deterministic aggregate SHA-256 over canonically ordered trials."""
    return sha256_canonical(list(ordered_trial_payloads))
