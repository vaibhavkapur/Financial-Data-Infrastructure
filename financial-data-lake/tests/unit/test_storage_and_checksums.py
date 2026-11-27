"""Unit tests for immutable object storage, SHA-256 verification, and quarantine."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from financial_data_lake import CorruptPartitionError, ImmutabilityViolationError
from financial_data_lake.storage import LocalObjectStore


def test_write_once_immutability_and_checksum_verification(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "store")
    obj = store.put_bytes_immutable("raw/test/part1.jsonl", b'{"a":1}\n')
    assert store.verify_object(obj.relative_path, obj.sha256)
    assert store.read_bytes(obj.relative_path, expected_sha256=obj.sha256) == b'{"a":1}\n'

    with pytest.raises(ImmutabilityViolationError):
        store.put_bytes_immutable("raw/test/part1.jsonl", b'{"a":2}\n')

    # Tamper bytes on disk
    obj.absolute_path.write_bytes(b'{"a":999}\n')
    assert not store.verify_object(obj.relative_path, obj.sha256)
    with pytest.raises(CorruptPartitionError, match="Checksum mismatch"):
        store.read_bytes(obj.relative_path, expected_sha256=obj.sha256)


def test_parquet_immutability_and_quarantine(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "store")
    tbl = pa.table({"x": [1, 2, 3]})
    obj = store.put_parquet_immutable("curated/test/v1.parquet", tbl)

    with pytest.raises(ImmutabilityViolationError):
        store.put_parquet_immutable("curated/test/v1.parquet", tbl)

    obj.absolute_path.write_bytes(b"NOT_PARQUET")
    with pytest.raises(CorruptPartitionError):
        store.read_parquet(obj.relative_path, expected_sha256=obj.sha256)

    q_path = store.copy_to_quarantine(obj.relative_path)
    assert store.exists(q_path)
    assert store.read_bytes(q_path) == b"NOT_PARQUET"


def test_path_traversal_rejected(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "store")
    with pytest.raises(ValueError, match="Invalid relative object path"):
        store.resolve_path("../escape.txt")
