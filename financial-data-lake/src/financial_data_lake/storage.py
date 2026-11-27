"""Immutable object storage abstraction with atomic writes, SHA-256 checksums, and integrity checks."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from financial_data_lake.errors import CorruptPartitionError, ImmutabilityViolationError


@dataclass(frozen=True)
class StoredObject:
    relative_path: str
    absolute_path: Path
    sha256: str
    byte_size: int


class ObjectStore(Protocol):
    """Abstract immutable object storage boundary (local FS prototype; swappable for S3/GCS)."""

    def put_bytes_immutable(self, relative_path: str, payload: bytes) -> StoredObject: ...

    def put_parquet_immutable(
        self,
        relative_path: str,
        table: pa.Table,
        *,
        compression: str = "snappy",
    ) -> StoredObject: ...

    def read_bytes(self, relative_path: str, expected_sha256: str | None = None) -> bytes: ...

    def read_parquet(self, relative_path: str, expected_sha256: str | None = None) -> pa.Table: ...

    def verify_object(self, relative_path: str, expected_sha256: str) -> bool: ...

    def exists(self, relative_path: str) -> bool: ...

    def resolve_path(self, relative_path: str) -> Path: ...


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class LocalObjectStore:
    """Local filesystem immutable object store.

    Guarantees:
    - Write-once semantics: attempting to write to an existing path raises ImmutabilityViolationError
      unless the exact same bytes are written (idempotent retry check only when allow_identical=True).
    - Atomic writes via temporary file + os.replace to prevent partial files.
    - SHA-256 verification on read and explicit verify_object calls.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def resolve_path(self, relative_path: str) -> Path:
        clean = Path(relative_path)
        if clean.is_absolute() or ".." in clean.parts:
            raise ValueError(f"Invalid relative object path: {relative_path}")
        resolved = (self.root / clean).resolve()
        if not str(resolved).startswith(str(self.root)):
            raise ValueError(f"Path escapes object store root: {relative_path}")
        return resolved

    def exists(self, relative_path: str) -> bool:
        return self.resolve_path(relative_path).exists()

    def put_bytes_immutable(
        self,
        relative_path: str,
        payload: bytes,
        *,
        allow_identical: bool = False,
    ) -> StoredObject:
        target = self.resolve_path(relative_path)
        digest = sha256_bytes(payload)

        if target.exists():
            existing_digest = sha256_file(target)
            if allow_identical and existing_digest == digest:
                return StoredObject(
                    relative_path=relative_path,
                    absolute_path=target,
                    sha256=existing_digest,
                    byte_size=target.stat().st_size,
                )
            raise ImmutabilityViolationError(
                f"Refusing to overwrite immutable object '{relative_path}'.",
                context={
                    "relative_path": relative_path,
                    "existing_sha256": existing_digest,
                    "attempted_sha256": digest,
                },
            )

        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp_", suffix=".obj")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, target)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

        return StoredObject(
            relative_path=relative_path,
            absolute_path=target,
            sha256=digest,
            byte_size=len(payload),
        )

    def put_parquet_immutable(
        self,
        relative_path: str,
        table: pa.Table,
        *,
        compression: str = "snappy",
    ) -> StoredObject:
        target = self.resolve_path(relative_path)
        if target.exists():
            raise ImmutabilityViolationError(
                f"Refusing to overwrite immutable Parquet partition '{relative_path}'.",
                context={"relative_path": relative_path},
            )

        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp_", suffix=".parquet")
        os.close(fd)
        try:
            pq.write_table(
                table,
                tmp_name,
                compression=None if compression == "none" else compression,
                use_dictionary=True,
                write_statistics=True,
            )
            os.replace(tmp_name, target)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

        digest = sha256_file(target)
        return StoredObject(
            relative_path=relative_path,
            absolute_path=target,
            sha256=digest,
            byte_size=target.stat().st_size,
        )

    def verify_object(self, relative_path: str, expected_sha256: str) -> bool:
        target = self.resolve_path(relative_path)
        if not target.exists():
            return False
        return sha256_file(target) == expected_sha256

    def read_bytes(self, relative_path: str, expected_sha256: str | None = None) -> bytes:
        target = self.resolve_path(relative_path)
        if not target.exists():
            raise CorruptPartitionError(
                f"Object missing from storage: '{relative_path}'.",
                context={"relative_path": relative_path},
            )
        data = target.read_bytes()
        if expected_sha256 is not None:
            actual = sha256_bytes(data)
            if actual != expected_sha256:
                raise CorruptPartitionError(
                    f"Checksum mismatch for '{relative_path}'.",
                    context={
                        "relative_path": relative_path,
                        "expected_sha256": expected_sha256,
                        "actual_sha256": actual,
                    },
                )
        return data

    def read_parquet(self, relative_path: str, expected_sha256: str | None = None) -> pa.Table:
        target = self.resolve_path(relative_path)
        if not target.exists():
            raise CorruptPartitionError(
                f"Parquet partition missing: '{relative_path}'.",
                context={"relative_path": relative_path},
            )
        if expected_sha256 is not None:
            actual = sha256_file(target)
            if actual != expected_sha256:
                raise CorruptPartitionError(
                    f"Parquet partition checksum mismatch for '{relative_path}'.",
                    context={
                        "relative_path": relative_path,
                        "expected_sha256": expected_sha256,
                        "actual_sha256": actual,
                    },
                )
        try:
            return pq.ParquetFile(target).read()
        except Exception as exc:
            raise CorruptPartitionError(
                f"Parquet partition unreadable or corrupt: '{relative_path}' ({exc}).",
                context={"relative_path": relative_path, "reason": str(exc)},
            ) from exc

    def copy_to_quarantine(self, relative_path: str, quarantine_prefix: str = "quarantine") -> str:
        """Preserve a corrupt artifact under quarantine/ for forensic inspection without deleting history."""
        src = self.resolve_path(relative_path)
        quarantine_rel = f"{quarantine_prefix}/{relative_path}"
        dst = self.resolve_path(quarantine_rel)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.exists():
            shutil.copy2(src, dst)
        return quarantine_rel
