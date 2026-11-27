"""Verified fixture loader enforcing manifest checksums, licenses, and provenance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from financial_data_lake.errors import FixtureVerificationError


@dataclass(frozen=True)
class VerifiedFixture:
    file_name: str
    path: Path
    dataset_id: str
    format: str
    source_vendor: str
    license_id: str
    provenance: str
    record_count: int
    byte_size: int
    sha256: str
    raw_bytes: bytes

    def parse_jsonl(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in self.raw_bytes.decode("utf-8").splitlines():
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
        return rows


class FixtureLoader:
    """Loads and cryptographically verifies synthetic/permitted fixture files against manifest.json."""

    def __init__(self, fixtures_dir: Path | str) -> None:
        self.fixtures_dir = Path(fixtures_dir).resolve()
        self.manifest_path = self.fixtures_dir / "manifest.json"
        if not self.manifest_path.exists():
            raise FixtureVerificationError(
                f"Fixture manifest not found at '{self.manifest_path}'.",
                context={"fixtures_dir": str(self.fixtures_dir)},
            )
        self._manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def list_entries(self) -> list[dict[str, Any]]:
        return list(self._manifest.get("fixtures", []))

    def load_verified(self, file_name: str) -> VerifiedFixture:
        entry = next(
            (e for e in self._manifest.get("fixtures", []) if e["file"] == file_name),
            None,
        )
        if entry is None:
            raise FixtureVerificationError(
                f"Fixture '{file_name}' is not declared in manifest.json.",
                context={"file_name": file_name},
            )

        path = self.fixtures_dir / file_name
        if not path.exists():
            raise FixtureVerificationError(
                f"Declared fixture file missing: '{path}'.",
                context={"file_name": file_name},
            )

        raw = path.read_bytes()
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != entry["sha256"]:
            raise FixtureVerificationError(
                f"Fixture checksum mismatch for '{file_name}'.",
                context={
                    "file_name": file_name,
                    "expected_sha256": entry["sha256"],
                    "actual_sha256": actual_sha,
                },
            )

        lines = [ln for ln in raw.decode("utf-8").splitlines() if ln.strip()]
        if len(lines) != entry["record_count"]:
            raise FixtureVerificationError(
                f"Fixture row count mismatch for '{file_name}': expected {entry['record_count']}, got {len(lines)}.",
                context={"file_name": file_name},
            )

        return VerifiedFixture(
            file_name=file_name,
            path=path,
            dataset_id=entry["dataset_id"],
            format=entry["format"],
            source_vendor=entry["source_vendor"],
            license_id=entry["license_id"],
            provenance=entry["provenance"],
            record_count=len(lines),
            byte_size=len(raw),
            sha256=actual_sha,
            raw_bytes=raw,
        )

    def verify_all(self) -> list[VerifiedFixture]:
        return [self.load_verified(e["file"]) for e in self.list_entries()]
