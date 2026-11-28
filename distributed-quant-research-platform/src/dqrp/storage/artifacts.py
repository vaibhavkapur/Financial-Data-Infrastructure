"""Content-addressed durable artifact store with atomic commit and corruption quarantine."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from dqrp.contracts import (
    SCHEMA_VERSION,
    ArtifactRecord,
    JobContract,
    TrialMetrics,
    utc_now_iso,
)
from dqrp.errors import ArtifactIntegrityError
from dqrp.hashing import canonical_json, sha256_bytes


@dataclass(frozen=True)
class VerifiedArtifact:
    """Deserialized and integrity-verified trial artifact."""

    record: ArtifactRecord
    metrics: TrialMetrics
    equity_curve: Tuple[Tuple[str, float], ...]
    payload: Dict[str, Any]


class ArtifactStore:
    """Manages immutable per-trial JSON artifacts with atomic write-and-verify.

    Commit protocol:
      1. Serialize canonical payload bytes and compute SHA-256.
      2. Write payload to `<trial_id>.attempt-<n>.tmp`, flush & fsync.
      3. Atomically replace `<trial_id>.json` and write `<trial_id>.sha256`.
      4. Verify the committed artifact before returning an ArtifactRecord.

    Any partial/truncated `.tmp` artifact or committed file whose SHA-256 does not
    match its sidecar is quarantined and rejected with ArtifactIntegrityError.
    """

    def __init__(self, root_dir: Union[str, Path]) -> None:
        self.root_dir = Path(root_dir)
        self.trials_dir = self.root_dir / "trials"
        self.quarantine_dir = self.root_dir / "quarantine"
        self.trials_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def artifact_path(self, trial_id: str) -> Path:
        return self.trials_dir / f"{trial_id}.json"

    def sidecar_path(self, trial_id: str) -> Path:
        return self.trials_dir / f"{trial_id}.sha256"

    def temp_path(self, trial_id: str, attempt: int) -> Path:
        return self.trials_dir / f"{trial_id}.attempt-{attempt}.tmp"

    def sweep_partial_temp_files(self) -> List[str]:
        """Quarantine any leftover `.tmp` files from crashed workers."""
        quarantined: List[str] = []
        for tmp_file in sorted(self.trials_dir.glob("*.tmp")):
            dest = self.quarantine_dir / f"{tmp_file.name}.{utc_now_iso().replace(':', '-')}"
            os.replace(tmp_file, dest)
            quarantined.append(tmp_file.name)
        return quarantined

    def quarantine_file(self, path: Path, reason: str) -> Optional[Path]:
        if not path.exists():
            return None
        dest = self.quarantine_dir / f"{path.name}.{reason}"
        os.replace(path, dest)
        return dest

    def write_partial_corrupt_artifact(self, contract: JobContract) -> Path:
        """Test/fault-injection helper: write a truncated partial artifact without valid commit."""
        tmp = self.temp_path(contract.trial_id, contract.attempt)
        tmp.write_bytes(b'{"schema_version":"dqrp.v1","trial_id":"' + contract.trial_id.encode("utf-8") + b'","trunc')
        # Also simulate an unverified partial target file with mismatched sidecar
        target = self.artifact_path(contract.trial_id)
        sidecar = self.sidecar_path(contract.trial_id)
        target.write_bytes(b'{"partial":true,')
        sidecar.write_text("0" * 64 + "\n", encoding="utf-8")
        return target

    def commit_trial_artifact(
        self,
        contract: JobContract,
        *,
        metrics: TrialMetrics,
        equity_curve: Tuple[Tuple[str, float], ...],
    ) -> ArtifactRecord:
        """Atomically write and SHA-256 seal an immutable trial artifact."""
        payload = {
            "schema_version": SCHEMA_VERSION,
            "trial_id": contract.trial_id,
            "experiment_id": contract.experiment_id,
            "experiment_sha256": contract.experiment_sha256,
            "dataset_sha256": contract.dataset_sha256,
            "input_sha256": contract.input_sha256,
            "trial_index": contract.trial_index,
            "strategy_name": contract.strategy_name,
            "params": contract.params,
            "seed": contract.seed,
            "metrics": metrics.to_dict(),
            "equity_curve": [[ts, eq] for ts, eq in equity_curve],
        }
        raw_bytes = canonical_json(payload).encode("utf-8")
        digest = sha256_bytes(raw_bytes)

        tmp_data = self.temp_path(contract.trial_id, contract.attempt)
        tmp_sha = self.trials_dir / f"{contract.trial_id}.attempt-{contract.attempt}.sha256.tmp"

        with tmp_data.open("wb") as fh:
            fh.write(raw_bytes)
            fh.flush()
            os.fsync(fh.fileno())

        with tmp_sha.open("w", encoding="utf-8") as fh:
            fh.write(digest + "\n")
            fh.flush()
            os.fsync(fh.fileno())

        target_data = self.artifact_path(contract.trial_id)
        target_sha = self.sidecar_path(contract.trial_id)
        os.replace(tmp_data, target_data)
        os.replace(tmp_sha, target_sha)

        # Immediately verify on read to guarantee integrity before returning
        verified = self.verify_and_load(contract)
        return verified.record

    def verify_and_load(self, contract: JobContract) -> VerifiedArtifact:
        """Load and verify a committed artifact; quarantine and raise on corruption."""
        target_data = self.artifact_path(contract.trial_id)
        target_sha = self.sidecar_path(contract.trial_id)

        if not target_data.exists() or not target_sha.exists():
            # Clean up half-written pair if one exists without the other
            if target_data.exists() and not target_sha.exists():
                self.quarantine_file(target_data, "missing-sidecar")
            if target_sha.exists() and not target_data.exists():
                self.quarantine_file(target_sha, "orphan-sidecar")
            raise ArtifactIntegrityError(
                f"Incomplete or missing artifact files for trial {contract.trial_id}",
                details={"trial_id": contract.trial_id},
            )

        raw_bytes = target_data.read_bytes()
        expected_sha = target_sha.read_text(encoding="utf-8").strip()
        actual_sha = sha256_bytes(raw_bytes)

        if len(expected_sha) != 64 or actual_sha != expected_sha:
            self.quarantine_file(target_data, "hash-mismatch")
            self.quarantine_file(target_sha, "hash-mismatch")
            raise ArtifactIntegrityError(
                f"Artifact SHA-256 mismatch for trial {contract.trial_id}: "
                f"expected {expected_sha}, actual {actual_sha}",
                details={"trial_id": contract.trial_id, "expected": expected_sha, "actual": actual_sha},
            )

        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self.quarantine_file(target_data, "corrupt-json")
            self.quarantine_file(target_sha, "corrupt-json")
            raise ArtifactIntegrityError(
                f"Artifact JSON corrupted for trial {contract.trial_id}: {exc}",
                details={"trial_id": contract.trial_id},
            ) from exc

        if (
            not isinstance(payload, dict)
            or payload.get("trial_id") != contract.trial_id
            or payload.get("input_sha256") != contract.input_sha256
            or payload.get("schema_version") != SCHEMA_VERSION
        ):
            self.quarantine_file(target_data, "contract-mismatch")
            self.quarantine_file(target_sha, "contract-mismatch")
            raise ArtifactIntegrityError(
                f"Artifact metadata mismatch for trial {contract.trial_id}",
                details={"trial_id": contract.trial_id},
            )

        metrics = TrialMetrics.from_dict(payload["metrics"])
        equity_curve = tuple((str(p[0]), float(p[1])) for p in payload.get("equity_curve", []))
        rel_path = str(target_data.relative_to(self.root_dir))
        record = ArtifactRecord(
            trial_id=contract.trial_id,
            input_sha256=contract.input_sha256,
            artifact_sha256=actual_sha,
            relative_path=rel_path,
            size_bytes=len(raw_bytes),
            schema_version=SCHEMA_VERSION,
        )
        return VerifiedArtifact(record=record, metrics=metrics, equity_curve=equity_curve, payload=payload)

    def try_load_cached(self, contract: JobContract) -> Optional[VerifiedArtifact]:
        """Return verified artifact if present and valid; return None if absent or quarantined."""
        target_data = self.artifact_path(contract.trial_id)
        if not target_data.exists():
            return None
        try:
            return self.verify_and_load(contract)
        except ArtifactIntegrityError:
            return None
