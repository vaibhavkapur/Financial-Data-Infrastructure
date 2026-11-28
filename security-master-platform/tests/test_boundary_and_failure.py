"""M4 boundary and failure tests: historical lookup before observation time and operational edge cases."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sec_master.cli import main as cli_main
from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.errors import NotFoundError, PreObservationLookupError, ValidationError
from sec_master.models import IdentifierNamespace, ReviewAction, parse_ts
from sec_master.service import SecurityMasterService

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestBoundaryAndFailureBehavior(unittest.TestCase):
    def setUp(self) -> None:
        base = load_config(PROJECT_ROOT / "config/default.json")
        self.cfg = PlatformConfig(
            config_version=base.config_version,
            schema_version=base.schema_version,
            environment=base.environment,
            storage=StorageConfig(backend="memory", sqlite_path=":memory:"),
            licensing=base.licensing,
            conflict_resolution=base.conflict_resolution,
            quality=base.quality,
            reproducibility=base.reproducibility,
            http=base.http,
        )

    def test_historical_lookup_before_observation_time_raises_explicit_error(self) -> None:
        svc = SecurityMasterService(self.cfg)
        ref = json.loads(
            (PROJECT_ROOT / "data/fixtures/reference_minimal.json").read_text(encoding="utf-8")
        )
        svc.ingest_raw_records(ref["events"])

        # Instrument is valid from 2020-01-01T00:00:00Z, but was only observed at 2020-01-01T08:00:00Z.
        # Querying at valid=2020-01-01T04:00:00Z, observed=2020-01-01T04:00:00Z MUST raise PreObservationLookupError.
        with self.assertRaises(PreObservationLookupError) as ctx:
            svc.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNYS:ACME",
                parse_ts("2020-01-01T04:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2020-01-01T04:00:00Z"),  # type: ignore[arg-type]
            )
        self.assertEqual(ctx.exception.code, "PRE_OBSERVATION_LOOKUP")
        self.assertEqual(ctx.exception.http_status, 422)
        self.assertEqual(ctx.exception.details["earliest_observed_at"], "2020-01-01T08:00:00Z")

        # Exactly at observation time (2020-01-01T08:00:00Z) succeeds!
        ok = svc.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            "XNYS:ACME",
            parse_ts("2020-01-01T04:00:00Z"),  # type: ignore[arg-type]
            parse_ts("2020-01-01T08:00:00Z"),  # type: ignore[arg-type]
        )
        self.assertEqual(ok.instrument.instrument_id, "SEC-E19954A68605")
        svc.close()

    def test_resolving_missing_or_already_resolved_conflict_fails_explicitly(self) -> None:
        svc = SecurityMasterService(self.cfg)
        with self.assertRaises(NotFoundError):
            svc.resolve_conflict(
                conflict_id="CNF-DOESNOTEXIST",
                reviewer_id="analyst_1",
                action=ReviewAction.KEEP_EXISTING,
                rationale="N/A",
                observed_at=parse_ts("2024-01-01T00:00:00Z"),
            )

        lc = json.loads(
            (PROJECT_ROOT / "data/fixtures/late_correction_conflicts.json").read_text(
                encoding="utf-8"
            )
        )
        outs = svc.ingest_raw_records(lc["events"])
        cid = outs[-1].conflict_id
        self.assertIsNotNone(cid)

        svc.resolve_conflict(
            conflict_id=cid,  # type: ignore[arg-type]
            reviewer_id="analyst_1",
            action=ReviewAction.KEEP_EXISTING,
            rationale="First resolution",
            observed_at=parse_ts("2024-01-13T00:00:00Z"),
        )
        with self.assertRaises(ValidationError):
            svc.resolve_conflict(
                conflict_id=cid,  # type: ignore[arg-type]
                reviewer_id="analyst_2",
                action=ReviewAction.ACCEPT_INCOMING,
                rationale="Duplicate resolution attempt",
                observed_at=parse_ts("2024-01-14T00:00:00Z"),
            )
        svc.close()

    def test_cli_returns_nonzero_on_missing_config(self) -> None:
        rc = cli_main(["--config", "/nonexistent/config.json", "replay"])
        self.assertEqual(rc, 2)

    def test_reproducibility_identical_inputs_yield_identical_snapshot_hash(self) -> None:
        fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/ticker_reuse_dual_listing.json").read_text(
                encoding="utf-8"
            )
        )
        hashes = []
        for _ in range(2):
            svc = SecurityMasterService(self.cfg)
            svc.ingest_raw_records(fixture["events"])
            hashes.append(svc.diagnostics()["snapshot_hash"])
            svc.close()
        self.assertEqual(hashes[0], hashes[1])


if __name__ == "__main__":
    unittest.main()
