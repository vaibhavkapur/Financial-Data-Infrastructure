"""M2 pure domain unit tests: ticker reuse, dual listing, ambiguity guard, and bitemporal slicing."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from sec_master.adapters import raw_record_to_envelope
from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.domain import BitemporalStateEngine
from sec_master.errors import AmbiguousMappingError, NotFoundError, ValidationError
from sec_master.models import ConflictStatus, IdentifierNamespace, parse_ts

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestDomainBitemporal(unittest.TestCase):
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
        self.engine = BitemporalStateEngine(self.cfg)
        fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/ticker_reuse_dual_listing.json").read_text(
                encoding="utf-8"
            )
        )
        for raw in fixture["events"]:
            out = self.engine.apply_event(raw_record_to_envelope(raw))
            self.assertEqual(out.status, "APPLIED")

    def test_ticker_reuse_resolves_distinct_instruments_by_valid_time(self) -> None:
        obs = parse_ts("2024-01-01T00:00:00Z")

        # Era 1: 2018-01-01 to 2022-07-01 -> Synthetic Acme Legacy
        res_2020 = self.engine.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            "XNYS:ACME",
            parse_ts("2020-06-01T00:00:00Z"),  # type: ignore[arg-type]
            obs,  # type: ignore[arg-type]
        )
        self.assertEqual(res_2020.instrument.instrument_id, "SEC-E19954A68605")
        self.assertEqual(res_2020.instrument.name, "Synthetic Acme Legacy Corp")
        self.assertEqual(res_2020.listing.listing_id, "LST-19D2D62E8597")  # type: ignore[union-attr]

        # Gap period (2022-09-01): neither issuer holds XNYS:ACME
        with self.assertRaises(NotFoundError):
            self.engine.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNYS:ACME",
                parse_ts("2022-09-01T00:00:00Z"),  # type: ignore[arg-type]
                obs,  # type: ignore[arg-type]
            )

        # Era 2: 2023-01-01 onward -> Synthetic Acme Robotics
        res_2023 = self.engine.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            "XNYS:ACME",
            parse_ts("2023-06-01T00:00:00Z"),  # type: ignore[arg-type]
            obs,  # type: ignore[arg-type]
        )
        self.assertEqual(res_2023.instrument.instrument_id, "SEC-B12CCB780542")
        self.assertEqual(res_2023.instrument.name, "Synthetic Acme Robotics Inc")
        self.assertEqual(res_2023.listing.listing_id, "LST-4795D4729D86")  # type: ignore[union-attr]
        self.assertNotEqual(res_2020.instrument.instrument_id, res_2023.instrument.instrument_id)

    def test_overlapping_ticker_reuse_is_quarantined_never_silently_resolved(self) -> None:
        # Attempt to assign XNYS:ACME to a third listing during 2023-06-01 (overlaps Robotics!)
        collision = raw_record_to_envelope(
            {
                "event_type": "LISTING_UPSERT",
                "schema_version": "1.0.0",
                "source_system": "VENDOR_ALPHA",
                "source_event_id": "COLLIDE-ACME-1",
                "valid_from": "2023-05-01T00:00:00Z",
                "valid_to": None,
                "observed_at": "2023-05-02T00:00:00Z",
                "payload": {
                    "listing_id": "LST-6EDB8EF1CE87",
                    "instrument_id": "SEC-E19954A68605",
                    "mic": "XNYS",
                    "ticker": "ACME",
                    "currency": "USD",
                    "is_primary": False,
                    "status": "ACTIVE",
                },
            }
        )
        outcome = self.engine.apply_event(collision)
        self.assertEqual(outcome.status, "QUARANTINED")
        self.assertIsNotNone(outcome.conflict_id)
        ticket = self.engine.conflicts[outcome.conflict_id]  # type: ignore[index]
        self.assertEqual(ticket.conflict_type, "LISTING_SYMBOL_COLLISION")
        self.assertEqual(ticket.status, ConflictStatus.OPEN)

    def test_dual_listing_requires_mic_and_shares_canonical_instrument(self) -> None:
        t = parse_ts("2023-06-01T00:00:00Z")

        # Bare ticker ORION is ambiguous across XNYS and XLON -> MUST raise AmbiguousMappingError
        with self.assertRaises(AmbiguousMappingError) as ctx:
            self.engine.lookup_as_of(IdentifierNamespace.TICKER, "ORION", t, t)  # type: ignore[arg-type]
        candidates = ctx.exception.details["candidates"]
        self.assertEqual(len(candidates), 2)
        self.assertEqual({c["mic"] for c in candidates}, {"XNYS", "XLON"})

        # Disambiguating by MIC resolves each listing cleanly to the SAME canonical instrument
        ny = self.engine.lookup_as_of(IdentifierNamespace.TICKER, "ORION", t, t, mic="XNYS")  # type: ignore[arg-type]
        ln = self.engine.lookup_as_of(IdentifierNamespace.TICKER, "ORION", t, t, mic="XLON")  # type: ignore[arg-type]
        self.assertEqual(ny.instrument.instrument_id, "SEC-0C62E574E753")
        self.assertEqual(ln.instrument.instrument_id, "SEC-0C62E574E753")
        self.assertNotEqual(ny.listing.listing_id, ln.listing.listing_id)  # type: ignore[union-attr]
        self.assertEqual(ny.listing.currency, "USD")  # type: ignore[union-attr]
        self.assertEqual(ln.listing.currency, "GBP")  # type: ignore[union-attr]

        # Instrument-level listings query returns both venues
        listings = self.engine.get_listings_for_instrument("SEC-0C62E574E753", t, t)  # type: ignore[arg-type]
        self.assertEqual(len(listings), 2)
        self.assertTrue(listings[0].is_primary)
        self.assertEqual(listings[0].mic, "XNYS")
        self.assertEqual(listings[1].mic, "XLON")

    def test_idempotent_duplicate_skip_and_mutation_rejection(self) -> None:
        fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/ticker_reuse_dual_listing.json").read_text(
                encoding="utf-8"
            )
        )
        dup = raw_record_to_envelope(fixture["events"][0])
        out = self.engine.apply_event(dup)
        self.assertEqual(out.status, "DUPLICATE_SKIPPED")

        mutated = dict(fixture["events"][0])
        mutated["payload"] = dict(mutated["payload"], name="Tampered Name")
        with self.assertRaises(ValidationError):
            self.engine.apply_event(raw_record_to_envelope(mutated))


if __name__ == "__main__":
    unittest.main()
