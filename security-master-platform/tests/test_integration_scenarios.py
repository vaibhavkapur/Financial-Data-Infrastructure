"""M3 integration tests: late reference correction, conflicting IDs, corporate-action linkage,
durable SQLite restart, and deterministic journal replay.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sec_master.adapters import CorporateActionFeedAdapter
from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.errors import ConflictBlockedError
from sec_master.models import ConflictStatus, IdentifierNamespace, ReviewAction, parse_ts
from sec_master.service import SecurityMasterService
from sec_master.storage import SQLiteBitemporalStore

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestIntegrationScenarios(unittest.TestCase):
    def _make_sqlite_config(self, db_path: Path) -> PlatformConfig:
        base = load_config(PROJECT_ROOT / "config/default.json")
        return PlatformConfig(
            config_version=base.config_version,
            schema_version=base.schema_version,
            environment=base.environment,
            storage=StorageConfig(backend="sqlite", sqlite_path=str(db_path)),
            licensing=base.licensing,
            conflict_resolution=base.conflict_resolution,
            quality=base.quality,
            reproducibility=base.reproducibility,
            http=base.http,
        )

    def test_late_correction_conflicting_ids_and_corporate_action_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sec_master.db"
            cfg = self._make_sqlite_config(db_path)
            svc = SecurityMasterService(cfg)

            lc_fixture = json.loads(
                (PROJECT_ROOT / "data/fixtures/late_correction_conflicts.json").read_text(
                    encoding="utf-8"
                )
            )
            outcomes = svc.ingest_raw_records(lc_fixture["events"])
            self.assertEqual([o.status for o in outcomes[:-1]], ["APPLIED"] * 5)
            self.assertEqual(outcomes[-1].status, "QUARANTINED")
            conflict_id = outcomes[-1].conflict_id
            self.assertIsNotNone(conflict_id)

            # Open conflict blocks ambiguous lookup until reviewed
            with self.assertRaises(ConflictBlockedError) as ctx:
                svc.lookup_as_of(
                    IdentifierNamespace.MIC_TICKER,
                    "XNAS:NOVA",
                    parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
                    parse_ts("2024-01-15T00:00:00Z"),  # type: ignore[arg-type]
                )
            self.assertIn(conflict_id, ctx.exception.details["conflict_ids"])

            # Resolve conflict keeping existing primary exchange ISIN
            svc.resolve_conflict(
                conflict_id=conflict_id,  # type: ignore[arg-type]
                reviewer_id="reviewer_alice",
                action=ReviewAction.KEEP_EXISTING,
                rationale="Primary filing confirms US0000004046.",
                observed_at=parse_ts("2024-01-13T10:00:00Z"),
            )
            self.assertEqual(len(svc.list_conflicts(ConflictStatus.OPEN)), 0)
            self.assertEqual(len(svc.list_conflicts(ConflictStatus.RESOLVED)), 1)

            # Late reference correction: as-of 2024-01-05 sees OLD name; as-of 2024-01-15 sees CORRECTED name
            pre_corr = svc.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNAS:NOVA",
                parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
            )
            post_corr = svc.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNAS:NOVA",
                parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2024-01-15T00:00:00Z"),  # type: ignore[arg-type]
            )
            self.assertEqual(pre_corr.instrument.name, "Synthetic Nova Holding Co")
            self.assertEqual(post_corr.instrument.name, "Synthetic Nova Holdings Inc")
            # Lineage preserved on both versions
            self.assertEqual(pre_corr.instrument.lineage.source_event_id, "LC-INS-001")
            self.assertEqual(post_corr.instrument.lineage.source_event_id, "LC-INS-001-CORR")
            self.assertEqual(
                post_corr.instrument.lineage.supersedes_version_id, pre_corr.instrument.version_id
            )

            # Ingest corporate actions fixture
            ca_fixture = json.loads(
                (PROJECT_ROOT / "data/fixtures/corporate_actions.json").read_text(encoding="utf-8")
            )
            ca_outcomes = svc.ingest_raw_records(ca_fixture["events"])
            self.assertTrue(all(o.status == "APPLIED" for o in ca_outcomes))

            # Ticker rename bitemporal check: NOVA in Feb 2024, NVAX in Apr 2024
            feb = svc.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNAS:NOVA",
                parse_ts("2024-02-15T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2024-05-01T00:00:00Z"),  # type: ignore[arg-type]
            )
            apr = svc.lookup_as_of(
                IdentifierNamespace.MIC_TICKER,
                "XNAS:NVAX",
                parse_ts("2024-04-15T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2024-05-01T00:00:00Z"),  # type: ignore[arg-type]
            )
            self.assertEqual(feb.instrument.instrument_id, apr.instrument.instrument_id)
            self.assertEqual(feb.listing.listing_id, apr.listing.listing_id)  # type: ignore[union-attr]

            # Corporate action lineage traces NOVA -> MERGER -> VERTEX -> SPLIT & SPINOFF(SPCO)
            trace = svc.trace_corporate_actions(
                "SEC-4524211962DF",
                parse_ts("2024-12-01T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2024-12-01T00:00:00Z"),  # type: ignore[arg-type]
            )
            self.assertEqual(trace["terminal_instrument_ids"], ["SEC-8CD5A4F2A1D5"])
            action_types = [a["action_type"] for a in trace["successors"]]
            self.assertIn("TICKER_CHANGE", action_types)
            self.assertIn("MERGER", action_types)
            self.assertIn("SPLIT", action_types)
            self.assertIn("SPINOFF", action_types)

            # Verify durable SQLite table counts, restart rehydration, and deterministic replay
            self.assertIsInstance(svc.store, SQLiteBitemporalStore)
            counts = svc.store.table_counts()  # type: ignore[union-attr]
            self.assertGreater(counts["event_journal"], 10)
            self.assertGreater(counts["corporate_action_links"], 3)

            original_hash = svc.diagnostics()["snapshot_hash"]
            replay = svc.replay_from_journal()
            self.assertEqual(replay["snapshot_hash"], original_hash)
            svc.close()

            restarted = SecurityMasterService(cfg)
            self.assertEqual(restarted.diagnostics()["snapshot_hash"], original_hash)
            restarted.close()

    def test_cross_instrument_isin_collision_quarantined(self) -> None:
        base = load_config(PROJECT_ROOT / "config/default.json")
        cfg = PlatformConfig(
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
        svc = SecurityMasterService(cfg)
        ref = json.loads(
            (PROJECT_ROOT / "data/fixtures/reference_minimal.json").read_text(encoding="utf-8")
        )
        svc.ingest_raw_records(ref["events"])

        # Another vendor assigns the SAME ISIN (US0000001018) to a DIFFERENT canonical instrument
        outcomes = svc.ingest_raw_records(
            [
                {
                    "event_type": "IDENTIFIER_MAP",
                    "schema_version": "1.0.0",
                    "source_system": "VENDOR_BETA",
                    "source_event_id": "COLLIDE-ISIN-1",
                    "valid_from": "2020-06-01T00:00:00Z",
                    "valid_to": None,
                    "observed_at": "2020-06-02T00:00:00Z",
                    "payload": {
                        "instrument_id": "SEC-B12CCB780542",
                        "listing_id": None,
                        "namespace": "ISIN",
                        "identifier_value": "US0000001018",
                        "mic": None,
                    },
                }
            ]
        )
        self.assertEqual(outcomes[0].status, "QUARANTINED")
        tickets = svc.list_conflicts(ConflictStatus.OPEN)
        self.assertEqual(len(tickets), 1)
        self.assertEqual(tickets[0].conflict_type, "IDENTIFIER_IDENTITY_COLLISION")

        with self.assertRaises(ConflictBlockedError):
            svc.lookup_as_of(
                IdentifierNamespace.ISIN,
                "US0000001018",
                parse_ts("2020-07-01T00:00:00Z"),  # type: ignore[arg-type]
                parse_ts("2020-07-01T00:00:00Z"),  # type: ignore[arg-type]
            )
        svc.close()

    def test_corporate_action_feed_adapter(self) -> None:
        adapter = CorporateActionFeedAdapter()
        env = adapter.adapt_action(
            {
                "source_event_id": "FEED-SPLIT-1",
                "action_type": "SPLIT",
                "from_instrument_id": "SEC-E19954A68605",
                "to_instrument_id": "SEC-E19954A68605",
                "ratio": "4.0",
                "effective_at": "2025-01-01T00:00:00Z",
                "observed_at": "2024-12-20T00:00:00Z",
            }
        )
        self.assertEqual(env.source_system, "CORP_ACTION_ENGINE")
        self.assertEqual(env.payload["ratio"], "4.0")


if __name__ == "__main__":
    unittest.main()
