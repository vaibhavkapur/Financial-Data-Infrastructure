"""M4 boundary/failure & operational tests: chain snapshot consistency, missing config, restart/rerun idempotency."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle, parse_option_quote
from omdp.config import ConfigurationError, StorageConfig, load_config, parse_config_dict
from omdp.domain.contracts import SnapshotConsistencyError, parse_utc_timestamp
from omdp.service.platform import OptionsMarketDataPlatform


class TestSnapshotConsistencyAndOperations(unittest.TestCase):
    def setUp(self) -> None:
        cfg = load_config(ROOT / "config" / "default_config.json")
        self.config = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))
        self.consistency_raw = json.loads(
            (ROOT / "data" / "fixtures" / "consistency_scenarios.json").read_text(encoding="utf-8")
        )["scenarios"]

    def test_mixed_underlying_snapshot_raises_consistency_error(self) -> None:
        sc = self.consistency_raw["mixed_underlying"]
        quotes = [parse_option_quote(q) for q in sc["quotes"]]
        platform = OptionsMarketDataPlatform(config=self.config)
        try:
            with self.assertRaises(SnapshotConsistencyError) as ctx:
                platform.ingest_chain_snapshot(
                    snapshot_id=sc["snapshot_id"],
                    underlying_symbol=sc["underlying_symbol"],
                    snapshot_time=parse_utc_timestamp(sc["snapshot_time"]),
                    ingestion_time=parse_utc_timestamp(sc["ingestion_time"]),
                    source_id=sc["source_id"],
                    schema_version=sc["schema_version"],
                    quotes=quotes,
                )
            self.assertEqual(ctx.exception.code, "MIXED_UNDERLYING_SNAPSHOT")
            self.assertEqual(platform.counters.consistency_errors, 1)
            self.assertIsNone(platform.get_chain_snapshot(sc["snapshot_id"]))
        finally:
            platform.close()

    def test_duplicate_quote_version_in_snapshot_raises_consistency_error(self) -> None:
        sc = self.consistency_raw["duplicate_quote_version"]
        quotes = [parse_option_quote(q) for q in sc["quotes"]]
        platform = OptionsMarketDataPlatform(config=self.config)
        try:
            with self.assertRaises(SnapshotConsistencyError) as ctx:
                platform.ingest_chain_snapshot(
                    snapshot_id=sc["snapshot_id"],
                    underlying_symbol=sc["underlying_symbol"],
                    snapshot_time=parse_utc_timestamp(sc["snapshot_time"]),
                    ingestion_time=parse_utc_timestamp(sc["ingestion_time"]),
                    source_id=sc["source_id"],
                    schema_version=sc["schema_version"],
                    quotes=quotes,
                )
            self.assertEqual(ctx.exception.code, "DUPLICATE_QUOTE_VERSION")
            self.assertEqual(platform.counters.consistency_errors, 1)
        finally:
            platform.close()

    def test_empty_snapshot_and_mismatched_snapshot_id_rejected(self) -> None:
        platform = OptionsMarketDataPlatform(config=self.config)
        try:
            with self.assertRaises(SnapshotConsistencyError) as ctx_empty:
                platform.ingest_chain_snapshot(
                    snapshot_id="EMPTY-001",
                    underlying_symbol="SYNTH",
                    snapshot_time=datetime(2026, 6, 19, 20, 0, 0, tzinfo=timezone.utc),
                    ingestion_time=datetime(2026, 6, 19, 20, 0, 2, tzinfo=timezone.utc),
                    source_id="test",
                    schema_version="1.0.0",
                    quotes=[],
                )
            self.assertEqual(ctx_empty.exception.code, "EMPTY_SNAPSHOT")

            q = parse_option_quote(self.consistency_raw["mixed_underlying"]["quotes"][0])
            with self.assertRaises(SnapshotConsistencyError) as ctx_sid:
                platform.ingest_chain_snapshot(
                    snapshot_id="DIFFERENT-SNAPSHOT-ID",
                    underlying_symbol="SYNTH",
                    snapshot_time=datetime(2026, 6, 19, 20, 0, 0, tzinfo=timezone.utc),
                    ingestion_time=datetime(2026, 6, 19, 20, 0, 2, tzinfo=timezone.utc),
                    source_id="test",
                    schema_version="1.0.0",
                    quotes=[q],
                )
            self.assertEqual(ctx_sid.exception.code, "SNAPSHOT_ID_MISMATCH")
        finally:
            platform.close()

    def test_missing_and_invalid_configuration_fails_loudly(self) -> None:
        with self.assertRaises(ConfigurationError):
            load_config("/nonexistent/path/config.json")

        with self.assertRaises(ConfigurationError):
            parse_config_dict({"config_version": "1.0.0"})

        bad_backend = self.config.to_canonical_dict()
        bad_backend["storage"]["backend"] = "clickhouse"
        with self.assertRaises(ConfigurationError):
            parse_config_dict(bad_backend)

    def test_restart_and_rerun_idempotency_with_persistent_sqlite(self) -> None:
        bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "reference_chain.json")
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "persistent.sqlite3"
            cfg = replace(self.config, storage=StorageConfig(backend="sqlite", sqlite_path=str(db_path)))

            # First process run
            p1 = OptionsMarketDataPlatform(config=cfg)
            res1 = p1.ingest_bundle(bundle)[0]
            hash1 = res1.snapshot.snapshot_hash
            fids1 = tuple(sorted(f.feature_id for f in res1.features))
            p1.close()

            # Simulate restart: open a fresh platform instance against same SQLite file
            p2 = OptionsMarketDataPlatform(config=cfg)
            try:
                replay = p2.replay_snapshot(res1.snapshot.snapshot_id)
                self.assertTrue(replay.is_identical)
                self.assertEqual(replay.replayed_snapshot_hash, hash1)
                self.assertEqual(replay.replayed_feature_ids, fids1)

                # Rerun ingestion idempotently produces identical hashes
                res2 = p2.ingest_bundle(bundle)[0]
                self.assertEqual(res2.snapshot.snapshot_hash, hash1)
                self.assertEqual(tuple(sorted(f.feature_id for f in res2.features)), fids1)
            finally:
                p2.close()


if __name__ == "__main__":
    unittest.main()
