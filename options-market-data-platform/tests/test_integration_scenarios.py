"""M3 required integration tests: missing underlying, delayed quotes, stale quotes, versioned revisions, and replay."""

from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import StorageConfig, load_config
from omdp.domain.contracts import DataQualityFlag, IVStatus, ValidationStatus
from omdp.service.platform import OptionsMarketDataPlatform


class TestIntegrationScenarios(unittest.TestCase):
    def setUp(self) -> None:
        cfg = load_config(ROOT / "config" / "default_config.json")
        self.config = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))

    def test_missing_underlying_and_delayed_or_stale_quotes(self) -> None:
        bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "integration_scenarios.json")
        platform = OptionsMarketDataPlatform(config=self.config)
        try:
            results = platform.ingest_bundle(bundle)
            self.assertEqual(len(results), 2)

            # Scenario A: Missing underlying (NOUND)
            nound_res = results[0]
            self.assertEqual(nound_res.snapshot.underlying_symbol, "NOUND")
            self.assertEqual(len(nound_res.features), 1)
            nound_feat = nound_res.features[0]
            self.assertEqual(nound_feat.iv_status, IVStatus.EXCLUDED_MISSING_UNDERLYING)
            self.assertIn(DataQualityFlag.MISSING_UNDERLYING, nound_feat.quality_flags)
            self.assertIsNone(nound_feat.implied_vol)
            self.assertIsNone(nound_feat.provenance.underlying_quote_version)
            self.assertEqual(nound_res.quality_report.flag_counts.get("MISSING_UNDERLYING"), 1)

            # Scenario B: Delayed quote + multi-version revision + stale quote (DELAY)
            delay_res = results[1]
            self.assertEqual(delay_res.snapshot.underlying_symbol, "DELAY")
            # 3 raw quotes (2 versions for Call + 1 stale Put) -> 2 derived contract features
            self.assertEqual(delay_res.snapshot.quote_count, 3)
            self.assertEqual(len(delay_res.features), 2)

            feat_by_oid = {f.option_id: f for f in delay_res.features}

            # Call: v1 was crossed, superseded by v2 which was delayed (123s ingestion delay) but fresh relative to snapshot_time
            call_feat = feat_by_oid["OPT:DELAY:20270619:C:100.0000"]
            self.assertEqual(call_feat.provenance.option_quote_version, 2)
            self.assertIn(DataQualityFlag.DELAYED_QUOTE, call_feat.quality_flags)
            self.assertEqual(call_feat.iv_status, IVStatus.COMPUTED)
            self.assertAlmostEqual(call_feat.implied_vol, 0.20, places=5)

            # Put: event_time was 600s before snapshot_time (> 300s max_quote_staleness_seconds) -> STALE_QUOTE & excluded
            put_feat = feat_by_oid["OPT:DELAY:20270619:P:100.0000"]
            self.assertIn(DataQualityFlag.STALE_QUOTE, put_feat.quality_flags)
            self.assertIn(DataQualityFlag.DELAYED_QUOTE, put_feat.quality_flags)
            self.assertEqual(put_feat.iv_status, IVStatus.EXCLUDED_STALE_QUOTE)
            self.assertIsNone(put_feat.implied_vol)

            # Replay returns identical chain snapshots and derived feature IDs
            for r in results:
                replay = platform.replay_snapshot(r.snapshot.snapshot_id)
                self.assertTrue(replay.snapshot_hash_match)
                self.assertTrue(replay.feature_ids_match)
                self.assertTrue(replay.is_identical)

            # Diagnostic counters and structured logs captured
            self.assertEqual(platform.counters.snapshots_ingested, 2)
            self.assertEqual(platform.counters.replays_executed, 2)
            self.assertEqual(platform.counters.replay_mismatches, 0)
            self.assertGreaterEqual(len(platform.structured_events), 4)
        finally:
            platform.close()


if __name__ == "__main__":
    unittest.main()
