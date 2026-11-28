"""M1 reference oracle test comparing engine outputs against hand-calculated expectations."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from sec_master.adapters import raw_record_to_envelope
from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.domain import BitemporalStateEngine
from sec_master.errors import SecurityMasterError
from sec_master.models import IdentifierNamespace, parse_ts

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestReferenceMinimalOracle(unittest.TestCase):
    def test_minimal_reference_matches_independent_oracle(self) -> None:
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
        fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/reference_minimal.json").read_text(encoding="utf-8")
        )
        expected = json.loads(
            (PROJECT_ROOT / "data/fixtures/reference_minimal_expected.json").read_text(
                encoding="utf-8"
            )
        )

        engine = BitemporalStateEngine(cfg)
        for raw in fixture["events"]:
            out = engine.apply_event(raw_record_to_envelope(raw))
            self.assertEqual(out.status, "APPLIED")

        metrics = engine.compute_quality_metrics(parse_ts("2020-06-01T00:00:00Z"))  # type: ignore[arg-type]
        exp_counts = expected["expected_counts"]
        self.assertEqual(metrics.active_instruments, exp_counts["active_instruments"])
        self.assertEqual(metrics.active_listings, exp_counts["active_listings"])
        self.assertEqual(metrics.open_conflicts, exp_counts["open_conflicts"])
        self.assertEqual(metrics.completeness_ratio, exp_counts["completeness_ratio"])
        self.assertEqual(metrics.lineage_coverage_ratio, exp_counts["lineage_coverage_ratio"])

        for check in expected["expected_lookups"]:
            q = check["query"]
            ns = IdentifierNamespace(q["namespace"])
            as_of_v = parse_ts(q["as_of_valid"])
            as_of_o = parse_ts(q["as_of_observed"])
            if check["expect_status"] == "OK":
                res = engine.lookup_as_of(ns, q["value"], as_of_v, as_of_o)  # type: ignore[arg-type]
                self.assertEqual(res.instrument.instrument_id, check["expected_instrument_id"])
                self.assertIsNotNone(res.listing)
                self.assertEqual(res.listing.listing_id, check["expected_listing_id"])  # type: ignore[union-attr]
                self.assertEqual(res.instrument.name, check["expected_name"])
                self.assertEqual(res.listing.currency, check["expected_currency"])  # type: ignore[union-attr]
                if "expected_isin" in check:
                    id_map = {m.namespace.value: m.identifier_value for m in res.identifiers}
                    self.assertEqual(id_map["ISIN"], check["expected_isin"])
                    self.assertEqual(id_map["OPENFIGI"], check["expected_openfigi"])
            else:
                with self.assertRaises(SecurityMasterError) as ctx:
                    engine.lookup_as_of(ns, q["value"], as_of_v, as_of_o)  # type: ignore[arg-type]
                self.assertEqual(ctx.exception.code, check["expected_error_code"])


if __name__ == "__main__":
    unittest.main()
