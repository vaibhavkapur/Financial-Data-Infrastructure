"""Tests for Black-Scholes pricing oracle, IV recovery accuracy, and feature provenance linkage."""

from __future__ import annotations

import math
import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import StorageConfig, load_config
from omdp.domain.contracts import IVStatus, OptionRight
from omdp.domain.pricing import bs_price_and_greeks, solve_implied_volatility
from omdp.service.platform import OptionsMarketDataPlatform


class TestPricingAndIV(unittest.TestCase):
    def test_textbook_black_scholes_oracle_and_put_call_parity(self) -> None:
        # Classic Hull textbook benchmark: S=100, K=100, T=1, r=0.05, q=0.0, sigma=0.20
        call = bs_price_and_greeks(100.0, 100.0, 1.0, 0.05, 0.0, 0.20, OptionRight.CALL)
        put = bs_price_and_greeks(100.0, 100.0, 1.0, 0.05, 0.0, 0.20, OptionRight.PUT)

        self.assertAlmostEqual(call.price, 10.4505835722, places=9)
        self.assertAlmostEqual(put.price, 5.5735260223, places=9)

        # Put-Call parity: C - P = S*e^(-qT) - K*e^(-rT)
        parity_rhs = 100.0 - 100.0 * math.exp(-0.05)
        self.assertAlmostEqual(call.price - put.price, parity_rhs, places=10)

        # Invert both call and put prices back to sigma=0.20
        iv_c = solve_implied_volatility(call.price, 100.0, 100.0, 1.0, 0.05, 0.0, OptionRight.CALL)
        iv_p = solve_implied_volatility(put.price, 100.0, 100.0, 1.0, 0.05, 0.0, OptionRight.PUT)
        self.assertTrue(iv_c.converged)
        self.assertTrue(iv_p.converged)
        self.assertAlmostEqual(iv_c.implied_vol, 0.20, places=9)
        self.assertAlmostEqual(iv_p.implied_vol, 0.20, places=9)

    def test_reference_fixture_oracle_and_full_provenance(self) -> None:
        cfg = load_config(ROOT / "config" / "default_config.json")
        cfg = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))
        bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "reference_chain.json")

        platform = OptionsMarketDataPlatform(config=cfg)
        try:
            res = platform.ingest_bundle(bundle)[0]
            oracle = bundle.expected_oracle
            iv_tol = float(oracle["iv_tolerance"])
            tte_tol = float(oracle["tte_tolerance"])

            self.assertEqual(len(res.features), 6)
            for feat in res.features:
                exp = oracle["contracts"][feat.option_id]
                self.assertEqual(feat.iv_status, IVStatus.COMPUTED)
                self.assertIsNotNone(feat.implied_vol)
                self.assertLessEqual(abs(feat.implied_vol - exp["implied_vol"]), iv_tol)
                self.assertLessEqual(abs(feat.time_to_expiry_years - exp["time_to_expiry_years"]), tte_tol)
                self.assertAlmostEqual(feat.spot_moneyness, exp["spot_moneyness"], places=6)

                # Acceptance criterion: every derived feature links to input versions and hashes
                p = feat.provenance
                self.assertEqual(p.snapshot_id, res.snapshot.snapshot_id)
                self.assertEqual(p.snapshot_hash, res.snapshot.snapshot_hash)
                self.assertEqual(p.contract_version, 1)
                self.assertIsNotNone(p.contract_hash)
                self.assertEqual(p.option_quote_version, 1)
                self.assertIsNotNone(p.option_quote_hash)
                self.assertEqual(p.underlying_quote_version, 1)
                self.assertIsNotNone(p.underlying_quote_hash)
                self.assertEqual(p.rate_curve_version, 1)
                self.assertGreaterEqual(len(p.rate_curve_hashes), 1)
                self.assertEqual(p.calendar_version, 1)
                self.assertIsNotNone(p.calendar_hash)
                self.assertEqual(p.config_hash, cfg.config_hash())
        finally:
            platform.close()


if __name__ == "__main__":
    unittest.main()
