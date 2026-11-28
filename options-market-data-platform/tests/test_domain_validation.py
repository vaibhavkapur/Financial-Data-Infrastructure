"""M2 required domain validation tests: expired options, strike/decimal mismatch, crossed quotes, unsupported inputs."""

from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import StorageConfig, load_config
from omdp.domain.contracts import (
    ContractMetadata,
    DataQualityFlag,
    IVStatus,
    OptionQuote,
    OptionRight,
    UnsupportedInputError,
    ValidationStatus,
)
from omdp.domain.validation import validate_option_quote
from omdp.service.platform import OptionsMarketDataPlatform


class TestDomainValidation(unittest.TestCase):
    def setUp(self) -> None:
        cfg = load_config(ROOT / "config" / "default_config.json")
        self.config = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))

    def test_invalid_and_edge_cases_fixture_end_to_end(self) -> None:
        bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "invalid_and_edge_cases.json")
        platform = OptionsMarketDataPlatform(config=self.config)
        try:
            results = platform.ingest_bundle(bundle)
            self.assertEqual(len(results), 1)
            res = results[0]
            by_oid = {vq.quote.option_id: vq for vq in res.snapshot.quotes}
            feat_by_oid = {f.option_id: f for f in res.features}

            # 1. Expired option rejected and excluded from IV
            expired_q = by_oid["OPT:EDGE:20260619:C:100.0000"]
            self.assertEqual(expired_q.status, ValidationStatus.REJECTED)
            self.assertIn(DataQualityFlag.EXPIRED_OPTION, expired_q.flags)
            self.assertEqual(feat_by_oid["OPT:EDGE:20260619:C:100.0000"].iv_status, IVStatus.EXCLUDED_EXPIRED)
            self.assertIsNone(feat_by_oid["OPT:EDGE:20260619:C:100.0000"].implied_vol)

            # 2. Strike/decimal scaling mismatch rejected and excluded
            mismatch_q = by_oid["OPT:EDGE:20270619:C:100.0000"]
            self.assertEqual(mismatch_q.status, ValidationStatus.REJECTED)
            self.assertIn(DataQualityFlag.STRIKE_DECIMAL_MISMATCH, mismatch_q.flags)
            self.assertEqual(feat_by_oid["OPT:EDGE:20270619:C:100.0000"].iv_status, IVStatus.EXCLUDED_INVALID_QUOTE)

            # 3. Crossed quote rejected and excluded
            crossed_q = by_oid["OPT:EDGE:20270619:P:100.0000"]
            self.assertEqual(crossed_q.status, ValidationStatus.REJECTED)
            self.assertIn(DataQualityFlag.CROSSED_QUOTE, crossed_q.flags)
            self.assertEqual(feat_by_oid["OPT:EDGE:20270619:P:100.0000"].iv_status, IVStatus.EXCLUDED_INVALID_QUOTE)
            self.assertIsNone(feat_by_oid["OPT:EDGE:20270619:P:100.0000"].mid_price)

            # 4. Locked quote flagged (not rejected when allow_locked_quotes=True)
            locked_q = by_oid["OPT:EDGE:20270619:C:105.0000"]
            self.assertEqual(locked_q.status, ValidationStatus.FLAGGED)
            self.assertIn(DataQualityFlag.LOCKED_QUOTE, locked_q.flags)
            self.assertEqual(feat_by_oid["OPT:EDGE:20270619:C:105.0000"].iv_status, IVStatus.COMPUTED)

            # 5. Unsupported exercise style (AMERICAN) rejected explicitly
            american_q = by_oid["OPT:EDGE:20270619:C:110.0000"]
            self.assertEqual(american_q.status, ValidationStatus.REJECTED)
            self.assertIn(DataQualityFlag.UNSUPPORTED_EXERCISE_STYLE, american_q.flags)
            self.assertEqual(
                feat_by_oid["OPT:EDGE:20270619:C:110.0000"].iv_status,
                IVStatus.EXCLUDED_UNSUPPORTED_STYLE,
            )

            # 6. Arbitrage bound violation detected explicitly (call priced below discounted intrinsic)
            arb_feat = feat_by_oid["OPT:EDGE:20270619:C:80.0000"]
            self.assertEqual(arb_feat.iv_status, IVStatus.ARBITRAGE_BOUND_VIOLATION)
            self.assertIsNone(arb_feat.implied_vol)
        finally:
            platform.close()

    def test_excess_decimal_precision_mismatch(self) -> None:
        contract = ContractMetadata(
            option_id="OPT:DEC:20270619:C:150.0000",
            contract_version=1,
            underlying_symbol="DEC",
            expiry_date=date(2027, 6, 19),
            option_right=OptionRight.CALL,
            strike=Decimal("150.00"),
            strike_scale_decimals=2,
            contract_multiplier=100,
            exercise_style="EUROPEAN",
            currency="USD",
            source_id="test",
            schema_version="1.0.0",
            ingestion_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        )
        quote = OptionQuote(
            snapshot_id="S-DEC",
            option_id="OPT:DEC:20270619:C:150.0000",
            quote_version=1,
            underlying_symbol="DEC",
            reported_strike=Decimal("150.005"),  # 3 decimals > strike_scale_decimals=2
            event_time=datetime(2026, 6, 19, 19, 59, 55, tzinfo=timezone.utc),
            ingestion_time=datetime(2026, 6, 19, 19, 59, 56, tzinfo=timezone.utc),
            bid=Decimal("5.00"),
            ask=Decimal("5.20"),
            bid_size=10,
            ask_size=10,
            source_id="test",
            source_sequence=1,
            schema_version="1.0.0",
        )
        res = validate_option_quote(
            quote=quote,
            snapshot_time=datetime(2026, 6, 19, 20, 0, 0, tzinfo=timezone.utc),
            contract=contract,
            calendar_by_date={},
            config=self.config,
        )
        self.assertEqual(res.status, ValidationStatus.REJECTED)
        self.assertIn(DataQualityFlag.STRIKE_DECIMAL_MISMATCH, res.flags)

    def test_unsupported_schema_version_raises_explicit_error(self) -> None:
        quote = OptionQuote(
            snapshot_id="S-SCH",
            option_id="OPT:DEC:20270619:C:150.0000",
            quote_version=1,
            underlying_symbol="DEC",
            reported_strike=Decimal("150.00"),
            event_time=datetime(2026, 6, 19, 19, 59, 55, tzinfo=timezone.utc),
            ingestion_time=datetime(2026, 6, 19, 19, 59, 56, tzinfo=timezone.utc),
            bid=Decimal("5.00"),
            ask=Decimal("5.20"),
            bid_size=10,
            ask_size=10,
            source_id="test",
            source_sequence=1,
            schema_version="9.9.9",
        )
        with self.assertRaises(UnsupportedInputError):
            validate_option_quote(
                quote=quote,
                snapshot_time=datetime(2026, 6, 19, 20, 0, 0, tzinfo=timezone.utc),
                contract=None,
                calendar_by_date={},
                config=self.config,
            )


if __name__ == "__main__":
    unittest.main()
