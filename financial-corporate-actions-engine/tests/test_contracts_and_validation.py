"""M1 tests: canonical contracts, schema versioning, valid/invalid/boundary validation."""

from __future__ import annotations

import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from corp_actions.adapters.json_adapter import load_fixture_bundle, parse_action_dict
from corp_actions.config import EngineConfig
from corp_actions.contracts import FxRate, PositionSnapshot, RawPriceBar
from corp_actions.errors import (
    ConfigurationError,
    SchemaVersionError,
    UnsupportedActionError,
    ValidationError,
)
from corp_actions.validation import (
    validate_batch,
    validate_corporate_action,
    validate_fx_rate,
    validate_position_snapshot,
    validate_price_bar,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"


class TestContractsAndValidation(unittest.TestCase):
    def test_valid_fixture_bundle_passes_validation(self) -> None:
        prices, positions, fx_rates, actions = load_fixture_bundle(FIXTURES_DIR)
        self.assertGreaterEqual(len(prices), 20)
        self.assertEqual(len(positions), 3)
        self.assertGreaterEqual(len(fx_rates), 5)
        self.assertGreaterEqual(len(actions), 10)
        warnings = validate_batch(
            prices=prices,
            positions=positions,
            fx_rates=fx_rates,
            actions=actions,
            allow_unsupported=True,
        )
        # Unsupported merger produces explicit warning
        self.assertTrue(any("CA-EURO-MRG-01" in w for w in warnings))

    def test_invalid_action_fixtures_raise_expected_errors(self) -> None:
        raw = json.loads((FIXTURES_DIR / "invalid_actions.json").read_text(encoding="utf-8"))
        error_map = {
            "VALIDATION_ERROR": ValidationError,
            "SCHEMA_VERSION_ERROR": SchemaVersionError,
            "UNSUPPORTED_ACTION": UnsupportedActionError,
        }
        for case in raw["invalid_cases"]:
            with self.subTest(case=case["case_name"]):
                event = parse_action_dict(case["event"])
                expected_exc = error_map[case["expected_error"]]
                with self.assertRaises(expected_exc) as ctx:
                    validate_corporate_action(event, allow_unsupported=False)
                self.assertEqual(ctx.exception.code, case["expected_error"])

    def test_price_bar_validation_invariants(self) -> None:
        utc = datetime(2025, 1, 2, 21, 0, tzinfo=timezone.utc)
        # High < Low rejected
        bad_ohlc = RawPriceBar(
            security_id="SEC-X",
            symbol="X",
            trade_date=date(2025, 1, 2),
            open=Decimal("10"),
            high=Decimal("9"),
            low=Decimal("11"),
            close=Decimal("10"),
            volume=Decimal("100"),
            currency="USD",
            event_time=utc,
            ingestion_time=utc,
            source="test",
        )
        with self.assertRaises(ValidationError):
            validate_price_bar(bad_ohlc)

        # Naive timestamp rejected
        bad_tz = RawPriceBar(
            security_id="SEC-X",
            symbol="X",
            trade_date=date(2025, 1, 2),
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10"),
            volume=Decimal("100"),
            currency="USD",
            event_time=datetime(2025, 1, 2, 21, 0),  # naive
            ingestion_time=utc,
            source="test",
        )
        with self.assertRaises(ValidationError):
            validate_price_bar(bad_tz)

    def test_fx_and_position_validation(self) -> None:
        utc = datetime(2025, 1, 2, 16, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValidationError):
            validate_fx_rate(
                FxRate("EUR", "USD", date(2025, 1, 2), Decimal("0"), utc, utc, "test")
            )
        with self.assertRaises(ValidationError):
            validate_position_snapshot(
                PositionSnapshot(
                    "ACCT-1", "SEC-X", "X", date(2025, 1, 2),
                    Decimal("-10"), Decimal("50"), "USD", Decimal("0"), utc, utc, "test"
                )
            )

    def test_config_validation_and_missing_file(self) -> None:
        cfg = EngineConfig.from_file(PROJECT_ROOT / "config" / "engine_config.json")
        self.assertEqual(cfg.schema_version, "1.0.0")
        with self.assertRaises(ConfigurationError):
            EngineConfig.from_file(PROJECT_ROOT / "config" / "nonexistent.json")
        with self.assertRaises(ConfigurationError):
            EngineConfig(decimal_precision=4)
        with self.assertRaises(ConfigurationError):
            EngineConfig.from_dict({"unknown_param": 123})


if __name__ == "__main__":
    unittest.main()
